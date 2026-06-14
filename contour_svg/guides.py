from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from .config import RunConfig
from .contracts import GuideSet, LinePrimitive
from .dependencies import MissingDependencyError, optional_import, require_module


def _save_pil_from_array(arr, path: Path) -> Path:
    Image = optional_import("PIL.Image")
    Image.fromarray(arr).save(path)
    return path


def _apply_focus_hint(mask, config: RunConfig):
    if not config.input.bbox_hint_xyxy:
        return mask
    np = optional_import("numpy")
    if np is None:
        return mask
    x1, y1, x2, y2 = [int(round(v)) for v in config.input.bbox_hint_xyxy]
    h, w = mask.shape[:2]
    x1, x2 = max(0, min(w, x1)), max(0, min(w, x2))
    y1, y2 = max(0, min(h, y1)), max(0, min(h, y2))
    if x2 <= x1 or y2 <= y1:
        return mask
    focused = np.zeros_like(mask)
    focused[y1:y2, x1:x2] = mask[y1:y2, x1:x2]
    return focused


def build_guides(image, primary_mask_path: Path, occluder_mask_path: Path | None, out_dir: Path, config: RunConfig) -> GuideSet:
    np = optional_import("numpy")
    cv2 = optional_import("cv2")
    if np is None or cv2 is None:
        raise MissingDependencyError("Guide extraction requires numpy and opencv-python-headless")
    Image = optional_import("PIL.Image")
    debug = out_dir / "debug"
    debug.mkdir(parents=True, exist_ok=True)

    rgb = np.array(image.convert("RGB"))
    primary = np.array(Image.open(primary_mask_path).convert("L"))
    primary = _apply_focus_hint(primary, config)
    occluder = np.array(Image.open(occluder_mask_path).convert("L")) if occluder_mask_path else np.zeros_like(primary)
    gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)
    blur = cv2.bilateralFilter(clahe, 7, 45, 45)

    edges = cv2.Canny(blur, 65, 150)
    edges_primary = cv2.bitwise_and(edges, primary)
    edges = edges_primary.copy()
    edges[occluder > 32] = 0
    edge_path = _save_pil_from_array(edges, debug / "edge_map.png")
    # Keep root-level line artifacts for downstream neural/style probes and
    # operator review. `edge_map.png` remains the occluder-subtracted pipeline
    # guide; `edge_mask.png` is a primary-object edge mask before occluder
    # subtraction, which is often the better source for line-to-line neural
    # cleanup when occluder segmentation is over-aggressive. The misspelled
    # alias is intentional for robust ad-hoc checks against the common
    # "egde_mask" typo.
    _save_pil_from_array(edges, out_dir / "edge_map.png")
    for alias in [
        out_dir / "edge_mask.png",
        out_dir / "egde_mask.png",
        debug / "edge_mask.png",
        debug / "egde_mask.png",
    ]:
        _save_pil_from_array(edges_primary, alias)

    lines: list[LinePrimitive] = []
    warnings: list[str] = []

    contours, _ = cv2.findContours(primary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    contours = sorted(contours, key=cv2.contourArea, reverse=True)
    for contour in contours[:4]:
        if cv2.contourArea(contour) < image.width * image.height * 0.005:
            continue
        epsilon = max(2.0, config.geometry.simplify_tolerance) * 0.8
        approx = cv2.approxPolyDP(contour, epsilon, True)
        pts = [(float(x), float(y)) for [[x, y]] in approx]
        if len(pts) >= 3:
            pts.append(pts[0])
            lines.append(LinePrimitive(pts, role="silhouette", priority=10, source="sam2_mask", confidence=0.75))

    if config.geometry.use_hough:
        hough = cv2.HoughLinesP(
            edges,
            rho=1,
            theta=np.pi / 180,
            threshold=42,
            minLineLength=max(20, int(config.style.min_line_length)),
            maxLineGap=18,
        )
        if hough is not None:
            for row in hough[:500]:
                x1, y1, x2, y2 = row[0]
                role = "roofline" if y1 < image.height * 0.35 and y2 < image.height * 0.35 else "structure"
                lines.append(
                    LinePrimitive(
                        [(float(x1), float(y1)), (float(x2), float(y2))],
                        role=role,
                        priority=8 if role == "roofline" else 6,
                        source="hough",
                        confidence=0.72,
                    )
                )

    if config.geometry.use_lsd and hasattr(cv2, "createLineSegmentDetector"):
        try:
            detector = cv2.createLineSegmentDetector(0)
            detected = detector.detect(edges)[0]
            if detected is not None:
                for row in detected[:500]:
                    x1, y1, x2, y2 = row[0]
                    length = ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
                    if length >= config.style.min_line_length:
                        lines.append(
                            LinePrimitive(
                                [(float(x1), float(y1)), (float(x2), float(y2))],
                                role="structure",
                                priority=6,
                                source="lsd",
                                confidence=0.65,
                            )
                        )
        except Exception as exc:
            warnings.append(f"lsd_failed:{type(exc).__name__}:{exc}")

    # Curved guide: keep simplified edge contours that are long enough and not texture speckles.
    edge_contours, _ = cv2.findContours(edges, cv2.RETR_LIST, cv2.CHAIN_APPROX_NONE)
    for contour in edge_contours[:800]:
        arc = cv2.arcLength(contour, False)
        if arc < max(45, config.style.min_line_length * 2.5):
            continue
        x, y, w, h = cv2.boundingRect(contour)
        if w < 8 or h < 8:
            continue
        epsilon = max(1.5, config.geometry.simplify_tolerance)
        approx = cv2.approxPolyDP(contour, epsilon, False)
        pts = [(float(px), float(py)) for [[px, py]] in approx]
        if 3 <= len(pts) <= 80:
            role = "arc" if h > 0 and 0.45 <= (w / max(1, h)) <= 3.5 else "accent"
            lines.append(LinePrimitive(pts, role=role, priority=7 if role == "arc" else 4, source="edge_contour", confidence=0.55))

    mlsd_path = None
    if config.geometry.use_mlsd:
        mlsd_path, mlsd_lines = _extract_mlsd_lines(image, primary, occluder, debug, config)
        lines.extend(mlsd_lines)
        if not mlsd_lines:
            raise RuntimeError("M-LSD guide produced no usable architectural line segments")
    if config.geometry.use_deeplsd:
        deeplsd_lines = _extract_deeplsd_lines(image, primary, occluder, debug, config)
        lines.extend(deeplsd_lines)
        if not deeplsd_lines:
            raise RuntimeError("DeepLSD produced no usable architectural line segments")
    if config.geometry.use_hawp:
        hawp_lines = _extract_hawp_lines(image, primary, occluder, debug, config)
        lines.extend(hawp_lines)
        if not hawp_lines:
            raise RuntimeError("HAWP produced no usable wireframe line segments")

    overlay = rgb.copy()
    for line in lines[:900]:
        pts = [(int(x), int(y)) for x, y in line.points]
        if line.source == "mlsd":
            color = (255, 230, 80)
        else:
            color = (255, 255, 255) if line.priority >= 7 else (80, 220, 255)
        for a, b in zip(pts, pts[1:]):
            cv2.line(overlay, a, b, color, 2, cv2.LINE_AA)
    overlay_path = _save_pil_from_array(overlay, debug / "line_segments_overlay.png")
    source_counts: dict[str, int] = {}
    for line in lines:
        source_counts[line.source] = source_counts.get(line.source, 0) + 1
    (debug / "guide_source_counts.json").write_text(
        json.dumps(source_counts, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return GuideSet(
        primary_mask=primary_mask_path,
        occluder_mask=occluder_mask_path,
        edge_map=edge_path,
        mlsd_guide=mlsd_path,
        line_overlay=overlay_path,
        lines=lines,
        warnings=warnings,
    )


def _extract_mlsd_lines(image, primary, occluder, debug: Path, config: RunConfig) -> tuple[Path, list[LinePrimitive]]:
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    Image = require_module("PIL.Image", "Pillow")
    controlnet_aux = require_module("controlnet_aux", "controlnet-aux")

    detector = controlnet_aux.MLSDdetector.from_pretrained(
        "lllyasviel/Annotators",
        cache_dir=config.runtime.hf_cache_dir,
    )
    mlsd_image = detector(image.convert("RGB"))
    mlsd = np.array(mlsd_image.convert("L").resize((primary.shape[1], primary.shape[0])))
    _, mlsd_edges = cv2.threshold(mlsd, 16, 255, cv2.THRESH_BINARY)
    mlsd_edges = cv2.bitwise_and(mlsd_edges, primary)
    mlsd_edges[occluder > 32] = 0
    mlsd_path = debug / "mlsd_guide.png"
    Image.fromarray(mlsd_edges).save(mlsd_path)

    lines: list[LinePrimitive] = []
    hough = cv2.HoughLinesP(
        mlsd_edges,
        rho=1,
        theta=np.pi / 180,
        threshold=26,
        minLineLength=max(18, int(config.style.min_line_length)),
        maxLineGap=14,
    )
    if hough is not None:
        for row in hough[:700]:
            x1, y1, x2, y2 = row[0]
            role = _role_from_segment(float(x1), float(y1), float(x2), float(y2), image.height)
            lines.append(
                LinePrimitive(
                    [(float(x1), float(y1)), (float(x2), float(y2))],
                    role=role,
                    priority=8 if role in {"roofline", "structure"} else 6,
                    source="mlsd",
                    confidence=0.78,
                )
            )

    if hasattr(cv2, "createLineSegmentDetector"):
        detector_lsd = cv2.createLineSegmentDetector(0)
        detected = detector_lsd.detect(mlsd_edges)[0]
        if detected is not None:
            for row in detected[:500]:
                x1, y1, x2, y2 = row[0]
                length = ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
                if length < config.style.min_line_length:
                    continue
                role = _role_from_segment(float(x1), float(y1), float(x2), float(y2), image.height)
                lines.append(
                    LinePrimitive(
                        [(float(x1), float(y1)), (float(x2), float(y2))],
                        role=role,
                        priority=7 if role in {"roofline", "structure"} else 5,
                        source="mlsd",
                        confidence=0.70,
                    )
                )
    return mlsd_path, lines


def _role_from_segment(x1: float, y1: float, x2: float, y2: float, image_height: int) -> str:
    import math

    angle = math.degrees(math.atan2(y2 - y1, x2 - x1)) % 180.0
    y_mid = (y1 + y2) / 2.0
    horizontal = angle <= 10.0 or angle >= 170.0
    vertical = 80.0 <= angle <= 100.0
    shallow = 10.0 < angle < 38.0 or 142.0 < angle < 170.0
    if shallow and y_mid < image_height * 0.42:
        return "roofline"
    if horizontal or vertical:
        return "structure"
    return "accent"


def _extract_deeplsd_lines(image, primary, occluder, debug: Path, config: RunConfig) -> list[LinePrimitive]:
    checkpoint = config.geometry.deeplsd_checkpoint
    if not checkpoint:
        raise RuntimeError("DeepLSD is enabled but geometry.deeplsd_checkpoint is not configured")
    ckpt_path = Path(checkpoint)
    if not ckpt_path.exists():
        raise RuntimeError(f"DeepLSD checkpoint does not exist: {ckpt_path}")
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    torch = require_module("torch", "torch")
    Image = require_module("PIL.Image", "Pillow")
    if config.geometry.deeplsd_repo_dir:
        repo_dir = Path(config.geometry.deeplsd_repo_dir)
        if repo_dir.exists() and str(repo_dir) not in sys.path:
            sys.path.insert(0, str(repo_dir))
    stub_dir = Path(__file__).resolve().parent / "_stubs"
    if str(stub_dir) not in sys.path:
        sys.path.insert(0, str(stub_dir))
    deeplsd_model = require_module("deeplsd.models.deeplsd", "DeepLSD")

    if not torch.cuda.is_available():
        raise MissingDependencyError("DeepLSD requires CUDA/GPU for contour_svg v0.3")
    gray = cv2.cvtColor(np.array(image.convert("RGB")), cv2.COLOR_RGB2GRAY)
    conf = {
        "sharpen": True,
        "detect_lines": True,
        "line_detection_params": {
            "merge": False,
            "optimize": False,
            "use_vps": True,
            "optimize_vps": True,
            "filtering": True,
            "grad_thresh": 3,
            "grad_nfa": True,
        },
    }
    checkpoint_data = torch.load(str(ckpt_path), map_location="cpu", weights_only=False)
    net = deeplsd_model.DeepLSD(conf)
    net.load_state_dict(checkpoint_data["model"])
    net = net.to("cuda:0").eval()
    inputs = {"image": torch.tensor(gray, dtype=torch.float, device="cuda:0")[None, None] / 255.0}
    with torch.no_grad():
        out = net(inputs)
    pred_lines = out["lines"][0]
    if hasattr(pred_lines, "detach"):
        pred_lines = pred_lines.detach().cpu().numpy()

    lines: list[LinePrimitive] = []
    overlay = np.zeros((primary.shape[0], primary.shape[1], 3), dtype=np.uint8)
    for raw in pred_lines[:900]:
        x1, y1 = float(raw[0][0]), float(raw[0][1])
        x2, y2 = float(raw[1][0]), float(raw[1][1])
        sampled = _sample_segment_pixels(x1, y1, x2, y2)
        if _segment_mask_ratio(sampled, primary) < 0.45 or _segment_mask_ratio(sampled, occluder > 32) > 0.12:
            continue
        length = ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
        if length < config.style.min_line_length:
            continue
        role = _role_from_segment(x1, y1, x2, y2, image.height)
        line = LinePrimitive(
            [(x1, y1), (x2, y2)],
            role=role,
            priority=8 if role in {"roofline", "structure"} else 5,
            source="deeplsd",
            confidence=0.80,
        )
        lines.append(line)
        cv2.line(overlay, (int(x1), int(y1)), (int(x2), int(y2)), (255, 255, 255), 2, cv2.LINE_AA)

    Image.fromarray(overlay).save(debug / "deeplsd_lines_overlay.png")
    (debug / "deeplsd_lines.json").write_text(
        json.dumps([_line_to_dict(line) for line in lines], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return lines


def _sample_segment_pixels(x1: float, y1: float, x2: float, y2: float) -> list[tuple[int, int]]:
    import math

    length = math.hypot(x2 - x1, y2 - y1)
    steps = max(2, int(length / 6.0))
    return [
        (int(round(x1 + (x2 - x1) * idx / steps)), int(round(y1 + (y2 - y1) * idx / steps)))
        for idx in range(steps + 1)
    ]


def _segment_mask_ratio(points: list[tuple[int, int]], mask) -> float:
    if not points:
        return 0.0
    h, w = mask.shape[:2]
    inside = 0
    for x, y in points:
        if 0 <= x < w and 0 <= y < h and bool(mask[y, x]):
            inside += 1
    return inside / len(points)


def _extract_hawp_lines(image, primary, occluder, debug: Path, config: RunConfig) -> list[LinePrimitive]:
    checkpoint = config.geometry.hawp_checkpoint
    if not checkpoint:
        raise RuntimeError("HAWP is enabled but geometry.hawp_checkpoint is not configured")
    ckpt_path = Path(checkpoint)
    if not ckpt_path.exists():
        raise RuntimeError(f"HAWP checkpoint does not exist: {ckpt_path}")
    np = require_module("numpy", "numpy")
    cv2 = require_module("cv2", "opencv-python-headless")
    Image = require_module("PIL.Image", "Pillow")
    if config.geometry.hawp_repo_dir:
        repo_dir = Path(config.geometry.hawp_repo_dir)
        if repo_dir.exists() and str(repo_dir) not in sys.path:
            sys.path.insert(0, str(repo_dir))
    require_module("hawp.ssl.predict", "hawp")

    hawp_dir = debug / "hawp"
    hawp_dir.mkdir(parents=True, exist_ok=True)
    input_path = debug / "hawp_input.png"
    image.convert("RGB").save(input_path)
    env = dict(__import__("os").environ)
    if config.geometry.hawp_repo_dir:
        env["PYTHONPATH"] = f"{config.geometry.hawp_repo_dir}:{env.get('PYTHONPATH', '')}"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "hawp.ssl.predict",
            "--ckpt",
            str(ckpt_path),
            "--threshold",
            "0.05",
            "--img",
            str(input_path),
            "--saveto",
            str(hawp_dir),
            "--ext",
            "json",
            "--device",
            "cuda",
            "--disable-show",
        ],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
    )
    graph_path = hawp_dir / "hawp_input.json"
    if not graph_path.exists():
        raise RuntimeError(f"HAWP did not write expected JSON graph: {graph_path}")
    graph = json.loads(graph_path.read_text(encoding="utf-8"))
    vertices = graph.get("vertices") or []
    edges = graph.get("edges") or []
    weights = graph.get("edges-weights") or []
    lines: list[LinePrimitive] = []
    overlay = np.zeros((primary.shape[0], primary.shape[1], 3), dtype=np.uint8)
    for edge, weight in zip(edges[:1600], weights[:1600]):
        if not isinstance(edge, list) or len(edge) != 2:
            continue
        try:
            p1 = vertices[int(edge[0])]
            p2 = vertices[int(edge[1])]
        except (IndexError, TypeError, ValueError):
            continue
        x1, y1 = float(p1[0]), float(p1[1])
        x2, y2 = float(p2[0]), float(p2[1])
        sampled = _sample_segment_pixels(x1, y1, x2, y2)
        if _segment_mask_ratio(sampled, primary) < 0.42 or _segment_mask_ratio(sampled, occluder > 32) > 0.12:
            continue
        length = ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
        if length < config.style.min_line_length:
            continue
        role = _role_from_segment(x1, y1, x2, y2, image.height)
        confidence = max(0.0, min(1.0, float(weight)))
        lines.append(
            LinePrimitive(
                [(x1, y1), (x2, y2)],
                role=role,
                priority=8 if role in {"roofline", "structure"} else 5,
                source="hawp",
                confidence=max(0.62, confidence),
            )
        )
        cv2.line(overlay, (int(x1), int(y1)), (int(x2), int(y2)), (255, 255, 255), 2, cv2.LINE_AA)
    Image.fromarray(overlay).save(debug / "hawp_lines_overlay.png")
    (debug / "hawp_lines.json").write_text(
        json.dumps([_line_to_dict(line) for line in lines], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return lines


def _line_to_dict(line: LinePrimitive) -> dict[str, object]:
    return {
        "points": line.points,
        "role": line.role,
        "priority": line.priority,
        "source": line.source,
        "confidence": line.confidence,
        "length": line.length,
    }
