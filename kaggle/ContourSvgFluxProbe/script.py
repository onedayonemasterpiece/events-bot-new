from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
KAGGLE_INPUT = Path("/kaggle/input")
DEFAULT_OUT = Path("/kaggle/working/contour_svg_flux_probe")
STATUS_PROGRESS = {"phase": "bootstrap", "progress_percent": 0, "progress_label": "bootstrap"}
REQUIRED_IMPORTS = [
    ("PIL", "pillow"),
    ("numpy", "numpy"),
    ("cv2", "opencv-python-headless"),
    ("torch", "torch"),
    ("diffusers", "diffusers"),
    ("transformers", "transformers"),
    ("accelerate", "accelerate"),
    ("sentencepiece", "sentencepiece"),
    ("protobuf", "protobuf"),
]

DEFAULT_PROMPT = (
    "Clean architectural line art of the same building/object from the input image. "
    "A strict black ink technical drawing on a pure white background, postcard-style architectural sketch. "
    "Preserve the global silhouette, roofline or dome, main perspective, facade rhythm, windows, arches, cornices and balconies. "
    "Use fewer larger confident lines, straightened geometry, smooth curves, clean ellipses and rounded architectural joins. "
    "Complete small occluded architectural gaps conservatively. Remove trees, foliage, sky, road, shadows, brick texture, stone texture, hatching, random edge noise, people and cars. "
    "No photorealism, no grayscale wash, no color, no filled surfaces, no background scene."
)


def _find_repo_root() -> Path:
    local_root = Path(__file__).resolve().parents[2]
    if (local_root / "contour_svg").exists():
        return local_root
    for root in (KAGGLE_INPUT, Path("/kaggle/working")):
        if not root.exists():
            continue
        for candidate in root.rglob("contour_svg"):
            if candidate.is_dir() and (candidate / "guide_bank.py").exists():
                return candidate.parent
    return local_root


ROOT = _find_repo_root()
_CONFIG_CACHE: dict | None = None


def _load_status_client():
    for path in [SCRIPT_DIR, ROOT / "kaggle"]:
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    from kaggle_status_client import load_status_client

    return load_status_client(output_dir=DEFAULT_OUT, log=lambda message: print(message, flush=True))


def _status_progress() -> dict:
    return dict(STATUS_PROGRESS)


def _status_event(client, event: str, *, phase: str, status: str = "running", progress: dict | None = None, message: str | None = None) -> None:
    payload = dict(progress or {})
    payload.setdefault("phase", phase)
    STATUS_PROGRESS.update(payload)
    STATUS_PROGRESS["phase"] = phase
    client.event(event, phase=phase, status=status, progress=payload, message=message)


def _install_requirements(status_client) -> None:
    _status_event(status_client, "install_started", phase="install", progress={"progress_percent": 2, "progress_label": "install FLUX probe dependencies"})
    missing = [package for module, package in REQUIRED_IMPORTS if importlib.util.find_spec(module) is None]
    # FLUX pipelines move quickly; upgrade the small HF stack to get Flux img2img/control support.
    packages = [
        "diffusers>=0.35.0",
        "transformers>=4.50.0",
        "accelerate",
        "sentencepiece",
        "protobuf<6,>=3.20.3",
        "safetensors",
        "bitsandbytes",
    ]
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-U", *packages])
    optional_missing = [package for module, package in [("cv2", "opencv-python-headless"), ("PIL", "pillow")] if importlib.util.find_spec(module) is None]
    if optional_missing:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *optional_missing])
    _status_event(status_client, "install_done", phase="install", status="done", progress={"progress_percent": 7, "progress_label": "dependency install done", "initially_missing": missing})


def _load_config() -> dict:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE
    for root in [KAGGLE_INPUT, Path("/kaggle/working"), ROOT]:
        if not root.exists():
            continue
        matches = sorted(root.rglob("flux_probe_config.json"))
        if matches:
            _CONFIG_CACHE = json.loads(matches[0].read_text(encoding="utf-8"))
            return _CONFIG_CACHE
    raise RuntimeError("Cannot locate flux_probe_config.json")


def _resolve(raw: str) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else ROOT / path


def _source_images() -> tuple[Path, ...]:
    config = _load_config()
    images = []
    for raw in config.get("source_images") or []:
        path = _resolve(str(raw))
        if not path.exists():
            raise RuntimeError(f"source image not found: {path}")
        images.append(path)
    if not images:
        todo = ROOT / "docs/features/countur_svg_generator/to_do"
        images = sorted([*todo.glob("*.jpg"), *todo.glob("*.jpeg"), *todo.glob("*.png"), *todo.glob("*.webp")])
    if not images:
        raise RuntimeError("No source images for FLUX probe")
    return tuple(images)


def _torch_info() -> dict:
    import torch
    info = {
        "torch": getattr(torch, "__version__", None),
        "cuda": getattr(torch.version, "cuda", None),
        "cuda_available": torch.cuda.is_available(),
        "device_name": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
        "capability": torch.cuda.get_device_capability(0) if torch.cuda.is_available() else None,
    }
    if not torch.cuda.is_available():
        raise RuntimeError(f"CUDA is not available: {info}")
    return info


def _fit_pad_rgb(image, *, size: tuple[int, int], fill=(255, 255, 255)):
    from PIL import Image
    resampling = getattr(Image, "Resampling", Image)
    image = image.convert("RGB")
    width, height = image.size
    target_w, target_h = size
    scale = min(target_w / max(1, width), target_h / max(1, height))
    resized = image.resize((max(1, int(round(width * scale))), max(1, int(round(height * scale)))), resampling.LANCZOS)
    out = Image.new("RGB", size, fill)
    out.paste(resized, ((target_w - resized.width) // 2, (target_h - resized.height) // 2))
    return out


def _line_mask(image):
    import numpy as np
    import cv2
    gray = np.array(image.convert("L"))
    # Keep dark line art as black-on-white after FLUX output.
    th = int(max(32, min(220, np.percentile(gray, 18))))
    binary = np.where(gray <= th, 0, 255).astype("uint8")
    line = np.where(binary == 0, 255, 0).astype("uint8")
    labels_count, labels, stats, _ = cv2.connectedComponentsWithStats(line, connectivity=8)
    clean = np.zeros_like(line)
    for label in range(1, labels_count):
        x, y, w, h, area = stats[label]
        if area >= 8 and max(w, h) >= 3:
            clean[labels == label] = 255
    out = np.where(clean > 0, 0, 255).astype("uint8")
    from PIL import Image
    return Image.fromarray(out).convert("RGB")


def _burgundy(line_mask):
    from PIL import Image
    gray = line_mask.convert("L")
    out = Image.new("RGB", gray.size, (92, 26, 19))
    out.putdata([(246, 239, 224) if p < 128 else (92, 26, 19) for p in gray.getdata()])
    return out


def _write_contact_sheet(rows: list[dict], path: Path) -> None:
    from PIL import Image, ImageDraw
    cols = [("guide", "guide_path"), ("raw", "raw_path"), ("line", "line_mask_path"), ("preview", "preview_path")]
    tw, th, lh = 220, 180, 42
    sheet = Image.new("RGB", (len(cols) * tw, max(1, len(rows)) * (th + lh) + lh), (245, 247, 250))
    d = ImageDraw.Draw(sheet)
    for c, (label, _) in enumerate(cols):
        d.text((c * tw + 8, 8), label, fill=(20, 24, 32))
    for r, row in enumerate(rows):
        y = lh + r * (th + lh)
        for c, (_, key) in enumerate(cols):
            p = Path(row[key])
            if p.exists():
                im = Image.open(p).convert("RGB"); im.thumbnail((tw - 12, th - 12)); sheet.paste(im, (c * tw + (tw - im.width)//2, y + (th - im.height)//2))
        label = f"{Path(row['source_image']).name} · {row['variant_id']}"
        d.text((8, y + th + 4), label[:120], fill=(20, 24, 32))
    path.parent.mkdir(parents=True, exist_ok=True)
    sheet.save(path)


def main() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    status_client = _load_status_client()
    status_client.start_alive(interval_seconds=60, progress_provider=_status_progress)
    _status_event(status_client, "kernel_started", phase="bootstrap", progress={"progress_percent": 0, "progress_label": "kernel started"})
    try:
        _install_requirements(status_client)
        from PIL import Image
        import torch
        from contour_svg.guide_bank import GuideBankConfig, build_guide_bank

        config = _load_config()
        backend = str(config.get("backend") or "flux_img2img")
        model_id = str(config.get("model_id") or "ModelsLab/flux.1-dev")
        output_size = tuple(int(x) for x in str(config.get("output_size") or "512,512").split(","))
        steps = int(config.get("steps") or 4)
        guidance_scale = float(config.get("guidance_scale") if config.get("guidance_scale") is not None else 3.5)
        direct_strength = float(config.get("direct_strength") or 0.92)
        guide_strength = float(config.get("guide_strength") or 0.68)
        minimal_strength = float(config.get("minimal_strength") or 0.76)
        variants = list(config.get("variants") or ["direct_photo", "edge_mask", "CG3_fused_balanced", "CG4_minimal_clean"])
        sources = _source_images()
        info = _torch_info()
        _status_event(status_client, "preflight_ok", phase="preflight", status="done", progress={"progress_percent": 10, "progress_label": "FLUX preflight ok", "backend": backend, "model_id": model_id, "source_count": len(sources), "cuda": info})

        dtype = torch.float16
        _status_event(status_client, "model_load_started", phase="model_load", progress={"progress_percent": 12, "progress_label": f"load {backend}: {model_id}"})
        if backend in {"flux_img2img", "schnell_img2img"}:
            from diffusers import FluxImg2ImgPipeline
            pipe = FluxImg2ImgPipeline.from_pretrained(model_id, torch_dtype=dtype)
            # T4 has 16 GB VRAM; CPU offload is slower but avoids OOM for smaller/public FLUX mirrors.
            pipe.enable_model_cpu_offload()
            if hasattr(pipe, "enable_vae_slicing"):
                pipe.enable_vae_slicing()
            if hasattr(pipe, "enable_vae_tiling"):
                pipe.enable_vae_tiling()
        elif backend == "flux_img2img_bnb4":
            # Official Diffusers bitsandbytes route: quantize the two large FLUX
            # components (transformer + T5 text_encoder_2) to fit under Kaggle T4.
            from diffusers import AutoModel, BitsAndBytesConfig as DiffusersBitsAndBytesConfig, FluxImg2ImgPipeline
            from transformers import BitsAndBytesConfig as TransformersBitsAndBytesConfig, T5EncoderModel
            text_quant = TransformersBitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_compute_dtype=torch.float16,
            )
            text_encoder_2 = T5EncoderModel.from_pretrained(
                model_id,
                subfolder="text_encoder_2",
                quantization_config=text_quant,
                torch_dtype=torch.float16,
            )
            transformer_quant = DiffusersBitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_compute_dtype=torch.float16,
            )
            transformer = AutoModel.from_pretrained(
                model_id,
                subfolder="transformer",
                quantization_config=transformer_quant,
                torch_dtype=torch.float16,
            )
            pipe = FluxImg2ImgPipeline.from_pretrained(
                model_id,
                transformer=transformer,
                text_encoder_2=text_encoder_2,
                torch_dtype=torch.float16,
                device_map="balanced",
            )
            if hasattr(pipe, "enable_vae_slicing"):
                pipe.enable_vae_slicing()
            if hasattr(pipe, "enable_vae_tiling"):
                pipe.enable_vae_tiling()
        elif backend == "flux_control_canny":
            # This is the official FLUX.1 Canny route. It requires HF credentials
            # that have accepted the gated BFL license; fail loudly otherwise.
            from diffusers import FluxControlPipeline
            token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
            if not token:
                raise RuntimeError("flux_control_canny requires HF_TOKEN/HUGGINGFACE_TOKEN with access to black-forest-labs/FLUX.1-Canny-dev")
            pipe = FluxControlPipeline.from_pretrained(model_id, torch_dtype=dtype, token=token)
            pipe.enable_model_cpu_offload()
            if hasattr(pipe, "enable_vae_slicing"):
                pipe.enable_vae_slicing()
            if hasattr(pipe, "enable_vae_tiling"):
                pipe.enable_vae_tiling()
        else:
            raise RuntimeError(f"Unsupported FLUX backend: {backend}")
        if hasattr(pipe, "set_progress_bar_config"):
            pipe.set_progress_bar_config(disable=True)
        _status_event(status_client, "model_load_done", phase="model_load", status="done", progress={"progress_percent": 25, "progress_label": "FLUX model loaded", "backend": backend})

        prompt = str(config.get("prompt") or DEFAULT_PROMPT)
        rows=[]
        total=len(sources)*len(variants)
        done=0
        for si, source in enumerate(sources, start=1):
            stem = ''.join(ch.lower() if ch.isalnum() else '_' for ch in source.stem).strip('_')[:80]
            image_dir = DEFAULT_OUT / stem
            image_dir.mkdir(parents=True, exist_ok=True)
            guide_bank = build_guide_bank(GuideBankConfig(source_image=source, out_dir=image_dir, output_size=output_size))
            guide_map = {"edge_mask": guide_bank.edge_mask, **guide_bank.guides, **guide_bank.composite_guides}
            source_crop = Image.open(guide_bank.source_crop).convert("RGB")
            for variant in variants:
                done += 1
                _status_event(status_client, "alive", phase="generate", progress={"progress_percent": 25 + int(68*done/max(1,total)), "progress_label": f"{source.name} · {variant} ({done}/{total})", "source_index": si, "sources_total": len(sources), "variant": variant})
                if variant == "direct_photo":
                    guide = _fit_pad_rgb(source_crop, size=output_size, fill=(255,255,255))
                    guide_path = image_dir / "flux_direct_photo_input.png"; guide.save(guide_path)
                else:
                    if variant not in guide_map:
                        raise RuntimeError(f"unknown variant {variant}; available={sorted(guide_map)}")
                    guide_path = Path(guide_map[variant])
                    guide = _fit_pad_rgb(Image.open(guide_path).convert("RGB"), size=output_size, fill=(255,255,255))
                seed = int(config.get("seed") or 42)
                generator = torch.Generator(device="cpu").manual_seed(seed)
                strength = direct_strength if variant == "direct_photo" else (minimal_strength if variant == "CG4_minimal_clean" else guide_strength)
                if backend in {"flux_img2img", "flux_img2img_bnb4", "schnell_img2img"}:
                    result = pipe(
                        prompt=prompt,
                        image=guide,
                        height=output_size[1],
                        width=output_size[0],
                        num_inference_steps=steps,
                        strength=strength,
                        guidance_scale=guidance_scale,
                        generator=generator,
                        max_sequence_length=int(config.get("max_sequence_length") or (256 if "schnell" in model_id.lower() else 512)),
                    ).images[0]
                else:
                    result = pipe(
                        prompt=prompt,
                        control_image=guide,
                        height=output_size[1],
                        width=output_size[0],
                        num_inference_steps=steps,
                        guidance_scale=guidance_scale,
                        generator=generator,
                    ).images[0]
                cand_dir = image_dir / "flux_candidates" / variant
                cand_dir.mkdir(parents=True, exist_ok=True)
                raw_path = cand_dir / "candidate_raw.png"; result.save(raw_path)
                input_path = cand_dir / "input_guide.png"; guide.save(input_path)
                line = _line_mask(result); line_path = cand_dir / "candidate_line_mask.png"; line.save(line_path)
                preview_path = cand_dir / "candidate_burgundy_preview.png"; _burgundy(line).save(preview_path)
                row = {"source_image": str(source), "variant_id": variant, "backend": backend, "strength": strength, "guide_path": str(input_path), "raw_path": str(raw_path), "line_mask_path": str(line_path), "preview_path": str(preview_path)}
                (cand_dir / "candidate_report.json").write_text(json.dumps(row, ensure_ascii=False, indent=2), encoding="utf-8")
                rows.append(row)
            # Pick a conservative default for operator review: prefer CG variants over direct photo.
            preferred = next((r for r in rows if r["source_image"] == str(source) and r["variant_id"] == "CG3_fused_balanced"), None) or next(r for r in rows if r["source_image"] == str(source))
            for src_key, dst in [("line_mask_path", "result.png"), ("raw_path", "result_raw.png"), ("preview_path", "result_burgundy_preview.png")]:
                Path(preferred[src_key]).replace(image_dir / dst) if False else __import__('shutil').copy2(preferred[src_key], image_dir / dst)
        _write_contact_sheet(rows, DEFAULT_OUT / "flux_contact_sheet_all.png")
        report={"status":"ok", "backend": backend, "model_id": model_id, "steps": steps, "guidance_scale": guidance_scale, "direct_strength": direct_strength, "guide_strength": guide_strength, "minimal_strength": minimal_strength, "output_size": output_size, "variants": variants, "sources": [str(s) for s in sources], "candidates": rows, "contact_sheet": str(DEFAULT_OUT / "flux_contact_sheet_all.png")}
        (DEFAULT_OUT / "flux_probe_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        _status_event(status_client, "report_written", phase="report", status="done", progress={"progress_percent": 100, "progress_label": "FLUX probe report written", "output_dir": str(DEFAULT_OUT), "candidate_count": len(rows)})
        print(json.dumps(report, ensure_ascii=False, indent=2))
    except Exception as exc:
        _status_event(status_client, "report_written", phase="failed", status="failed", progress={**_status_progress(), "progress_label": "FLUX probe failed"}, message=f"{exc.__class__.__name__}: {exc}\n{traceback.format_exc(limit=6)}")
        raise
    finally:
        status_client.stop_alive()


if __name__ == "__main__":
    main()
