#!/usr/bin/env python3
"""CPU-only BGE-M3 retrieval and gated dual-head age assessment.

Nearest-prototype similarity is evidence retrieval only.  An age candidate is
emitted solely when two independently fitted calibrated heads agree and an
automatic quality-gate manifest approves the exact encoder/bank/head hashes.
Without that bundle the fail-closed shadow result is ``insufficient_evidence``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterable

MODEL_ID = "BAAI/bge-m3"
ENCODER_CONTRACT = "bge_m3_cpu_dense_retrieval_v1"
OUTPUT_DIR = Path(os.getenv("EVENT_AGE_BGE_OUTPUT_DIR", "/kaggle/working"))
STATE: dict[str, Any] = {"phase": "bootstrap", "events_done": 0, "events_total": 0, "bge_batches_done": 0}
AGE_TOKEN = re.compile(
    r"(?iu)(?<!\d)(?:0|6|12|16|18)\s*\+|"
    r"\b(?:старше|от)\s+(?:0|6|12|16|18)\s*(?:лет|года?)?\b|"
    r"\b(?:возраст(?:ное)?\s+ограничение|ценз)\s*[:—-]?\s*(?:0|6|12|16|18)\b"
)


def mask_assessment_age_tokens(text: str) -> str:
    """Remove declared-age hints before fallback assessment."""

    return AGE_TOKEN.sub("[AGE_MARK_REMOVED]", str(text))


def getenv_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        return max(minimum, int((os.getenv(name) or str(default)).strip()))
    except Exception:
        return default


def stable_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def find_input(filename: str) -> Path:
    explicit = (os.getenv(filename.upper().replace(".", "_") + "_PATH") or "").strip()
    if explicit and Path(explicit).exists():
        return Path(explicit)
    for root in (Path("/kaggle/input"), Path.cwd()):
        if root.exists():
            matches = sorted(root.rglob(filename))
            if matches:
                return matches[0]
    raise FileNotFoundError(filename)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        if isinstance(item, dict):
            rows.append(item)
    return rows


def load_run_config() -> dict[str, Any]:
    try:
        path = find_input("event_age_bge_run.json")
    except FileNotFoundError:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    payload["__input_root"] = str(path.parent)
    return payload


def config_int(config: dict[str, Any], key: str, env_name: str, default: int) -> int:
    if (os.getenv(env_name) or "").strip():
        return getenv_int(env_name, default)
    try:
        return max(1, int(config.get(key, default)))
    except Exception:
        return default


def load_status_client():
    try:
        from kaggle_status_client import load_status_client as loader  # type: ignore
    except Exception:
        for root in (Path("/kaggle/input"), Path("/kaggle/working"), Path.cwd()):
            matches = sorted(root.rglob("kaggle_status_client.py")) if root.exists() else []
            if not matches:
                continue
            sys.path.insert(0, str(matches[0].parent))
            from kaggle_status_client import load_status_client as loader  # type: ignore
            break
        else:
            return None
    return loader(output_dir=OUTPUT_DIR)


def ensure_dependencies() -> None:
    try:
        # Import the exact runtime symbol, not only the package root.  Kaggle
        # may preinstall FlagEmbedding 1.3.x next to Transformers 5: the root
        # import succeeds, while BGEM3FlagModel fails later because that release
        # is not Transformers-5 compatible.
        from FlagEmbedding import BGEM3FlagModel  # noqa: F401
        import huggingface_hub  # noqa: F401
    except Exception:
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "-q",
                "--upgrade",
                "FlagEmbedding==1.4.0",
                "huggingface-hub>=0.34,<2.0",
            ]
        )


def load_encoder(model_revision: str):
    ensure_dependencies()
    from huggingface_hub import snapshot_download  # type: ignore
    from FlagEmbedding import BGEM3FlagModel  # type: ignore

    local_model = snapshot_download(repo_id=MODEL_ID, revision=model_revision)
    return BGEM3FlagModel(local_model, use_fp16=False)


def encode_dense(model: Any, texts: list[str], *, batch_size: int, max_length: int):
    import numpy as np

    result = model.encode(
        texts,
        batch_size=batch_size,
        max_length=max_length,
        return_dense=True,
        return_sparse=False,
        return_colbert_vecs=False,
    )
    vectors = np.asarray(result["dense_vecs"], dtype="float32")
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    return vectors / np.maximum(norms, 1e-12)


def retrieve_features(event_vectors: Any, prototype_vectors: Any, prototypes: list[dict[str, Any]], *, top_k: int) -> list[dict[str, Any]]:
    import numpy as np

    scores = np.matmul(event_vectors, prototype_vectors.T)
    outputs: list[dict[str, Any]] = []
    for row in scores:
        indices = np.argsort(-row)[:top_k]
        neighbors = [
            {
                "prototype_id": str(prototypes[int(idx)].get("id") or int(idx)),
                "rubric_code": prototypes[int(idx)].get("rubric_code"),
                "prototype_kind": prototypes[int(idx)].get("kind"),
                "score": round(float(row[int(idx)]), 6),
            }
            for idx in indices
        ]
        outputs.append({"neighbors": neighbors})
    return outputs


def softmax(logits: Any):
    import numpy as np

    shifted = logits - np.max(logits, axis=1, keepdims=True)
    exp = np.exp(shifted)
    return exp / np.maximum(exp.sum(axis=1, keepdims=True), 1e-12)


def _char_wb_ngrams(text: str, *, min_n: int = 3, max_n: int = 5) -> list[str]:
    """Match sklearn's lowercase ``char_wb`` analyzer without sklearn.

    The calibrated artifact contains only raw matrices and vocabulary bytes.
    Keeping inference here avoids pickle/joblib and sklearn-version drift in
    the Kaggle runtime.
    """

    normalized = re.sub(r"\s\s+", " ", str(text).lower())
    ngrams: list[str] = []
    for word in normalized.split():
        padded = " " + word + " "
        word_length = len(padded)
        for n in range(min_n, max_n + 1):
            offset = 0
            ngrams.append(padded[offset : offset + n])
            while offset + n < word_length:
                offset += 1
                ngrams.append(padded[offset : offset + n])
            if offset == 0:
                break
    return ngrams


def _tfidf_features(text: str, vocabulary: dict[str, int], idf: Any) -> dict[int, float]:
    """Return sparse sublinear-TF/IDF/L2 features as an index/value map."""

    import math

    counts: dict[int, int] = {}
    for ngram in _char_wb_ngrams(text):
        index = vocabulary.get(ngram)
        if index is not None:
            counts[index] = counts.get(index, 0) + 1
    values = {
        index: (1.0 + math.log(count)) * float(idf[index])
        for index, count in counts.items()
    }
    norm = math.sqrt(sum(value * value for value in values.values()))
    if norm > 0:
        values = {index: value / norm for index, value in values.items()}
    return values


def load_safety_cascade_runtime(bundle: Any) -> dict[str, Any]:
    """Validate and materialize immutable matrices once per worker batch."""

    import numpy as np

    terms = [str(value) for value in bundle["tfidf_terms"].tolist()]
    if len(set(terms)) != len(terms):
        raise ValueError("TF-IDF vocabulary terms must be unique")
    vocabulary = {term: index for index, term in enumerate(terms)}
    idf = np.asarray(bundle["tfidf_idf"], dtype="float64")
    head_weights = np.asarray(bundle["head_weights"], dtype="float64")
    head_bias = np.asarray(bundle["head_bias"], dtype="float64")
    boundary_weights = np.asarray(bundle["boundary_weights"], dtype="float64")
    boundary_bias = np.asarray(bundle["boundary_bias"], dtype="float64")
    if idf.shape != (len(terms),):
        raise ValueError("TF-IDF idf shape does not match vocabulary")
    if head_weights.ndim != 3 or head_weights.shape[1:] != (5, len(terms)):
        raise ValueError("multiclass head matrix has an invalid shape")
    if head_bias.shape != head_weights.shape[:2]:
        raise ValueError("multiclass head bias has an invalid shape")
    if boundary_weights.shape != (3, len(terms)) or boundary_bias.shape != (3,):
        raise ValueError("ordinal boundary matrices have an invalid shape")
    return {
        "vocabulary": vocabulary,
        "idf": idf,
        "head_weights": head_weights,
        "head_bias": head_bias,
        "boundary_weights": boundary_weights,
        "boundary_bias": boundary_bias,
    }


def safety_cascade_logits(
    text: str, bundle: Any, *, runtime: dict[str, Any] | None = None
) -> tuple[Any, Any]:
    """Compute raw multiclass and ordinal logits from a matrix-only bundle."""

    runtime = runtime or load_safety_cascade_runtime(bundle)
    features = _tfidf_features(text, runtime["vocabulary"], runtime["idf"])
    multi_logits = runtime["head_bias"].copy()
    boundary_logits = runtime["boundary_bias"].copy()
    for index, value in features.items():
        multi_logits += runtime["head_weights"][:, :, index] * value
        boundary_logits += runtime["boundary_weights"][:, index] * value
    return multi_logits, boundary_logits


def classify_with_safety_cascade(
    texts: list[str], bundle: Any, gate: dict[str, Any]
) -> list[dict[str, Any]]:
    """Classify with bootstrap consensus plus an asymmetric ordinal veto."""

    import numpy as np

    classes = [str(value) for value in bundle["classes"].tolist()]
    if classes != ["0+", "6+", "12+", "16+", "18+"]:
        raise ValueError("classifier classes must use the canonical order")
    kind = str(bundle["classifier_kind"].item())
    if kind != "char_tfidf_safety_cascade_v1":
        raise ValueError(f"unsupported safety cascade kind: {kind}")
    params = gate.get("cascade_thresholds") or {}
    required = {
        "confidence",
        "low_consensus",
        "high_consensus",
        "max_severe_votes",
        "max_severe_risk",
        "min_high_votes",
        "min_high_risk",
    }
    if not required.issubset(params):
        raise ValueError("evaluation gate has incomplete cascade thresholds")
    runtime = load_safety_cascade_runtime(bundle)
    outputs: list[dict[str, Any]] = []
    for text in texts:
        multi_logits, boundary_logits = safety_cascade_logits(text, bundle, runtime=runtime)
        probabilities = softmax(multi_logits)
        head_predictions = np.argmax(probabilities, axis=1)
        counts = np.bincount(head_predictions, minlength=5)
        predicted = int(np.argmax(counts))
        consensus = int(counts[predicted])
        confidence = float(np.mean(probabilities[:, predicted]))
        boundary_risk = 1.0 / (1.0 + np.exp(-np.clip(boundary_logits, -30, 30)))
        boundary_votes = (boundary_risk >= 0.5).astype("int64")
        accepted = bool(
            confidence >= float(params["confidence"])
            and (
                (predicted <= 2 and consensus >= int(params["low_consensus"]))
                or (predicted >= 3 and consensus >= int(params["high_consensus"]))
            )
        )
        severe_boundary = {0: 2, 1: 3, 2: 4}.get(predicted)
        if severe_boundary is not None:
            boundary_index = severe_boundary - 2
            accepted = bool(
                accepted
                and boundary_votes[boundary_index] <= int(params["max_severe_votes"])
                and boundary_risk[boundary_index] <= float(params["max_severe_risk"])
            )
        corroboration_boundary = {2: 2, 3: 3, 4: 4}.get(predicted)
        if corroboration_boundary is not None:
            boundary_index = corroboration_boundary - 2
            accepted = bool(
                accepted
                and boundary_votes[boundary_index] >= int(params["min_high_votes"])
                and boundary_risk[boundary_index] >= float(params["min_high_risk"])
            )
        outputs.append(
            {
                "status": "assessed" if accepted else "insufficient_evidence",
                "age_assessment": classes[predicted] if accepted else None,
                "provenance": "model_assessed" if accepted else None,
                "confidence": round(confidence, 6) if accepted else None,
                "verification": {
                    "classifier_kind": kind,
                    "head_predictions": [classes[int(value)] for value in head_predictions],
                    "consensus": consensus,
                    "confidence": round(confidence, 6),
                    "ordinal_risk": {
                        "gte_12": round(float(boundary_risk[0]), 6),
                        "gte_16": round(float(boundary_risk[1]), 6),
                        "gte_18": round(float(boundary_risk[2]), 6),
                    },
                },
            }
        )
    return outputs


def validate_safety_cascade_self_tests(bundle: Any, gate: dict[str, Any]) -> None:
    """Reject an artifact whose pure inference no longer reproduces logits."""

    import numpy as np

    cases = gate.get("deterministic_self_tests")
    if not isinstance(cases, list) or len(cases) < 5:
        raise ValueError("safety cascade requires at least five deterministic self-tests")
    runtime = load_safety_cascade_runtime(bundle)
    decisions = classify_with_safety_cascade(
        [str(case.get("text") or "") for case in cases], bundle, gate
    )
    for case, actual in zip(cases, decisions):
        multi_logits, boundary_logits = safety_cascade_logits(
            str(case.get("text") or ""), bundle, runtime=runtime
        )
        expected_multi = np.asarray(case.get("head_logits"), dtype="float64")
        expected_boundary = np.asarray(case.get("boundary_logits"), dtype="float64")
        if not np.allclose(multi_logits, expected_multi, rtol=0.0, atol=1e-5):
            raise ValueError("safety cascade multiclass self-test mismatch")
        if not np.allclose(boundary_logits, expected_boundary, rtol=0.0, atol=1e-5):
            raise ValueError("safety cascade ordinal self-test mismatch")
        if actual["status"] != case.get("status") or actual["age_assessment"] != case.get("age_assessment"):
            raise ValueError("safety cascade decision self-test mismatch")


def classify_with_dual_heads(vectors: Any, bundle: Any, gate: dict[str, Any]) -> list[dict[str, Any]]:
    """Return assessed candidates only under artifact-owned calibrated cutoffs."""

    import numpy as np

    classes = [str(x) for x in bundle["classes"].tolist()]
    if classes != ["0+", "6+", "12+", "16+", "18+"]:
        raise ValueError("classifier classes must use the canonical order")
    dimension = int(vectors.shape[1])
    for key in ("weights_a", "weights_b"):
        if tuple(bundle[key].shape) != (dimension, len(classes)):
            raise ValueError(f"{key} has an invalid shape")
    for key in ("bias_a", "bias_b"):
        if tuple(bundle[key].shape) != (len(classes),):
            raise ValueError(f"{key} has an invalid shape")
    pa = softmax(np.matmul(vectors, bundle["weights_a"]) + bundle["bias_a"])
    pb = softmax(np.matmul(vectors, bundle["weights_b"]) + bundle["bias_b"])
    ia, ib = np.argmax(pa, axis=1), np.argmax(pb, axis=1)
    cutoff_a = float(gate["head_a_min_probability"])
    cutoff_b = float(gate["head_b_min_probability"])
    outputs: list[dict[str, Any]] = []
    for row, (a, b) in enumerate(zip(ia, ib)):
        confidence_a = float(pa[row, a])
        confidence_b = float(pb[row, b])
        accepted = bool(a == b and confidence_a >= cutoff_a and confidence_b >= cutoff_b)
        outputs.append(
            {
                "status": "assessed" if accepted else "insufficient_evidence",
                "age_assessment": classes[int(a)] if accepted else None,
                "provenance": "bge_assessed" if accepted else None,
                "confidence": round(min(confidence_a, confidence_b), 6) if accepted else None,
                "verification": {
                    "head_a": classes[int(a)],
                    "head_b": classes[int(b)],
                    "head_a_probability": round(confidence_a, 6),
                    "head_b_probability": round(confidence_b, 6),
                    "agreement": bool(a == b),
                },
            }
        )
    return outputs


def load_prepared_artifacts(
    *, bank_payload: dict[str, Any], model_revision: str, model: Any, prototype_texts: list[str]
) -> tuple[Any, Any | None, dict[str, Any] | None, str | None]:
    """Load vectors and an optional automatically gated classifier."""

    import numpy as np

    bank_hash = stable_hash(bank_payload)
    try:
        vector_path = find_input("event_age_bge_prototype_vectors.npz")
    except FileNotFoundError:
        # Cold-start calibration run: compute the bank once on Kaggle CPU and
        # export the exact pinned vectors. Production runs should reuse this
        # output as a prepared artifact rather than repeatedly encoding it.
        prototype_vectors = encode_dense(
            model,
            prototype_texts,
            batch_size=max(1, min(8, len(prototype_texts))),
            max_length=1024,
        )
        np.savez_compressed(
            OUTPUT_DIR / "event_age_bge_prototype_vectors.npz",
            vectors=prototype_vectors,
            model_revision=np.asarray(model_revision),
            encoder_contract=np.asarray(ENCODER_CONTRACT),
            prototype_bank_hash=np.asarray(bank_hash),
        )
    else:
        prepared = np.load(vector_path, allow_pickle=False)
        if str(prepared["prototype_bank_hash"].item()) != bank_hash:
            raise ValueError("prepared prototype vectors do not match prototype bank")
        if str(prepared["model_revision"].item()) != model_revision:
            raise ValueError("prepared prototype vectors do not match pinned model revision")
        if str(prepared["encoder_contract"].item()) != ENCODER_CONTRACT:
            raise ValueError("prepared prototype vectors use another encoder contract")
        prototype_vectors = np.asarray(prepared["vectors"], dtype="float32")
    norms = np.linalg.norm(prototype_vectors, axis=1, keepdims=True)
    prototype_vectors = prototype_vectors / np.maximum(norms, 1e-12)
    classifier = None
    gate = None
    classifier_hash = None
    try:
        classifier_path = find_input("event_age_bge_classifier.npz")
        gate_path = find_input("event_age_bge_evaluation.json")
    except FileNotFoundError:
        return prototype_vectors, None, None, None
    classifier_bytes = classifier_path.read_bytes()
    classifier_hash = hashlib.sha256(classifier_bytes).hexdigest()
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    required = {
        "approval_status": "approved",
        "quality_gates_passed": True,
        "model_revision": model_revision,
        "encoder_contract": ENCODER_CONTRACT,
        "prototype_bank_hash": bank_hash,
        "classifier_sha256": classifier_hash,
    }
    if any(gate.get(key) != value for key, value in required.items()):
        return prototype_vectors, None, gate, classifier_hash
    authority = gate.get("gate_authority")
    common_fields = (
        "evaluation_dataset_hash",
        "labeled_case_count",
        "critical_over_permissive_rate",
        "generated_at",
        "class_support",
        "exact_accuracy",
        "within_one_accuracy",
    )
    if any(key not in gate for key in common_fields):
        return prototype_vectors, None, gate, classifier_hash
    classifier = np.load(classifier_path, allow_pickle=False)
    kind = (
        str(classifier["classifier_kind"].item())
        if "classifier_kind" in classifier.files
        else "bge_dense_dual_head_v1"
    )
    if authority == "automatic_quality_gate_v1" and kind == "bge_dense_dual_head_v1":
        if any(
            key not in gate
            for key in ("assessment_agreement", "head_a_min_probability", "head_b_min_probability")
        ):
            return prototype_vectors, None, gate, classifier_hash
    elif authority == "automatic_quality_gate_v2" and kind == "char_tfidf_safety_cascade_v1":
        try:
            validate_safety_cascade_self_tests(classifier, gate)
        except (KeyError, TypeError, ValueError):
            return prototype_vectors, None, gate, classifier_hash
    else:
        return prototype_vectors, None, gate, classifier_hash
    return prototype_vectors, classifier, gate, classifier_hash


def atomic_json(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def main() -> int:
    started = time.monotonic()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    status = load_status_client()
    if status:
        status.event("kernel_started", phase="bootstrap", status="running", progress=STATE)
        status.start_alive(interval_seconds=60, progress_provider=lambda: dict(STATE))
    run_config = load_run_config()
    revision = (
        os.getenv("EVENT_AGE_BGE_MODEL_REVISION")
        or str(run_config.get("model_revision") or "")
    ).strip()
    if not revision:
        raise RuntimeError("EVENT_AGE_BGE_MODEL_REVISION is required; unpinned model downloads are forbidden")
    input_root = Path(str(run_config.get("__input_root") or ""))
    input_path = input_root / "event_age_bge_input.jsonl"
    bank_path = input_root / "event_age_bge_prototypes.json"
    if not input_path.exists() or not bank_path.exists():
        raise FileNotFoundError("run config, event input, and prototype bank must share one dataset")
    events = load_jsonl(input_path)
    bank_payload = json.loads(bank_path.read_text(encoding="utf-8"))
    prototypes = bank_payload.get("prototypes") if isinstance(bank_payload, dict) else None
    if not isinstance(prototypes, list) or not prototypes:
        raise ValueError("prototype bank is empty")
    bank_hash = stable_hash(bank_payload)
    batch_limit = config_int(run_config, "batch_limit", "EVENT_AGE_BGE_BATCH_LIMIT", 64)
    batch_size = config_int(run_config, "batch_size", "EVENT_AGE_BGE_BATCH_SIZE", 4)
    max_length = config_int(run_config, "max_length", "EVENT_AGE_BGE_MAX_LENGTH", 2048)
    top_k = config_int(run_config, "top_k", "EVENT_AGE_BGE_TOP_K", 8)
    events = events[:batch_limit]
    STATE.update({"phase": "preflight", "events_total": len(events)})
    if status:
        status.event("preflight_ok", phase="preflight", status="running", progress=STATE)
    model = load_encoder(revision)
    prototype_texts = [str(item.get("text") or "").strip() for item in prototypes]
    if not all(prototype_texts):
        raise ValueError("each prototype requires text")
    prototype_vectors, classifier, evaluation_gate, classifier_hash = load_prepared_artifacts(
        bank_payload=bank_payload,
        model_revision=revision,
        model=model,
        prototype_texts=prototype_texts,
    )
    expected_classifier_hash = str(run_config.get("expected_classifier_sha256") or "").strip()
    if classifier is not None and (
        not expected_classifier_hash or classifier_hash != expected_classifier_hash
    ):
        # A classifier may never become active merely because a different
        # Kaggle dataset happened to be attached to the notebook.
        classifier = None
    if prototype_vectors.shape[0] != len(prototypes):
        raise ValueError("prepared vector count does not match prototype count")
    results: list[dict[str, Any]] = []
    vector_chunks: list[Any] = []
    vector_event_ids: list[int] = []
    vector_input_hashes: list[str] = []
    max_runtime_seconds = config_int(
        run_config, "max_runtime_seconds", "EVENT_AGE_BGE_MAX_RUNTIME_SECONDS", 25 * 60
    )
    runtime_guard_seconds = config_int(
        run_config, "runtime_guard_seconds", "EVENT_AGE_BGE_RUNTIME_GUARD_SECONDS", 90
    )
    checkpoint_path = OUTPUT_DIR / "event_age_bge_checkpoint.json"
    STATE["phase"] = "run"
    for offset in range(0, len(events), batch_size):
        if time.monotonic() - started >= max_runtime_seconds - runtime_guard_seconds:
            STATE["phase"] = "partial"
            break
        batch = events[offset : offset + batch_size]
        texts = [mask_assessment_age_tokens(str(item.get("text") or "").strip()) for item in batch]
        vectors = encode_dense(model, texts, batch_size=batch_size, max_length=max_length)
        vector_chunks.append(vectors)
        vector_event_ids.extend(int(item.get("event_id") or 0) for item in batch)
        vector_input_hashes.extend(str(item.get("input_hash") or "") for item in batch)
        features = retrieve_features(vectors, prototype_vectors, prototypes, top_k=top_k)
        if classifier is not None and evaluation_gate is not None:
            classifier_kind = (
                str(classifier["classifier_kind"].item())
                if "classifier_kind" in classifier.files
                else "bge_dense_dual_head_v1"
            )
            if classifier_kind == "char_tfidf_safety_cascade_v1":
                classifications = classify_with_safety_cascade(texts, classifier, evaluation_gate)
            else:
                classifications = classify_with_dual_heads(vectors, classifier, evaluation_gate)
        else:
            classifications = [
                {
                    "status": "insufficient_evidence",
                    "age_assessment": None,
                    "provenance": None,
                    "confidence": None,
                    "verification": {"reason": "needs_approved_calibrated_dual_head"},
                }
                for _ in batch
            ]
        for item, feature, classification in zip(batch, features, classifications):
            # A source-declared value always wins and is handled by Smart Update,
            # never by this content-assessment worker.
            if item.get("declared_age"):
                classification = {
                    "status": "insufficient_evidence",
                    "age_assessment": None,
                    "provenance": None,
                    "confidence": None,
                    "verification": {"reason": "declared_age_present"},
                }
            results.append(
                {
                    "event_id": item.get("event_id"),
                    "input_hash": item.get("input_hash"),
                    **classification,
                    "next_action": (
                        "automatic_hash_bound_import"
                        if classification["status"] == "assessed"
                        else "terminal_insufficient_evidence"
                    ),
                    "age_restriction": None,
                    "retrieval": feature,
                    "model_id": MODEL_ID,
                    "model_revision": revision,
                    "encoder_contract": ENCODER_CONTRACT,
                    "prototype_bank_hash": bank_hash,
                    "classifier_sha256": classifier_hash,
                }
            )
        STATE["events_done"] = len(results)
        STATE["bge_batches_done"] += 1
        STATE["progress_label"] = f"события {len(results)}/{len(events)}"
        atomic_json(
            checkpoint_path,
            {
                "schema_version": "event-age-bge-checkpoint-v1",
                "events_done": len(results),
                "last_input_hash": results[-1].get("input_hash") if results else None,
                "model_revision": revision,
                "prototype_bank_hash": bank_hash,
                "partial_results": results,
            },
        )
    if vector_chunks:
        import numpy as np

        np.savez_compressed(
            OUTPUT_DIR / "event_age_bge_event_vectors.npz",
            vectors=np.concatenate(vector_chunks, axis=0),
            event_ids=np.asarray(vector_event_ids, dtype="int64"),
            input_hashes=np.asarray(vector_input_hashes),
            model_revision=np.asarray(revision),
            encoder_contract=np.asarray(ENCODER_CONTRACT),
            prototype_bank_hash=np.asarray(bank_hash),
        )
    report = {
        "schema_version": "event-age-bge-shadow-v1",
        "status": "complete" if len(results) == len(events) else "partial",
        "cpu_only": True,
        "model_id": MODEL_ID,
        "model_revision": revision,
        "encoder_contract": ENCODER_CONTRACT,
        "run_id": run_config.get("run_id"),
        "assessment_policy_version": run_config.get("assessment_policy_version"),
        "prototype_bank_hash": bank_hash,
        "classifier_sha256": classifier_hash,
        "classifier_active": classifier is not None,
        "evaluation_approval_status": (
            evaluation_gate.get("approval_status") if isinstance(evaluation_gate, dict) else "missing"
        ),
        "events_total": len(events),
        "events_done": len(results),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "results": results,
    }
    atomic_json(OUTPUT_DIR / "event_age_bge_result.json", report)
    final_status = str(report["status"])
    progress_percent = round(100 * len(results) / len(events)) if events else 100
    STATE.update(
        {
            "phase": "report" if final_status == "complete" else "partial",
            "events_done": len(results),
            "progress_percent": progress_percent,
            "progress_label": f"события {len(results)}/{len(events)}",
        }
    )
    if status:
        status.event(
            "report_written",
            phase=str(STATE["phase"]),
            status="done" if final_status == "complete" else "partial",
            progress=STATE,
        )
        status.stop_alive()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
