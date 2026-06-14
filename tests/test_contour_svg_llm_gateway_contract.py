from pathlib import Path


def test_contour_svg_gemini_uses_shared_google_ai_gateway() -> None:
    semantic_source = Path("contour_svg/semantic_gemini.py").read_text(encoding="utf-8")
    judge_source = Path("contour_svg/gemini_judge.py").read_text(encoding="utf-8")
    gateway_source = Path("contour_svg/llm_gateway.py").read_text(encoding="utf-8")

    combined = "\n".join([semantic_source, judge_source])
    assert "run_gateway_json_call" in combined
    assert "genai.Client" not in combined
    assert "models.generate_content" not in combined
    assert "Part.from_bytes" not in combined
    assert "GoogleAIClient" in gateway_source
    assert 'consumer="contour_svg"' in gateway_source
    assert "supabase_client=_build_supabase_client()" in gateway_source
    assert "assert_gateway_ready" in gateway_source
    assert "provider_sdk_google_genai" in gateway_source
    assert "registered_default_env_key_count" in gateway_source
    assert "reserve_fallback_enabled" in gateway_source
    assert "local_limiter_fallback_enabled" in gateway_source


def test_style_reference_is_judge_context_not_identity_source() -> None:
    judge_source = Path("contour_svg/gemini_judge.py").read_text(encoding="utf-8")

    assert "style_reference_path" in judge_source
    assert "style reference only; never copy its object identity" in judge_source
    assert "preserve the original source building identity" in judge_source


def test_contour_svg_kaggle_preflights_shared_limiter_before_run() -> None:
    source = Path("kaggle/ContourSvgGenerator/script.py").read_text(encoding="utf-8")

    assert "assert_gateway_ready" in source
    assert "preflight_ok" in source
    assert "llm_gateway" in source


def test_contour_svg_docs_do_not_advertise_direct_gemini_or_surrogate_outputs() -> None:
    doc_paths = [
        Path("docs/features/countur_svg_generator/README.md"),
        Path("docs/features/countur_svg_generator/requirements/requirements.md"),
        Path("docs/features/countur_svg_generator/requirements/gemini_prompts_and_schemas_v0_1.md"),
        Path("docs/features/countur_svg_generator/requirements/implementation_backlog_v0_1.md"),
    ]
    text = "\n".join(path.read_text(encoding="utf-8") for path in doc_paths)

    assert "genai.Client" not in text
    assert "model_fallback" not in text
    assert "best.svg" not in text
    assert "top_3" not in text
