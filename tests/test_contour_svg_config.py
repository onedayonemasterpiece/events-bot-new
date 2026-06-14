from pathlib import Path

from contour_svg import RunConfig, load_config


def test_load_sample_config_has_neural_defaults():
    config = load_config("docs/features/countur_svg_generator/examples/sample_building.yaml")

    assert config.segmentation.backend == "groundingdino_sam2"
    assert config.diffusion.enabled is True
    assert config.diffusion.backend == "sd15_controlnet_img2img"
    assert config.diffusion.strength == 0.40
    assert config.diffusion.depth_controlnet == "lllyasviel/control_v11f1p_sd15_depth"
    assert config.diffusion.style_reference_adapter_enabled is True
    assert config.diffusion.style_reference_adapter_model == "h94/IP-Adapter"
    assert config.diffusion.style_reference_adapter_weight_name == "ip-adapter_sd15.bin"
    assert config.diffusion.style_reference_adapter_scale == 0.35
    assert config.diffusion.style_reference_strength == 0.34
    assert config.candidate_search.variants == ["B1", "B2", "B3", "B4", "E1"]
    assert config.candidate_search.allow_gemini_line_review is True
    assert config.candidate_search.max_gemini_line_groups == 75
    assert config.style.stroke_color == "#FFFFFF"
    assert config.style.stroke_style == "sketch"
    assert config.input.style_reference_path == "docs/features/countur_svg_generator/samples/output/IMG_20260614_115550.webp"


def test_cli_overrides_merge_without_losing_nested_defaults(tmp_path: Path):
    config = load_config(
        "docs/features/countur_svg_generator/examples/sample_building.yaml",
        overrides={
            "input": {"image_path": "photo.jpg"},
            "output": {"output_dir": str(tmp_path)},
            "gemini": {"model": "gemini-2.5-pro"},
        },
    )

    assert isinstance(config, RunConfig)
    assert config.input.image_path == "photo.jpg"
    assert config.output.output_dir == str(tmp_path)
    assert config.gemini.enabled is True
    assert config.gemini.model == "gemini-2.5-pro"
    assert config.diffusion.lineart_controlnet == "lllyasviel/control_v11p_sd15_lineart"
    assert config.diffusion.backend == "sd15_controlnet_img2img"


def test_controlnet_branch_uses_source_preserving_img2img_pipeline() -> None:
    source = Path("contour_svg/diffusion_controlnet.py").read_text(encoding="utf-8")

    assert "StableDiffusionControlNetImg2ImgPipeline" in source
    assert "source_preserving_init.png" in source
    assert "style_only_identity_from_init_image_and_controls" in source


def test_style_reference_branch_uses_ip_adapter_as_separate_variant() -> None:
    source = Path("contour_svg/diffusion_controlnet.py").read_text(encoding="utf-8")
    pipeline_source = Path("contour_svg/pipeline.py").read_text(encoding="utf-8")

    assert "load_ip_adapter" in source
    assert "set_ip_adapter_scale" in source
    assert "ip_adapter_image" in source
    assert "B3_ref_lineart_depth" in source
    assert "B4_ref_mlsd_depth" in source
    assert "CONTROLNET_REFERENCE_LINEART" in pipeline_source
    assert "CONTROLNET_REFERENCE_MLSD" in pipeline_source


def test_gemini_defaults_use_registered_google_ai_key_env():
    config = load_config("docs/features/countur_svg_generator/examples/sample_building.yaml")

    assert config.gemini.api_key_env == "GOOGLE_API_KEY"
