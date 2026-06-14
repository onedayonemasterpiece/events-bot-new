from __future__ import annotations

import json
from pathlib import Path

from .config import RunConfig
from .contracts import GuideSet, MaskBundle
from .dependencies import MissingDependencyError, has_cuda, require_module


def _lineart_condition_image(path: Path, *, size: tuple[int, int]):
    Image = require_module("PIL.Image", "Pillow")
    ImageOps = require_module("PIL.ImageOps", "Pillow")
    image = Image.open(path).convert("L")
    # The v1.1 line-art ControlNet was trained for line drawings; black
    # strokes on a white canvas are the safest condition format.
    if float(sum(image.histogram()[128:])) / max(1.0, image.width * image.height) < 0.5:
        image = ImageOps.invert(image)
    return image.convert("RGB").resize(size)


def generate_controlnet_rasters(
    image,
    guides: GuideSet,
    masks: MaskBundle,
    out_dir: Path,
    config: RunConfig,
) -> tuple[list[tuple[str, Path]], list[str]]:
    warnings: list[str] = []
    if not config.diffusion.enabled:
        raise RuntimeError("ControlNet diffusion is required for contour_svg")
    if not has_cuda():
        raise MissingDependencyError("ControlNet diffusion requires CUDA/GPU for this pipeline")
    torch = require_module("torch", "torch")
    diffusers = require_module("diffusers", "diffusers")
    if guides.edge_map is None:
        raise RuntimeError("ControlNet diffusion requires an edge guide")
    if guides.mlsd_guide is None:
        raise RuntimeError("ControlNet MLSD branch requires an M-LSD guide")

    rasters: list[tuple[str, Path]] = []
    candidates_dir = out_dir / "candidates"
    candidates_dir.mkdir(parents=True, exist_ok=True)

    debug_dir = out_dir / "debug" / "neural_branch_raw"
    debug_dir.mkdir(parents=True, exist_ok=True)
    size = tuple(config.diffusion.output_size)
    init_image, crop_box, occluder_mask = _source_preserving_init_image(image, masks, size=size)
    init_image.save(debug_dir / "source_preserving_init.png")
    occluder_mask.save(debug_dir / "source_preserving_occluder_mask.png")
    lineart_condition = _condition_from_guide(
        guides.edge_map,
        crop_box=crop_box,
        size=size,
        occluder_mask=occluder_mask,
    )
    lineart_condition.save(candidates_dir / "B1_controlnet_condition_lineart.png")
    mlsd_condition = _condition_from_guide(
        guides.mlsd_guide,
        crop_box=crop_box,
        size=size,
        occluder_mask=occluder_mask,
    )
    mlsd_condition.save(candidates_dir / "B2_controlnet_condition_mlsd.png")
    depth_condition = _depth_condition_image(init_image, config, size=size)
    depth_condition.save(candidates_dir / "controlnet_condition_depth.png")
    style_reference = _style_reference_image(config, size=size)
    if style_reference is not None:
        style_reference.save(debug_dir / "style_reference_adapter_image.png")
    prompt = (
        "clean minimal monoline contour drawing, white ink on dark background, "
        "even line weight, transparent-style line art, vector poster aesthetic, "
        "geometric perspective lines, calm composition"
    )
    negative = (
        "different building, alternate facade, modified massing, added wings, "
        "extra windows, hallucinated columns, generic classical building, white marble, "
        "stock illustration, photo, realistic, color fill, shading, gradient, sketch noise, "
        "scribbles, crosshatching, texture, bricks, trees, foliage, fence, people, cars, "
        "street, pavement, watermark, text, blur, dense lines, tiny details"
    )
    rasters.extend(
        _run_controlnet_img2img_branch(
            label="B1_lineart_depth",
            controlnet_models=[config.diffusion.lineart_controlnet, config.diffusion.depth_controlnet],
            control_images=[lineart_condition, depth_condition],
            init_image=init_image,
            conditioning_scales=[float(config.diffusion.control_scale), float(config.diffusion.depth_control_scale)],
            prompt=prompt,
            negative_prompt=negative,
            candidates_dir=candidates_dir,
            config=config,
        )
    )
    rasters.extend(
        _run_controlnet_img2img_branch(
            label="B2_mlsd_depth",
            controlnet_models=[config.diffusion.mlsd_controlnet, config.diffusion.depth_controlnet],
            control_images=[mlsd_condition, depth_condition],
            init_image=init_image,
            conditioning_scales=[float(config.diffusion.control_scale), float(config.diffusion.depth_control_scale)],
            prompt=prompt,
            negative_prompt=negative,
            candidates_dir=candidates_dir,
            config=config,
        )
    )
    if _variant_enabled(config, "B3") or _variant_enabled(config, "B4"):
        if not config.diffusion.style_reference_adapter_enabled:
            raise RuntimeError("B3/B4 style-reference branches require style_reference_adapter_enabled=true")
        if style_reference is None:
            raise RuntimeError("B3/B4 style-reference branches require input.style_reference_path")
    if _variant_enabled(config, "B3"):
        rasters.extend(
            _run_controlnet_img2img_branch(
                label="B3_ref_lineart_depth",
                controlnet_models=[config.diffusion.lineart_controlnet, config.diffusion.depth_controlnet],
                control_images=[lineart_condition, depth_condition],
                init_image=init_image,
                conditioning_scales=[float(config.diffusion.control_scale), float(config.diffusion.depth_control_scale)],
                prompt=prompt,
                negative_prompt=negative,
                candidates_dir=candidates_dir,
                config=config,
                ip_adapter_image=style_reference,
                strength=float(config.diffusion.style_reference_strength),
            )
        )
    if _variant_enabled(config, "B4"):
        rasters.extend(
            _run_controlnet_img2img_branch(
                label="B4_ref_mlsd_depth",
                controlnet_models=[config.diffusion.mlsd_controlnet, config.diffusion.depth_controlnet],
                control_images=[mlsd_condition, depth_condition],
                init_image=init_image,
                conditioning_scales=[float(config.diffusion.control_scale), float(config.diffusion.depth_control_scale)],
                prompt=prompt,
                negative_prompt=negative,
                candidates_dir=candidates_dir,
                config=config,
                ip_adapter_image=style_reference,
                strength=float(config.diffusion.style_reference_strength),
            )
        )
    if not (_variant_enabled(config, "B1") or _variant_enabled(config, "B2")):
        raise RuntimeError("At least one base ControlNet branch B1 or B2 is required")
    if not rasters:
        raise RuntimeError("ControlNet produced no raster candidates")
    (debug_dir / "neural_branch_meta.json").write_text(
        json.dumps(
            {
                "backend": config.diffusion.backend,
                "pipeline": "StableDiffusionControlNetImg2ImgPipeline",
                "crop_box_xyxy": crop_box,
                "prompt_policy": "style_only_identity_from_init_image_and_controls",
                "base_model": config.diffusion.base_model,
                "lineart_controlnet": config.diffusion.lineart_controlnet,
                "mlsd_controlnet": config.diffusion.mlsd_controlnet,
                "depth_controlnet": config.diffusion.depth_controlnet,
                "depth_model": config.diffusion.depth_model,
                "strength": config.diffusion.strength,
                "style_reference_path": config.input.style_reference_path,
                "style_reference_adapter_enabled": config.diffusion.style_reference_adapter_enabled,
                "style_reference_adapter_model": config.diffusion.style_reference_adapter_model,
                "style_reference_adapter_subfolder": config.diffusion.style_reference_adapter_subfolder,
                "style_reference_adapter_weight_name": config.diffusion.style_reference_adapter_weight_name,
                "style_reference_adapter_scale": config.diffusion.style_reference_adapter_scale,
                "style_reference_strength": config.diffusion.style_reference_strength,
                "guidance_scale": config.diffusion.guidance_scale,
                "control_scale": config.diffusion.control_scale,
                "depth_control_scale": config.diffusion.depth_control_scale,
                "control_guidance_end": config.diffusion.control_guidance_end,
                "seeds": config.diffusion.seeds,
                "proposal_only": True,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return rasters, warnings


def _variant_enabled(config: RunConfig, variant: str) -> bool:
    return variant in set(config.candidate_search.variants)


def _source_preserving_init_image(image, masks: MaskBundle, *, size: tuple[int, int]):
    Image = require_module("PIL.Image", "Pillow")
    ImageStat = require_module("PIL.ImageStat", "Pillow")

    object_mask = Image.open(masks.object_visible).convert("L")
    bbox = object_mask.getbbox() or (0, 0, image.width, image.height)
    crop_box = _expand_box(bbox, image.size, margin_ratio=0.12)
    source_crop = image.crop(crop_box).convert("RGB")
    occluder = Image.open(masks.occluder).convert("L").crop(crop_box) if masks.occluder else Image.new("L", source_crop.size, 0)

    stat = ImageStat.Stat(source_crop)
    neutral = tuple(int(max(96, min(176, channel))) for channel in stat.median[:3])
    neutral_img = Image.new("RGB", source_crop.size, neutral)
    neutralized = Image.composite(neutral_img, source_crop, occluder)
    init_image, paste_box = _fit_pad(neutralized, size=size, fill=neutral)
    occluder_fitted = _fit_pad_mask(occluder, size=size, paste_box=paste_box, fitted_source_size=_fit_size(neutralized.size, size))
    return init_image, crop_box, occluder_fitted


def _condition_from_guide(path: Path, *, crop_box: tuple[int, int, int, int], size: tuple[int, int], occluder_mask):
    Image = require_module("PIL.Image", "Pillow")
    ImageOps = require_module("PIL.ImageOps", "Pillow")
    guide = Image.open(path).convert("L").crop(crop_box)
    if float(sum(guide.histogram()[128:])) / max(1.0, guide.width * guide.height) < 0.5:
        guide = ImageOps.invert(guide)
    fitted, paste_box = _fit_pad(guide.convert("RGB"), size=size, fill=(255, 255, 255))
    erase = occluder_mask.convert("L")
    white = Image.new("RGB", fitted.size, (255, 255, 255))
    return Image.composite(white, fitted, erase)


def _style_reference_image(config: RunConfig, *, size: tuple[int, int]):
    if not config.input.style_reference_path:
        return None
    Image = require_module("PIL.Image", "Pillow")
    raw_path = Path(config.input.style_reference_path).expanduser()
    if not raw_path.is_absolute():
        raw_path = Path.cwd() / raw_path
    if not raw_path.exists():
        raise RuntimeError(f"style_reference_path does not exist: {raw_path}")
    reference = Image.open(raw_path).convert("RGB")
    fitted, _ = _fit_pad(reference, size=size, fill=(0, 0, 0))
    return fitted


def _depth_condition_image(init_image, config: RunConfig, *, size: tuple[int, int]):
    transformers = require_module("transformers", "transformers")
    torch = require_module("torch", "torch")
    pipe = transformers.pipeline(
        task="depth-estimation",
        model=config.diffusion.depth_model,
        model_kwargs={"cache_dir": config.runtime.hf_cache_dir},
        device=0 if torch.cuda.is_available() else -1,
    )
    depth = pipe(init_image)["depth"].convert("L").resize(size)
    del pipe
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    return depth.convert("RGB")


def _expand_box(box: tuple[int, int, int, int], image_size: tuple[int, int], *, margin_ratio: float) -> tuple[int, int, int, int]:
    x1, y1, x2, y2 = box
    width, height = image_size
    margin_x = int((x2 - x1) * margin_ratio)
    margin_y = int((y2 - y1) * margin_ratio)
    return (
        max(0, x1 - margin_x),
        max(0, y1 - margin_y),
        min(width, x2 + margin_x),
        min(height, y2 + margin_y),
    )


def _fit_size(source_size: tuple[int, int], size: tuple[int, int]) -> tuple[int, int]:
    source_w, source_h = source_size
    target_w, target_h = size
    scale = min(target_w / max(1, source_w), target_h / max(1, source_h))
    return max(1, int(round(source_w * scale))), max(1, int(round(source_h * scale)))


def _fit_pad(image, *, size: tuple[int, int], fill: tuple[int, int, int]):
    Image = require_module("PIL.Image", "Pillow")
    resampling = getattr(Image, "Resampling", Image)
    fitted_size = _fit_size(image.size, size)
    resized = image.resize(fitted_size, resampling.LANCZOS)
    canvas = Image.new("RGB", size, fill)
    paste_box = ((size[0] - fitted_size[0]) // 2, (size[1] - fitted_size[1]) // 2)
    canvas.paste(resized, paste_box)
    return canvas, paste_box


def _fit_pad_mask(mask, *, size: tuple[int, int], paste_box: tuple[int, int], fitted_source_size: tuple[int, int]):
    Image = require_module("PIL.Image", "Pillow")
    resampling = getattr(Image, "Resampling", Image)
    resized = mask.resize(fitted_source_size, resampling.NEAREST)
    canvas = Image.new("L", size, 0)
    canvas.paste(resized, paste_box)
    return canvas


def _run_controlnet_img2img_branch(
    *,
    label: str,
    controlnet_models: list[str],
    control_images: list[object],
    init_image,
    conditioning_scales: list[float],
    prompt: str,
    negative_prompt: str,
    candidates_dir: Path,
    config: RunConfig,
    ip_adapter_image=None,
    strength: float | None = None,
) -> list[tuple[str, Path]]:
    torch = require_module("torch", "torch")
    diffusers = require_module("diffusers", "diffusers")
    dtype = torch.float16 if config.runtime.dtype == "float16" and has_cuda() else torch.float32
    ControlNetModel = diffusers.ControlNetModel
    StableDiffusionControlNetImg2ImgPipeline = diffusers.StableDiffusionControlNetImg2ImgPipeline
    controlnets = [
        ControlNetModel.from_pretrained(
            model,
            torch_dtype=dtype,
            cache_dir=config.runtime.hf_cache_dir,
        )
        for model in controlnet_models
    ]
    controlnet_arg = _multi_controlnet(diffusers, controlnets)
    pipe = StableDiffusionControlNetImg2ImgPipeline.from_pretrained(
        config.diffusion.base_model,
        controlnet=controlnet_arg,
        torch_dtype=dtype,
        cache_dir=config.runtime.hf_cache_dir,
        safety_checker=None,
    ).to("cuda")
    scheduler_cls = getattr(diffusers, "DPMSolverMultistepScheduler", None)
    if scheduler_cls is not None:
        pipe.scheduler = scheduler_cls.from_config(pipe.scheduler.config, use_karras_sigmas=True)
    if hasattr(pipe, "enable_attention_slicing"):
        pipe.enable_attention_slicing()
    if ip_adapter_image is not None:
        if not hasattr(pipe, "load_ip_adapter") or not hasattr(pipe, "set_ip_adapter_scale"):
            raise MissingDependencyError("Diffusers pipeline must support IP-Adapter for B3/B4 style-reference branches")
        pipe.load_ip_adapter(
            config.diffusion.style_reference_adapter_model,
            subfolder=config.diffusion.style_reference_adapter_subfolder,
            weight_name=config.diffusion.style_reference_adapter_weight_name,
        )
        pipe.set_ip_adapter_scale(float(config.diffusion.style_reference_adapter_scale))
    rasters: list[tuple[str, Path]] = []
    for seed in config.diffusion.seeds:
        generator = torch.Generator(device="cuda").manual_seed(int(seed))
        kwargs = {}
        if ip_adapter_image is not None:
            kwargs["ip_adapter_image"] = ip_adapter_image
        result = pipe(
            prompt=prompt,
            negative_prompt=negative_prompt,
            image=init_image,
            control_image=control_images,
            num_inference_steps=int(config.diffusion.steps),
            guidance_scale=float(config.diffusion.guidance_scale),
            controlnet_conditioning_scale=conditioning_scales,
            control_guidance_end=[float(config.diffusion.control_guidance_end)] * len(control_images),
            strength=float(config.diffusion.strength if strength is None else strength),
            generator=generator,
            **kwargs,
        )
        out = candidates_dir / f"{label}_controlnet_seed{seed}.png"
        result.images[0].save(out)
        rasters.append((f"{label}_seed{seed}", out))
    del pipe
    del controlnet_arg
    del controlnets
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    return rasters


def _multi_controlnet(diffusers, controlnets: list[object]):
    multi = getattr(diffusers, "MultiControlNetModel", None)
    if multi is None:
        try:
            from diffusers.pipelines.controlnet.multicontrolnet import MultiControlNetModel

            multi = MultiControlNetModel
        except Exception:
            multi = None
    return multi(controlnets) if multi is not None else controlnets
