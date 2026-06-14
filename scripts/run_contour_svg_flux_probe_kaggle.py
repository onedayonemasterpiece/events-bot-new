#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, logging, shutil, tempfile, time, uuid
from pathlib import Path
try:
    from run_contour_svg_kaggle_sample import DEFAULT_ENV_FILE, PROJECT_ROOT, apply_env_file, create_or_replace_dataset, delete_dataset, download_kernel_output, get_kaggle_api, kernel_status, poll_kernel, compact_unique_slug, project_relative, require_kaggle_username, slugify, wait_dataset_ready
except ModuleNotFoundError:
    from scripts.run_contour_svg_kaggle_sample import DEFAULT_ENV_FILE, PROJECT_ROOT, apply_env_file, create_or_replace_dataset, delete_dataset, download_kernel_output, get_kaggle_api, kernel_status, poll_kernel, compact_unique_slug, project_relative, require_kaggle_username, slugify, wait_dataset_ready

logger=logging.getLogger('contour_svg_flux_probe_kaggle')
DEFAULT_KERNEL_PATH=PROJECT_ROOT/'kaggle'/'ContourSvgFluxProbe'
DEFAULT_OUTPUT_ROOT=PROJECT_ROOT/'artifacts'/'codex'/'contour-svg-flux-probe-kaggle'
PAYLOAD_PREFIX='csv-flux-payload'
DEFAULT_SOURCE_DIR='docs/features/countur_svg_generator/to_do'

def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists(): shutil.rmtree(dst)
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns('__pycache__','*.pyc','.pytest_cache'))

def source_images(source_dir: str, raw: str|None) -> list[Path]:
    if raw: paths=[Path(x.strip()) for x in raw.split(',') if x.strip()]
    else:
        root=PROJECT_ROOT/source_dir if not Path(source_dir).is_absolute() else Path(source_dir)
        paths=sorted([*root.glob('*.jpg'),*root.glob('*.jpeg'),*root.glob('*.png'),*root.glob('*.webp')])
    out=[]
    for p in paths:
        q=p if p.is_absolute() else PROJECT_ROOT/p
        if not q.exists(): raise RuntimeError(f'source image does not exist: {q}')
        out.append(q)
    if not out: raise RuntimeError('no source images found')
    return out

def write_payload(path: Path, *, run_id: str, images: list[Path], backend: str, model_id: str, variants: str, steps: int, output_size: str, guidance_scale: float, direct_strength: float, guide_strength: float, minimal_strength: float, seed: int) -> None:
    repo=path/'repo_bundle'
    copy_tree(PROJECT_ROOT/'contour_svg', repo/'contour_svg')
    copy_tree(PROJECT_ROOT/'docs'/'features'/'countur_svg_generator', repo/'docs'/'features'/'countur_svg_generator')
    (repo/'kaggle').mkdir(parents=True, exist_ok=True)
    shutil.copy2(PROJECT_ROOT/'kaggle'/'kaggle_status_client.py', repo/'kaggle'/'kaggle_status_client.py')
    config={'run_id':run_id,'source_images':[project_relative(p) for p in images],'backend':backend,'model_id':model_id,'variants':[x.strip() for x in variants.split(',') if x.strip()],'steps':steps,'output_size':output_size,'guidance_scale':guidance_scale,'direct_strength':direct_strength,'guide_strength':guide_strength,'minimal_strength':minimal_strength,'seed':seed}
    (path/'flux_probe_config.json').write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding='utf-8')
    (path/'kaggle_run.json').write_text(json.dumps({'run_id':run_id,'kind':'contour_svg_flux_probe','notebook':'ContourSvgFluxProbe','resource_leases':[]}, ensure_ascii=False, indent=2), encoding='utf-8')

def push_kernel(api, *, kernel_slug: str, dataset_sources: list[str], accelerator: str, session_timeout_seconds: int) -> str:
    with tempfile.TemporaryDirectory() as td:
        tmp=Path(td)/'ContourSvgFluxProbe'; shutil.copytree(DEFAULT_KERNEL_PATH,tmp)
        shutil.copy2(PROJECT_ROOT/'kaggle'/'kaggle_status_client.py', tmp/'kaggle_status_client.py')
        meta_path=tmp/'kernel-metadata.json'; meta=json.loads(meta_path.read_text())
        username=require_kaggle_username(); meta.update({'id':f'{username}/{kernel_slug}','slug':kernel_slug,'title':'Contour SVG Flux Probe','dataset_sources':dataset_sources,'enable_gpu':True,'enable_internet':True,'machine_shape':accelerator})
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
        api.kernels_push(str(tmp), timeout=str(session_timeout_seconds), acc=accelerator)
    return f'{require_kaggle_username()}/{kernel_slug}'

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--timeout-minutes', type=int, default=120)
    ap.add_argument('--poll-interval-seconds', type=int, default=45)
    ap.add_argument('--output-root', type=Path, default=DEFAULT_OUTPUT_ROOT)
    ap.add_argument('--accelerator', default='NvidiaTeslaT4')
    ap.add_argument('--kernel-slug', default='contour-svg-flux-probe')
    ap.add_argument('--run-label', default='contour-svg-flux-probe')
    ap.add_argument('--source-dir', default=DEFAULT_SOURCE_DIR)
    ap.add_argument('--source-images')
    ap.add_argument('--backend', default='flux_img2img_bnb4', choices=['flux_img2img','flux_img2img_bnb4','schnell_img2img','flux_control_canny'])
    ap.add_argument('--model-id', default='ModelsLab/flux.1-dev')
    ap.add_argument('--variants', default='direct_photo,edge_mask,CG3_fused_balanced,CG4_minimal_clean')
    ap.add_argument('--steps', type=int, default=4)
    ap.add_argument('--output-size', default='512,512')
    ap.add_argument('--guidance-scale', type=float, default=3.5)
    ap.add_argument('--direct-strength', type=float, default=0.92)
    ap.add_argument('--guide-strength', type=float, default=0.68)
    ap.add_argument('--minimal-strength', type=float, default=0.76)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--session-timeout-seconds', type=int, default=7200)
    ap.add_argument('--keep-datasets', action='store_true')
    args=ap.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    apply_env_file(DEFAULT_ENV_FILE)
    images=source_images(args.source_dir,args.source_images)
    run_id=f"{slugify(args.run_label, max_len=32)}-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    run_slug=compact_unique_slug(run_id, max_len=40)
    username=require_kaggle_username(); api=get_kaggle_api()
    payload_slug=compact_unique_slug(f'{PAYLOAD_PREFIX}-{run_slug}', max_len=50)
    payload_ref=create_or_replace_dataset(api, username, payload_slug, f'CSV FLUX payload {run_slug[:24]}', lambda p: write_payload(p, run_id=run_id, images=images, backend=args.backend, model_id=args.model_id, variants=args.variants, steps=args.steps, output_size=args.output_size, guidance_scale=args.guidance_scale, direct_strength=args.direct_strength, guide_strength=args.guide_strength, minimal_strength=args.minimal_strength, seed=args.seed))
    wait_dataset_ready(api, payload_ref, timeout_seconds=180, expected_files=['kaggle_run.json','flux_probe_config.json'])
    kernel_ref=push_kernel(api, kernel_slug=slugify(args.kernel_slug, max_len=48), dataset_sources=[payload_ref], accelerator=args.accelerator, session_timeout_seconds=args.session_timeout_seconds)
    status=poll_kernel(api,kernel_ref,timeout_minutes=args.timeout_minutes,poll_interval_seconds=args.poll_interval_seconds)
    out=args.output_root/run_slug; out.mkdir(parents=True, exist_ok=True)
    downloaded=download_kernel_output(api,kernel_ref,out)
    summary={'run_id':run_id,'kernel_ref':kernel_ref,'kernel_slug':slugify(args.kernel_slug,max_len=48),'source_images':[str(p) for p in images],'backend':args.backend,'model_id':args.model_id,'variants':args.variants,'steps':args.steps,'kernel_status':status,'accelerator':args.accelerator,'dataset_sources':[payload_ref],'download_dir':str(out),'downloaded':downloaded,'latest_status':kernel_status(api,kernel_ref)}
    (out/'local_run_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if not args.keep_datasets:
        try: delete_dataset(api,payload_ref)
        except Exception: logger.info('dataset cleanup failed dataset=%s', payload_ref, exc_info=True)
if __name__=='__main__': main()
