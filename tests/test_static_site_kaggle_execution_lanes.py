from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_static_site_kernel_ref_supports_only_production_and_review_lanes() -> None:
    from scripts import run_static_site_builder_kaggle as runner

    assert runner.resolve_kernel_ref("", env_user="zigomaro") == (
        "zigomaro/kenigevents-static-site-builder",
        "production",
    )
    assert runner.resolve_kernel_ref(
        "zigomaro/kenigevents-static-site-builder-review-preview",
        env_user="zigomaro",
    ) == (
        "zigomaro/kenigevents-static-site-builder-review-preview",
        "review-preview",
    )
    with pytest.raises(ValueError, match="owner must match"):
        runner.resolve_kernel_ref(
            "another-owner/kenigevents-static-site-builder-review-preview",
            env_user="zigomaro",
        )
    with pytest.raises(ValueError, match="supported static-site slug"):
        runner.resolve_kernel_ref(
            "zigomaro/unrelated-kernel",
            env_user="zigomaro",
        )


def test_review_lane_has_isolated_lock_scratch_and_resource_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import run_static_site_builder_kaggle as runner

    monkeypatch.setattr(runner, "ARTIFACT_ROOT", tmp_path)
    monkeypatch.setattr(runner, "LOCK_PATH", tmp_path / "static-site-kaggle.lock")
    production_scratch = tmp_path / "static-site-kaggle-production123"
    review_scratch = tmp_path / "static-site-kaggle-review-preview-review123"
    production_scratch.mkdir()
    review_scratch.mkdir()

    assert runner.execution_lane_lock_path("production").name == "static-site-kaggle.lock"
    assert (
        runner.execution_lane_lock_path("review-preview").name
        == "static-site-kaggle-review-preview.lock"
    )
    assert runner.execution_lane_resource_lease("production") == "static_site:builder"
    assert (
        runner.execution_lane_resource_lease("review-preview")
        == "static_site:builder:review-preview"
    )

    review_cleanup = runner.prune_abandoned_static_site_scratch(
        tmp_path, execution_lane="review-preview"
    )
    assert review_cleanup["removed_directories"] == [review_scratch.name]
    assert production_scratch.exists()


def test_staged_kernel_metadata_binds_selected_execution_ref_and_slug_title(
    tmp_path: Path,
) -> None:
    from scripts import run_static_site_builder_kaggle as runner

    staged = tmp_path / "kernel"
    staged.mkdir()
    original = {
        "id": "eventsbot/kenigevents-static-site-builder",
        "title": "kenigevents static site builder",
        "code_file": "static_site_builder.py",
        "language": "python",
        "dataset_sources": [],
    }
    metadata_path = staged / "kernel-metadata.json"
    metadata_path.write_text(json.dumps(original), encoding="utf-8")

    runner.rewrite_staged_kernel_metadata_id(
        staged,
        "zigomaro/kenigevents-static-site-builder-review-preview",
    )

    rewritten = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert rewritten == {
        **original,
        "id": "zigomaro/kenigevents-static-site-builder-review-preview",
        "title": "kenigevents-static-site-builder-review-preview",
    }


def test_main_binds_one_selected_ref_to_all_kaggle_lifecycle_operations() -> None:
    """A structural guard against reintroducing the historical fixed slug."""

    source = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_static_site_builder_kaggle.py"
    ).read_text(encoding="utf-8")
    main = source[source.index("def main() -> int:") :]

    assert "kernel_ref = args.kernel_ref" in main
    assert "review-preview --kernel-ref is restricted to --profile preview" in main
    assert "adopt_existing_kernel_output(args, client, kernel_ref)" in main
    assert "kernel_ref=kernel_ref," in main
    assert "client.push_kernel(kernel_path=staging, dataset_sources=dataset_sources)" in main
    assert "wait_kernel_dataset_sources(client, kernel_ref, dataset_sources)" in main
    assert "client.get_kernel_status(kernel_ref)" in main
    assert "client.download_kernel_output(kernel_ref, path=out_dir, force=True)" in main
