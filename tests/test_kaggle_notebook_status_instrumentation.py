import json

from video_announce.kaggle_client import (
    KERNELS_ROOT_PATH,
    _copy_kernel_tree,
    _copy_status_client_to_kernel,
    _instrument_notebook_kernel,
    _instrument_script_kernel,
)


def _write_minimal_notebook(path):
    path.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "cell_type": "code",
                        "execution_count": None,
                        "metadata": {},
                        "outputs": [],
                        "source": ["print('hello')\n"],
                    }
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            }
        ),
        encoding="utf-8",
    )


def test_notebook_status_instrumentation_is_idempotent(tmp_path):
    nb_path = tmp_path / "kernel.ipynb"
    _write_minimal_notebook(nb_path)
    meta = {"code_file": "kernel.ipynb", "title": "Test Kernel"}

    _instrument_notebook_kernel(tmp_path, meta)
    _instrument_notebook_kernel(tmp_path, meta)

    notebook = json.loads(nb_path.read_text(encoding="utf-8"))
    tagged = [
        cell
        for cell in notebook["cells"]
        if cell.get("metadata", {}).get("events_bot_kaggle_status")
    ]
    assert len(tagged) == 3

    bootstrap = "".join(tagged[0]["source"])
    terminal = "".join(tagged[-1]["source"])
    assert "kernel_started" in bootstrap
    assert "acquire_resource" in bootstrap
    assert "release_resource" in bootstrap
    assert "report_written" in terminal


def test_script_status_instrumentation_wraps_plain_script(tmp_path):
    script = tmp_path / "kernel.py"
    script.write_text("print('plain script')\n", encoding="utf-8")
    meta = {"code_file": "kernel.py", "title": "Script Kernel"}

    _instrument_script_kernel(tmp_path, meta)

    wrapper = script.read_text(encoding="utf-8")
    original = tmp_path / "_events_bot_original_kernel.py"
    assert original.exists()
    assert "kernel_started" in wrapper
    assert "report_written" in wrapper
    assert "runpy.run_path" in wrapper


def test_script_status_instrumentation_skips_status_aware_script(tmp_path):
    script = tmp_path / "kernel.py"
    script.write_text("from kaggle_status_client import load_status_client\n", encoding="utf-8")
    meta = {"code_file": "kernel.py", "title": "Script Kernel"}

    _instrument_script_kernel(tmp_path, meta)

    assert not (tmp_path / "_events_bot_original_kernel.py").exists()


def test_real_notebook_kernels_are_status_instrumentable(tmp_path):
    for folder_name in (
        "CherryFlash",
        "CrumpleVideo",
        "VideoAfisha",
        "KoenigsbergStories",
        "Preview3D",
        "ParseTheatres",
    ):
        src = KERNELS_ROOT_PATH / folder_name
        dst = tmp_path / folder_name
        _copy_kernel_tree(src, dst)
        _copy_status_client_to_kernel(dst)
        meta = json.loads((dst / "kernel-metadata.json").read_text(encoding="utf-8"))

        _instrument_notebook_kernel(dst, meta)

        notebook = json.loads((dst / meta["code_file"]).read_text(encoding="utf-8"))
        tagged = [
            cell
            for cell in notebook["cells"]
            if cell.get("metadata", {}).get("events_bot_kaggle_status")
        ]
        assert (dst / "kaggle_status_client.py").exists(), folder_name
        assert len(tagged) >= 3, folder_name
        assert "kernel_started" in "".join(tagged[0]["source"])
        assert "report_written" in "".join(tagged[-1]["source"])
        source = json.dumps(notebook, ensure_ascii=False)
        assert "_kaggle_business_progress" in source, folder_name
        assert "progress_label" in source, folder_name


def test_real_script_kernels_are_status_aware():
    for relative_path in (
        "TelegramMonitor/telegram_monitor.py",
        "GuideExcursionsMonitor/guide_excursions_monitor.py",
        "ParsePhilharmonia/philharmonia_parser.py",
        "ParseQtickets/parse_qtickets.py",
        "ParsePyramida/parse_pyramida.py",
        "ParseDomIskusstv/parse_dom_iskusstv.py",
        "UniversalFestivalParser/universal_festival_parser.py",
    ):
        source = (KERNELS_ROOT_PATH / relative_path).read_text(encoding="utf-8")
        assert "load_status_client" in source, relative_path
        assert "kernel_started" in source, relative_path
        assert "report_written" in source, relative_path
        assert "progress_label" in source, relative_path


def test_crumple_publish_only_kernel_is_self_contained_not_wrapped(tmp_path):
    src = KERNELS_ROOT_PATH / "CrumpleStoryPublishOnly"
    dst = tmp_path / "CrumpleStoryPublishOnly"
    _copy_kernel_tree(src, dst)
    _copy_status_client_to_kernel(dst)
    meta = json.loads((dst / "kernel-metadata.json").read_text(encoding="utf-8"))

    _instrument_script_kernel(dst, meta)

    assert not (dst / "_events_bot_original_publish_only.py").exists()
    assert "Publish-only story publish completed" in (dst / "publish_only.py").read_text(encoding="utf-8")
