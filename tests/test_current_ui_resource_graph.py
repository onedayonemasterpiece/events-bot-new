import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest


REPO = Path(__file__).resolve().parents[1]
DECODER = REPO / "scripts/current_ui_resource_graph/decode.mjs"
REQUIRED = {
    "manifest.json",
    "summary.md",
    "source-components.jsonl",
    "observed-ui-families.jsonl",
    "runtime-observations.jsonl",
    "page-families.jsonl",
    "desktop-mobile-analysis.jsonl",
    "style-observations.jsonl",
    "fragmentation-report.jsonl",
    "candidate-component-graph.jsonl",
    "unresolved-questions.md",
    "coverage-report.md",
    "screenshots-index.jsonl",
}


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _tree_hash(root: Path) -> str:
    payload = "".join(
        f"{path.relative_to(root).as_posix()}\0{_sha(path.read_bytes())}\n"
        for path in sorted(item for item in root.rglob("*") if item.is_file())
    )
    return _sha(payload.encode())


def _repo_source_hash() -> str:
    root = REPO / "site/src"
    return _tree_hash(root)


def _fixture(tmp_path: Path):
    source = tmp_path / "fixture-src"
    pages = source / "pages"
    components = source / "components"
    runtime = tmp_path / "runtime"
    (pages / "vyhodnye").mkdir(parents=True)
    components.mkdir(parents=True)
    (runtime / "vyhodnye/date-2026-08-08").mkdir(parents=True)
    (pages / "index.astro").write_text(
        "---\nimport HomeHeroTalk from '../components/HomeHeroTalk.astro';\n---\n"
        "<main data-home-hero-talk><HomeHeroTalk /></main>\n",
        encoding="utf-8",
    )
    (pages / "vyhodnye/date-[date].astro").write_text(
        "---\nconst { date } = Astro.props;\n---\n"
        "<main><ul><li><button>{date}</button></li></ul></main>\n"
        "<style>@media (max-width: 600px) { button { color: #123456; } }</style>\n",
        encoding="utf-8",
    )
    (components / "HomeHeroTalk.astro").write_text(
        '<section class="home-hero-talk"><slot /></section>\n', encoding="utf-8"
    )
    html = {
        "index.html": b"<!doctype html><main data-home-hero-talk><section class='home-hero-talk'>Hi</section></main>",
        "vyhodnye/date-2026-08-08/index.html": b"<!doctype html><main><ul><li><button>Open event</button></li></ul></main>",
    }
    files = []
    for key, data in html.items():
        path = runtime / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        files.append({"key": key, "sha256": _sha(data), "size": len(data)})
    manifest = {
        "counts": {"file_count": len(files), "page_count": len(files)},
        "files": files,
    }
    manifest_path = tmp_path / "runtime-manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return source, runtime, manifest_path


def _run(source: Path, runtime: Path, manifest: Path, output: Path, *, env=None):
    command = [
        "node",
        str(DECODER),
        "--source-root",
        str(source),
        "--site-root",
        str(REPO / "site"),
        "--source-sha",
        "1" * 40,
        "--source-tree-hash",
        _tree_hash(source),
        "--runtime-root",
        str(runtime),
        "--runtime-manifest",
        str(manifest),
        "--verify-production-identity",
        "false",
        "--snapshot-id",
        "snapshot-test",
        "--snapshot-time",
        "2026-08-08T12:48:42Z",
        "--output",
        str(output),
    ]
    return subprocess.run(command, cwd=REPO, env=env, text=True, capture_output=True)


@pytest.fixture(scope="module")
def decoded(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("current-ui-graph")
    source, runtime, manifest = _fixture(tmp_path)
    before = _repo_source_hash()
    output_a = tmp_path / "a"
    output_b = tmp_path / "b"
    secret = "https://candidate.invalid/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/"
    env = {**os.environ, "CURRENT_UI_GRAPH_CANDIDATE_BASE_URL": secret}
    first = _run(source, runtime, manifest, output_a, env=env)
    second = _run(source, runtime, manifest, output_b, env=env)
    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert _repo_source_hash() == before
    return output_a, output_b, secret


def test_deterministic_byte_identical_rerun(decoded):
    first, second, _ = decoded
    names = REQUIRED | {"receipt.json"}
    assert {name: (first / name).read_bytes() for name in names} == {
        name: (second / name).read_bytes() for name in names
    }


def test_required_output_set_and_nonempty_jsonl(decoded):
    output, _, _ = decoded
    assert REQUIRED.issubset({path.name for path in output.iterdir()})
    for name in REQUIRED:
        assert (output / name).stat().st_size > 0
        if name.endswith(".jsonl"):
            assert all(json.loads(line) for line in (output / name).read_text().splitlines())


def test_mixed_dynamic_route_is_not_lost(decoded):
    output, _, _ = decoded
    families = [json.loads(line) for line in (output / "page-families.jsonl").read_text().splitlines()]
    templates = {template for family in families for template in family["source_templates"]}
    assert "/vyhodnye/date-:date/" in templates


def test_not_merged_and_unresolved_are_invariants(decoded):
    output, _, _ = decoded
    for name in ("fragmentation-report.jsonl", "candidate-component-graph.jsonl"):
        rows = [json.loads(line) for line in (output / name).read_text().splitlines()]
        assert rows
        assert {row["decision"] for row in rows} == {"NOT_MERGED"}
        assert {row["recommendation"] for row in rows} == {"unresolved"}


def test_secret_is_redacted_and_source_is_not_mutated(decoded):
    output, _, secret = decoded
    token = secret.split("/")[-2]
    corpus = b"\n".join(path.read_bytes() for path in output.iterdir() if path.is_file())
    assert secret.encode() not in corpus
    assert token.encode() not in corpus


def test_named_coverage_surfaces_cannot_silently_disappear(decoded):
    output, _, _ = decoded
    report = (output / "coverage-report.md").read_text()
    for label in (
        "Exhibitions",
        "For Me / personal feed",
        "Interest Clubs",
        "Hero-talk",
        "Hero-talk page-end",
    ):
        assert f"| {label} |" in report
    assert "| Hero-talk | FOUND |" in report
    assert "| Hero-talk page-end | MISSING |" in report
    assert "onboarding page_end slot is not Hero-talk evidence" in report


def test_partial_receipt_survives_failure(tmp_path):
    source, runtime, manifest = _fixture(tmp_path)
    payload = json.loads(manifest.read_text())
    payload["files"][0]["sha256"] = "0" * 64
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    output = tmp_path / "failed"
    secret = "https://candidate.invalid/_review/BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB/"
    result = _run(source, runtime, manifest, output, env={**os.environ, "CURRENT_UI_GRAPH_CANDIDATE_BASE_URL": secret})
    assert result.returncode != 0
    receipt = json.loads((output / "receipt.json").read_text())
    assert receipt["status"] == "failed"
    assert secret not in result.stderr
    assert secret not in json.dumps(receipt)
