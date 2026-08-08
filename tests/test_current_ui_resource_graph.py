import hashlib
import json
import os
from pathlib import Path
import re
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
    "event-presentation-formats.jsonl",
    "desktop-mobile-analysis.jsonl",
    "style-observations.jsonl",
    "fragmentation-report.jsonl",
    "candidate-component-graph.jsonl",
    "unresolved-questions.md",
    "coverage-report.md",
    "screenshots-index.jsonl",
}
V1_REQUIRED = {
    "manifest.json",
    "receipt.json",
    "summary.md",
    "artifact-index.json",
    "source-files.jsonl",
    "source-bindings.jsonl",
    "component-families.jsonl",
    "composition-edges.jsonl",
    "consumers.jsonl",
    "route-families.jsonl",
    "page-state-signatures.jsonl",
    "specimen-plan.jsonl",
    "specimen-observations.jsonl",
    "page-verification.jsonl",
    "mismatches.jsonl",
    "unresolved.jsonl",
    "penpot-materialization-candidates.json",
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
    root_source = tmp_path / "fixture-root-src"
    pages = source / "pages"
    components = source / "components"
    styles = source / "styles"
    runtime = tmp_path / "runtime"
    root_runtime = tmp_path / "root-index.html"
    (pages / "vyhodnye").mkdir(parents=True)
    (pages / "segodnya").mkdir(parents=True)
    (pages / "populyarnoe").mkdir(parents=True)
    (pages / "sobytiya").mkdir(parents=True)
    components.mkdir(parents=True)
    styles.mkdir(parents=True)
    (root_source / "pages").mkdir(parents=True)
    (root_source / "components").mkdir(parents=True)
    (runtime / "vyhodnye/date-2026-08-08").mkdir(parents=True)
    (pages / "index.astro").write_text(
        "---\nimport HomeHeroTalk from '../components/HomeHeroTalk.astro';\n"
        "import SiteHeader from '../components/SiteHeader.astro';\n---\n"
        "<SiteHeader /><main data-home-hero-talk><HomeHeroTalk /></main>\n",
        encoding="utf-8",
    )
    (pages / "robots.txt.ts").write_text(
        "export const GET = () => new Response('User-agent: *');\n",
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
    (components / "EventCard.astro").write_text(
        '<article class="event-card"><slot /></article>\n', encoding="utf-8"
    )
    (components / "DesktopEventActionPanel.astro").write_text(
        "---\nenum ActionMode { Inline = 'inline', Stacked = 'stacked' }\n"
        "interface Props { family?: 'split' | 'editorial'; allow?: boolean; mode?: ActionMode; }\n"
        "const { family = 'split', allow = true } = Astro.props;\n"
        "const layout = family === 'split' ? 'inline' : 'stacked';\n---\n"
        '<section data-desktop-action-panel data-action-family={family} data-action-layout={family === "split" ? "inline" : "stacked"}><button>Tickets</button></section>\n',
        encoding="utf-8",
    )
    (components / "ClubCatalogNavigation.mjs").write_text(
        "export const keys = ['ArrowLeft', 'ArrowRight'];\n", encoding="utf-8"
    )
    (components / "ClubCatalogKeyboard.astro").write_text(
        '<script>import { keys } from "./ClubCatalogNavigation.mjs"; if (keys.length) window.__keys = keys;</script>\n',
        encoding="utf-8",
    )
    (components / "DesktopEventPage.astro").write_text(
        "---\nimport DesktopEventActionPanel from './DesktopEventActionPanel.astro';\n"
        "const { candidate } = Astro.props;\n---\n"
        '<main data-desktop-family={candidate}><figure data-media-frame></figure><DesktopEventActionPanel family={candidate} /></main>\n',
        encoding="utf-8",
    )
    for component_root in (components, root_source / "components"):
        (component_root / "SiteHeader.astro").write_text(
            '<header class="site-header">Header</header>\n', encoding="utf-8"
        )
    for page, body in {
        "segodnya/index.astro": "<main><EventCard /></main>",
        "populyarnoe/index.astro": "<main><EventCard /></main>",
        "sobytiya/[slug].astro": "<main class='event-detail'><DesktopEventPage candidate='split' /><EventCard /></main>",
    }.items():
        (pages / page).write_text(
            "---\nimport EventCard from '../../components/EventCard.astro';\n"
            + ("import DesktopEventPage from '../../components/DesktopEventPage.astro';\n" if page.startswith("sobytiya/") else "")
            + "---\n"
            + body
            + "\n",
            encoding="utf-8",
        )
    (styles / "global.css").write_text(
        ".event-card { padding: 8px; }\n"
        ".event-card { padding: 12px; }\n"
        ".primary-button { border-radius: 4px; }\n",
        encoding="utf-8",
    )
    (root_source / "pages/index.astro").write_text(
        "---\nimport SiteHeader from '../components/SiteHeader.astro';\n---\n"
        "<SiteHeader /><main><p>Public root without a Hero-talk marker.</p></main>\n",
        encoding="utf-8",
    )
    html = {
        "index.html": b"<!doctype html><main data-home-hero-talk><section class='home-hero-talk'>Hi</section></main>",
        "segodnya/index.html": b"<!doctype html><main><article class='event-card'>Today</article></main>",
        "populyarnoe/index.html": b"<!doctype html><main><article class='event-card'>Popular</article></main>",
        "sobytiya/example/index.html": b"<!doctype html><main class='event-detail' data-desktop-clean-event data-desktop-family='split' data-presentation-reason='split-portrait-or-square-visual'><figure data-media-frame><img src='poster.jpg'><nav data-split-media-rail><button><img src='photo-thumb.jpg'></button></nav></figure><section data-desktop-action-panel data-action-family='split' data-action-layout='inline'><button>Tickets</button></section><aside data-event-transport-schedule data-event-city='svetlogorsk' data-outbound-count='2' data-return-count='2' data-event-end-basis='explicit'></aside><div class='event-token-layout' data-medallion-layout='desktop-slots' data-top-slot-enabled='true'><section data-medallion-slot='top' data-identity-resolution='resolved'><span data-medallion-role='main' data-medallion-category='venue_brand'></span></section></div></main>",
        "sobytiya/editorial/index.html": b"<!doctype html><main class='event-detail' data-desktop-clean-event data-desktop-family='editorial' data-presentation-reason='editorial-primary-qualified-landscape'><figure data-media-frame><img src='hero.jpg'></figure><nav data-hero-rail><button><img src='photo-thumb.jpg'></button></nav><section data-desktop-action-panel data-action-family='editorial' data-action-layout='stacked'><button>Tickets</button></section><button data-editorial-ocr-companion><img src='poster.jpg'></button><div><button data-companion-preview-item><img src='photo-small.jpg'></button></div></main>",
        "sobytiya/no-image/index.html": b"<!doctype html><main class='event-detail' data-desktop-clean-event data-desktop-family='split' data-presentation-reason='split-no-image-fallback' data-presentation-fallback='venue-identity'><figure data-media-frame><div class='event-fallback-art'>Fallback</div></figure><section data-desktop-action-panel data-action-family='split' data-action-layout='inline'><button>Calendar</button></section></main>",
        "vyhodnye/date-2026-08-08/index.html": b"<!doctype html><main><ul><li><button>Open event</button></li></ul></main>",
    }
    files = []
    for key, data in html.items():
        path = runtime / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        files.append({"key": key, "sha256": _sha(data), "size": len(data)})
    tree_hash = _sha(
        "".join(
            f"{item['key']}\0{item['sha256']}\0{item['size']}\n"
            for item in sorted(files, key=lambda item: item["key"])
        ).encode()
    )
    manifest = {
        "schema_version": "static_secret_candidate_manifest_v1",
        "counts": {
            "file_count": len(files),
            "html_count": len(files),
            "page_count": len(files),
            "bytes": sum(item["size"] for item in files),
        },
        "tree_sha256": tree_hash,
        "files": files,
    }
    manifest_path = tmp_path / "runtime-manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    root_runtime.write_bytes(
        b"<!doctype html><main><p>Public root without a Hero-talk marker.</p></main>"
    )
    return source, root_source, runtime, manifest_path, root_runtime


def _run(
    source: Path,
    root_source: Path,
    runtime: Path,
    manifest: Path,
    root_runtime: Path,
    output: Path,
    *,
    env=None,
    root_expected_hash=None,
):
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
        "--root-source-root",
        str(root_source),
        "--root-source-sha",
        "2" * 40,
        "--root-source-tree-hash",
        _tree_hash(root_source),
        "--runtime-root",
        str(runtime),
        "--runtime-manifest",
        str(manifest),
        "--root-runtime-file",
        str(root_runtime),
        "--root-html-sha256",
        root_expected_hash or _sha(root_runtime.read_bytes()),
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
    source, root_source, runtime, manifest, root_runtime = _fixture(tmp_path)
    before = _repo_source_hash()
    output_a = tmp_path / "a"
    output_b = tmp_path / "b"
    secret = "https://candidate.invalid/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/"
    env = {**os.environ, "CURRENT_UI_GRAPH_CANDIDATE_BASE_URL": secret}
    first = _run(source, root_source, runtime, manifest, root_runtime, output_a, env=env)
    second = _run(source, root_source, runtime, manifest, root_runtime, output_b, env=env)
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
    first_v1 = first / "catalog/component-decoder/decoder-v1-snapshot-test"
    second_v1 = second / "catalog/component-decoder/decoder-v1-snapshot-test"
    assert {
        path.relative_to(first_v1): path.read_bytes()
        for path in first_v1.rglob("*")
        if path.is_file()
    } == {
        path.relative_to(second_v1): path.read_bytes()
        for path in second_v1.rglob("*")
        if path.is_file()
    }


def test_required_output_set_and_nonempty_jsonl(decoded):
    output, _, _ = decoded
    assert REQUIRED.issubset({path.name for path in output.iterdir()})
    for name in REQUIRED:
        assert (output / name).stat().st_size > 0
        if name.endswith(".jsonl"):
            assert all(json.loads(line) for line in (output / name).read_text().splitlines())


def test_v1_compact_snapshot_is_complete_as_a_tree_and_fail_closed(decoded):
    output, _, _ = decoded
    root = output / "catalog/component-decoder/decoder-v1-snapshot-test"
    assert V1_REQUIRED.issubset({path.name for path in root.iterdir()})
    manifest = json.loads((root / "manifest.json").read_text())
    receipt = json.loads((root / "receipt.json").read_text())
    assert manifest["schema_version"] == "current_ui_component_decoder_v1"
    assert manifest["go_no_go"]["status"] == "NO_GO"
    assert "controlled_specimen_evidence" in manifest["go_no_go"]["blockers"]
    assert receipt["status"] == "complete"
    assert receipt["evidence_completion"] == "partial"
    assert receipt["handoff_status"] == "NO_GO"
    assert manifest["constraints"]["normalization"] is False
    assert manifest["constraints"]["astro_css_mutation"] is False
    assert len(list((root / "candidate-contracts").glob("*.contract.json"))) == 12
    capsule_dirs = sorted(path for path in (root / "conformance-capsules").iterdir() if path.is_dir())
    assert len(capsule_dirs) == 6
    capsule_required = {
        "capsule.json", "source-facts.jsonl", "candidate-contract-ref.json",
        "specimen-observation-refs.jsonl", "real-page-verification-refs.jsonl",
        "state-token-dependency-map.json", "override-findings.jsonl",
        "mismatch-refs.jsonl", "unresolved-refs.jsonl", "evidence-index.json", "REVIEW.md",
    }
    assert all(capsule_required == {path.name for path in capsule.iterdir()} for capsule in capsule_dirs)
    components = list((root / "components").glob("*.json"))
    source_components = {
        row["path"]
        for row in map(json.loads, (output / "source-components.jsonl").read_text().splitlines())
        if row["type"] == "component"
    }
    assert len(components) == len(source_components)
    for path in components:
        row = json.loads(path.read_text())
        assert row["decision"] == "NOT_MERGED"
        assert row["recommendation"] == "unresolved"
        assert row["disposition_basis"]
        assert row["reachability_basis"]


def test_state_aware_ast_facts_and_inline_script_import_edges(decoded):
    output, _, _ = decoded
    rows = list(map(json.loads, (output / "source-components.jsonl").read_text().splitlines()))
    action = next(
        row
        for row in rows
        if row["plane"] == "latest_checked_kaggle_candidate"
        and row["name"] == "DesktopEventActionPanel"
    )
    facts = action["source_state"]
    assert facts["parser_status"] == "parsed"
    props = {item["name"]: item for item in facts["props"]}
    assert props["family"]["allowed_literals"] == ["split", "editorial"]
    assert props["family"]["default"] == {"observed": True, "value": "split"}
    assert props["allow"]["default"] == {"observed": True, "value": True}
    assert props["mode"]["allowed_literals"] == ["inline", "stacked"]
    assert facts["enums"][0]["name"] == "ActionMode"
    assert any(item["name"] == "layout" for item in facts["derived_state"])
    assert {item["name"] for item in facts["state_attributes"]} >= {
        "data-desktop-action-panel",
        "data-action-family",
        "data-action-layout",
    }

    keyboard = next(
        row
        for row in rows
        if row["plane"] == "latest_checked_kaggle_candidate"
        and row["name"] == "ClubCatalogKeyboard"
    )
    support = next(
        row
        for row in rows
        if row["plane"] == "latest_checked_kaggle_candidate"
        and row["name"] == "ClubCatalogNavigation"
    )
    assert "./ClubCatalogNavigation.mjs" in keyboard["imports"]
    assert support["id"] in keyboard["direct_dependencies"]
    assert keyboard["id"] in support["consumers"]
    assert any(
        item.get("source_scope") == "inline_script"
        and item["kind"] == "IfStatement"
        for item in keyboard["source_state"]["branches"]
    )


def test_css_sources_and_exact_component_state_signatures_are_provenanced(decoded):
    output, _, _ = decoded
    sources = [json.loads(line) for line in (output / "source-components.jsonl").read_text().splitlines()]
    stylesheet = next(row for row in sources if row["path"].endswith("styles/global.css"))
    assert stylesheet["type"] == "stylesheet"
    assert stylesheet["evidence"]["parser"] == "postcss"
    assert stylesheet["source_state"]["parser_status"] == "parsed"
    style_rows = [json.loads(line) for line in (output / "style-observations.jsonl").read_text().splitlines()]
    assert any(row.get("source_id") == stylesheet["id"] for row in style_rows)

    signatures = [json.loads(line) for line in (output / "catalog/component-decoder/decoder-v1-snapshot-test/page-state-signatures.jsonl").read_text().splitlines()]
    event = next(row for row in signatures if row["page_family"] == "page-family.event-detail" and row["component_states"].get("data-event-end-basis"))
    assert event["component_states"]["data-event-end-basis"] == {"explicit": 1}
    assert event["component_states"]["data-outbound-count"] == {"2": 1}
    assert event["component_states"]["data-medallion-layout"] == {"desktop-slots": 1}
    assert event["component_states"]["data-medallion-role"] == {"main": 1}


def test_v1_known_exceptions_are_not_synthesized(decoded):
    output, _, _ = decoded
    manifest = json.loads(
        (output / "catalog/component-decoder/decoder-v1-snapshot-test/manifest.json").read_text()
    )
    exceptions = {item["id"]: item for item in manifest["known_exceptions"]}
    assert exceptions["exception.labs-preview-special"]["classification"] == "lab-only"
    for key in (
        "exception.editorial-collections",
        "exception.legal",
        "exception.hero-talk-page-end",
    ):
        assert exceptions[key]["classification"] == "absent-as-is-future-requirement"
        assert exceptions[key]["synthesis_allowed"] is False
    assert exceptions["exception.transport-timetable-experiment"]["current_status"] == "not-a-production-variant"


def test_mixed_dynamic_route_is_not_lost(decoded):
    output, _, _ = decoded
    families = [json.loads(line) for line in (output / "page-families.jsonl").read_text().splitlines()]
    templates = {template for family in families for template in family["source_templates"]}
    assert "/vyhodnye/date-:date/" in templates


def test_distinct_page_families_and_event_mapping_are_preserved(decoded):
    output, _, _ = decoded
    families = {
        row["id"]: row
        for row in map(json.loads, (output / "page-families.jsonl").read_text().splitlines())
    }
    assert {
        "page-family.day-listing",
        "page-family.weekend-listing",
        "page-family.popular",
        "page-family.event-detail",
    }.issubset(families)
    runtime = [
        row
        for row in map(
            json.loads, (output / "runtime-observations.jsonl").read_text().splitlines()
        )
        if row["plane"] == "latest_checked_kaggle_candidate"
    ]
    event = next(row for row in runtime if row["page_family"] == "page-family.event-detail")
    assert event["source_mapping"] == "exact_route_template"
    assert event["source_page_ids"]
    assert event["component_candidates"]


def test_event_layout_cta_and_media_formats_are_first_class_resources(decoded):
    output, _, _ = decoded
    formats = {
        row["id"]: row
        for row in map(
            json.loads,
            (output / "event-presentation-formats.jsonl").read_text().splitlines(),
        )
    }
    expected = {
        "event-format.desktop.editorial-landscape",
        "event-format.desktop.split-portrait-poster",
        "event-format.desktop.split-portrait-visual",
        "event-format.desktop.no-image-fallback",
        "event-format.cta.editorial-side-stacked",
        "event-format.cta.split-inline",
        "event-format.media.primary-large-frame",
        "event-format.media.split-small-photo-rail",
        "event-format.media.editorial-small-photo-rail",
        "event-format.media.editorial-large-poster-companion",
        "event-format.media.editorial-small-companion-previews",
    }
    assert expected == set(formats)
    assert all(formats[row]["decision"] == "NOT_MERGED" for row in expected)
    assert all(formats[row]["recommendation"] == "unresolved" for row in expected)
    assert all(formats[row]["status"] == "observed" for row in expected)
    assert formats["event-format.desktop.editorial-landscape"]["desktop_family"] == "editorial"
    assert formats["event-format.desktop.split-portrait-poster"]["desktop_family"] == "split"
    assert "split-no-image-fallback" in formats["event-format.desktop.no-image-fallback"]["presentation_reasons"]
    assert formats["event-format.cta.editorial-side-stacked"]["action_layout"] == "stacked"
    assert formats["event-format.cta.split-inline"]["action_layout"] == "inline"
    assert "large" in formats["event-format.media.editorial-large-poster-companion"]["observed_structure"]
    assert "small" in formats["event-format.media.editorial-small-companion-previews"]["observed_structure"]

    runtime = list(
        map(json.loads, (output / "runtime-observations.jsonl").read_text().splitlines())
    )
    event_resources = [
        row["event_resources"]
        for row in runtime
        if row["page_family"] == "page-family.event-detail"
    ]
    assert any(row["desktop_families"].get("editorial") for row in event_resources)
    assert any(row["desktop_families"].get("split") for row in event_resources)


def test_non_astro_page_endpoints_are_not_ui_page_families(decoded):
    output, _, _ = decoded
    sources = list(
        map(json.loads, (output / "source-components.jsonl").read_text().splitlines())
    )
    endpoint = next(row for row in sources if row["path"].endswith("pages/robots.txt.ts"))
    assert endpoint["type"] == "controller_or_module"
    assert endpoint["route_template"] is None

    families = list(
        map(json.loads, (output / "page-families.jsonl").read_text().splitlines())
    )
    assert not any("robots" in family["id"] for family in families)


def test_duplicate_source_planes_do_not_create_false_fragmentation(decoded):
    output, _, _ = decoded
    families = {
        row["id"]: row
        for row in map(
            json.loads, (output / "observed-ui-families.jsonl").read_text().splitlines()
        )
    }
    headers = families["family.headers"]
    assert headers["logical_implementation_count"] == 1
    assert set(headers["implementations_by_plane"]) == {
        "latest_checked_kaggle_candidate",
        "current_root_prelaunch",
    }

    fragmentation = {
        row["family"]: row
        for row in map(
            json.loads, (output / "fragmentation-report.jsonl").read_text().splitlines()
        )
    }
    candidates = {
        row["family"]: row
        for row in map(
            json.loads,
            (output / "candidate-component-graph.jsonl").read_text().splitlines(),
        )
    }
    assert fragmentation["family.headers"]["status"] != "fragmented"
    assert candidates["family.headers"]["status"] != "fragmented"


def test_summary_separates_plane_counts_and_style_inconsistencies(decoded):
    output, _, _ = decoded
    manifest = json.loads((output / "manifest.json").read_text())
    counts = manifest["counts"]
    assert counts["candidate_routes"] == 7
    assert counts["public_root_observations"] == 1
    assert 0 < counts["style_inconsistencies"] < counts["styles"]

    summary = (output / "summary.md").read_text()
    assert "Candidate HTML routes: 7" in summary
    assert "Separate public-root observations: 1" in summary
    assert "Layouts by plane:" in summary
    assert "Source components by plane:" in summary
    assert "Event presentation resource formats: 11 (11 runtime-observed)" in summary
    assert "editorial landscape and split portrait/poster" in summary
    assert f"Style inconsistencies: {counts['style_inconsistencies']}" in summary


def test_not_merged_and_unresolved_are_invariants(decoded):
    output, _, _ = decoded
    for name in ("fragmentation-report.jsonl", "candidate-component-graph.jsonl", "event-presentation-formats.jsonl"):
        rows = [json.loads(line) for line in (output / name).read_text().splitlines()]
        assert rows
        assert {row["decision"] for row in rows} == {"NOT_MERGED"}
        assert {row["recommendation"] for row in rows} == {"unresolved"}
    assert (output / "summary.md").read_text().rstrip().endswith(
        "Complete component specimens, source-to-page reconciliation, capsule review, and immutable handoff. STOP before normalization."
    )


def test_secret_is_redacted_and_source_is_not_mutated(decoded):
    output, _, secret = decoded
    token = secret.split("/")[-2]
    corpus = b"\n".join(path.read_bytes() for path in output.rglob("*") if path.is_file())
    assert secret.encode() not in corpus
    assert token.encode() not in corpus
    assert b"outerHTML" not in corpus
    assert b"innerHTML" not in corpus


def test_v1_closed_enums_canonical_counts_and_evidence_sanitizer():
    script = """
      import { CANONICAL_DISPOSITION_COUNTS, DISPOSITIONS, REACHABILITY } from './scripts/current_ui_resource_graph/v1/classification.mjs';
      import { assertSafeComponentEvidence, sanitizeEvidenceString } from './scripts/current_ui_resource_graph/v1/evidence.mjs';
      const secret = sanitizeEvidenceString('https://candidate.invalid/_review/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/');
      let rejected = false;
      try { assertSafeComponentEvidence({ proof_label: 'exact-candidate-browser-element', url: 'https://unsafe.invalid/' }); }
      catch { rejected = true; }
      process.stdout.write(JSON.stringify({ counts: CANONICAL_DISPOSITION_COUNTS, dispositions: DISPOSITIONS, reachability: REACHABILITY, secret, rejected }));
    """
    result = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        cwd=REPO,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert sum(payload["counts"].values()) == 107
    assert payload["counts"] == {
        "production-ui": 51,
        "composition-layout": 20,
        "lab-only": 20,
        "experiment-only": 4,
        "support-data": 1,
        "nonvisual": 8,
        "dead-unreachable": 2,
        "needs-verification": 1,
    }
    assert len(payload["dispositions"]) == 8
    assert len(payload["reachability"]) == 7
    assert payload["secret"]["redacted"] is True
    assert payload["rejected"] is True


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

    runtime = [
        row
        for row in map(
            json.loads, (output / "runtime-observations.jsonl").read_text().splitlines()
        )
        if row["plane"] == "current_root_prelaunch"
    ]
    assert runtime[0]["surface_markers"] == []
    assert "hero-talk" not in runtime[0]["surface_hypotheses"]


def test_dual_source_and_runtime_planes_are_exact(decoded):
    output, _, _ = decoded
    manifest = json.loads((output / "manifest.json").read_text())
    assert set(manifest["source_pins"]) == {
        "latest_checked_kaggle_candidate",
        "current_root_prelaunch",
    }
    assert set(manifest["runtime_planes"]) == {
        "latest_checked_kaggle_candidate",
        "current_root_prelaunch",
    }
    sources = list(
        map(json.loads, (output / "source-components.jsonl").read_text().splitlines())
    )
    runtime = list(
        map(json.loads, (output / "runtime-observations.jsonl").read_text().splitlines())
    )
    assert {row["plane"] for row in sources} == {
        "latest_checked_kaggle_candidate",
        "current_root_prelaunch",
    }
    assert {row["plane"] for row in runtime} == {
        "latest_checked_kaggle_candidate",
        "current_root_prelaunch",
    }


def test_standalone_css_semantic_cohorts_and_fragmentation_channels(decoded):
    output, _, _ = decoded
    styles = list(map(json.loads, (output / "style-observations.jsonl").read_text().splitlines()))
    assert any(
        row.get("source_path", "").endswith("styles/global.css")
        and "card-listing" in row.get("semantic_cohorts", [])
        for row in styles
    )
    assert any(
        row.get("kind") == "source_semantic_cohort"
        and row.get("semantic_cohort") == "card-listing"
        and row.get("source_divergence") == "distinct_literals_observed"
        for row in styles
    )
    fragmentation = {
        row["family"]: row
        for row in map(
            json.loads, (output / "fragmentation-report.jsonl").read_text().splitlines()
        )
    }
    assert {
        "source_ast",
        "exact_runtime_source_mapping",
        "source_style",
    }.issubset(fragmentation["family.event-representations"]["evidence_channels"])


def test_manifest_rejects_unsafe_and_duplicate_keys(tmp_path):
    source, root_source, runtime, manifest, root_runtime = _fixture(tmp_path)
    for key_mode in ("unsafe", "duplicate"):
        payload = json.loads(manifest.read_text())
        if key_mode == "unsafe":
            payload["files"][0]["key"] = "../index.html"
        else:
            payload["files"][1]["key"] = payload["files"][0]["key"]
        manifest.write_text(json.dumps(payload), encoding="utf-8")
        result = _run(
            source,
            root_source,
            runtime,
            manifest,
            root_runtime,
            tmp_path / f"failed-{key_mode}",
        )
        assert result.returncode != 0
        assert "unsafe relative key" in result.stderr or "duplicate key" in result.stderr
        # Restore an exact fixture before the next mutation.
        source, root_source, runtime, manifest, root_runtime = _fixture(
            tmp_path / f"restore-{key_mode}"
        )


def test_public_root_html_hash_is_verified(tmp_path):
    source, root_source, runtime, manifest, root_runtime = _fixture(tmp_path)
    result = _run(
        source,
        root_source,
        runtime,
        manifest,
        root_runtime,
        tmp_path / "failed-root-hash",
        root_expected_hash="f" * 64,
    )
    assert result.returncode != 0
    assert "Public root HTML SHA-256 mismatch" in result.stderr
    receipt = json.loads((tmp_path / "failed-root-hash/receipt.json").read_text())
    assert receipt["status"] == "failed"


def test_family_specific_computed_viewport_evidence():
    script = """
      import { computedStyleObservations, desktopMobile } from './scripts/current_ui_resource_graph/graph-lib.mjs';
      const base = {
        page_family: 'page-family.event-detail',
        route_hash: 'a'.repeat(64),
        structure: 'same-dom',
        ui_families: ['family.event-representations'],
      };
      const cohort = (display, fontSize) => ({
        order: 0, visible: display !== 'none', display,
        color: 'rgb(0, 0, 0)', background_color: 'rgba(0, 0, 0, 0)',
        font_family: 'sans-serif', font_size: fontSize, padding: '0px',
        margin: '0px', gap: 'normal', border_radius: '0px', object_fit: 'fill',
      });
      const evidence = [
        { ...base, viewport: { width: 390, height: 844 }, computed: {
          regions: { header: { display: 'block' } }, cohorts: { typography: [cohort('block', '16px')] }
        } },
        { ...base, viewport: { width: 1728, height: 900 }, computed: {
          regions: { header: { display: 'flex' } }, cohorts: { typography: [cohort('block', '20px')] }
        } },
      ];
      const pages = [{ id: 'page-family.event-detail' }];
      const families = [{ id: 'family.event-representations', implementations: ['source.one'] }];
      process.stdout.write(JSON.stringify({
        styles: computedStyleObservations(evidence),
        desktop: desktopMobile(pages, families, evidence),
      }));
    """
    result = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        cwd=REPO,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    scopes = {(row["observation_scope"], row["family"]) for row in payload["styles"]}
    assert ("page_family", "page-family.event-detail") in scopes
    assert ("ui_family", "family.event-representations") in scopes
    page_record = next(row for row in payload["desktop"] if row["scope"] == "page_family")
    ui_record = next(row for row in payload["desktop"] if row["scope"] == "ui_family")
    assert page_record["relation"] == "divergent_structure_observed"
    assert ui_record["ui_family"] == "family.event-representations"
    assert ui_record["relation"] == "divergent_structure_observed"
    assert ui_record["interpretation"] == "independent_observations_not_responsive_variants"


def test_screenshot_selection_is_representative_first_and_outlier_fair():
    script = """
      import { selectScreenshotPages } from './scripts/current_ui_resource_graph/graph-lib.mjs';
      const row = (key, route, structure) => ({ file: { key }, observation: {
        route_hash: route.padEnd(64, route), structure_hash: structure,
      } });
      const byFamily = new Map([
        ['page-family.home', [row('rare.html', 'a', 'rare'), row('common-1.html', 'b', 'common'), row('common-2.html', 'c', 'common')]],
        ['page-family.event-detail', [row('event.html', 'd', 'event')]],
        ['page-family.day-listing', [row('day.html', 'e', 'day')]],
      ]);
      const result = selectScreenshotPages(byFamily, 4);
      process.stdout.write(JSON.stringify(result.selected.map((item) => ({
        family: item.pageFamily, selection: item.selection, key: item.file.key,
      }))));
    """
    result = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        cwd=REPO,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    selected = json.loads(result.stdout)
    assert [row["selection"] for row in selected[:3]] == [
        "family_representative",
        "family_representative",
        "family_representative",
    ]
    assert {row["family"] for row in selected[:3]} == {
        "page-family.home",
        "page-family.event-detail",
        "page-family.day-listing",
    }
    assert selected[0]["key"] == "common-1.html"
    assert selected[3]["selection"] == "structural_outlier"


def test_screenshot_selection_guarantees_both_desktop_event_formats_after_page_representatives():
    script = """
      import { selectScreenshotPages } from './scripts/current_ui_resource_graph/graph-lib.mjs';
      const row = (key, route, structure, desktopFamily, presentationReason = null, markers = {}) => ({ file: { key }, observation: {
        route_hash: route.padEnd(64, route), structure_hash: structure,
        event_resources: {
          desktop_families: desktopFamily ? { [desktopFamily]: 1 } : {},
          presentation_reasons: presentationReason ? { [presentationReason]: 1 } : {},
          markers,
        },
      } });
      const byFamily = new Map([
        ['page-family.home', [row('home.html', 'a', 'home')]],
        ['page-family.event-detail', [
          row('split-1.html', 'b', 'split-common', 'split'),
          row('split-2.html', 'c', 'split-common', 'split'),
          row('editorial.html', 'd', 'editorial', 'editorial'),
          row('portrait.html', 'h', 'portrait', 'split', 'split-portrait-or-square-visual'),
          row('no-image.html', 'f', 'no-image', 'split', 'split-no-image-fallback'),
          row('companion.html', 'g', 'companion', 'editorial', 'editorial-with-classified-identity-poster', {
            editorial_poster_companion_large: 1,
            editorial_companion_photo_preview_small: 3,
          }),
        ]],
        ['page-family.day-listing', [row('day.html', 'e', 'day')]],
      ]);
      const result = selectScreenshotPages(byFamily, 7);
      process.stdout.write(JSON.stringify(result.selected.map((item) => ({
        family: item.pageFamily, selection: item.selection, key: item.file.key,
        formats: Object.keys(item.observation.event_resources?.desktop_families || {}),
      }))));
    """
    result = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        cwd=REPO,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    selected = json.loads(result.stdout)
    assert [row["selection"] for row in selected[:3]] == [
        "family_representative",
        "family_representative",
        "family_representative",
    ]
    event_rows = [row for row in selected if row["family"] == "page-family.event-detail"]
    assert {row["formats"][0] for row in event_rows} == {"editorial", "split"}
    assert any(row["selection"] == "resource_format_representative" for row in event_rows)
    assert any(row["selection"] == "portrait_visual_format_representative" for row in event_rows)
    assert any(row["selection"] == "no_image_format_representative" for row in event_rows)
    assert any(row["key"] == "companion.html" for row in event_rows)


def test_exact_candidate_constants_and_retry_bound():
    script = """
      import { DEFAULT_IDENTITIES, REQUIRED_CANDIDATE_CHECKS, withRetry } from './scripts/current_ui_resource_graph/graph-lib.mjs';
      let attempts = 0;
      let error = '';
      try {
        await withRetry('probe', async () => { attempts += 1; throw new Error('no'); }, { attempts: 3, baseDelayMs: 0 });
      } catch (caught) { error = caught.message; }
      process.stdout.write(JSON.stringify({ identity: DEFAULT_IDENTITIES.latest_checked_kaggle_candidate, checks: REQUIRED_CANDIDATE_CHECKS, attempts, error }));
    """
    result = subprocess.run(
        ["node", "--input-type=module", "-e", script], cwd=REPO, text=True, capture_output=True
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    identity = payload["identity"]
    assert identity["manifest_sha256"] == "d615f6e447dc8c6ae3b876bf4a99123d1c85afee55276c26645f020b26074322"
    assert identity["generated_at"] == "2026-08-08T13:32:56.163Z"
    assert identity["tree_sha256"] == "0aad3919fccd996a5d32bcc760af8ee9b72249742c9db53196b009759bd0e7f4"
    assert identity["production_manifest_sha256"] == "baa0f29da3205ac81ddd4804bf6ff8e22b4585abb58d7d378e8dd87b9d395e45"
    assert identity["production_tree_sha256"] == "47df3798686dfbdde43589ba6a6498effd82f6fd091de6883a4899b7b4e57769"
    assert set(payload["checks"]) == {
        "astro_build",
        "browser_visual",
        "candidate_contract",
        "catalog_parity",
        "no_referrer",
        "noindex",
        "prefix_containment",
        "root_isolation",
    }
    assert payload["attempts"] == 3
    assert "failed after 3 attempts" in payload["error"]


def test_workflow_has_an_explicit_bounded_budget_for_expanded_event_specimens():
    workflow = (REPO / ".github/workflows/current-ui-resource-graph.yml").read_text()
    assert "--browser-max-pages 23" in workflow
    assert "--output-byte-budget 134217728" in workflow
    assert ".output_byte_budget == 134217728" in workflow


def test_partial_receipt_survives_failure(tmp_path):
    source, root_source, runtime, manifest, root_runtime = _fixture(tmp_path)
    payload = json.loads(manifest.read_text())
    payload["files"][0]["sha256"] = "0" * 64
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    output = tmp_path / "failed"
    secret = "https://candidate.invalid/_review/BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB/"
    result = _run(
        source,
        root_source,
        runtime,
        manifest,
        root_runtime,
        output,
        env={**os.environ, "CURRENT_UI_GRAPH_CANDIDATE_BASE_URL": secret},
    )
    assert result.returncode != 0
    receipt = json.loads((output / "receipt.json").read_text())
    assert receipt["status"] == "failed"
    assert secret not in result.stderr
    assert secret not in json.dumps(receipt)
    v1_receipt = json.loads(
        (output / "catalog/component-decoder/decoder-v1-snapshot-test/receipt.json").read_text()
    )
    assert v1_receipt["status"] == "failed"
    assert v1_receipt["handoff_status"] == "NO_GO"
    assert secret not in json.dumps(v1_receipt)


def test_workflow_uses_validated_env_inputs_and_honest_validation_receipt():
    workflow = (REPO / ".github/workflows/current-ui-resource-graph.yml").read_text()
    dispatch_inputs = workflow.split("    inputs:\n", 1)[1].split("\npermissions:", 1)[0]
    input_entries = re.findall(
        r"^      ([a-z0-9_]+):\n((?:        .*\n)+)", dispatch_inputs, re.MULTILINE
    )
    assert len(input_entries) == 19
    assert all("        type: string\n" in body for _, body in input_entries)
    assert all(
        re.search(r'^        default: "[^"]+"$', body, re.MULTILINE)
        for _, body in input_entries
    )
    job_env = workflow.split("    env:\n", 1)[1].split("    steps:\n", 1)[0]
    assert "${{ runner." not in job_env
    assert job_env.count("${{ github.workspace }}") == 4
    assert "${{ runner.temp }}" not in workflow
    assert "working-directory: ${{ env.CANDIDATE_WORKTREE }}/site" in workflow
    run_blocks = []
    lines = workflow.splitlines()
    for index, line in enumerate(lines):
        if line.strip() == "run: |":
            indent = len(line) - len(line.lstrip())
            body = []
            for candidate in lines[index + 1 :]:
                if candidate.strip() and len(candidate) - len(candidate.lstrip()) <= indent:
                    break
                body.append(candidate)
            run_blocks.append("\n".join(body))
    assert run_blocks
    assert all("${{ inputs." not in block for block in run_blocks)
    assert "Materialize exact candidate and public-root source trees" in workflow
    assert "--root-source-root" in workflow
    assert "CURRENT_UI_GRAPH_ROOT_HTML_SHA256" in workflow
    assert "workflow_validation_failed" in workflow
    assert '"$receipt_status" == "complete"' in workflow
    assert "Current UI Decoder v1 evidence completion" in workflow
    assert 'test "$(find "$v1_root/components"' in workflow
    assert ".classification.total == 107" in workflow
    assert '.handoff_status == "NO_GO"' in workflow
    assert 'index("capsule_human_visual_review")' in workflow
    assert ".constraints.normalization == false" in workflow
    assert "does not authorize merge, split, normalization" in workflow


def test_browser_capture_uses_and_checks_exact_playwright_viewports():
    source = (REPO / "scripts/current_ui_resource_graph/graph-lib.mjs").read_text(encoding="utf-8")

    assert "browser.newPage({ viewport," in source
    assert "viewportSize: viewport" not in source
    assert "const actualViewport = page.viewportSize();" in source
    assert "Browser viewport contract mismatch" in source


def test_browser_capture_freezes_fixture_clock_and_waits_for_stable_layout():
    source = (REPO / "scripts/current_ui_resource_graph/graph-lib.mjs").read_text(
        encoding="utf-8"
    )

    assert "fixedEpochMs" in source
    assert "globalThis.Date = FrozenDate" in source
    assert "waitForLoadState('networkidle'" in source
    assert "image.decode()" in source
    assert "near-viewport media did not settle" in source
    assert "Browser layout did not stabilize" in source
    assert "previousScreenshot?.equals(currentScreenshot)" in source
    assert "Browser pixels did not stabilize" in source
    assert "perceptual_dhash_64" in source
    assert "raw_raster_role: 'noncanonical_visual_evidence'" in source
    assert "Screenshot exceeds deterministic byte reservation" in source
    decoder = DECODER.read_text(encoding="utf-8")
    assert "cross_run_acceptance: 'equal_perceptual_dhash_64'" in decoder
