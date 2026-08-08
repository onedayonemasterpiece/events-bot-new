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
    for component_root in (components, root_source / "components"):
        (component_root / "SiteHeader.astro").write_text(
            '<header class="site-header">Header</header>\n', encoding="utf-8"
        )
    for page, body in {
        "segodnya/index.astro": "<main><EventCard /></main>",
        "populyarnoe/index.astro": "<main><EventCard /></main>",
        "sobytiya/[slug].astro": "<main class='event-detail'><EventCard /></main>",
    }.items():
        (pages / page).write_text(
            "---\nimport EventCard from '../../components/EventCard.astro';\n---\n"
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
        "sobytiya/example/index.html": b"<!doctype html><main class='event-detail'><article class='event-card'>Event</article></main>",
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
    assert counts["candidate_routes"] == 5
    assert counts["public_root_observations"] == 1
    assert 0 < counts["style_inconsistencies"] < counts["styles"]

    summary = (output / "summary.md").read_text()
    assert "Candidate HTML routes: 5" in summary
    assert "Separate public-root observations: 1" in summary
    assert "Layouts by plane:" in summary
    assert "Source components by plane:" in summary
    assert f"Style inconsistencies: {counts['style_inconsistencies']}" in summary


def test_not_merged_and_unresolved_are_invariants(decoded):
    output, _, _ = decoded
    for name in ("fragmentation-report.jsonl", "candidate-component-graph.jsonl"):
        rows = [json.loads(line) for line in (output / name).read_text().splitlines()]
        assert rows
        assert {row["decision"] for row in rows} == {"NOT_MERGED"}
        assert {row["recommendation"] for row in rows} == {"unresolved"}
    assert (output / "summary.md").read_text().rstrip().endswith(
        "Proceed to normalization workshop"
    )


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
