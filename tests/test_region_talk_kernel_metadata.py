from __future__ import annotations

import json
import importlib.util
import sys
from types import SimpleNamespace
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def _load_candidate_module():
    path = ROOT / "kaggle" / "RegionTalkCandidateReport" / "region_talk_candidate_report.py"
    spec = importlib.util.spec_from_file_location("region_talk_candidate_iam_contract", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_region_talk_script_kernels_disable_sibling_file_instrumentation() -> None:
    """Kaggle script pushes upload only ``code_file`` as the executable body.

    The shared status wrapper depends on a renamed sibling source file, so
    Region Talk workers (which already write their own durable heartbeats) must
    explicitly opt out of that wrapper.
    """

    kernel_dirs = (
        "RegionTalkCandidateReport",
        "RegionTalkBgeM3Enrichment",
        "RegionTalkImageDiagnostic",
        "RegionTalkQwen3Embedding06BEnrichment",
    )
    for directory in kernel_dirs:
        metadata_path = ROOT / "kaggle" / directory / "kernel-metadata.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        assert metadata["kernel_type"] == "script"
        assert metadata["events_bot_disable_status_instrumentation"] is True


def test_region_talk_kaggle_workers_do_not_install_yandexcloud_extra() -> None:
    kernel_dirs = (
        "RegionTalkCandidateReport",
        "RegionTalkBgeM3Enrichment",
        "RegionTalkImageDiagnostic",
        "RegionTalkQwen3Embedding06BEnrichment",
    )
    for directory in kernel_dirs:
        source_path = next((ROOT / "kaggle" / directory).glob("region_talk_*.py"))
        source = source_path.read_text(encoding="utf-8")
        assert '"ydb[yc]"' not in source
        assert '"ydb==3.31.2"' in source
        assert "https://iam.api.cloud.yandex.net/iam/v1/tokens" in source
        assert "ServiceAccountCredentials.from_file" not in source


def test_service_account_key_is_exchanged_for_short_lived_access_token() -> None:
    module = _load_candidate_module()
    encoded: dict[str, object] = {}

    def encode(payload, private_key, *, algorithm, headers):
        encoded.update(payload=payload, private_key=private_key, algorithm=algorithm, headers=headers)
        return "signed-jwt"

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"iamToken":"short-lived-token"}'

    key = json.dumps({"id": "key-id", "service_account_id": "sa-id", "private_key": "private"})
    with mock.patch.dict(sys.modules, {"jwt": SimpleNamespace(encode=encode)}), mock.patch.object(
        module.urllib.request, "urlopen", return_value=Response()
    ) as urlopen:
        token = module.service_account_iam_token(key)

    assert token == "short-lived-token"
    assert encoded["algorithm"] == "PS256"
    assert encoded["headers"] == {"typ": "JWT", "alg": "PS256", "kid": "key-id"}
    assert encoded["payload"]["iss"] == "sa-id"
    assert encoded["payload"]["aud"] == "https://iam.api.cloud.yandex.net/iam/v1/tokens"
    request = urlopen.call_args.args[0]
    assert request.full_url == "https://iam.api.cloud.yandex.net/iam/v1/tokens"
    assert json.loads(request.data) == {"jwt": "signed-jwt"}
