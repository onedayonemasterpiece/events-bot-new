from __future__ import annotations

import sys
from pathlib import Path
import unittest
import asyncio
import hashlib
import io
import json
import struct
import zipfile

from aiohttp.test_utils import TestClient, TestServer

RELAY_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RELAY_DIR))

from server import create_app  # noqa: E402


class RelayApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = TestClient(TestServer(create_app(command_ttl_ms=80, agent_timeout_ms=500)))
        await self.client.start_server()

    async def asyncTearDown(self) -> None:
        await self.client.close()

    async def json(self, method: str, path: str, **kwargs):
        response = await self.client.request(method, path, **kwargs)
        payload = await response.json()
        return response, payload

    async def test_control_and_initial_state_are_available(self):
        response = await self.client.get("/control/")
        text = await response.text()
        self.assertEqual(response.status, 200)
        self.assertIn("<title>Пульт презентации</title>", text)
        self.assertIn('rel="manifest" href="/control/manifest.webmanifest"', text)
        self.assertIn("Запустить «Завтра»", text)
        self.assertIn(">Стоп<", text)
        self.assertIn(">Сброс<", text)
        self.assertNotIn("Выключить", text)
        self.assertIn("Сбросить доступ на этом устройстве", text)

        response, payload = await self.json("GET", "/api/state")
        self.assertEqual(response.status, 200)
        self.assertEqual(payload["state"]["status"], "disconnected")

    async def test_idempotent_command_poll_and_ack_flow(self):
        response, payload = await self.json(
            "POST", "/api/state/agent", json={"agent_id": "agent-one", "status": "idle"}
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(payload["state"]["status"], "idle")

        body = {"action": "run", "command_id": "stable-command"}
        first_response, first = await self.json("POST", "/api/commands", json=body)
        second_response, second = await self.json("POST", "/api/commands", json=body)
        self.assertEqual(first_response.status, 202)
        self.assertEqual(second_response.status, 202)
        self.assertEqual(first["command"]["sequence"], 1)
        self.assertEqual(second["command"]["sequence"], 1)
        self.assertEqual(first["state"]["status"], "running")

        response, payload = await self.json(
            "GET", "/api/commands/next?agent_id=agent-one&after_seq=0&wait_ms=0"
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(payload["command"]["action"], "run")

        response, payload = await self.json(
            "POST",
            "/api/commands/stable-command/ack",
            json={
                "agent_id": "agent-one",
                "sequence": 1,
                "status": "completed",
                "detail": "Reached /zavtra/",
            },
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(payload["state"]["status"], "completed")
        self.assertEqual(payload["state"]["detail"], "Reached /zavtra/")

    async def test_sequence_conflicts_and_stop_transition(self):
        await self.json("POST", "/api/state/agent", json={"agent_id": "agent-one", "status": "idle"})
        await self.json("POST", "/api/commands", json={"action": "run", "command_id": "same"})
        response, payload = await self.json(
            "POST", "/api/commands", json={"action": "stop", "command_id": "same"}
        )
        self.assertEqual(response.status, 409)
        self.assertEqual(payload["error"], "idempotency_conflict")

        response, payload = await self.json(
            "POST", "/api/commands", json={"action": "stop", "command_id": "stop-one"}
        )
        self.assertEqual(payload["command"]["sequence"], 2)
        self.assertEqual(payload["state"]["status"], "stopping")

        response, payload = await self.json(
            "POST",
            "/api/commands/stop-one/ack",
            json={"agent_id": "agent-one", "sequence": 99, "status": "idle"},
        )
        self.assertEqual(response.status, 409)
        self.assertEqual(payload["error"], "sequence_mismatch")

    async def test_expired_command_is_not_delivered(self):
        await self.json("POST", "/api/commands", json={"action": "reset", "command_id": "short"})
        await asyncio.sleep(0.1)
        response, payload = await self.json(
            "GET", "/api/commands/next?agent_id=agent-one&after_seq=0&wait_ms=0"
        )
        self.assertEqual(response.status, 200)
        self.assertIsNone(payload["command"])

    async def test_long_poll_wakes_when_command_arrives(self):
        poll = asyncio.create_task(
            self.json(
                "GET",
                "/api/commands/next?agent_id=agent-one&after_seq=0&wait_ms=250",
            )
        )
        await asyncio.sleep(0.02)
        await self.json(
            "POST",
            "/api/commands",
            json={"action": "reset", "command_id": "wake-poll"},
        )
        response, payload = await poll
        self.assertEqual(response.status, 200)
        self.assertEqual(payload["command"]["id"], "wake-poll")

    async def test_only_one_live_agent_is_accepted(self):
        await self.json("POST", "/api/state/agent", json={"agent_id": "agent-one", "status": "idle"})
        response, payload = await self.json(
            "POST", "/api/state/agent", json={"agent_id": "agent-two", "status": "idle"}
        )
        self.assertEqual(response.status, 409)
        self.assertEqual(payload["error"], "agent_conflict")


class RelayAuthAndPackageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = TestClient(
            TestServer(
                create_app(
                    control_token="control-secret",
                    agent_token="agent-secret",
                    public_base_url_value="https://presenter.example",
                )
            )
        )
        await self.client.start_server()

    async def asyncTearDown(self) -> None:
        await self.client.close()

    async def test_public_api_is_fail_closed_by_role(self):
        response = await self.client.get("/healthz")
        self.assertEqual(response.status, 200)

        response = await self.client.get("/api/state")
        self.assertEqual(response.status, 401)

        response = await self.client.get(
            "/api/state",
            headers={"Authorization": "Bearer control-secret"},
        )
        self.assertEqual(response.status, 200)

        response = await self.client.get(
            "/api/commands/next?agent_id=test&after_seq=0&wait_ms=0",
            headers={"Authorization": "Bearer control-secret"},
        )
        self.assertEqual(response.status, 401)

        response = await self.client.get(
            "/api/commands/next?agent_id=test&after_seq=0&wait_ms=0",
            headers={"Authorization": "Bearer agent-secret"},
        )
        self.assertEqual(response.status, 200)

    async def test_control_token_downloads_scoped_windows_first_test(self):
        response = await self.client.get("/api/download/windows-test.zip")
        self.assertEqual(response.status, 401)

        response = await self.client.get(
            "/api/download/windows-test.zip",
            headers={"Authorization": "Bearer control-secret"},
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(response.content_type, "application/zip")
        archive_bytes = await response.read()
        repeat = await self.client.get(
            "/api/download/windows-test.zip",
            headers={"Authorization": "Bearer control-secret"},
        )
        self.assertEqual(await repeat.read(), archive_bytes)
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
            names = set(archive.namelist())
            self.assertIn("START-DEMONSTRATOR.cmd", names)
            self.assertIn("SELF-TEST.cmd", names)
            self.assertIn("agent/agent.mjs", names)
            self.assertIn("agent/abort-utils.mjs", names)
            self.assertIn("agent/scenario-contract.mjs", names)
            self.assertTrue(
                all(info.date_time == (2025, 1, 1, 0, 0, 0) for info in archive.infolist())
            )
            bootstrap = archive.read("bootstrap.ps1").decode("utf-8")
            self.assertIn("public static extern IntPtr GetStdHandle", bootstrap)
            self.assertNotIn("internal static extern", bootstrap)
            self.assertIn(
                '[Environment]::GetFolderPath("LocalApplicationData")',
                bootstrap,
            )
            self.assertIn(
                '"KenigEvents\\Autopresenter\\cache-v1"',
                bootstrap,
            )
            config = json.loads(archive.read("test-config.json"))
        self.assertEqual(config["relay_url"], "https://presenter.example")
        self.assertEqual(
            config["stage_url"],
            "https://presenter.example/internal/presenter-stage/",
        )
        self.assertEqual(config["agent_token"], "agent-secret")
        self.assertEqual(
            config["agent_id"],
            "first-test-" + hashlib.sha256(b"agent-secret").hexdigest()[:12],
        )
        self.assertEqual(config["release_kind"], "FIRST_TEST_NOT_M3")

    async def test_control_pwa_assets_are_public_but_api_stays_no_store(self):
        response = await self.client.get("/control/manifest.webmanifest")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.content_type, "application/manifest+json")
        self.assertEqual(response.headers["Cache-Control"], "no-cache")
        manifest = await response.json()
        self.assertEqual(manifest["name"], "Пульт презентации")
        self.assertEqual(manifest["short_name"], "Пульт")
        self.assertNotIn(" ", manifest["short_name"])
        self.assertEqual(manifest["start_url"], "/control/")
        self.assertEqual(manifest["scope"], "/control/")
        self.assertEqual(manifest["display"], "standalone")
        self.assertEqual(manifest["theme_color"], "#090f1f")
        self.assertEqual(manifest["background_color"], "#090f1f")
        self.assertNotIn("token", json.dumps(manifest).lower())
        self.assertEqual(
            {(icon["sizes"], icon["purpose"]) for icon in manifest["icons"]},
            {
                ("192x192", "any"),
                ("512x512", "any"),
                ("512x512", "maskable"),
            },
        )

        response = await self.client.get("/control/service-worker.js")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.content_type, "text/javascript")
        self.assertEqual(response.headers["Cache-Control"], "no-cache")
        self.assertEqual(response.headers["Service-Worker-Allowed"], "/control/")
        service_worker = await response.text()
        self.assertIn("url.pathname.startsWith('/api/')", service_worker)
        self.assertIn("request.method !== 'GET'", service_worker)
        self.assertIn("CONTROL_SHELL_PATHS.has(url.pathname)", service_worker)
        self.assertIn("'/control/auth-storage.js'", service_worker)
        self.assertNotIn("cache.put(", service_worker)

        response = await self.client.get("/control/auth-storage.js")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.content_type, "text/javascript")
        self.assertEqual(response.headers["Cache-Control"], "no-cache")

        for name, expected_size in (
            ("icon-192.png", 192),
            ("icon-512.png", 512),
            ("icon-maskable-512.png", 512),
        ):
            response = await self.client.get(f"/control/icons/{name}")
            self.assertEqual(response.status, 200)
            self.assertEqual(response.content_type, "image/png")
            self.assertEqual(
                response.headers["Cache-Control"],
                "public, max-age=31536000, immutable",
            )
            icon = await response.read()
            self.assertEqual(icon[:8], b"\x89PNG\r\n\x1a\n")
            width, height = struct.unpack(">II", icon[16:24])
            self.assertEqual((width, height), (expected_size, expected_size))

        response = await self.client.get("/control/icons/not-an-icon.png")
        self.assertEqual(response.status, 404)

        response = await self.client.get(
            "/api/state",
            headers={"Authorization": "Bearer control-secret"},
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    async def test_control_pwa_keeps_fragment_session_auth_contract(self):
        response = await self.client.get("/control/")
        control = await response.text()
        self.assertIn("new URLSearchParams(location.hash.slice(1))", control)
        self.assertIn("fragment.get('token')", control)
        self.assertIn(
            "accessStorage.remember(fragmentToken, sessionStorage, localStorage",
            control,
        )
        self.assertIn("history.replaceState", control)
        self.assertIn(
            "accessStorage.restore(sessionStorage, localStorage",
            control,
        )
        self.assertIn("Authorization: `Bearer ${token}`", control)
        self.assertIn(
            "navigator.serviceWorker.register('/control/service-worker.js'",
            control,
        )
        self.assertIn("scope: '/control/'", control)

    async def test_control_pwa_recovers_and_can_clear_persisted_access(self):
        response = await self.client.get("/control/")
        control = await response.text()
        self.assertIn(
            "accessStorage.remember(fragmentToken, sessionStorage, localStorage",
            control,
        )
        self.assertIn(
            "accessStorage.restore(sessionStorage, localStorage",
            control,
        )
        self.assertIn(
            "accessStorage.forget(sessionStorage, localStorage",
            control,
        )
        self.assertIn("location.replace('/control/')", control)
        self.assertNotIn("localStorage.clear(", control)
        self.assertNotIn("sessionStorage.clear(", control)

    def test_pwa_assets_are_in_the_relay_container_package(self):
        dockerfile = (RELAY_DIR / "Dockerfile.internet-test").read_text(
            encoding="utf-8"
        )
        dockerignore = (RELAY_DIR / ".dockerignore.internet-test").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "COPY tools/autopresenter/relay /app/tools/autopresenter/relay",
            dockerfile,
        )
        self.assertIn("!tools/autopresenter/relay/**", dockerignore)
        for relative_path in (
            "control/index.html",
            "control/auth-storage.js",
            "control/manifest.webmanifest",
            "control/service-worker.js",
            "control/icons/icon-192.png",
            "control/icons/icon-512.png",
            "control/icons/icon-maskable-512.png",
        ):
            self.assertTrue((RELAY_DIR / relative_path).is_file(), relative_path)


if __name__ == "__main__":
    unittest.main()
