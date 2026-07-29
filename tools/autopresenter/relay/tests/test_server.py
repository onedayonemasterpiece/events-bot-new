from __future__ import annotations

import sys
from pathlib import Path
import unittest
import asyncio
import hashlib
import io
import json
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
        self.assertIn("Запустить «Завтра»", text)
        self.assertIn(">Стоп<", text)
        self.assertIn(">Сброс<", text)

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


if __name__ == "__main__":
    unittest.main()
