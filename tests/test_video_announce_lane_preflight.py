from __future__ import annotations

import pytest

import video_announce.scenario as scenario_module
from video_announce.scenario import VideoAnnounceScenario


@pytest.mark.asyncio
async def test_kaggle_kernel_target_available_treats_wrong_slug_as_missing(monkeypatch):
    class MissingKernelClient:
        def get_kernel_status(self, ref: str) -> dict:  # noqa: ARG002
            raise RuntimeError(
                "Cannot access kernel 'zigomaro/cherryflash-video1' "
                "(Permission 'kernels.get' was denied). The most likely cause is a wrong kernel slug."
            )

    monkeypatch.setattr(scenario_module, "KaggleClient", MissingKernelClient)

    scenario = VideoAnnounceScenario(db=None, bot=object(), chat_id=1, user_id=1)
    available, error = await scenario._kaggle_kernel_target_available(
        "zigomaro/cherryflash-video1"
    )

    assert available is False
    assert error
