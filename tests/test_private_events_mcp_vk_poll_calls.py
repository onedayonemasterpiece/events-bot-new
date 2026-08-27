from private_events_mcp_vk_adapter import (
    VK_FIXED_METHOD_ALLOWLIST,
    VK_OPERATION_ACTORS,
    VKActor,
)


def test_native_poll_calls_extend_the_closed_vk_contract() -> None:
    assert {
        "polls.create",
        "polls.edit",
        "polls.getById",
        "polls.getVoters",
    } <= VK_FIXED_METHOD_ALLOWLIST
    assert VK_OPERATION_ACTORS["poll_create"] == (
        VKActor.COMMUNITY_EDITOR.value,
        "post_publish",
    )
    assert VK_OPERATION_ACTORS["poll_get"] == (
        VKActor.COMMUNITY_EDITOR.value,
        "post_publish",
    )
    assert VK_OPERATION_ACTORS["poll_edit"] == (
        VKActor.COMMUNITY_EDITOR.value,
        "edit",
    )
    assert VK_OPERATION_ACTORS["poll_voters"] == (
        VKActor.COMMUNITY_EDITOR.value,
        "post_publish",
    )
    assert VK_OPERATION_ACTORS["poll_wall_edit"] == (
        VKActor.COMMUNITY_EDITOR.value,
        "edit",
    )
