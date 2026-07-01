from __future__ import annotations

class AcquisitionSafetyError(RuntimeError):
    pass


def ensure_review_chat(chat_id: int | None, *, review_chat_id: int | None) -> None:
    if review_chat_id is None:
        raise AcquisitionSafetyError("ACQ_REVIEW_CHAT_ID is not configured")
    if int(chat_id or 0) != int(review_chat_id):
        raise AcquisitionSafetyError("acquisition may only send to ACQ_REVIEW_CHAT_ID")


FORBIDDEN_VK_METHODS = {"wall.createComment", "wall.post", "messages.send"}


def ensure_vk_read_only(method_name: str) -> None:
    if method_name in FORBIDDEN_VK_METHODS:
        raise AcquisitionSafetyError(f"forbidden VK write method: {method_name}")


def ensure_no_join_private(is_private: bool) -> None:
    if is_private:
        raise AcquisitionSafetyError("joining private groups is forbidden")
