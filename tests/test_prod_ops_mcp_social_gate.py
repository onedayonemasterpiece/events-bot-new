from prod_ops_mcp.social_gate import SocialAction, SocialCapabilityGate


def test_social_gate_is_fail_closed_and_requires_aliases():
    gate = SocialCapabilityGate(
        {"telegram": {"read_cached": True, "plan_publish": True, "targets": ["telegram:test"]}},
        write_enabled=False,
    )
    assert gate.decide("telegram", SocialAction.READ_CACHED).allowed
    decision = gate.decide(
        "telegram", SocialAction.PLAN_PUBLISH, target="telegram:test"
    )
    assert not decision.allowed
    assert decision.reason == "global_write_gate_disabled"
    assert not gate.decide("max", SocialAction.READ_CACHED).allowed
