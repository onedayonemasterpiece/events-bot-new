"""Server-owned application identity; transports authenticate before constructing it."""

from dataclasses import dataclass
import time
from .oauth import SUBJECT
from .tool_catalog import ToolExecutionError


@dataclass(frozen=True)
class ActorContext:
    subject: str
    client_id: str
    audience: str
    scopes: frozenset[str]
    expires_at: int

    @classmethod
    def from_mcp(cls, context):
        identity = context.identity
        if context.resource != identity.audience:
            raise ToolExecutionError("ACCESS_DENIED")
        return cls(
            identity.subject,
            identity.client_id,
            identity.audience,
            identity.scopes,
            identity.expires_at,
        )

    def binding(self):
        return {
            "subject": self.subject,
            "client_id": self.client_id,
            "audience": self.audience,
        }


class CommandPolicy:
    def __init__(self, config_getter, partners):
        self.config_getter, self.partners = config_getter, partners

    def check(self, conn, actor, action, *, event_id=None, revision=None, review=False):
        c = self.config_getter()
        if not c.enabled or actor.expires_at <= int(time.time()):
            raise ToolExecutionError("ACCESS_REVOKED")
        promo = action.startswith("promo_")
        read = action in {
            "event_read",
            "operation_read",
            "publication_read",
            "promo_read",
        }
        if actor.audience == c.partner_resource:
            if review or not c.partner_enabled or self.partners is None:
                raise ToolExecutionError("ACCESS_DENIED")
            enabled = (
                c.partner_promo_enabled
                if promo
                else c.partner_event_create_enabled and c.event_operations_enabled
            )
            if not read and not enabled:
                raise ToolExecutionError("CAPABILITY_DISABLED")
            scope = (
                "partner:promo:read"
                if promo and read
                else "partner:promo:request"
                if promo
                else "partner:publications:read"
                if action == "publication_read"
                else "partner:events:read"
                if read
                else "partner:events:propose"
            )
            if scope not in actor.scopes:
                raise ToolExecutionError("SCOPE_DENIED")
            grant = self.partners._resolve_actor(
                actor.subject,
                actor.client_id,
                actor.audience,
                scope=scope,
                action=None if read else action,
                event_id=event_id,
                conn=conn,
            )
            if revision is not None and revision != grant.policy_revision:
                raise ToolExecutionError("PARTNER_POLICY_REVISION_STALE")
            return grant
        scope = (
            "partners:manage"
            if review
            else (
                "promo:read"
                if promo and read
                else "promo:write"
                if promo
                else "operations:read"
                if action in {"publication_read", "operation_read"}
                else "events:read"
                if read
                else "events:write"
            )
        )
        if not (
            actor.audience == c.resource
            and actor.subject == SUBJECT
            and actor.client_id
            and actor.client_id in {c.oauth_client_id, c.opencode_oauth_client_id}
            and scope in actor.scopes
        ):
            raise ToolExecutionError("ACCESS_DENIED")
        if (
            not read
            and not review
            and not (c.owner_promo_enabled if promo else c.event_operations_enabled)
        ):
            raise ToolExecutionError("CAPABILITY_DISABLED")
        return None

    async def check_session(self, session, actor, action, **kwargs):
        class Connection:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, sql, parameters=()):
                return self.connection.exec_driver_sql(sql, parameters).mappings()

        return await session.run_sync(
            lambda sync: self.check(
                Connection(sync.connection()), actor, action, **kwargs
            )
        )
