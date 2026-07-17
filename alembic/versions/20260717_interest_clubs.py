"""Add versioned interest-club identities and event relations."""

from alembic import op
import sqlalchemy as sa


revision = "20260717_interest_clubs"
down_revision = "20260715_social_metric_snapshots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "interest_club",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("slug", sa.Text(), nullable=False, unique=True),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column("topic", sa.Text()),
        sa.Column("description", sa.Text()),
        sa.Column("city", sa.Text()),
        sa.Column("typical_place", sa.Text()),
        sa.Column("public_status", sa.Text(), nullable=False, server_default="shadow"),
        sa.Column("identity_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("policy_version", sa.Text(), nullable=False, server_default="interest-club-relation-v1"),
        sa.Column("aliases_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("source_anchors_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("provenance_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("public_status IN ('shadow','approved','archived','merged')"),
    )
    op.create_index("ix_interest_club_public_status", "interest_club", ["public_status"])
    op.create_index("ix_interest_club_updated_at", "interest_club", ["updated_at"])

    op.create_table(
        "interest_club_event",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("club_id", sa.Integer(), sa.ForeignKey("interest_club.id", ondelete="CASCADE"), nullable=False),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("event.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="active"),
        sa.Column("decision_lane", sa.Text(), nullable=False),
        sa.Column("evidence_quote", sa.Text()),
        sa.Column("evidence_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("model", sa.Text()),
        sa.Column("policy_version", sa.Text(), nullable=False, server_default="interest-club-relation-v1"),
        sa.Column("input_hash", sa.Text(), nullable=False),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("club_id", "event_id", name="ux_interest_club_event_pair"),
        sa.CheckConstraint("status IN ('active','deferred','review')"),
    )
    op.create_index("ix_interest_club_event_event_status", "interest_club_event", ["event_id", "status"])
    op.create_index("ix_interest_club_event_club_status", "interest_club_event", ["club_id", "status"])

    op.create_table(
        "interest_club_evaluation",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("club_id", sa.Integer(), sa.ForeignKey("interest_club.id", ondelete="CASCADE"), nullable=False),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("event.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("verdict", sa.Text(), nullable=False),
        sa.Column("decision_lane", sa.Text(), nullable=False),
        sa.Column("evidence_quote", sa.Text()),
        sa.Column("evidence_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("model", sa.Text()),
        sa.Column("policy_version", sa.Text(), nullable=False, server_default="interest-club-relation-v1"),
        sa.Column("input_hash", sa.Text(), nullable=False),
        sa.Column("error_code", sa.Text()),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("club_id", "event_id", name="ux_interest_club_evaluation_pair"),
        sa.CheckConstraint("status IN ('accepted','no_match','review','deferred','ineligible')"),
    )
    op.create_index("ix_interest_club_evaluation_status", "interest_club_evaluation", ["status", "updated_at"])
    op.create_index("ix_interest_club_evaluation_event", "interest_club_evaluation", ["event_id"])


def downgrade() -> None:
    op.drop_index("ix_interest_club_evaluation_event", table_name="interest_club_evaluation")
    op.drop_index("ix_interest_club_evaluation_status", table_name="interest_club_evaluation")
    op.drop_table("interest_club_evaluation")
    op.drop_index("ix_interest_club_event_club_status", table_name="interest_club_event")
    op.drop_index("ix_interest_club_event_event_status", table_name="interest_club_event")
    op.drop_table("interest_club_event")
    op.drop_index("ix_interest_club_updated_at", table_name="interest_club")
    op.drop_index("ix_interest_club_public_status", table_name="interest_club")
    op.drop_table("interest_club")
