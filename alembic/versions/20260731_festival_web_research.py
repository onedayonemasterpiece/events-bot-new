"""Add collect-only festival web research operational persistence."""

from alembic import op
import sqlalchemy as sa


revision = "20260731_festival_web_research"
down_revision = "20260726_festival_calendar_items"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "festival_web_research_run",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_uid", sa.Text(), nullable=False),
        sa.Column("target_key", sa.Text(), nullable=False),
        sa.Column("series_candidate", sa.Text()),
        sa.Column("edition_candidate", sa.Text()),
        sa.Column("state", sa.Text(), nullable=False, server_default="pending"),
        sa.Column(
            "mode", sa.Text(), nullable=False, server_default="collect_only"
        ),
        sa.Column(
            "review_status", sa.Text(), nullable=False, server_default="pending"
        ),
        sa.Column("input_fingerprint", sa.Text(), nullable=False),
        sa.Column("orchestration_version", sa.Text(), nullable=False),
        sa.Column("contract_version", sa.Text(), nullable=False),
        sa.Column("taxonomy_version", sa.Text(), nullable=False),
        sa.Column("taxonomy_sha256", sa.Text(), nullable=False),
        sa.Column(
            "primary_queue_item_id",
            sa.Integer(),
            sa.ForeignKey("festival_queue.id", ondelete="SET NULL"),
        ),
        sa.Column("candidate_sha256", sa.Text()),
        sa.Column(
            "candidate_json", sa.JSON(), nullable=False, server_default="{}"
        ),
        sa.Column("quality_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column(
            "artifact_manifest_json",
            sa.JSON(),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("lease_owner", sa.Text()),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("reviewed_by", sa.Text()),
        sa.Column("reviewed_at", sa.DateTime(timezone=True)),
        sa.Column("review_reason", sa.Text()),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "run_uid", name="ux_festival_web_research_run_uid"
        ),
        sa.UniqueConstraint(
            "input_fingerprint",
            name="ux_festival_web_research_run_input_fingerprint",
        ),
    )
    op.create_index(
        "ix_festival_web_research_run_state_updated",
        "festival_web_research_run",
        ["state", "updated_at"],
    )
    op.create_index(
        "ix_festival_web_research_run_target_created",
        "festival_web_research_run",
        ["target_key", "created_at"],
    )
    op.create_index(
        "ix_festival_web_research_run_review_updated",
        "festival_web_research_run",
        ["review_status", "updated_at"],
    )

    op.create_table(
        "festival_web_research_lane_run",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "run_id",
            sa.Integer(),
            sa.ForeignKey("festival_web_research_run.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "lane", sa.Text(), nullable=False, server_default="antigravity"
        ),
        sa.Column("attempt_no", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("request_uid", sa.Text(), nullable=False),
        sa.Column(
            "provider_state", sa.Text(), nullable=False, server_default="pending"
        ),
        sa.Column(
            "semantic_state", sa.Text(), nullable=False, server_default="pending"
        ),
        sa.Column(
            "interaction_ids_json", sa.JSON(), nullable=False, server_default="[]"
        ),
        sa.Column("model_id", sa.Text()),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("contract_version", sa.Text(), nullable=False),
        sa.Column("taxonomy_version", sa.Text(), nullable=False),
        sa.Column("taxonomy_sha256", sa.Text(), nullable=False),
        sa.Column("input_fingerprint", sa.Text(), nullable=False),
        sa.Column(
            "artifact_manifest_json",
            sa.JSON(),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("usage_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column(
            "validation_json", sa.JSON(), nullable=False, server_default="{}"
        ),
        sa.Column("candidate_sha256", sa.Text()),
        sa.Column(
            "candidate_json", sa.JSON(), nullable=False, server_default="{}"
        ),
        sa.Column("provider_error_code", sa.Text()),
        sa.Column("semantic_error_code", sa.Text()),
        sa.Column("last_error", sa.Text()),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "run_id",
            "lane",
            "attempt_no",
            name="ux_festival_web_research_lane_attempt",
        ),
        sa.UniqueConstraint(
            "request_uid", name="ux_festival_web_research_lane_request_uid"
        ),
    )
    op.create_index(
        "ix_festival_web_research_lane_provider_updated",
        "festival_web_research_lane_run",
        ["provider_state", "updated_at"],
    )
    op.create_index(
        "ix_festival_web_research_lane_semantic_updated",
        "festival_web_research_lane_run",
        ["semantic_state", "updated_at"],
    )
    op.create_index(
        "ix_festival_web_research_lane_input_fingerprint",
        "festival_web_research_lane_run",
        ["input_fingerprint"],
    )

    op.create_table(
        "festival_web_research_item",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "run_id",
            sa.Integer(),
            sa.ForeignKey("festival_web_research_run.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "queue_item_id",
            sa.Integer(),
            sa.ForeignKey("festival_queue.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("original_status", sa.Text(), nullable=False),
        sa.Column("source_role", sa.Text(), nullable=False),
        sa.Column("decision", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("decision_reason", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "run_id",
            "queue_item_id",
            name="ux_festival_web_research_item_run_queue",
        ),
    )
    op.create_index(
        "ix_festival_web_research_item_queue",
        "festival_web_research_item",
        ["queue_item_id", "created_at"],
    )
    op.create_index(
        "ix_festival_web_research_item_decision",
        "festival_web_research_item",
        ["decision", "updated_at"],
    )

    op.create_table(
        "festival_web_research_source",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "lane_run_id",
            sa.Integer(),
            sa.ForeignKey(
                "festival_web_research_lane_run.id", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column("source_id", sa.Text(), nullable=False),
        sa.Column("requested_url", sa.Text(), nullable=False),
        sa.Column("resolved_url", sa.Text()),
        sa.Column("canonical_url", sa.Text()),
        sa.Column("source_role", sa.Text(), nullable=False),
        sa.Column(
            "edition_status", sa.Text(), nullable=False, server_default="unknown"
        ),
        sa.Column("content_sha256", sa.Text()),
        sa.Column("snapshot_ref", sa.Text()),
        sa.Column("normalizer_version", sa.Text()),
        sa.Column("quote_index_ref", sa.Text()),
        sa.Column("fetched_at", sa.DateTime(timezone=True)),
        sa.Column("decision", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("exclusion_reason", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "lane_run_id",
            "source_id",
            name="ux_festival_web_research_source_lane_source",
        ),
    )
    op.create_index(
        "ix_festival_web_research_source_canonical_url",
        "festival_web_research_source",
        ["canonical_url"],
    )
    op.create_index(
        "ix_festival_web_research_source_content_hash",
        "festival_web_research_source",
        ["content_sha256"],
    )
    op.create_index(
        "ix_festival_web_research_source_lane_decision",
        "festival_web_research_source",
        ["lane_run_id", "decision"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_festival_web_research_source_lane_decision",
        table_name="festival_web_research_source",
    )
    op.drop_index(
        "ix_festival_web_research_source_content_hash",
        table_name="festival_web_research_source",
    )
    op.drop_index(
        "ix_festival_web_research_source_canonical_url",
        table_name="festival_web_research_source",
    )
    op.drop_table("festival_web_research_source")

    op.drop_index(
        "ix_festival_web_research_item_decision",
        table_name="festival_web_research_item",
    )
    op.drop_index(
        "ix_festival_web_research_item_queue",
        table_name="festival_web_research_item",
    )
    op.drop_table("festival_web_research_item")

    op.drop_index(
        "ix_festival_web_research_lane_input_fingerprint",
        table_name="festival_web_research_lane_run",
    )
    op.drop_index(
        "ix_festival_web_research_lane_semantic_updated",
        table_name="festival_web_research_lane_run",
    )
    op.drop_index(
        "ix_festival_web_research_lane_provider_updated",
        table_name="festival_web_research_lane_run",
    )
    op.drop_table("festival_web_research_lane_run")

    op.drop_index(
        "ix_festival_web_research_run_review_updated",
        table_name="festival_web_research_run",
    )
    op.drop_index(
        "ix_festival_web_research_run_target_created",
        table_name="festival_web_research_run",
    )
    op.drop_index(
        "ix_festival_web_research_run_state_updated",
        table_name="festival_web_research_run",
    )
    op.drop_table("festival_web_research_run")
