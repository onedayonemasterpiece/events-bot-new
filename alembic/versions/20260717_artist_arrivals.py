"""Add compact artist-arrival registry overlay, appearances and publication ledger."""

from alembic import op
import sqlalchemy as sa

revision = "20260717_artist_arrivals"
down_revision = "20260715_event_age_rating"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "artist_registry_entity",
        sa.Column("artist_id", sa.Text(), primary_key=True),
        sa.Column("entity_type", sa.Text(), nullable=False, server_default="person"),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("canonical_name", sa.Text(), nullable=False),
        sa.Column("aliases_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("primary_domain", sa.Text()),
        sa.Column("locality_status", sa.Text(), nullable=False, server_default="unknown"),
        sa.Column("base_country_code", sa.Text()),
        sa.Column("base_region_code", sa.Text()),
        sa.Column("base_city", sa.Text()),
        sa.Column("locality_basis", sa.Text()),
        sa.Column("evidence_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("verification_status", sa.Text(), nullable=False, server_default="review"),
        sa.Column("confidence", sa.Float()),
        sa.Column("photo_url", sa.Text()),
        sa.Column("photo_rights_status", sa.Text(), nullable=False, server_default="none"),
        sa.Column("photo_rights_evidence_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("seed_version", sa.Text()),
        sa.Column("decision_version", sa.Text(), nullable=False, server_default="artist-locality-v1"),
        sa.Column("verified_at", sa.DateTime(timezone=True)),
        sa.Column("valid_until", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )
    op.create_index("ix_artist_registry_locality_valid", "artist_registry_entity", ["locality_status", "valid_until"])

    op.create_table(
        "event_artist_appearance",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("event.id", ondelete="CASCADE"), nullable=False),
        sa.Column("artist_id", sa.Text(), sa.ForeignKey("artist_registry_entity.artist_id"), nullable=False),
        sa.Column("role", sa.Text(), nullable=False, server_default="performer"),
        sa.Column("project_title", sa.Text(), nullable=False),
        sa.Column("project_key", sa.Text(), nullable=False),
        sa.Column("visit_cluster_key", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="review"),
        sa.Column("identity_confidence", sa.Float()),
        sa.Column("physical_visit_status", sa.Text(), nullable=False, server_default="review"),
        sa.Column("physical_visit_confidence", sa.Float()),
        sa.Column("participant_evidence_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("locality_evidence_ids_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("cancellation_evidence_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("visit_evidence_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("appearance_input_hash", sa.Text(), nullable=False),
        sa.Column("source_revision", sa.Text(), nullable=False),
        sa.Column("eligibility_status", sa.Text(), nullable=False, server_default="review"),
        sa.Column("exclusion_reason", sa.Text()),
        sa.Column("media_event_poster_id", sa.Integer()),
        sa.Column("media_identity_status", sa.Text(), nullable=False, server_default="unverified"),
        sa.Column("media_rights_status", sa.Text(), nullable=False, server_default="event_source"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("cancelled_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("event_id", "artist_id", "project_key", "role", name="ux_event_artist_appearance_identity"),
    )
    op.create_index("ix_event_artist_appearance_event", "event_artist_appearance", ["event_id", "eligibility_status"])
    op.create_index("ix_event_artist_appearance_artist", "event_artist_appearance", ["artist_id", "project_key"])

    op.create_table(
        "artist_digest_issue",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("manifest_hash", sa.Text(), nullable=False, unique=True),
        sa.Column("build_date", sa.Text(), nullable=False),
        sa.Column("window_start", sa.Text(), nullable=False),
        sa.Column("window_end", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="preview"),
        sa.Column("unique_artist_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unique_project_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("meets_threshold", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("threshold_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("items_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("excluded_counts_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("published_targets_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("published_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_artist_digest_issue_status_created", "artist_digest_issue", ["status", "created_at"])
    op.create_index("ix_artist_digest_issue_window", "artist_digest_issue", ["window_start", "window_end"])

    op.create_table(
        "artist_publication_ledger",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("issue_id", sa.Integer(), sa.ForeignKey("artist_digest_issue.id", ondelete="CASCADE"), nullable=False),
        sa.Column("activity_id", sa.Integer(), sa.ForeignKey("promo_activity.id")),
        sa.Column("artist_id", sa.Text(), nullable=False),
        sa.Column("project_key", sa.Text(), nullable=False),
        sa.Column("surface", sa.Text(), nullable=False),
        sa.Column("target_key", sa.Text(), nullable=False),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column("publish_status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("target_url", sa.Text()),
        sa.Column("target_message_id", sa.Integer()),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("details_json", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("surface", "target_key", "dedupe_key", name="ux_artist_publication_dedupe"),
    )
    op.create_index("ix_artist_publication_issue", "artist_publication_ledger", ["issue_id", "surface"])


def downgrade() -> None:
    op.drop_table("artist_publication_ledger")
    op.drop_table("artist_digest_issue")
    op.drop_table("event_artist_appearance")
    op.drop_table("artist_registry_entity")
