"""Add canonical people registry and evidence-bound event appearances."""

from alembic import op
import sqlalchemy as sa


revision = "20260727_event_people"
down_revision = "20260726_festival_calendar_items"
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
        sa.Column(
            "photo_rights_evidence_json",
            sa.JSON(),
            nullable=False,
            server_default="[]",
        ),
        sa.Column("seed_version", sa.Text()),
        sa.Column(
            "decision_version",
            sa.Text(),
            nullable=False,
            server_default="event-people-identity-v1",
        ),
        sa.Column("verified_at", sa.DateTime(timezone=True)),
        sa.Column("valid_until", sa.DateTime(timezone=True)),
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
    )
    op.create_index(
        "ix_artist_registry_locality_valid",
        "artist_registry_entity",
        ["locality_status", "valid_until"],
    )

    op.create_table(
        "event_artist_appearance",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "event_id",
            sa.Integer(),
            sa.ForeignKey("event.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "artist_id",
            sa.Text(),
            sa.ForeignKey("artist_registry_entity.artist_id"),
            nullable=False,
        ),
        sa.Column("role", sa.Text(), nullable=False, server_default="participant"),
        sa.Column("project_title", sa.Text(), nullable=False),
        sa.Column("project_key", sa.Text(), nullable=False),
        sa.Column("visit_cluster_key", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="review"),
        sa.Column("identity_confidence", sa.Float()),
        sa.Column(
            "physical_visit_status",
            sa.Text(),
            nullable=False,
            server_default="review",
        ),
        sa.Column("physical_visit_confidence", sa.Float()),
        sa.Column(
            "participant_evidence_json",
            sa.JSON(),
            nullable=False,
            server_default="[]",
        ),
        sa.Column(
            "locality_evidence_ids_json",
            sa.JSON(),
            nullable=False,
            server_default="[]",
        ),
        sa.Column(
            "cancellation_evidence_json",
            sa.JSON(),
            nullable=False,
            server_default="[]",
        ),
        sa.Column("visit_evidence_json", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("appearance_input_hash", sa.Text(), nullable=False),
        sa.Column("source_revision", sa.Text(), nullable=False),
        sa.Column(
            "eligibility_status",
            sa.Text(),
            nullable=False,
            server_default="review",
        ),
        sa.Column("exclusion_reason", sa.Text()),
        sa.Column("media_event_poster_id", sa.Integer()),
        sa.Column(
            "media_identity_status",
            sa.Text(),
            nullable=False,
            server_default="unverified",
        ),
        sa.Column(
            "media_rights_status",
            sa.Text(),
            nullable=False,
            server_default="event_source",
        ),
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
        sa.Column("cancelled_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint(
            "event_id",
            "artist_id",
            "project_key",
            "role",
            name="ux_event_artist_appearance_identity",
        ),
    )
    op.create_index(
        "ix_event_artist_appearance_event",
        "event_artist_appearance",
        ["event_id", "eligibility_status"],
    )
    op.create_index(
        "ix_event_artist_appearance_artist",
        "event_artist_appearance",
        ["artist_id", "project_key"],
    )


def downgrade() -> None:
    op.drop_table("event_artist_appearance")
    op.drop_table("artist_registry_entity")
