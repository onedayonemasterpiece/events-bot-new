"""Add normalized artist media assets and source/account provenance."""

from alembic import op
import sqlalchemy as sa


revision = "20260717_artist_media_bank"
down_revision = "20260717_artist_arrivals"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "artist_media_asset",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("candidate_key", sa.Text(), nullable=False),
        sa.Column(
            "artist_id",
            sa.Text(),
            sa.ForeignKey("artist_registry_entity.artist_id"),
            nullable=False,
        ),
        sa.Column("media_role", sa.Text(), nullable=False, server_default="portrait"),
        sa.Column("lifecycle_status", sa.Text(), nullable=False, server_default="candidate"),
        sa.Column("identity_status", sa.Text(), nullable=False, server_default="unverified"),
        sa.Column("identity_confidence", sa.Float()),
        sa.Column("quality_status", sa.Text(), nullable=False, server_default="review"),
        sa.Column("rights_status", sa.Text(), nullable=False, server_default="review"),
        sa.Column("rights_scope", sa.Text()),
        sa.Column("storage_status", sa.Text(), nullable=False, server_default="remote_candidate"),
        sa.Column("object_path", sa.Text()),
        sa.Column("cdn_url", sa.Text()),
        sa.Column("raw_sha256", sa.Text()),
        sa.Column("pixel_sha256", sa.Text()),
        sa.Column("encoded_sha256", sa.Text()),
        sa.Column("perceptual_hash", sa.Text()),
        sa.Column("width", sa.Integer()),
        sa.Column("height", sa.Integer()),
        sa.Column("focal_x", sa.Float()),
        sa.Column("focal_y", sa.Float()),
        sa.Column("safe_crop", sa.Boolean()),
        sa.Column("preferred", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("verified_at", sa.DateTime(timezone=True)),
        sa.Column("taken_down_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("candidate_key", name="ux_artist_media_asset_candidate"),
        sa.UniqueConstraint("artist_id", "pixel_sha256", name="ux_artist_media_asset_pixel"),
    )
    op.create_index(
        "ix_artist_media_asset_selection",
        "artist_media_asset",
        ["artist_id", "lifecycle_status", "storage_status", "preferred", "priority"],
    )

    op.create_table(
        "artist_media_provenance",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("asset_id", sa.Integer(), sa.ForeignKey("artist_media_asset.id", ondelete="CASCADE"), nullable=False),
        sa.Column("event_id", sa.Integer(), sa.ForeignKey("event.id", ondelete="SET NULL")),
        sa.Column("event_poster_id", sa.Integer(), sa.ForeignKey("eventposter.id", ondelete="SET NULL")),
        sa.Column("source_kind", sa.Text(), nullable=False, server_default="curated_discovery"),
        sa.Column("service", sa.Text(), nullable=False),
        sa.Column("account_handle", sa.Text(), nullable=False),
        sa.Column("account_name", sa.Text()),
        sa.Column("account_url", sa.Text()),
        sa.Column("source_page_url", sa.Text(), nullable=False),
        sa.Column("source_media_url", sa.Text()),
        sa.Column("original_source_url", sa.Text()),
        sa.Column("credit_text", sa.Text(), nullable=False),
        sa.Column("author_or_rightsholder", sa.Text()),
        sa.Column("rights_basis", sa.Text()),
        sa.Column("purpose", sa.Text()),
        sa.Column("review_status", sa.Text(), nullable=False, server_default="candidate"),
        sa.Column("reviewed_by", sa.Text()),
        sa.Column("reviewed_at", sa.DateTime(timezone=True)),
        sa.Column("observation_key", sa.Text(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("observation_key", name="ux_artist_media_provenance_observation"),
    )
    op.create_index("ix_artist_media_provenance_asset", "artist_media_provenance", ["asset_id", "source_kind"])
    op.create_index("ix_artist_media_provenance_event", "artist_media_provenance", ["event_id", "event_poster_id"])

    with op.batch_alter_table("event_artist_appearance") as batch:
        batch.add_column(sa.Column("selected_artist_media_asset_id", sa.Integer(), nullable=True))
        batch.create_foreign_key(
            "fk_event_artist_appearance_selected_media",
            "artist_media_asset",
            ["selected_artist_media_asset_id"],
            ["id"],
        )


def downgrade() -> None:
    with op.batch_alter_table("event_artist_appearance") as batch:
        batch.drop_constraint("fk_event_artist_appearance_selected_media", type_="foreignkey")
        batch.drop_column("selected_artist_media_asset_id")
    op.drop_table("artist_media_provenance")
    op.drop_table("artist_media_asset")
