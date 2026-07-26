"""Add DB-backed public festival calendar editions."""

from alembic import op
import sqlalchemy as sa


revision = "20260726_festival_calendar_items"
down_revision = "20260718_event_image_geometry"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "festival_calendar_item",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("calendar_year", sa.Integer(), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("start_date", sa.Text()),
        sa.Column("end_date", sa.Text()),
        sa.Column("date_precision", sa.Text(), nullable=False, server_default="exact"),
        sa.Column("date_label", sa.Text(), nullable=False),
        sa.Column("sort_date", sa.Text(), nullable=False),
        sa.Column("month_key", sa.Text(), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=False),
        sa.Column("place_label", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("status_label", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_label", sa.Text(), nullable=False),
        sa.Column(
            "internal_event_id",
            sa.Integer(),
            sa.ForeignKey("event.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "festival_id",
            sa.Integer(),
            sa.ForeignKey("festival.id", ondelete="SET NULL"),
        ),
        sa.Column("cover_key", sa.Text(), nullable=False),
        sa.Column("image_width", sa.Integer(), nullable=False),
        sa.Column("image_height", sa.Integer(), nullable=False),
        sa.Column("media_mode", sa.Text(), nullable=False, server_default="visual"),
        sa.Column("object_position", sa.Text()),
        sa.Column("catalog_version", sa.Text(), nullable=False),
        sa.Column("is_public", sa.Boolean(), nullable=False, server_default=sa.true()),
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
            "calendar_year",
            "slug",
            name="ux_festival_calendar_item_year_slug",
        ),
        sa.UniqueConstraint(
            "calendar_year",
            "display_order",
            name="ux_festival_calendar_item_year_order",
        ),
    )
    op.create_index(
        "ix_festival_calendar_item_public_month",
        "festival_calendar_item",
        ["calendar_year", "is_public", "month_key", "display_order"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_festival_calendar_item_public_month",
        table_name="festival_calendar_item",
    )
    op.drop_table("festival_calendar_item")
