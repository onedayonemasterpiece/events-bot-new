"""Add versioned, content-addressed image geometry cache."""

from alembic import op
import sqlalchemy as sa


revision = "20260718_event_image_geometry"
down_revision = "20260717_interest_clubs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "event_image_geometry",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("pixel_sha256", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="classified"),
        sa.Column("source_width", sa.Integer()),
        sa.Column("source_height", sa.Integer()),
        sa.Column("face_boxes_yxyx_json", sa.JSON()),
        sa.Column("valuable_region_yxyx_json", sa.JSON()),
        sa.Column("valuable_region_confidence", sa.Float()),
        sa.Column("reason_code", sa.Text()),
        sa.Column("prompt_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("completion_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("analyzed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint(
            "pixel_sha256", "model", "prompt_version", name="ux_event_image_geometry_version"
        ),
    )
    op.create_index(
        "ix_event_image_geometry_status", "event_image_geometry", ["status", "updated_at"]
    )
    with op.batch_alter_table("eventposter") as batch:
        batch.add_column(sa.Column("image_geometry_id", sa.Integer(), nullable=True))
        batch.create_foreign_key(
            "fk_eventposter_image_geometry",
            "event_image_geometry",
            ["image_geometry_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch.create_index("ix_eventposter_image_geometry", ["image_geometry_id"])
        batch.create_index("ix_eventposter_pixel_sha256_global", ["pixel_sha256"])


def downgrade() -> None:
    with op.batch_alter_table("eventposter") as batch:
        batch.drop_index("ix_eventposter_pixel_sha256_global")
        batch.drop_index("ix_eventposter_image_geometry")
        batch.drop_constraint("fk_eventposter_image_geometry", type_="foreignkey")
        batch.drop_column("image_geometry_id")
    op.drop_index("ix_event_image_geometry_status", table_name="event_image_geometry")
    op.drop_table("event_image_geometry")
