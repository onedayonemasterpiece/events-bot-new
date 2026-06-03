"""Add config_json to promo_activity."""

from alembic import op
import sqlalchemy as sa


revision = "20260603_promo_activity_config_json"
down_revision = "20260514_partner_story_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("promo_activity") as batch:
        batch.add_column(
            sa.Column(
                "config_json",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'{}'"),
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("promo_activity") as batch:
        batch.drop_column("config_json")
