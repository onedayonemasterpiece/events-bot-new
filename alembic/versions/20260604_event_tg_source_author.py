"""Add tg_source_author to event (Telegram chat post author for promo trigger)."""

from alembic import op
import sqlalchemy as sa


revision = "20260604_event_tg_source_author"
down_revision = "20260603_promo_activity_config_json"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("event") as batch:
        batch.add_column(sa.Column("tg_source_author", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("event") as batch:
        batch.drop_column("tg_source_author")
