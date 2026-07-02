"""Add Telegram event publishing fields to event."""

from alembic import op
import sqlalchemy as sa


revision = "20260607_tg_event_publishing"
down_revision = "20260604_event_tg_source_author"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("event") as batch:
        batch.add_column(sa.Column("tg_event_post_url", sa.Text(), nullable=True))
        batch.add_column(sa.Column("tg_event_post_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("tg_event_post_mode", sa.Text(), nullable=True))
        batch.add_column(sa.Column("tg_event_source_hash", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("event") as batch:
        batch.drop_column("tg_event_source_hash")
        batch.drop_column("tg_event_post_mode")
        batch.drop_column("tg_event_post_id")
        batch.drop_column("tg_event_post_url")
