"""Track Telegram calendar document content independently from storage ICS."""

from alembic import op
import sqlalchemy as sa


revision = "20260714_event_ics_post_hash"
down_revision = "20260607_tg_event_publishing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("event") as batch:
        batch.add_column(sa.Column("ics_post_hash", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("event") as batch:
        batch.drop_column("ics_post_hash")
