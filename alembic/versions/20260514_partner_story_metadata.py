"""Persist partner-track story metadata on videoannounce_session.

Revision ID: 20260514_partner_story_metadata
"""
from alembic import op
import sqlalchemy as sa


revision = "20260514_partner_story_metadata"
down_revision = "20260217_telegram_source_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("videoannounce_session", schema=None) as batch_op:
        batch_op.add_column(sa.Column("partner_track_id", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("partner_story_id", sa.String(), nullable=True))
        batch_op.add_column(
            sa.Column("partner_story_connection_hash", sa.String(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "partner_story_deleted_at",
                sa.DateTime(timezone=True),
                nullable=True,
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("videoannounce_session", schema=None) as batch_op:
        batch_op.drop_column("partner_story_deleted_at")
        batch_op.drop_column("partner_story_connection_hash")
        batch_op.drop_column("partner_story_id")
        batch_op.drop_column("partner_track_id")
