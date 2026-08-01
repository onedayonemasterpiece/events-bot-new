"""Add nullable source-bound static collection factual decisions."""

import sqlalchemy as sa

from alembic import op


revision = "20260801_static_collection_facts"
down_revision = "20260731_festival_web_research"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "event",
        sa.Column("collection_decisions", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("event", "collection_decisions")
