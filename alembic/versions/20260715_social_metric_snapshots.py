"""Add compact hourly social metric snapshots and Telegram forwards."""

from alembic import op
import sqlalchemy as sa


revision = "20260715_social_metric_snapshots"
down_revision = "20260715_event_age_rating"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("telegram_post_metric") as batch:
        batch.add_column(sa.Column("forwards", sa.Integer(), nullable=True))

    op.create_table(
        "social_metric_snapshot",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("platform", sa.Text(), nullable=False),
        sa.Column("publisher_id", sa.Text(), nullable=False),
        sa.Column("post_id", sa.Integer(), nullable=False),
        sa.Column("age_bucket", sa.Text(), nullable=False),
        sa.Column(
            "publication_kind",
            sa.Text(),
            nullable=False,
            server_default="external_event_source",
        ),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("post_ts", sa.Integer(), nullable=True),
        sa.Column("collected_ts", sa.Integer(), nullable=False),
        sa.Column("views", sa.Integer(), nullable=True),
        sa.Column("likes", sa.Integer(), nullable=True),
        sa.Column("comments", sa.Integer(), nullable=True),
        sa.Column("shares", sa.Integer(), nullable=True),
        sa.Column("reactions_json", sa.JSON(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="collected"),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.UniqueConstraint(
            "platform",
            "publisher_id",
            "post_id",
            "age_bucket",
            name="ux_social_metric_post_bucket",
        ),
    )
    op.create_index(
        "ix_social_metric_due",
        "social_metric_snapshot",
        ["platform", "publisher_id", "post_id", "age_bucket", "status"],
    )
    op.create_index(
        "ix_social_metric_url",
        "social_metric_snapshot",
        ["source_url", "collected_ts"],
    )


def downgrade() -> None:
    op.drop_index("ix_social_metric_url", table_name="social_metric_snapshot")
    op.drop_index("ix_social_metric_due", table_name="social_metric_snapshot")
    op.drop_table("social_metric_snapshot")
    with op.batch_alter_table("telegram_post_metric") as batch:
        batch.drop_column("forwards")
