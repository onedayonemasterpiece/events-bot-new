"""Add declared and assessed event age-rating fields."""

from alembic import op
import sqlalchemy as sa


revision = "20260715_event_age_rating"
down_revision = "20260714_event_ics_post_hash"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("event") as batch:
        batch.add_column(sa.Column("age_restriction", sa.Text(), nullable=True))
        batch.add_column(sa.Column("age_restriction_status", sa.Text(), nullable=False, server_default="unknown"))
        batch.add_column(sa.Column("age_restriction_provenance", sa.Text(), nullable=True))
        batch.add_column(sa.Column("age_restriction_source_url", sa.Text(), nullable=True))
        batch.add_column(sa.Column("age_restriction_confidence", sa.Float(), nullable=True))
        batch.add_column(sa.Column("age_restriction_evidence", sa.JSON(), nullable=True))
        batch.add_column(sa.Column("age_restriction_decision_version", sa.Text(), nullable=True))
        batch.add_column(sa.Column("age_restriction_input_hash", sa.Text(), nullable=True))
        batch.add_column(sa.Column("age_restriction_updated_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("age_assessment", sa.Text(), nullable=True))
        batch.add_column(sa.Column("age_assessment_provenance", sa.Text(), nullable=True))
        batch.add_column(sa.Column("age_assessment_confidence", sa.Float(), nullable=True))
        batch.add_column(sa.Column("age_assessment_evidence", sa.JSON(), nullable=True))
        batch.add_column(sa.Column("age_assessment_decision_version", sa.Text(), nullable=True))
        batch.add_column(sa.Column("age_assessment_input_hash", sa.Text(), nullable=True))
        batch.add_column(sa.Column("age_assessment_engine", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("event") as batch:
        for name in (
            "age_assessment_engine",
            "age_assessment_input_hash",
            "age_assessment_decision_version",
            "age_assessment_evidence",
            "age_assessment_confidence",
            "age_assessment_provenance",
            "age_assessment",
            "age_restriction_updated_at",
            "age_restriction_input_hash",
            "age_restriction_decision_version",
            "age_restriction_evidence",
            "age_restriction_confidence",
            "age_restriction_source_url",
            "age_restriction_provenance",
            "age_restriction_status",
            "age_restriction",
        ):
            batch.drop_column(name)
