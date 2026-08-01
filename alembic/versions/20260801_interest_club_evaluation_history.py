"""Preserve hash-versioned interest-club evaluation history."""

from alembic import op


revision = "20260801_club_eval_history"
down_revision = "20260801_static_collection_facts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ``recreate=always`` gives SQLite a deterministic table-copy migration;
    # Alembic copies every existing row before replacing the pair uniqueness.
    with op.batch_alter_table(
        "interest_club_evaluation", recreate="always"
    ) as batch_op:
        batch_op.drop_constraint(
            "ux_interest_club_evaluation_pair", type_="unique"
        )
        batch_op.create_unique_constraint(
            "ux_interest_club_evaluation_history",
            ["club_id", "event_id", "policy_version", "input_hash"],
        )


def downgrade() -> None:
    # A legacy schema can retain only one decision per club/event. Keep the
    # most recently inserted history row instead of making downgrade fail.
    op.execute(
        """
        DELETE FROM interest_club_evaluation
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM interest_club_evaluation
            GROUP BY club_id, event_id
        )
        """
    )
    with op.batch_alter_table(
        "interest_club_evaluation", recreate="always"
    ) as batch_op:
        batch_op.drop_constraint(
            "ux_interest_club_evaluation_history", type_="unique"
        )
        batch_op.create_unique_constraint(
            "ux_interest_club_evaluation_pair", ["club_id", "event_id"]
        )
