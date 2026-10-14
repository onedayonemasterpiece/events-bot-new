"""Bound redundant heartbeat history without removing durable outcomes."""
import logging


async def cleanup_terminal_heartbeats(db, *, retention_days=30, batch_size=1000):
    days = max(30, int(retention_days))
    limit = max(1, min(1000, int(batch_size)))
    async with db.raw_conn() as conn:
        cursor = await conn.execute(
            """
            DELETE FROM kaggle_run_event WHERE id IN (
                SELECT e.id FROM kaggle_run_event e
                JOIN kaggle_run_ledger r ON r.run_id=e.run_id
                WHERE e.event_name IN ('alive', 'heartbeat')
                  AND r.status IN ('done', 'complete')
                  AND r.terminal_at IS NOT NULL
                  AND julianday(r.terminal_at) < julianday('now', ?)
                  AND julianday(e.created_at) < julianday('now', ?)
                ORDER BY e.id LIMIT ?
            )
            """,
            (f"-{days} days", f"-{days} days", limit),
        )
        deleted = cursor.rowcount
        await conn.commit()
    logging.info("operational_retention terminal_heartbeats_deleted=%s retention_days=%s batch_limit=%s", deleted, days, limit)
    return deleted
