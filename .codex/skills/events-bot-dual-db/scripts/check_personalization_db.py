#!/usr/bin/env python3
"""Redacted health/size check for the personalization Supabase/Postgres DB."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parents[4]
ARTIFACT_DRIVER_DIR = ROOT / "artifacts" / "codex" / "tmp_pg_driver"
ENV_PATH = ROOT / ".env"
CONN_KEY = "PERSONALIZATION_DIRECT_CONNECTION_STRING"


def _load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(errors="ignore").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def _install_driver() -> None:
    ARTIFACT_DRIVER_DIR.mkdir(parents=True, exist_ok=True)
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--quiet",
            "--target",
            str(ARTIFACT_DRIVER_DIR),
            "psycopg[binary]~=3.2",
        ]
    )


def _import_psycopg():
    if ARTIFACT_DRIVER_DIR.exists():
        sys.path.insert(0, str(ARTIFACT_DRIVER_DIR))
    try:
        import psycopg  # type: ignore
    except Exception as exc:
        raise SystemExit(
            "psycopg is not available. Run with --install-driver first, or install psycopg separately. "
            f"Original error: {exc}"
        ) from exc
    return psycopg


def _redacted_conn_summary(conninfo: str) -> dict[str, object]:
    parsed = urlparse(conninfo)
    password = parsed.password or ""
    return {
        "scheme": parsed.scheme,
        "username": parsed.username,
        "password_present": bool(password),
        "password_len": len(password),
        "hostname": parsed.hostname,
        "port": parsed.port,
        "database_path": parsed.path,
        "query": parse_qs(parsed.query),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--install-driver", action="store_true", help="install psycopg into artifacts/codex/tmp_pg_driver")
    parser.add_argument("--env", default=str(ENV_PATH), help="dotenv path, default repo .env")
    args = parser.parse_args()

    if args.install_driver:
        _install_driver()

    env = _load_dotenv(Path(args.env))
    conninfo = env.get(CONN_KEY) or os.getenv(CONN_KEY) or ""
    if not conninfo:
        raise SystemExit(f"Missing {CONN_KEY} in {args.env} or environment")

    psycopg = _import_psycopg()
    result: dict[str, object] = {
        "connection": _redacted_conn_summary(conninfo),
    }
    query = """
    select
      current_database() as database_name,
      current_user as current_user,
      pg_database_size(current_database()) as database_bytes,
      pg_size_pretty(pg_database_size(current_database())) as database_pretty,
      version() as version
    """
    with psycopg.connect(conninfo, connect_timeout=20, options="-c statement_timeout=15000") as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            db_bytes = int(row[2])
            result.update(
                {
                    "database_name": row[0],
                    "current_user": row[1],
                    "database_bytes": db_bytes,
                    "database_pretty": row[3],
                    "free_vs_500MB_decimal_bytes": 500_000_000 - db_bytes,
                    "free_vs_500MB_decimal_mb": round((500_000_000 - db_bytes) / 1_000_000, 2),
                    "free_vs_500MiB_bytes": 500 * 1024 * 1024 - db_bytes,
                    "free_vs_500MiB_mib": round((500 * 1024 * 1024 - db_bytes) / (1024 * 1024), 2),
                    "version_prefix": str(row[4]).split(",")[0][:160],
                }
            )
            cur.execute(
                """
                select table_schema, count(*)
                from information_schema.tables
                where table_type='BASE TABLE'
                group by table_schema
                order by table_schema
                """
            )
            result["base_table_count_by_schema"] = [
                {"schema": schema, "tables": count} for schema, count in cur.fetchall()
            ]
            cur.execute("select extname from pg_extension order by extname")
            result["extensions"] = [row[0] for row in cur.fetchall()]
            cur.execute(
                """
                select schemaname, tablename,
                       pg_total_relation_size(format('%I.%I', schemaname, tablename)) as bytes,
                       pg_size_pretty(pg_total_relation_size(format('%I.%I', schemaname, tablename))) as pretty
                from pg_tables
                where schemaname not in ('pg_catalog','information_schema')
                order by bytes desc
                limit 20
                """
            )
            result["top_relations"] = [
                {"schema": s, "table": t, "bytes": b, "pretty": p}
                for s, t, b, p in cur.fetchall()
            ]

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
