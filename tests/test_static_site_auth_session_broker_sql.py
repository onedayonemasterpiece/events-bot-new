from __future__ import annotations

import shutil
import subprocess
import time
import uuid
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
V1 = ROOT / "supabase/migrations/20260808094500_static_site_auth_session_claim_replay.sql"
V2 = ROOT / "supabase/migrations/20260809143602_static_site_auth_broker_platform_claims.sql"


def _docker(*args: str, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], input=input_text, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
    )


def test_migration_first_rollout_keeps_deployed_v1_claim_executable() -> None:
    """Execute the old RPC after the platform migration in disposable Postgres."""
    if not shutil.which("docker"):
        pytest.skip("docker is unavailable for the ephemeral PostgreSQL contract")
    try:
        _docker("info")
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("docker daemon is unavailable for the ephemeral PostgreSQL contract")

    container = f"search-broker-sql-{uuid.uuid4().hex[:12]}"
    try:
        _docker(
            "run", "--rm", "--detach", "--name", container,
            "--env", "POSTGRES_PASSWORD=postgres", "postgres:17-alpine",
        )
        ready_since = None
        for _ in range(80):
            ready = subprocess.run(
                ["docker", "exec", container, "pg_isready", "-U", "postgres"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if ready.returncode == 0:
                ready_since = ready_since or time.monotonic()
                if time.monotonic() - ready_since >= 1.0:
                    break
            else:
                ready_since = None
            time.sleep(0.25)
        else:
            pytest.fail("ephemeral PostgreSQL did not become ready")

        bootstrap = """
do $$ begin
  create role anon;
  create role authenticated;
  create role service_role;
exception when duplicate_object then null;
end $$;
create schema cron;
create table cron.job (
  jobid bigint generated always as identity primary key,
  jobname text not null unique,
  schedule text not null,
  command text not null
);
create function cron.schedule(p_jobname text, p_schedule text, p_command text)
returns bigint language plpgsql as $$
declare v_jobid bigint;
begin
  insert into cron.job(jobname, schedule, command)
  values (p_jobname, p_schedule, p_command)
  on conflict (jobname) do update
    set schedule = excluded.schedule, command = excluded.command
  returning jobid into v_jobid;
  return v_jobid;
end;
$$;
create table public.static_site_auth_session_issue_claim (
  run_id text not null,
  run_attempt integer not null check (run_attempt between 1 and 1000),
  persona_id text not null,
  repository text not null,
  workflow_ref text not null,
  claimed_at timestamptz not null default now(),
  expires_at timestamptz not null default (now() + interval '20 minutes'),
  primary key (run_id, run_attempt, persona_id)
);
"""
        _docker(
            "exec", "-i", container, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
            input_text=bootstrap,
        )
        for migration in (V1, V2):
            _docker(
                "exec", "-i", container, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
                input_text=migration.read_text(encoding="utf-8"),
            )

        v1_result = _docker(
            "exec", "-i", container, "psql", "-At", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
            input_text="""
select public.claim_static_site_auth_session_issue_v1(
  'same-run', 1, 'search-cached-browser', 'owner/repo', 'owner/repo/.github/workflows/search.yml@refs/heads/main', 1
);
select platform from public.static_site_auth_session_issue_claim where run_id = 'same-run';
select public.claim_static_site_auth_session_issue_v1(
  'same-run', 1, 'search-cached-browser', 'owner/repo', 'owner/repo/.github/workflows/search.yml@refs/heads/main', 1
);
""",
        )
        assert v1_result.stdout.splitlines() == ["t", "browser", "f"]
        _docker(
            "exec", "-i", container, "psql", "-q", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
            input_text="delete from public.static_site_auth_session_issue_claim;",
        )

        purpose_result = _docker(
            "exec", "-i", container, "psql", "-At", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
            input_text="""
select public.claim_static_site_auth_session_issue_v2(
  'purpose-run', 1, 'browser', 'search-cached-browser', 'owner/repo',
  'owner/repo/.github/workflows/search.yml@refs/heads/main', 1
)->>'claim';
select public.complete_static_site_auth_session_issue_v2(
  'purpose-run', 1, 'browser', 'search-cached-browser', 'owner/repo',
  'owner/repo/.github/workflows/search.yml@refs/heads/main', repeat('a', 80)
);
select public.claim_static_site_auth_session_issue_v2(
  'purpose-run', 1, 'browser', 'search-cold-browser', 'owner/repo',
  'owner/repo/.github/workflows/search.yml@refs/heads/main', 1
)->>'claim';
select public.complete_static_site_auth_session_issue_v2(
  'purpose-run', 1, 'browser', 'search-cold-browser', 'owner/repo',
  'owner/repo/.github/workflows/search.yml@refs/heads/main', repeat('b', 80)
);
select public.claim_static_site_auth_session_issue_v2(
  'purpose-run', 1, 'browser', 'search-cold-browser', 'owner/repo',
  'owner/repo/.github/workflows/search.yml@refs/heads/main', 1
)->>'claim';
select pg_catalog.count(*) from public.static_site_auth_session_issue_claim
where run_id = 'purpose-run';
update public.static_site_auth_session_issue_claim
set credential_expires_at = pg_catalog.now() - interval '1 second'
where run_id = 'purpose-run';
select public.cleanup_static_site_auth_session_issue_credentials_v1();
select pg_catalog.bool_and(credential_ciphertext is null)
from public.static_site_auth_session_issue_claim where run_id = 'purpose-run';
select schedule || '|' || command from cron.job
where jobname = 'static-site-auth-session-credential-cleanup';
""",
        )
        assert purpose_result.stdout.splitlines() == [
            "new", "t", "new", "t", "replay", "2", "UPDATE 2", "2", "t",
            "* * * * *|select public.cleanup_static_site_auth_session_issue_credentials_v1()",
        ]

        _docker(
            "exec", "-i", container, "psql", "-q", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
            input_text="delete from public.static_site_auth_session_issue_claim;",
        )
        identity_result = _docker(
            "exec", "-i", container, "psql", "-At", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
            input_text="""
select public.claim_static_site_auth_session_issue_v2(
  'identity-run', 1, 'browser', 'same-persona', 'owner/repo-a', 'owner/repo-a/w.yml@refs/heads/main', 3
)->>'claim';
select public.claim_static_site_auth_session_issue_v2(
  'identity-run', 1, 'browser', 'same-persona', 'owner/repo-b', 'owner/repo-b/w.yml@refs/heads/main', 3
)->>'claim';
select pg_catalog.count(*) from public.static_site_auth_session_issue_claim;
delete from public.static_site_auth_session_issue_claim;
select public.claim_static_site_auth_session_issue_v2(
  'identity-run', 1, 'browser', 'same-persona', 'owner/repo', 'owner/repo/a.yml@refs/heads/main', 3
)->>'claim';
select public.claim_static_site_auth_session_issue_v2(
  'identity-run', 1, 'browser', 'same-persona', 'owner/repo', 'owner/repo/b.yml@refs/heads/main', 3
)->>'claim';
select pg_catalog.count(*) from public.static_site_auth_session_issue_claim;
delete from public.static_site_auth_session_issue_claim;
select public.claim_static_site_auth_session_issue_v2(
  'identity-run', 1, 'browser', 'same-persona', 'owner/repo', 'owner/repo/w.yml@refs/heads/main', 3
)->>'claim';
select public.claim_static_site_auth_session_issue_v2(
  'identity-run', 1, 'android', 'same-persona', 'owner/repo', 'owner/repo/w.yml@refs/heads/main', 3
)->>'claim';
select pg_catalog.count(*) from public.static_site_auth_session_issue_claim;
""",
        )
        assert identity_result.stdout.splitlines() == [
            "new", "new", "2", "DELETE 2",
            "new", "new", "2", "DELETE 2",
            "new", "new", "2",
        ]
    finally:
        subprocess.run(
            ["docker", "rm", "--force", container],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
