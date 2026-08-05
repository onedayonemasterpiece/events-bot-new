#!/usr/bin/env bash
set -euo pipefail

copy_from() {
  local commit="$1"
  local path="$2"
  git cat-file -e "${commit}^{commit}"
  git cat-file -e "${commit}:${path}"
  mkdir -p "$(dirname "$path")"
  git show "${commit}:${path}" > "$path"
}

HERO=52f3afe73acb4bde2a6983500a534fd74d5a4116
copy_from "$HERO" docs/features/hero-talk/README.md
copy_from "$HERO" docs/features/hero-talk/deep-research-prompt.md
copy_from "$HERO" docs/features/hero-talk/release-plan.md
copy_from "$HERO" docs/features/hero-talk/testing.md
copy_from "$HERO" docs/features/static-site-pages/hero-talk-release-track.md

KEYBOARD=787eb1fb077e079166477b58e5affb5f4e5ce2ba
copy_from "$KEYBOARD" docs/features/static-site-pages/keyboard-event-navigation-v8-product-model.md
copy_from "$KEYBOARD" docs/features/static-site-pages/keyboard-event-navigation-v8-onboarding-artifacts.md
copy_from "$KEYBOARD" docs/testing/keyboard-event-navigation-scenarios.v2.yml

VOLUNTEER=dcba48a4bf194e5fcce253e80e081f55c474538a
copy_from "$VOLUNTEER" docs/features/static-site-pages/volunteer-recruitment/README.md
copy_from "$VOLUNTEER" docs/features/static-site-pages/volunteer-recruitment/implementation-handoff.md
copy_from "$VOLUNTEER" docs/features/static-site-pages/volunteer-recruitment/test-plan.md

P13N_PROFILE=5c3cabe45937e29e1f7c2f3660a52419149b9e18
copy_from "$P13N_PROFILE" docs/features/static-site-pages/personalizaion/transport-ecology-profile-architecture.md
copy_from "$P13N_PROFILE" docs/features/static-site-pages/user-profile.md
copy_from "$P13N_PROFILE" docs/testing/personalization-transport-profile-test-plan.md

P13N_EXT=ad466f9643a25fe9b11c9d135278189f3ba5db61
copy_from "$P13N_EXT" docs/features/static-site-pages/personalizaion/focus-group-interest-questionnaire-prompt.md
copy_from "$P13N_EXT" docs/features/static-site-pages/personalizaion/identity-linking-personalization.md
copy_from "$P13N_EXT" docs/features/static-site-pages/personalizaion/longitudinal-e2e-personalization.md
copy_from "$P13N_EXT" docs/features/static-site-pages/personalizaion/personalization-test-report-template.md
copy_from "$P13N_EXT" docs/features/static-site-pages/personalizaion/temporal-profile-simulation.md

LEGAL=e6e3dbcd0dfe9b7a0e41db8078395f7c860c41c2
copy_from "$LEGAL" docs/features/static-site-pages/personalization-legal-release-gate-rf.md

REMINDERS=9a3a7cb2027e0379f4ac2ae6651420fa830e45d9
copy_from "$REMINDERS" docs/features/static-site-pages/event-reminders-calendar-strategy.md
copy_from "$REMINDERS" docs/features/static-site-pages/calendar-reminder-strategy.md
copy_from "$REMINDERS" docs/testing/event-reminders-calendar-e2e.md
copy_from "$REMINDERS" docs/features/static-site-focus-group/event-reminders-acceptance.md

python3 - <<'PY'
from pathlib import Path
path = Path('docs/routes.yml')
text = path.read_text(encoding='utf-8')
block = (
    '\n\ndocumentation_governance:\n'
    '  to_be_consolidation: docs/features/static-site-pages/to-be-documentation-consolidation.md\n'
    '  branch_auditor: scripts/audit_to_be_documentation.py\n'
    '  branch_audit_workflow: .github/workflows/to-be-documentation-audit.yml\n'
    '  static_site_analytics: docs/features/static-site-pages/analytics/README.md\n'
    '  analytics_storage_retention: docs/features/static-site-pages/analytics/storage-retention-architecture.md\n'
    '  analytics_product_measurement: docs/features/static-site-pages/analytics/product-measurement-extension.md\n'
    '  focus_group_current_contract: docs/features/static-site-pages/focus-group.md\n'
    '  auth_session_fixture: docs/testing/static-site-auth-session-fixture.md\n'
    '  hero_talk: docs/features/hero-talk/README.md\n'
    '  keyboard_navigation_v8: docs/features/static-site-pages/keyboard-event-navigation-v8-product-model.md\n'
    '  volunteer_recruitment: docs/features/static-site-pages/volunteer-recruitment/README.md\n'
    '  personalization_transport_profile: docs/features/static-site-pages/personalizaion/transport-ecology-profile-architecture.md\n'
    '  personalization_legal_gate: docs/features/static-site-pages/personalization-legal-release-gate-rf.md\n'
    '  event_reminders_target: docs/features/static-site-pages/event-reminders-calendar-strategy.md\n'
)
if 'documentation_governance:\n' not in text:
    marker = '\nartifacts:\n'
    if marker not in text:
        raise SystemExit('docs/routes.yml artifacts marker missing')
    text = text.replace(marker, block + marker, 1)
    path.write_text(text, encoding='utf-8')
PY

python3 -m py_compile scripts/audit_to_be_documentation.py
python3 - <<'PY'
from pathlib import Path
required = [
    'docs/features/static-site-pages/to-be-documentation-consolidation.md',
    'docs/features/static-site-pages/analytics/storage-retention-architecture.md',
    'docs/features/static-site-pages/analytics/product-measurement-extension.md',
    'docs/features/static-site-pages/focus-group.md',
    'docs/testing/static-site-auth-session-fixture.md',
    'docs/features/hero-talk/README.md',
    'docs/features/static-site-pages/keyboard-event-navigation-v8-product-model.md',
    'docs/testing/keyboard-event-navigation-scenarios.v2.yml',
    'docs/features/static-site-pages/volunteer-recruitment/README.md',
    'docs/features/static-site-pages/personalizaion/transport-ecology-profile-architecture.md',
    'docs/features/static-site-pages/personalization-legal-release-gate-rf.md',
    'docs/features/static-site-pages/event-reminders-calendar-strategy.md',
]
missing = [item for item in required if not Path(item).is_file()]
if missing:
    raise SystemExit(f'missing canonical paths: {missing}')
if 'documentation_governance:' not in Path('docs/routes.yml').read_text(encoding='utf-8'):
    raise SystemExit('route registration missing')
PY
ruby -e "require 'yaml'; YAML.load_file('docs/routes.yml')"

python3 - <<'PY'
from pathlib import Path
paths = [
    'docs/features/static-site-pages/focus-group-release/README.md',
    'docs/features/static-site-pages/focus-group-release/nps-ui.md',
    'docs/features/static-site-pages/focus-group-release/prize-rules.md',
    'docs/features/static-site-pages/focus-group-release/status.md',
    'docs/features/static-site-pages/focus-group.md',
    'docs/testing/static-site-auth-session-fixture.md',
]
for name in paths:
    path = Path(name)
    lines = path.read_text(encoding='utf-8').splitlines()
    path.write_text('\n'.join(line.rstrip() for line in lines) + '\n', encoding='utf-8')
PY

git diff --check
rm -f .github/to-be-consolidation-payload-*.b64
rm -f .github/workflows/to-be-consolidation-materialize-temp.yml
rm -f .github/workflows/to-be-consolidation-materialize-v2.yml
rm -f scripts/materialize_to_be_consolidation.sh
