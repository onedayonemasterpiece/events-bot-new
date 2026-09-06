"""Exercise the actual Kaggle env writer without starting a notebook."""
import ast
import os
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
TREE = ast.parse((ROOT / 'kaggle/StaticSiteBuilder/static_site_builder.py').read_text())
FUNCTION = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == 'apply_public_authorized_search_env')
NAMESPACE = {'os': os}
exec(compile(ast.Module(body=[FUNCTION], type_ignores=[]), 'actual-kaggle-public-env-writer', 'exec'), NAMESPACE)
apply_env = NAMESPACE['apply_public_authorized_search_env']

class HomePreviewPublicEnvTests(unittest.TestCase):
    def test_explicit_preview_gates_without_secrets(self):
        env = {}
        with patch.dict(os.environ, {'PERSONALIZATION_SUPABASE_SECRET_KEY': 'do-not-expose'}, clear=True):
            apply_env(env, {'profile': 'preview', 'public_event_search_assistant_enabled': '1', 'public_event_search_assistant_host': 'devcoveer'})
        self.assertEqual(env['PUBLIC_EVENT_SEARCH_ASSISTANT_ENABLED'], '1')
        self.assertEqual(env['PUBLIC_EVENT_SEARCH_ASSISTANT_HOST'], 'devcoveer')
        self.assertNotIn('do-not-expose', env.values())

    def test_no_implicit_enable_and_no_production_enable(self):
        for config in ({}, {'profile': 'preview'}, {'profile': 'production-candidate', 'public_event_search_assistant_enabled': '1'}):
            env = {}
            with patch.dict(os.environ, {}, clear=True):
                apply_env(env, config)
            self.assertEqual(env['PUBLIC_EVENT_SEARCH_ASSISTANT_ENABLED'], '0')

    def test_invalid_host_and_nonboolean_are_rejected(self):
        for config in ({'public_event_search_assistant_host': 'https://untrusted.invalid'}, {'public_event_search_assistant_enabled': 'true'}, {'public_event_search_assistant_capture_only': 'yes'}):
            with self.assertRaisesRegex(RuntimeError, 'invalid public assistant'):
                apply_env({}, config)

if __name__ == '__main__':
    unittest.main()
