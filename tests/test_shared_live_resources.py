import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('kenig_live_resources',Path(__file__).resolve().parents[1]/'google_ai/live_resources.py')
resources=importlib.util.module_from_spec(spec);spec.loader.exec_module(resources)

class SharedLiveResources(unittest.IsolatedAsyncioTestCase):
    async def test_authorized_binding_and_controller_selected(self):
        calls=[]
        async def guard(**kwargs):calls.append(kwargs)
        with patch.dict(sys.modules,{'ai_resource_control':types.SimpleNamespace(run_guarded=guard)}):
            await resources.run_live_search(authorized_session_id='fixture_session_123',environment={},reader=object(),on_event=lambda e:None)
        self.assertEqual(calls[0]['consumer'],'kenigevents')
        self.assertEqual(len(calls[0]['binding']),64)
        self.assertNotEqual(calls[0]['binding'],'fixture_session_123')
        self.assertNotIn('load_key',calls[0])
    async def test_unscoped_browser_input_rejected(self):
        with self.assertRaises(ValueError):
            await resources.run_live_search(authorized_session_id='user',environment={},reader=object(),on_event=lambda e:None)
    async def test_missing_package_no_fallback(self):
        events=[]
        with patch.dict(sys.modules,{'ai_resource_control':None}):
            await resources.run_live_search(authorized_session_id='fixture_session_123',environment={'LIVE_API_KEY':'fixture'},reader=object(),on_event=events.append)
        self.assertEqual(events[0]['code'],'RESOURCE_PACKAGE_MISSING')
if __name__=='__main__':unittest.main()
