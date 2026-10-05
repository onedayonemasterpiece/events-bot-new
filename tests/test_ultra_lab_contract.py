"""No-network safety/contract regressions for the Ultra research harness."""
import sys,unittest
from pathlib import Path
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'/'inspect'))
from ultra_review_receipts import check_carrier
from ultra_assemble_albums import observed_bounds

class ContractTests(unittest.TestCase):
    def carrier(self,**kw):
        d={'disposition':'EVENTS_FOUND','evidence_complete':True,'events':[{'title':'demo'}],
           'lifecycle_actions':[],'ocr_blocks':[{'image_id':'a','text':'','unreadable':False}]};d.update(kw);return d
    def test_text_free_photo_is_not_unreadable(self):
        self.assertEqual([],check_carrier(self.carrier(),vision=True,expected_images=1))
    def test_unknown_disposition(self):
        self.assertIn('unknown_disposition',check_carrier(self.carrier(disposition='supported_with_unresolved_session_dates')))
    def test_events_plus_actions_requires_mixed(self):
        self.assertIn('disposition_content_mismatch',check_carrier(self.carrier(lifecycle_actions=[{'kind':'UPDATE_DETAILS'}])))
    def test_canonical_time_change(self):
        self.assertEqual([],check_carrier(self.carrier(disposition='MIXED',lifecycle_actions=[{'kind':'RESCHEDULE_TIME'}])))
    def test_noncanonical_time_change(self):
        for value in ('reschedule','time_changed'):
            with self.subTest(value=value):self.assertIn('action[0].noncanonical_kind',check_carrier(self.carrier(disposition='MIXED',lifecycle_actions=[{'kind':value}])))
    def test_missing_evidence_not_negative_success(self):
        self.assertIn('no_event_requires_complete_evidence',check_carrier({'disposition':'CONFIRMED_NO_EVENT','events':[],'lifecycle_actions':[]}))
    def test_incomplete_negative_rejected(self):
        self.assertIn('no_event_requires_complete_evidence',check_carrier(self.carrier(disposition='CONFIRMED_NO_EVENT',events=[],evidence_complete=False)))
    def test_complete_negative_structural_pass(self):
        self.assertEqual([],check_carrier(self.carrier(disposition='CONFIRMED_NO_EVENT',events=[])))
    def test_partial_positive_preserved(self):
        self.assertEqual([],check_carrier(self.carrier(evidence_complete=False),vision=True,expected_images=1,input_incomplete=True))
    def test_incomplete_input_cannot_be_promoted(self):
        self.assertIn('input_incompleteness_lost',check_carrier(self.carrier(),input_incomplete=True))
    def test_ocr_blocks_required(self):
        self.assertIn('ocr_image_count_mismatch',check_carrier(self.carrier(ocr_blocks=[]),vision=True,expected_images=1))
    def test_ocr_duplicate_id(self):
        x=self.carrier();x['ocr_blocks']*=2
        self.assertIn('duplicate_ocr_image_id',check_carrier(x,vision=True,expected_images=2))
    def test_observed_v2_support_type_drift(self):
        x=self.carrier(disposition='MIXED',lifecycle_actions=[{'kind':'UPDATE_DETAILS','support':['quote']}])
        self.assertIn('action[0].support_string_required',check_carrier(x,vision=True,expected_images=1))
    def test_type_bool_is_not_truthy_integer(self):
        self.assertIn('evidence_complete_boolean_required',check_carrier(self.carrier(evidence_complete=1),vision=True,expected_images=1))
    def test_wrong_container(self):self.assertEqual(['object_required'],check_carrier([]))
    def test_lifecycle_only(self):
        self.assertEqual([],check_carrier(self.carrier(disposition='LIFECYCLE_ONLY',events=[],lifecycle_actions=[{'kind':'CANCEL'}])))

class AlbumTests(unittest.TestCase):
    def msg(self,i,g=None):return SimpleNamespace(id=i,grouped_id=g)
    def test_both_neighbors(self):self.assertEqual((True,[99,103]),observed_bounds([self.msg(99),self.msg(100,7),self.msg(102,7),self.msg(103)],'7'))
    def test_missing_before(self):self.assertFalse(observed_bounds([self.msg(100,7),self.msg(101)],'7')[0])
    def test_missing_after(self):self.assertFalse(observed_bounds([self.msg(99),self.msg(100,7)],'7')[0])
    def test_not_found(self):self.assertEqual((False,[]),observed_bounds([self.msg(99)],'7'))
    def test_sparse_ids_are_not_missing_album(self):self.assertTrue(observed_bounds([self.msg(90),self.msg(100,7),self.msg(108,7),self.msg(115)],'7')[0])

if __name__=='__main__':unittest.main(verbosity=2)
