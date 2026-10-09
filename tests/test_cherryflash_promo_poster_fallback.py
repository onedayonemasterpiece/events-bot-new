"""Regression for verified CherryFlash promo scenes with missing poster OCR."""
import json
import pytest
from video_announce import poster_overlay
from video_announce.scenario import VideoAnnounceScenario

POSTER="https://static.kenigevents.ru/p/image/v2/verified.webp"

def scene(event_id, *, title="Форум «Мужская формула»",
          date="13 октября 12:30", location="Калининград, Дом молодежи",
          images=None):
    return {"event_id":event_id,"scene_variant":"primary","title":title,
            "date":date,"location":location,
            "images":[POSTER] if images is None else images}

@pytest.mark.asyncio
async def test_valid_campaign_scenes_survive_absent_ocr_and_enter_render_manifest(monkeypatch):
    async def no_ocr(_db,_ids):
        return {}
    monkeypatch.setattr(poster_overlay,"_load_latest_ocr_texts",no_ocr)
    payload={"selection_params":{"mode":"popular_review","allow_empty_ocr":False},
             "intro":{"count":2},"scenes":[scene(9671),scene(9684)]}
    enriched=json.loads(await poster_overlay.enrich_payload_with_poster_overlays(
        object(),json.dumps(payload,ensure_ascii=False)))
    assert [s["event_id"] for s in enriched["scenes"]]==[9671,9684]
    assert enriched["intro"]["count"]==2
    for s in enriched["scenes"]:
        assert s["images"]==[POSTER]
        assert s["poster_overlay"]["model"]=="empty_ocr_fallback"
        assert "13 октября" in s["poster_overlay"]["text"]
    manifest=VideoAnnounceScenario._build_cherryflash_selection_manifest(
        None,enriched,selection_params={"profile_key":"popular_review"},
        story_publish_enabled=False)
    assert manifest["selected_event_ids"]==[9671,9684]
    assert len(manifest["events"])==2

@pytest.mark.asyncio
async def test_unverified_empty_ocr_scenes_still_fail_closed(monkeypatch):
    async def no_ocr(_db,_ids):
        return {}
    monkeypatch.setattr(poster_overlay,"_load_latest_ocr_texts",no_ocr)
    payload={"selection_params":{"allow_empty_ocr":False},
             "intro":{"count":3},
             "scenes":[scene(9671),
                       scene(22,images=[]),
                       scene(23,title="",date="",location="",images=[])]}
    enriched=json.loads(await poster_overlay.enrich_payload_with_poster_overlays(
        object(),json.dumps(payload,ensure_ascii=False)))
    assert [s["event_id"] for s in enriched["scenes"]]==[9671]
    assert enriched["intro"]["count"]==1

@pytest.mark.asyncio
async def test_existing_allow_empty_ocr_partner_policy_is_unchanged(monkeypatch):
    async def no_ocr(_db,_ids):
        return {}
    monkeypatch.setattr(poster_overlay,"_load_latest_ocr_texts",no_ocr)
    payload={"selection_params":{"allow_empty_ocr":True},
             "intro":{"count":1},
             "scenes":[scene(123,title="",date="",location="",images=[])]}
    enriched=json.loads(await poster_overlay.enrich_payload_with_poster_overlays(
        object(),json.dumps(payload,ensure_ascii=False)))
    assert len(enriched["scenes"])==1
    assert enriched["intro"]["count"]==1
