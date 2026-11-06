"""Import-boundary contrasts; provider extraction is verified separately live."""
import json
from pathlib import Path
import pytest
from db import Database
from models import TelegramSource
import smart_event_update as su
from source_parsing.telegram import handlers as tg

REPLAY=Path(__file__).parent/'replays/INC-2026-11-06-eco-garazhka-attribution/telegram_results.json'

@pytest.mark.parametrize('index,expected',[(0,'Гаражка'),(1,'ЭкоКёниг')])
def test_source_publisher_does_not_replace_explicit_event_identity(index,expected):
    message=json.loads(REPLAY.read_text())['messages'][index]
    candidate=tg._build_candidate(TelegramSource(username='ecoklgd',title='ЭкоКёниг'),message,message['events'][0])
    assert candidate.festival==expected
    assert expected in candidate.title

@pytest.mark.asyncio
async def test_replay_crosses_real_telegram_import_and_smart_update(tmp_path,monkeypatch,no_network):
    db=Database(str(tmp_path/'replay.sqlite'));await db.init()
    monkeypatch.setenv('SMART_UPDATE_SKIP_PAST_EVENTS','0')
    monkeypatch.setattr(su,'SMART_UPDATE_LLM_DISABLED',True)
    async def no_fallback(*args,**kwargs):return []
    async def no_topics(*args,**kwargs):return None
    monkeypatch.setattr(su,'_classify_topics',no_topics)
    monkeypatch.setattr(tg,'_fallback_fetch_posters_from_public_tg_page',no_fallback)
    original=su.smart_event_update
    async def real_without_publication(db_arg,candidate,**kwargs):
        kwargs['schedule_tasks']=False
        return await original(db_arg,candidate,**kwargs)
    monkeypatch.setattr(tg,'smart_event_update',real_without_publication)
    try:
        report=await tg.process_telegram_results(REPLAY,db)
        assert report.events_created==2
        assert report.events_errored==0
        async with db.raw_conn()as c:
            rows=await(await c.execute('select title,festival from event order by id')).fetchall()
            assert rows==[('Гаражка','Гаражка'),('Фестиваль «ЭкоКёниг»','ЭкоКёниг')]
    finally:await db.close()
