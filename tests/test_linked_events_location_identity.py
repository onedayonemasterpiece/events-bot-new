from __future__ import annotations

import pytest
from db import Database
from models import Event
from linked_events import recompute_linked_event_ids


@pytest.mark.asyncio
async def test_source_grounded_address_keeps_course_dates_across_venue_labels(tmp_path):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    try:
        async with db.get_session() as session:
            for eid, title, venue, address, city in [
                (1, 'Занятие курса сценической импровизации', 'Просторный светлый зал', 'Гаражная 2', 'Калининград'),
                (2, 'Занятие курса сценической импровизации', 'Винный факультет', 'Гаражная 2', 'Калининград'),
                (3, 'Другая программа', 'Винный факультет', 'Гаражная 2', 'Калининград'),
                (4, 'Занятие курса сценической импровизации', 'Другой зал', 'Гаражная 2', 'Другой город'),
                (5, 'Занятие курса сценической импровизации', 'Другой зал', 'Гаражная 4', 'Калининград'),
                (6, 'Занятие курса сценической импровизации', 'Другой зал', None, 'Калининград'),
                (7, 'Занятие курса сценической импровизации', 'Другой зал', 'Гаражная 2', ''),
            ]:
                session.add(Event(id=eid, title=title, description='', source_text='', date=f'2099-11-{eid:02d}',
                    time='19:00', location_name=venue, location_address=address, city=city))
            await session.commit()
        result = await recompute_linked_event_ids(db, 1)
        assert result.group_event_ids == [1, 2]
        async with db.get_session() as session:
            assert (await session.get(Event, 1)).linked_event_ids == [2]
            assert (await session.get(Event, 2)).linked_event_ids == [1]
            assert (await session.get(Event, 3)).linked_event_ids == []
        # Recomputing from the alias must keep the same reciprocal family.
        assert (await recompute_linked_event_ids(db, 2)).group_event_ids == [1, 2]
    finally:
        await db.close()
