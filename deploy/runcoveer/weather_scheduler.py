"""Keep scheduled weather delivery independent of serial cache refreshes."""
import asyncio
import logging
import time


async def schedule_loop(bot, *, interval=30, collection_timeout=120):
    async def refresh():
        while bot.running:
            started = time.monotonic()
            try:
                async def collect():
                    await bot.collect_weather()
                    await bot.collect_sea()
                await asyncio.wait_for(collect(), timeout=collection_timeout)
                logging.info("weather_collection_completed duration_s=%.1f", time.monotonic() - started)
            except asyncio.TimeoutError:
                logging.warning("weather_collection_timeout timeout_s=%s", collection_timeout)
            except Exception:
                logging.exception("weather_collection_failed")
            await asyncio.sleep(interval)

    collector = asyncio.create_task(refresh(), name="weather_cache_refresh")
    try:
        while bot.running:
            # Publication calls retain their existing outcome/retry bookkeeping;
            # only read-only cache collection receives a deadline.
            for operation in (bot.process_due, bot.process_weather_channels, bot.process_rubric_schedule):
                try:
                    await operation()
                except Exception:
                    logging.exception("weather_scheduler_tick_failed operation=%s", operation.__name__)
            await asyncio.sleep(interval)
    finally:
        collector.cancel()
        await asyncio.gather(collector, return_exceptions=True)


def install(main):
    async def run(bot):
        await schedule_loop(bot, interval=main.SCHED_INTERVAL_SEC)
    main.Bot.schedule_loop = run
