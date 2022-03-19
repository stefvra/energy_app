
import pytest
import asyncio
import datetime
import pytz
import logging
import logging.config
from tools import tools

from services.events import Periodic_Event

logging.config.fileConfig(tools.get_log_config_file())
logger = logging.getLogger('tester')



@pytest.mark.asyncio
async def test_periodic_event():
    """Tests the time between intervals for the periodic event loop.
    Test passes if deviation between target and actual is less than
    3%.

    Returns:
        [type]: [description]
    """
    n_events = 5
    interval = .2
    periodic_event = Periodic_Event(loop_period=interval)
    task1 = asyncio.create_task(periodic_event.loop(n_events=n_events))

    async def time_event(periodic_event):
        interval = periodic_event.loop_period
        intervals = []
        while True:
            start_time = datetime.datetime.now()
            try:
                await asyncio.wait_for(periodic_event.event.wait(), interval * 1.5)
                stop_time = datetime.datetime.now()
                intervals.append((stop_time - start_time).total_seconds())
            except asyncio.TimeoutError:
                break
        return sum(intervals) / len(intervals)


    task2 = asyncio.create_task(time_event(periodic_event))

    await task1
    actual_interval = await task2
    pct_deviation = abs(actual_interval - interval) / interval * 100
    assert pct_deviation < 3

