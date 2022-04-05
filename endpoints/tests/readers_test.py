import pytest
import asyncio
import requests
import datetime
import pytz
import copy
import logging
import logging.config
from tools import tools

_timezone = pytz.timezone('Europe/Amsterdam')

logging.config.fileConfig(tools.get_log_config_file())
logger = logging.getLogger('tester')

from tools.test_tools.checks import (
    has_one_row,
    df_matches_reader,
    has_n_rows,
    has_localized_datetime_index
)

from endpoints.tests.readers_fixtures import reader_fixture

@pytest.mark.xfail(raises=requests.ReadTimeout)
def test_fronius_reader_timeout_read(reader_fixture):
    reader = reader_fixture.create_reader('mock_timeout_fronius_reader')
    df = reader.read()
    assert has_localized_datetime_index(df, _timezone)
    assert df_matches_reader(df, reader)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'reader_param',
    [
        'mock_reader',
        'fronius_reader',
        'mock_SMA_reader',        
        'ow_reader',
        'mock_fronius_reader',
        'dsmr_reader_ten_messages_in_buffer',
        ]
)
async def test_reader_read(reader_param, reader_fixture):
    reader = reader_fixture.create_reader(reader_param)
    df = reader.read()
    assert has_localized_datetime_index(df, _timezone)
    assert df_matches_reader(df, reader)


@pytest.mark.xfail
@pytest.mark.parametrize(
    'reader_param',
    ['faulty_fronius_reader', 'faulty_ow_reader', 'faulty_dsmr_reader']
)
def test_faulty_reader_read(reader_param, reader_fixture):
    reader = reader_fixture.create_reader(reader_param)
    df = reader.read()
    assert has_localized_datetime_index(df, _timezone)
    assert df_matches_reader(df, reader)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'reader_param',
    [
        'mock_reader',
        'fronius_reader',
        'mock_SMA_reader',        
        'SMA_Reader',
        'ow_reader',
        'mock_fronius_reader',
        'dsmr_reader_ten_messages_in_buffer',
        ]
)
async def test_reader_async_read(reader_param, reader_fixture):
    """Tests the time between intervals for the periodic event loop.
    Test passes if deviation between target and actual is less than
    3%.

    Returns:
        [type]: [description]
    """
    reader = reader_fixture.create_reader(reader_param)
    df = await reader.async_read()
    assert has_localized_datetime_index(df, _timezone)
    assert df_matches_reader(df, reader)





@pytest.mark.xfail
@pytest.mark.asyncio
@pytest.mark.parametrize(
    'reader_param',
    ['faulty_fronius_reader', 'faulty_ow_reader', 'faulty_dsmr_reader']
)
async def test_faulty_reader_async_read(reader_param, reader_fixture):
    """Tests the time between intervals for the periodic event loop.
    Test passes if deviation between target and actual is less than
    3%.

    Returns:
        [type]: [description]
    """
    reader = reader_fixture.create_reader(reader_param)
    task = asyncio.create_task(reader.async_read())
    df = await task
    assert has_localized_datetime_index(df, _timezone)
    assert df_matches_reader(df, reader)


# skipped for now. the dsmr-parser misses out on first two messages in queue. not able to find out why
#@pytest.mark.skip
@pytest.mark.asyncio
async def test_DSMR_reader_buffer(reader_fixture):
    reader = reader_fixture.create_reader('dsmr_reader_ten_messages')
    try:
        await asyncio.wait_for(reader._read_from_serial(), timeout=2)
    except asyncio.TimeoutError:
        assert reader._buffer.qsize() == 10



@pytest.mark.asyncio
async def test_DSMR_reader_buffer_faulty_message(reader_fixture):
    reader = reader_fixture.create_reader('dsmr_reader_faulty_messages')
    try:
        await asyncio.wait_for(reader._read_from_serial(), timeout=2)
    except asyncio.TimeoutError:
        assert reader._buffer.qsize() == 0
    




@pytest.mark.asyncio
@pytest.mark.parametrize(
    'reader_param',
    [
        'fronius_reader',
        'mock_SMA_reader',        
        'ow_reader',
        'dsmr_reader_ten_messages_in_buffer',
        'mock_fronius_reader'
        ]
)
async def test_reader_correct_time(reader_param, reader_fixture):
    """Tests the time between intervals for the periodic event loop.
    Test passes if deviation between target and actual is less than
    3%.

    Returns:
        [type]: [description]
    """
    now = datetime.datetime.now(tz=_timezone)
    reader = reader_fixture.create_reader(reader_param)
    df = reader.read()
    assert abs(df.index[0] - now) < datetime.timedelta(seconds=10)



@pytest.mark.parametrize(
    'reader_param',
    [
        'fronius_reader',
        'mock_SMA_reader',        
        'ow_reader',
        'dsmr_reader_ten_messages',
        'dsmr_reader_faulty_messages',
        'faulty_fronius_reader',
        'faulty_dsmr_reader',
        'faulty_ow_reader',
        'dsmr_reader_ten_messages_read_all',
        'dsmr_reader_ten_messages_in_buffer',
        'mcp3008_reader'
        ]
)
def test_reader_equallity(reader_param, reader_fixture):
    """Get df from existing store. ut in new data, retrieve again
    and check that retrieved data is old and new data combined.

    Args:
        store_param ([type]): [description]
        random_consistent_df ([type]): [description]
        request ([type]): [description]
    """
    reader1 = reader_fixture.create_reader(reader_param)
    reader2 = copy.copy(reader1)

    assert id(reader1) != id(reader2)
    assert reader1 == reader2
