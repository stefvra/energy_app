import pytest
import asyncio
import datetime
import time
import random
import pytz
import logging
import logging.config

from tools import tools


from endpoints.stores import ReadStoreError

from services.tests.logger_fixtures import logger_fixture
from services.tests.client_fixture import client_fixture

from endpoints.tests.readers_fixtures import reader_fixture


from endpoints.tests.stores_fixtures import (
    random_dataframe_generator,
    consistent_random_dataframe_generator,
    store_fixture_factory
)



from tools.test_tools.checks import (
    df_matches_reader,
    has_n_rows
)



logging.config.fileConfig(tools.get_log_config_file())
_logger = logging.getLogger('tester')




async def run_logger(agents, time_out):
    try:
        tasks = []
        for agent in agents:
            tasks.append(asyncio.create_task(agent.run()))
        await asyncio.wait_for(asyncio.gather(*tasks),  timeout=time_out)
    except asyncio.TimeoutError:
        return True
    return False





@pytest.mark.asyncio
@pytest.mark.parametrize(
    'reader_param',
    [
        ['mock_reader', {'target_period': 0.1, 'target_amount': 10}],
        ['fronius_reader', {'target_period': 3, 'target_amount': 3}],
        ['mock_fronius_reader', {'target_period': 0.1, 'target_amount': 10}],
        ['ow_reader', {'target_period': 5, 'target_amount': 3}],
        ['dsmr_reader_ten_messages', {'target_period': 0.1, 'target_amount': 20}],
        ]
)
@pytest.mark.parametrize(
    'store_param',
    ['mongo_store', 'CSV_store']
)
async def test_logger(logger_fixture, reader_param, store_param, request):
    logger_fixture.set_store_tag(store_param)
    logger_fixture.set_reader_tag(reader_param[0])
    logger_fixture.set_period(reader_param[1]['target_period'])
    logger_fixture.set_repetitions(reader_param[1]['target_amount'])
    time_out = logger_fixture.get_time_out()
    agents = logger_fixture.create_logger()
    logger_fixture.set_full_verification()

    await run_logger(agents, time_out)
    assert logger_fixture.verify()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'reader_param',
    [
        ['mock_timeout_fronius_reader', {'target_period': 1, 'target_amount': 2}],
        ['dsmr_reader_faulty_messages', {'target_period': 0.1, 'target_amount': 20}],
        ['faulty_dsmr_reader', {'target_period': 0.1, 'target_amount': 20}],
        ]
)
@pytest.mark.parametrize(
    'store_param',
    ['mongo_store', 'CSV_store']
)
async def test_faulty_logger(logger_fixture, reader_param, store_param, request):
    logger_fixture.set_store_tag(store_param)
    logger_fixture.set_reader_tag(reader_param[0])
    logger_fixture.set_period(reader_param[1]['target_period'])
    logger_fixture.set_repetitions(reader_param[1]['target_amount'])
    time_out = logger_fixture.get_time_out()
    agents = logger_fixture.create_logger()
    logger_fixture.set_faulty_verification()

    await run_logger(agents, time_out)
    assert logger_fixture.verify()

@pytest.mark.xfail
@pytest.mark.asyncio
@pytest.mark.parametrize(
    'reader_param',
    [
        ['faulty_ow_reader', {'target_period': 0.1, 'target_amount': 10}],
        ['faulty_fronius_reader', {'target_period': 0.1, 'target_amount': 10}],
        ]
)
@pytest.mark.parametrize(
    'store_param',
    ['mongo_store', 'CSV_store']
)
async def test_faulty_url_logger(logger_fixture, reader_param, store_param, request):
    logger_fixture.set_store_tag(store_param)
    logger_fixture.set_reader_tag(reader_param[0])
    logger_fixture.set_period(reader_param[1]['target_period'])
    logger_fixture.set_repetitions(reader_param[1]['target_amount'])
    time_out = logger_fixture.get_time_out()
    agents = logger_fixture.create_logger()
    logger_fixture.set_faulty_verification()

    await run_logger(agents, time_out)
    assert logger_fixture.verify()



@pytest.mark.asyncio
@pytest.mark.parametrize(
    'logger_param',
    [
        ['dsmr_logger', {'target_amount': 10}],
        ['pv_logger', {'target_amount': 3}],
        ]
)
async def test_logger_from_config(logger_fixture, logger_param, request):
    logger_fixture.set_logger_tag(logger_param[0])
    logger_fixture.set_repetitions(logger_param[1]['target_amount'])
    agents = logger_fixture.create_logger()
    time_out = logger_fixture.get_time_out()
    logger_fixture.set_full_verification()

    await run_logger(agents, time_out)
    assert logger_fixture.verify()


@pytest.mark.asyncio
async def test_requestable_logger(logger_fixture, client_fixture):

   
    logger_fixture.set_logger_tag('requestable_dsmr_logger')
    agents = logger_fixture.create_logger()
    time_out = 20
    period = 4

    network_strategy = agents[0].one_time_strategy
    client_fixture.set_port(network_strategy.port)
    client_fixture.set_period(period)
    agents.append(client_fixture)

    await run_logger(agents, time_out)
    assert client_fixture.validate_executed_requests()





