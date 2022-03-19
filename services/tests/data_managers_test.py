import pytest
import asyncio
import datetime
import time
import random
import pytz
import logging
import logging.config
from tools import tools

from services.tests.data_manager_fixtures import data_manager_fixture, block_list, state_process_register, block
from endpoints.tests.stores_fixtures import store_fixture_factory
from services.tests.client_fixture import client_fixture
from services.service_factories import Data_Manager_Factory




logging.config.fileConfig(tools.get_log_config_file())
_logger = logging.getLogger('tester')
config_store = tools.Config_Store(filename=tools.get_config_file())





async def run_manager(agents, time_out):
    try:
        tasks = []
        for agent in agents:
            tasks.append(asyncio.create_task(agent.run()))
        await asyncio.wait_for(asyncio.gather(*tasks),  timeout=time_out)
    except asyncio.TimeoutError:
        return True
    except:
        return False



@pytest.mark.asyncio
@pytest.mark.parametrize('algorithm', ['mean', 'diff'])
@pytest.mark.parametrize('tag', ['csv', 'mongo', 'dsmr', 'pv'])
async def test_data_manager(data_manager_fixture, algorithm, tag):
    data_manager_fixture.init(algorithm, tag)
    agents = data_manager_fixture.create_manager()
    time_out = 10
    await run_manager(agents, time_out)
    assert data_manager_fixture.verify()



def test_block_sort(block_list):
    block_list.sort()

    start_times = [b.start for b in block_list]

    is_not_sorted = 0
    i = 1
    while i < len(start_times):
        if(start_times[i] < start_times[i - 1]):
            is_not_sorted = 1
        i += 1
    
    assert not is_not_sorted



@pytest.mark.asyncio
async def test_block_states(block_list, state_process_register):

    for block in block_list:
        state = block.state
        for _ in range(random.randint(1,5)):
            transitions = [t for t in state_process_register if type(t['start_state']) is type(state)]
            transition = random.choice(transitions)
            state = transition['end_state']
            await block.process(None, None, transition['algorithm'])

        assert type(state) is type(block.state)
    


@pytest.mark.asyncio
async def test_requestable_data_manager(data_manager_fixture, client_fixture):

    data_manager_fixture.init('mean', 'requestable_pv_compressor')
    agents = data_manager_fixture.create_manager()
    time_out = 20
    period = 4

    network_strategy = agents[0].one_time_strategy
    client_fixture.set_port(network_strategy.port)
    client_fixture.set_period(period)
    agents.append(client_fixture)

    await run_manager(agents, time_out)
    assert client_fixture.validate_executed_requests()