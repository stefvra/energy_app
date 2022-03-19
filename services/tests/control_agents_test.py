
import pytest
import asyncio
import datetime
import time
import random
import pytz
import logging
import logging.config
import matplotlib.pyplot as plt
from tools import tools

from services.service_factories import Controller_Factory
import endpoints.stores as stores
from services.service_factories import Controller_Factory
from services.tests.client_fixture import client_fixture




logging.config.fileConfig(tools.get_log_config_file())
logger = logging.getLogger('tester')
config_store = tools.Config_Store(filename=tools.get_config_file())


async def run_controller(agents, time_out):
    try:
        tasks = []
        for agent in agents:
            tasks.append(asyncio.create_task(agent.run()))
        await asyncio.wait_for(asyncio.gather(*tasks),  timeout=time_out)
    except asyncio.TimeoutError:
        return True
    return False



@pytest.mark.asyncio
async def test_requestable_controller(client_fixture):
    agents = Controller_Factory().create_from_config(config_store, 'test_requestable_controller')
    time_out = 10

    network_strategy = agents[0].one_time_strategy
    client_fixture.set_port(network_strategy.port)
    agents.append(client_fixture)

    await run_controller(agents, time_out)
    assert client_fixture.validate_executed_requests()



@pytest.mark.asyncio
async def test_controller_functional():
    agents = Controller_Factory().create_from_config(config_store, 'test_controller')
    time_out = 10

    assert await run_controller(agents, time_out)

