import asyncio
import logging
import os
import sys
import logging.config

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from tools import tools
from services.service_factories import Controller_Factory
from apps.apps import App_Factory



logging.config.fileConfig(tools.get_log_config_file())
controller_factory = Controller_Factory()
config_store = tools.Config_Store(filename=tools.get_config_file())


factory_register = {
    'controller': controller_factory,
}

app_factory = App_Factory(factory_register)
app = app_factory.create_from_config(config_store)



if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.run())