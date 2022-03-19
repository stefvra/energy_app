import asyncio
import logging
import os
import sys
import logging.config

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


from tools import tools
from apps.apps import App_Factory
from services.service_factories import Logger_Factory


logging.config.fileConfig(tools.get_log_config_file())
config_store = tools.Config_Store(filename=tools.get_config_file())
logger_factory = Logger_Factory()

factory_register = {
    'meteo_logger': logger_factory,
    'dsmr_logger': logger_factory,
    'pv_logger': logger_factory,
}

app_factory = App_Factory(factory_register)
app = app_factory.create_from_config(config_store)


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.run())