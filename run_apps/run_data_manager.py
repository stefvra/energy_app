import asyncio
import logging
import os
import sys
import logging.config

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))


from tools import tools
from services.service_factories import Data_Manager_Factory
from apps.apps import App_Factory



logging.config.fileConfig(tools.get_log_config_file())
config_store = tools.Config_Store(filename=tools.get_config_file())
data_manager_factory = Data_Manager_Factory()


factory_register = {
    'pv_compressor': data_manager_factory,
    'dsmr_compressor': data_manager_factory,
    'pv_summarizer': data_manager_factory,
    'dsmr_summarizer': data_manager_factory,    
}

app_factory = App_Factory(factory_register)
app = app_factory.create_from_config(config_store)



if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.run())