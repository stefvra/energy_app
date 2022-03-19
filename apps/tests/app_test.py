import pytest
from apps.apps import App_Factory
from services.service_factories import Controller_Factory, Logger_Factory, Data_Manager_Factory
from tools import tools

config_file = tools.get_config_file(production_state=True)
config_store = tools.Config_Store(filename=config_file)


controller_factory = Controller_Factory()
logger_factory = Logger_Factory()
data_manager_factory = Data_Manager_Factory()

def create_app(factory_register):
    """
        Helper function to create an app from a factory register

    Args:
        factory_register (dict)

    Returns:
        App: created instance from App class
    """
    app_factory = App_Factory(factory_register)
    return app_factory.create_from_config(config_store) 



def test_controller_creation():
    """
        Test to check if controller is created
    """

    factory_register = {
        'controller': controller_factory,
    }

    app = create_app(factory_register)
    agents = app.agents

    assert len(agents) == 1
    assert None not in agents


def test_data_manager_creation():
    """
        Test to check if data managers are created
    """

    factory_register = {
        'pv_compressor': data_manager_factory,
        'dsmr_compressor': data_manager_factory,
    }

    app = create_app(factory_register)
    agents = app.agents

    assert len(agents) == 2
    assert None not in agents


def test_logger_creation():
    """
        Test to check if loggers are created
    """

    factory_register = {
        'meteo_logger': logger_factory,
        'dsmr_logger': logger_factory,
        'pv_logger': logger_factory,
    }

    app = create_app(factory_register)
    agents = app.agents

    assert len(agents) == 4
    assert None not in agents