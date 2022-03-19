import pytest
import time
import random
from endpoints.stores import ReadStoreError

from services import service_factories
from tools import tools


from endpoints.tests.emulators import (
    DSMR_Emulator,
    Fronius_Emulator
)

from services.strategies.basic import Log_Strategy
from services.events import Periodic_Event
from services.service_factories import Logger_Factory

from tools.test_tools.checks import (
    df_matches_reader,
    has_n_rows
)

from endpoints.tests.readers_fixtures import reader_fixture


logger_factory = service_factories.Logger_Factory()
config_store = tools.Config_Store(filename=tools.get_config_file())




@pytest.fixture(scope='function')
def dsmr_logger():
    dsmr_reader_key = config_store.get('dsmr_logger', 'reader')
    emulator = DSMR_Emulator(client_port=config_store.get(dsmr_reader_key, 'device'), n_steps=10, interval=.1)
    emulator.start()
    time.sleep(2)
    logger = logger_factory.create_from_config(config_store, 'dsmr_logger')
    yield logger
    logger[0].commands[0].store.delete_files()
    emulator.stop()


@pytest.fixture(scope='function')
def pv_logger():
    logger = logger_factory.create_from_config(config_store, 'pv_logger')
    yield logger
    logger[0].commands[0].store.delete_files()


@pytest.fixture(scope='function')
def meteo_logger():
    logger = logger_factory.create_from_config(config_store, 'meteo_logger')
    yield logger
    logger[0].commands[0].store.delete_files()




@pytest.fixture(scope='function')
def logger_fixture(store_fixture_factory, reader_fixture):
    """
    mock_reader,
    fronius_reader,
    mock_fronius_reader,
    ow_reader,
    dsmr_reader_ten_messages,
#    dsmr_reader_faulty_messages,
    faulty_fronius_reader,
    mock_timeout_fronius_reader,
#    faulty_dsmr_reader,
    faulty_ow_reader,    
    ):
    """
    class Logger_Fixture():

        def __init__(self):
            self.store_tag = None
            self.reader_tag = None
            self.logger_tag = None
            self.agents = None
            self.store = None
            self.reader = None
            self.period = None
            self.repetitions = None
            self.stores_to_delete_on_teardown = []
            self.methods_to_execute_on_teardown = []
            self.verifications = []
            self.reader_register = [
                'mock_reader',
                'fronius_reader',
                'mock_fronius_reader',
                'ow_reader',
                'dsmr_reader_ten_messages',
                'mock_timeout_fronius_reader',
                'dsmr_reader_faulty_messages',
                'faulty_dsmr_reader',
                'faulty_ow_reader',
                'faulty_fronius_reader'   
            ]


        def set_logger_tag(self, logger_tag):
            self.logger_tag = logger_tag

        def set_store_tag(self, store_tag):
            self.store_tag = store_tag

        def set_reader_tag(self, reader_tag):
            self.reader_tag = reader_tag

        def set_period(self, period):
            self.period = period

        def set_repetitions(self, repetitions):
            self.repetitions = repetitions


        def get_adjusted_period(self):
            return self.period * random.uniform(.8, 1.2)
        
        def get_time_out(self):
            return self.period * self.repetitions * random.uniform(.8, 1.2)



        def create_logger(self):

            if self.logger_tag is not None:
                if self.logger_tag == 'meteo_logger':
                    agents = logger_factory.create_from_config(config_store, 'meteo_logger')
                    self.stores_to_delete_on_teardown.append(agents[0].commands[0].store)
                if self.logger_tag == 'pv_logger':
                    agents = logger_factory.create_from_config(config_store, 'pv_logger')
                    self.stores_to_delete_on_teardown.append(agents[0].commands[0].store)
                if self.logger_tag == 'dsmr_logger':
                    agents = logger_factory.create_from_config(config_store, 'dsmr_logger')
                    self.stores_to_delete_on_teardown.append(agents[0].commands[0].store)
                    dsmr_reader_key = config_store.get('dsmr_logger', 'reader')
                    emulator = DSMR_Emulator(client_port=config_store.get(dsmr_reader_key, 'device'), n_steps=10, interval=.1)
                    emulator.start()
                    self.methods_to_execute_on_teardown.append(emulator.stop)
                    time.sleep(2)
                if self.logger_tag == 'requestable_dsmr_logger':
                    agents = logger_factory.create_from_config(config_store, 'test_requestable_dsmr_logger')
                    self.stores_to_delete_on_teardown.append(agents[0].commands[0].store)
                    dsmr_reader_key = config_store.get('dsmr_logger', 'reader')
                    emulator = DSMR_Emulator(client_port=config_store.get(dsmr_reader_key, 'device'), n_steps=10, interval=.1)
                    emulator.start()
                    self.methods_to_execute_on_teardown.append(emulator.stop)
                    time.sleep(2)
                self.set_period(agents[0].event.loop_period)
            else:
                if self.store_tag == 'mongo_store' or self.store_tag == 'CSV_store':
                    store = store_fixture_factory.create_from_tag(self.store_tag, randomize_filename=True, delete_on_teardown=True)
                if self.reader_tag in self.reader_register:
                    reader = reader_fixture.create_reader(self.reader_tag)

                store.set_index(reader.time_field)
                log_event = Periodic_Event(loop_period=self.get_adjusted_period())
                strategy = Log_Strategy()
                agents = Logger_Factory().create(log_event, reader, store, strategy)
            
            self.agents = agents
            self.store = self.agents[0].commands[0].store
            self.reader = self.agents[0].inputs[0].reader
            return self.agents




        def get_all_from_target_store(self):
            return self.store.get_all()


        def teardown(self):
            for store in self.stores_to_delete_on_teardown:
                store.delete_files()
            for method in self.methods_to_execute_on_teardown:
                method()


        def df_is_not_none(self):
            return self.get_all_from_target_store() is not None

        def df_is_none(self):
            try:
                self.get_all_from_target_store()
                return False
            except ReadStoreError:
                return True

        def df_matches_reader(self):
            return df_matches_reader(self.get_all_from_target_store(), self.reader)

        def correct_nr_of_records_logged(self):
            records_logged = self.agents[0].periodic_strategy.states['records_logged']
            correct_nr_records = has_n_rows(self.get_all_from_target_store(), n=records_logged)
            some_records_are_logged = records_logged > 0
            return correct_nr_records and some_records_are_logged

        def no_records_logged(self):
            records_logged = self.agents[0].periodic_strategy.states['records_logged']
            return records_logged == 0


        def set_full_verification(self):
            self.verifications.append(self.df_is_not_none)
            self.verifications.append(self.df_matches_reader)
            self.verifications.append(self.correct_nr_of_records_logged)


        def set_faulty_verification(self):
            self.verifications.append(self.df_is_none)
            self.verifications.append(self.no_records_logged)


        def verify(self):
            if len(self.verifications) == 0:
                return False
            else:
                verified = True
                for verification in self.verifications:
                    if not verification():
                        verified = False
                return verified





    logger_fixture = Logger_Fixture()
    yield logger_fixture
    logger_fixture.teardown()



