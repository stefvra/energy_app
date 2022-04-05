
import json
import pytest
import os
import time
import logging
from pytest_httpserver import httpserver


from tools import tools
from endpoints.readers import (
    Reader,
    DSMR_Reader,
    Fronius_Reader,
    SMA_Reader,
    MCP3008Reader,
    Reader_Factory
)

from endpoints.tests.emulators import (
    DSMR_Emulator,
    DSMR_message_emulator,
    Fronius_Emulator,
    SMA_Emulator,
)

from tools.test_tools.general_fixtures import Dataframe_Generator


logger = logging.getLogger('fixture')

config_store = tools.Config_Store(filename=tools.get_config_file())
reader_factory = Reader_Factory()


class Mock_Reader(Reader):

    def __init__(self):

        self.dataframe_generator = Dataframe_Generator()
        form = self.dataframe_generator.get_form()
        self.time_field = next(iter(form))
        self.fields = form



    def _read(self):
        return self.dataframe_generator.generate()


    async def _async_read(self):
        return self.dataframe_generator.generate()



@pytest.fixture(scope='function')
def reader_fixture(httpserver):
    class Reader_Factory_Fixture():

        def __init__(self):
            self.methods_to_execute_on_teardown = []
            self.reader_factory = Reader_Factory()


        def create_reader(self, tag):

            if tag == 'mcp3008_reader':
                reader =  MCP3008Reader(0, 0)
            elif tag == 'dsmr_reader_faulty_messages':
                emulator = DSMR_Emulator(client_port=config_store.get('dsmr_reader', 'device'), fault='random')
                emulator.start()
                time.sleep(.5)
                reader = self.reader_factory.create_from_config(config_store, 'dsmr_reader')
                self.methods_to_execute_on_teardown.append(emulator.stop)
            elif tag == 'dsmr_reader_ten_messages_in_buffer':
                emulator = DSMR_message_emulator()
                reader = DSMR_Reader()
                for _ in range(10):
                    reader._buffer.put(emulator.get_message())
            elif tag == 'dsmr_reader_ten_messages_read_all':
                emulator = DSMR_Emulator(client_port=config_store.get('dsmr_reader', 'device'), n_steps=10)
                emulator.start()
                time.sleep(.5)
                reader = self.reader_factory.create_from_config(config_store, 'dsmr_reader')
                reader.read_all = True
                self.methods_to_execute_on_teardown.append(emulator.stop)
            elif tag == 'dsmr_reader_ten_messages':
                emulator = DSMR_Emulator(client_port=config_store.get('dsmr_reader', 'device'), n_steps=10, interval=.1)
                emulator.start()
                time.sleep(2)
                reader = self.reader_factory.create_from_config(config_store, 'dsmr_reader')
                self.methods_to_execute_on_teardown.append(emulator.stop)
            elif tag == 'mock_reader':
                reader = Mock_Reader()
            elif tag == 'fronius_reader':
                reader = self.reader_factory.create_from_config(config_store, 'fronius_reader')
            elif tag == 'faulty_fronius_reader':
                reader = self.reader_factory.create_from_config(config_store, 'faulty_fronius_reader')
            elif tag == 'mock_fronius_reader':
                emulator = Fronius_Emulator()
                httpserver.expect_request("/").respond_with_handler(emulator.request_handler())
                reader = Fronius_Reader(httpserver.url_for("/"))
            elif tag == 'mock_SMA_reader':
                pwd = 'pwd'
                emulator = SMA_Emulator(pwd)
                query_string = f'sid={emulator.get_sid()}'
                logon_json = {'right': 'usr', 'pass': pwd}
                httpserver.expect_request("/dyn/login.json", method='POST', json=logon_json).respond_with_handler(emulator.login_request_handler())
                httpserver.expect_request("/dyn/logout.json", query_string=query_string, method='POST').respond_with_handler(emulator.logout_request_handler())
                httpserver.expect_request("/dyn/getValues.json", query_string=query_string, method='POST').respond_with_handler(emulator.getvalues_request_handler())
                reader = SMA_Reader(httpserver.url_for("/"), pwd)
            elif tag == 'mock_timeout_fronius_reader':
                response_time = 1
                emulator = Fronius_Emulator(response_time=response_time)
                httpserver.expect_request("/").respond_with_handler(emulator.request_handler())
                reader = Fronius_Reader(httpserver.url_for("/"), time_out=response_time - .5)
            elif tag == 'ow_reader':
                secret_config_store = tools.Config_Store(filename=tools.get_secret_config_file())
                reader = self.reader_factory.create_from_config(secret_config_store, 'open_weather_reader')
            elif tag == 'faulty_ow_reader':
                reader = self.reader_factory.create_from_config(config_store, 'faulty_open_weather_reader')
            elif tag == 'faulty_dsmr_reader':
                reader = reader_factory.create_from_config(config_store, 'faulty_dsmr_reader')
    


            return reader


        def teardown(self):
            for method in self.methods_to_execute_on_teardown:
                method()


    reader_factory_fixture = Reader_Factory_Fixture()
    yield reader_factory_fixture
    reader_factory_fixture.teardown()



