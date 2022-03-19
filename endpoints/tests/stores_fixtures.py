import os
import uuid
import pytest
import datetime

from endpoints import stores
from tools.test_tools.general_fixtures import Dataframe_Generator
from tools import tools


_INDEX = 'a'

store_factory = stores.Store_Factory()
config_store = tools.Config_Store(filename=tools.get_config_file())


@pytest.fixture(scope='function')
def store_fixture_factory():
    class Store_Fixture_Factory:

        def __init__(self):
            self.store_register = []

        def create_from_tag(self, tag, randomize_filename=True, config_store=config_store, delete_on_teardown=False):
            if tag == 'in_memory_store':
                store = self.create_in_memory_store(inject_faults=False)
            elif tag == 'zero_store':
                store = self.create_zero_store()
            else:
                store = self.create_from_config_key(tag, config_store)
            if randomize_filename:
                store.store_client.set_file(str(uuid.uuid4()))  
            self.store_register.append({'store': store, 'delete_on_teardown': delete_on_teardown})
            return store

        def tear_down(self):
            for store in self.store_register:
                if store['delete_on_teardown']:
                    store['store'].delete_files()

        def create_from_config_key(self, key, config_store):
            store = store_factory.create_from_config(config_store, key)
            return store

        def create_in_memory_store(self, inject_faults=False):
            transformer = stores.Transformer()
            store_client = stores.In_Memory_Store_Client(_INDEX)
            store = store_factory.create(store_client, transformer, decorators=None)
            return store            
    
        def create_zero_store(self):
            transformer = stores.Transformer()
            store_client = stores.Zero_Store_Client(['a', 'b', 'c'], 'time')
            store = store_factory.create(store_client, transformer, decorators=None)
            return store

    store_fixture_factory = Store_Fixture_Factory()
    yield store_fixture_factory
    store_fixture_factory.tear_down()



@pytest.fixture(scope='function')
def random_dataframe_generator():
    dg = Dataframe_Generator()
    dg.randomize_form()
    yield dg


@pytest.fixture(scope='function')
def consistent_random_dataframe_generator():
    yield Dataframe_Generator()



