import pytest
import datetime
import random

from tools import tools
from tools.test_tools.checks import daily_manage_df, dfs_have_equal_values
from tools.test_tools.general_fixtures import Dataframe_Generator
from services.events import Periodic_Event


from endpoints.tests.stores_fixtures import store_fixture_factory
from services.service_factories import Data_Manager_Factory
from services.strategies import data_managers

data_manager_factory = Data_Manager_Factory()
test_config_store = tools.Config_Store(filename=tools.get_config_file())
production_config_store = tools.Config_Store(filename=tools.get_config_file(production_state=True))




@pytest.fixture(scope='function')
def data_manager_fixture(store_fixture_factory):
    class Data_Manager_Fixture():

        def __init__(self):
            self.algorithm_register = [
                {'tag': 'diff', 'function': lambda x: (x.iloc[-1] - x.iloc[0]), 'algorithm': data_managers.Diff_Algorithm},
                {'tag': 'mean', 'function': lambda x: x.mean(), 'algorithm': data_managers.Mean_Algorithm},
            ]
            self.source_store = None
            self.target_store = None
            self.block_length_minutes = 24 * 60
            self.blocks_to_process = 10
            self.period = 3
            self.stores_to_delete_on_teardown = []
            self.verify = lambda: False


        def init(self, algorithm_tag, tag, columns_to_manage=None):
            self.algorithm_tag = algorithm_tag
            self.tag = tag
            self.columns_to_manage = columns_to_manage


        def create_manager(self):
            if self.tag == 'dsmr' or self.tag == 'pv':
                if self.tag == 'pv' and self.algorithm_tag == 'mean':
                    manager_tag = 'pv_compressor'
                elif self.tag == 'dsmr' and self.algorithm_tag == 'mean':
                    manager_tag = 'dsmr_compressor'
                elif self.tag == 'pv' and self.algorithm_tag == 'diff':
                    manager_tag = 'pv_summarizer'
                elif self.tag == 'dsmr' and self.algorithm_tag == 'diff':
                    manager_tag = 'dsmr_summarizer'
                agents = data_manager_factory.create_from_config(test_config_store, manager_tag)
                self.target_store = agents[0].commands[0].store
                self.stores_to_delete_on_teardown.append(self.target_store)
                self.verify = self.verify_on_execution


            elif self.tag == 'requestable_pv_compressor':
                agents = data_manager_factory.create_from_config(test_config_store, 'pv_requestable_compressor')
                self.target_store = agents[0].commands[0].store
                self.stores_to_delete_on_teardown.append(self.target_store)


            elif self.tag == 'csv' or self.tag == 'mongo':

                if self.tag == 'csv':
                    store_tag = 'CSV_store'
                elif self.tag == 'mongo':
                    store_tag = 'mongo_store'
                data_frame_generator = Dataframe_Generator(nr_records=100, days_back=20)
                # self.columns_to_manage = ['b', 'c', 'e', 'f'] # with diff
                self.columns_to_manage = ['e', 'f'] # with mean
                self.source_store = store_fixture_factory.create_from_tag(store_tag, delete_on_teardown=True)
                self.target_store = store_fixture_factory.create_from_tag(store_tag, delete_on_teardown=True)
                df = data_frame_generator.generate()
                self.source_store.put(df)

                algorithm = self.get_algorithm(self.algorithm_tag)['algorithm'](columns=self.columns_to_manage)
                
                strategy = data_managers.Block_Processing_Strategy(
                    algorithm,
                    blocks_to_process=self.blocks_to_process,
                    block_length_minutes=self.block_length_minutes
                    )

                log_event = Periodic_Event(loop_period=self.period)
                agents = Data_Manager_Factory().create(log_event, strategy, self.source_store, self.target_store)

                self.verify = self.verify_on_value

            return agents

        def get_all_from_target_store(self):
            return self.target_store.get_all()

        def get_algorithm(self, tag):
            for algo_dict in self.algorithm_register:
                if algo_dict['tag'] == tag:
                    return algo_dict

        def get_daily_managed_df(self):
            raw_df = self.source_store.get_all()
            function = self.get_algorithm(self.algorithm_tag)['function']
            return daily_manage_df(raw_df, self.columns_to_manage, function)

        def teardown(self):
            for store in self.stores_to_delete_on_teardown:
                store.delete_files()

        
        def verify_on_value(self):
            df_from_store = self.get_all_from_target_store()
            daily_managed_df = self.get_daily_managed_df()
            return dfs_have_equal_values(df_from_store, daily_managed_df)
        
        def verify_on_execution(self):
            self.get_all_from_target_store()
            return True




    data_manager_fixture = Data_Manager_Fixture()
    yield data_manager_fixture
    data_manager_fixture.teardown()



def generate_random_datetime(min_year=1900, max_year=datetime.datetime.now().year):
    # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
    start = datetime.datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + datetime.timedelta(days=365 * years)
    return start + (end - start) * random.random()



@pytest.fixture(scope='function')
def block_list():
    
    block_list = []
    for _ in range(random.randint(1, 50)):
        start = generate_random_datetime()
        end = generate_random_datetime()
        block_list.append(data_managers.Block(start=start, end=end))
    
    return block_list
    

@pytest.fixture(scope='function')
def block():
    
    start = generate_random_datetime()
    end = generate_random_datetime()
    return data_managers.Block(start=start, end=end)


@pytest.fixture(scope='function')
def state_process_register():


    class Algorithm:
        async def execute(self, x, y, z):
            pass


    class Faulty_Algorithm:
        async def execute(self, x, y, z):
            raise Exception

    Todo = data_managers.Todo_Block_State()
    Done = data_managers.Done_Block_State()
    Faulty = data_managers.Faulty_Block_State()
    Closed = data_managers.Closed_Block_State()
    algorithm = Algorithm()
    faulty_algorithm = Faulty_Algorithm()


    state_process_register = [
        {'start_state': Todo, 'algorithm': algorithm, 'end_state': Done},
        {'start_state': Done, 'algorithm': algorithm, 'end_state': Closed},
        {'start_state': Faulty, 'algorithm': algorithm, 'end_state': Done},
        {'start_state': Closed, 'algorithm': algorithm, 'end_state': Closed},
        {'start_state': Todo, 'algorithm': faulty_algorithm, 'end_state': Faulty},
        {'start_state': Done, 'algorithm': faulty_algorithm, 'end_state': Closed},
        {'start_state': Faulty, 'algorithm': faulty_algorithm, 'end_state': Faulty},
        {'start_state': Closed, 'algorithm': faulty_algorithm, 'end_state': Closed},
    ]

    return state_process_register