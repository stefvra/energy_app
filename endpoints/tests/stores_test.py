import pytest
import random
import pandas as pd
import datetime
import time
import copy
import logging
import logging.config
from tools import tools


logging.config.fileConfig(tools.get_log_config_file())
logger = logging.getLogger('tester')



from tools.test_tools.checks import (
    check_dataframes_are_appended,
    dfs_have_equal_form,
    dfs_have_equal_values,
)



from endpoints.tests.stores_fixtures import (
    random_dataframe_generator,
    consistent_random_dataframe_generator,
    store_fixture_factory
)





logging.config.fileConfig(tools.get_log_config_file())
logger = logging.getLogger('store_tester')

# name in config file, name of dataframe generator fixture, files can be deleted on teardown?
standard_store_params = [
        ['in_memory_store', 'random_dataframe_generator', True],
        ['mongo_store', 'random_dataframe_generator', True],
        ['lazy_mongo_store', 'random_dataframe_generator', True],
        ['CSV_store', 'random_dataframe_generator', True],
        ['CSV_store_date_distributed', 'consistent_random_dataframe_generator', True],
        ['CSV_store_decimal_distributed', 'consistent_random_dataframe_generator', True],
        ['lazy_distributed_CSV_store', 'consistent_random_dataframe_generator', True],
        ['mongo_store_date_distributed', 'consistent_random_dataframe_generator', True],
        ['mongo_store_decimal_distributed', 'consistent_random_dataframe_generator', True],
        ['lazy_distributed_mongo_store', 'consistent_random_dataframe_generator', True],
        ]


def test_CSV_Store_put(store_fixture_factory, random_dataframe_generator):
    """Read data from existing store. Add new data, reload data via
    raw read method (not via store) and check if reloaded data contains
    both initial and new data

    Args:
        existing_CSV_Store ([type]): [description]
        random_consistent_df ([type]): [description]
    """
    store = store_fixture_factory.create_from_tag('CSV_store', delete_on_teardown=True)
    store.store_client.set_index(next(iter(random_dataframe_generator.get_form())))
    df1 = random_dataframe_generator.generate()
    df2 = random_dataframe_generator.generate()
    store.put(df1)

    initial_df = pd.read_csv(store.store_client.get_full_file_name())
    store.put(df2)
    updated_df = pd.read_csv(store.store_client.get_full_file_name())
    assert updated_df.shape[0] == initial_df.shape[0] + df2.shape[0]
    assert updated_df.shape[1] == initial_df.shape[1]



def test_CSV_Store_get(store_fixture_factory, random_dataframe_generator):
    """Get data from existing CSV store, get data from CSV file
    with raw read method (not via stores) and compare. Select a
    subset timespan, read in data from the store in that timespan
    and validate start and end times.

    Args:
        existing_CSV_Store ([type]): [description]
    """
    store = store_fixture_factory.create_from_tag('CSV_store', delete_on_teardown=True)
    store.store_client.set_index(next(iter(random_dataframe_generator.get_form())))
    df = random_dataframe_generator.generate()
    store.put(df)
    df_to_test = store.get_all()
    reference_df = pd.read_csv(store.store_client.get_full_file_name())
    assert df_to_test.shape[0] == reference_df.shape[0]
    assert df_to_test.shape[1] == reference_df.shape[1] - 1

    times = random.sample(list(df_to_test.index), 2)
    times.sort()

    df_selected_times = store.get(times[0], times[1])

    assert df_selected_times.index.min() == times[0]
    assert df_selected_times.index.max() == times[1]




@pytest.mark.parametrize('store_param', standard_store_params)
def test_store_put(store_fixture_factory, store_param, request):
    """Get df from existing store. ut in new data, retrieve again
    and check that retrieved data is old and new data combined.

    Args:
        store_param ([type]): [description]
        random_consistent_df ([type]): [description]
        request ([type]): [description]
    """
    store = store_fixture_factory.create_from_tag(store_param[0], delete_on_teardown=store_param[2])
    dataframe_generator = request.getfixturevalue(store_param[1])
    store.store_client.set_index(next(iter(dataframe_generator.get_form())))
    df = dataframe_generator.generate()
    store.put(df)
    df_get = store.get_all()
    assert dfs_have_equal_form(df, df_get)
    assert dfs_have_equal_values(df, df_get)




@pytest.mark.parametrize('store_param', standard_store_params)
def test_store_put_multiple(store_fixture_factory, store_param, request):
    """Get df from existing store. ut in new data, retrieve again
    and check that retrieved data is old and new data combined.

    Args:
        store_param ([type]): [description]
        random_consistent_df ([type]): [description]
        request ([type]): [description]
    """
    store = store_fixture_factory.create_from_tag(store_param[0], delete_on_teardown=store_param[2])
    dataframe_generator = request.getfixturevalue(store_param[1])
    store.store_client.set_index(next(iter(dataframe_generator.get_form())))
    df1 = dataframe_generator.generate()
    df2 = dataframe_generator.generate()
    store.put(df1)
    store.put(df2)
    df2_get = store.get_all()
    assert check_dataframes_are_appended(df1, df2, df2_get)




@pytest.mark.parametrize('store_param', standard_store_params)
def test_store_put_wrong_df(store_fixture_factory, store_param, request):
    """Not clear what is purpose of this test

    Args:
        store_param ([type]): [description]
        random_df ([type]): [description]
        request ([type]): [description]
    """

    # change so that adding non compatible df will give error!

    store = store_fixture_factory.create_from_tag(store_param[0], delete_on_teardown=store_param[2])
    dataframe_generator = request.getfixturevalue(store_param[1])
    store.store_client.set_index(next(iter(dataframe_generator.get_form())))
    df1 = dataframe_generator.generate()
    dataframe_generator.randomize_form()
    df2 = dataframe_generator.generate()
    store.put(df1)
    try:
        store.put(df2)
        assert False
    except:
        df2_get = store.get_all()
        assert dfs_have_equal_form(df1, df2_get)
        assert dfs_have_equal_values(df1, df2_get)





@pytest.mark.parametrize('store_param', standard_store_params)
def test_store_get_boundary_index(store_fixture_factory, store_param, request):
    """Get data from store, select specific timestamps and
    get data between those timestamps. Check start time
    and end time from retrieved data.

    Args:
        store_param ([type]): [description]
        request ([type]): [description]
    """
    store = store_fixture_factory.create_from_tag(store_param[0], delete_on_teardown=store_param[2])
    dataframe_generator = request.getfixturevalue(store_param[1])
    store.store_client.set_index(next(iter(dataframe_generator.get_form())))
    df = dataframe_generator.generate()
    store.put(df)
    times = random.sample(list(store.get_all().index), 2)
    times.sort()
    df_get = store.get(start=times[0], stop=times[1])
    assert df_get.index.min() == times[0]
    assert df_get.index.max() == times[1]





@pytest.mark.parametrize('store_param', standard_store_params)
def test_store_get(store_fixture_factory, store_param, request):

    store = store_fixture_factory.create_from_tag(store_param[0], delete_on_teardown=store_param[2])
    dataframe_generator = request.getfixturevalue(store_param[1])
    store.store_client.set_index(next(iter(dataframe_generator.get_form())))
    df = dataframe_generator.generate()
    store.put(df)
    start_time = min(df.index) + (max(df.index) - min(df.index)) * random.random()
    stop_time = min(df.index) + (max(df.index) - min(df.index)) * random.random()
    if start_time > stop_time:
        start_time, stop_time = stop_time, start_time
    retrieved_df = store.get(start=start_time, stop=stop_time)
    expected_df = df[(df.index >= start_time) & (df.index <= stop_time)]
    assert dfs_have_equal_form(retrieved_df, expected_df)
    assert dfs_have_equal_values(retrieved_df, expected_df)



#test can fail if non possible time is in input (time between 2am and 3am when time shifts)
@pytest.mark.parametrize('store_param', standard_store_params)
def test_store_get_all(store_fixture_factory, store_param, request):

    store = store_fixture_factory.create_from_tag(store_param[0], delete_on_teardown=store_param[2])
    dataframe_generator = request.getfixturevalue(store_param[1])
    store.store_client.set_index(next(iter(dataframe_generator.get_form())))
    df = dataframe_generator.generate()
    store.put(df)
    df_get = store.get_all()
    assert dfs_have_equal_form(df_get, df)
    assert dfs_have_equal_values(df_get, df)
    



@pytest.mark.parametrize('store_param', standard_store_params)
def test_store_get_first(store_fixture_factory, store_param, request):
    store = store_fixture_factory.create_from_tag(store_param[0], delete_on_teardown=store_param[2])
    dataframe_generator = request.getfixturevalue(store_param[1])
    store.store_client.set_index(next(iter(dataframe_generator.get_form())))
    df = dataframe_generator.generate()
    store.put(df)
    get_df = store.get_first()
    record = df[df.index == df.index.min()]
    record = record.head(1)
    assert dfs_have_equal_form(record, get_df)
    assert dfs_have_equal_values(record, get_df)


@pytest.mark.parametrize('store_param', standard_store_params)
def test_store_get_last(store_fixture_factory, store_param, request):
    store = store_fixture_factory.create_from_tag(store_param[0], delete_on_teardown=store_param[2])
    dataframe_generator = request.getfixturevalue(store_param[1])
    store.store_client.set_index(next(iter(dataframe_generator.get_form())))
    df = dataframe_generator.generate()
    store.put(df)
    get_df = store.get_last()
    record = df[df.index == df.index.max()]
    record = record.head(1)
    assert dfs_have_equal_form(record, get_df)
    assert dfs_have_equal_values(record, get_df)



def test_zero_store(store_fixture_factory):
    
    store = store_fixture_factory.create_zero_store()
    fields = store.store_client.fields


    df_first = store.get_first()
    df_last = store.get_last()
    df_all = store.get_all()


    assert len(df_first) == 1
    assert len(df_first) == 1



    for field in fields:
        assert df_first[field].iloc[0] == 0
        assert df_last[field].iloc[0] == 0
        assert df_all[field].iloc[0] == 0
        assert df_all[field].iloc[0] == 0
        assert df_all[field].iloc[-1] == 0
        assert df_all[field].iloc[-1] == 0




@pytest.mark.parametrize('store_param', standard_store_params)
def test_store_equallity(store_fixture_factory, store_param, request):
    """Get df from existing store. ut in new data, retrieve again
    and check that retrieved data is old and new data combined.

    Args:
        store_param ([type]): [description]
        random_consistent_df ([type]): [description]
        request ([type]): [description]
    """
    store1 = store_fixture_factory.create_from_tag(store_param[0], randomize_filename=False)
    store2 = store_fixture_factory.create_from_tag(store_param[0], randomize_filename=False)

    assert id(store1) != id(store2)
    assert store1 == store2




@pytest.mark.parametrize('store_param', 
    [
        ['lazy_distributed_CSV_store', 'consistent_random_dataframe_generator', True],
        ['lazy_mongo_store', 'random_dataframe_generator', True],
        #['lazy_distributed_mongo_store', 'consistent_random_dataframe_generator'],
        ])
def test_store_lazy_get(store_fixture_factory, store_param, request):

    store = store_fixture_factory.create_from_tag(store_param[0], delete_on_teardown=store_param[2])
    dataframe_generator = request.getfixturevalue(store_param[1])
    store.store_client.set_index(next(iter(dataframe_generator.get_form())))
    df1, df2 = dataframe_generator.generate(), dataframe_generator.generate()
    store.put(df1)
    store.get_all()    
    store.put(df2)
    retrieved_df1 = store.get_all()
    time.sleep(5)
    retrieved_df2 = store.get_all()
    assert dfs_have_equal_form(df1, retrieved_df1)
    assert dfs_have_equal_values(df1, retrieved_df1)
    assert dfs_have_equal_form(df1, retrieved_df2)
    assert not dfs_have_equal_values(df1, retrieved_df2)

