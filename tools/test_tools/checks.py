import datetime
import numpy as np
import pytz


from sqlalchemy import types

_timezone_str = 'datetime64[ns, Europe/Amsterdam]'
_timedelta_str = 'timedelta64[ns]'


def has_one_row(df):
    return df.shape[0] == 1

def has_n_rows(df, n=1):
    return df.shape[0] == n

def has_localized_datetime_index(df, timezone):

    return df.index.tz == timezone

def df_matches_reader(df_to_check, reader):

    df = df_to_check.reset_index()
    fields = []
    types = []

    for key, value in reader.fields.items():
        fields.append(key)
        types.append(value)


    for column in df.columns:
        if column not in fields:
            return False
        i = fields.index(column)
        _type = types[i]


        if _type == 'float':
            if str(df.dtypes[column]) != 'float64':
                return False
        elif _type == 'str':
            if df.dtypes[column] != 'object':
                return False
        elif _type == 'datetime':
            if str(df.dtypes[column]) != _timezone_str:
                return False
        elif _type == 'date':
            if df.dtypes[column] != 'object':
                return False
        elif _type == 'timedelta':
            if df.dtypes[column] != _timedelta_str:
                return False
        else:
            return False
        fields.pop(i)
        types.pop(i)
    if len(fields) != 0:
        return False
    return True



def dfs_have_equal_form(df1, df2):


    dtypes_are_equal = df1.dtypes.equals(df2.dtypes)


    if len(df1.columns) != len(df2.columns):
        columns_are_equal = False
    else:
        columns_are_equal = all(df1.columns == df2.columns)

    return dtypes_are_equal & columns_are_equal


def dfs_have_equal_values(df1, df2):

    if df1.shape != df2.shape:
        return False

    
    df1_sorted = df1.sort_index().reset_index()
    df2_sorted = df2.sort_index().reset_index()

    index_is_equal = True

    values_are_equal = True
    for column in df1_sorted.columns:
        if df1_sorted[column].dtypes == 'float64':
            if not df1_sorted[column].to_numpy().all() == df2_sorted[column].to_numpy().all():
                print(f'non equal column {column}')
                values_are_equal = False
                break
        elif df1_sorted[column].dtypes == 'object':
            df1_list = df1_sorted[column].tolist()
            df2_list = df2_sorted[column].tolist()
            if df1_list != df2_list:
                print(f'non equal column {column}')
                values_are_equal = False
                break
        elif str(df1_sorted[column].dtypes) == _timezone_str:
            time_deltas = abs(df1_sorted[column].to_numpy() - df2_sorted[column].to_numpy())
            if not all(time_deltas < datetime.timedelta(seconds=.1)):
                print(f'non equal column {column}')
                values_are_equal = False
                break
        elif str(df1_sorted[column].dtypes) == _timedelta_str:
            time_deltas = abs(df1_sorted[column] - df2_sorted[column])
            if not all(time_deltas < datetime.timedelta(seconds=.1)):
                print(f'non equal column {column}')
                values_are_equal = False
                break
        else:
            raise Exception('Unknown data type')

    return values_are_equal & index_is_equal




def check_dataframes_are_appended(df1, df2, df3):
    rows_added = df3.shape[0] == df1.shape[0] + df2.shape[0]
    columns_remained = df1.shape[1] == df3.shape[1]
    return rows_added & columns_remained




def daily_manage_df(df, columns, f, ignore_today=True):
    df = df.copy()
    time_zone = pytz.timezone('Europe/Amsterdam')
    if ignore_today:
        today_start_time = datetime.datetime.combine(datetime.datetime.now(), datetime.time.min)
        today_start_time = time_zone.localize(today_start_time)
        df = df[df.index < today_start_time]
    index = df.index.name
    local_index = 'index'
    df.sort_index(inplace=True)
    df.reset_index(inplace=True)


    def local_index_extractor(x):
        t = datetime.datetime.combine(x, datetime.time.min) + datetime.timedelta(hours=12)
        t_localized = time_zone.localize(t)
        return t_localized

    df[local_index] = df[index].apply(local_index_extractor)
    df.drop(columns=index, inplace=True)
    df_summarized = df.groupby([local_index], as_index=False).agg({c: f for c in columns})
    df_summarized.rename(columns={local_index: index}, inplace=True)
    df_summarized.set_index(index, inplace=True)
    return df_summarized

