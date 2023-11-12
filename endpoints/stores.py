from abc import ABC, abstractmethod
import datetime
import random
import dateutil
from queue import Queue
import logging
import csv
import os
from itertools import compress
import pandas as pd
import pytz
import pytimeparse
from functools import wraps
from pymongo import MongoClient
import pymongo
import numpy as np
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from tools.factory_tools import Param_Parser




logger = logging.getLogger('stores')


class ReadStoreError(Exception):
    """
    Exception to raise when error occured in reading data
    """
    pass


class Store_Manager():
    """
    Class that manages reading from a store and converting data into dataframe
    """

    def __init__(self, store_client, transformer):
        """
        Initialization

        Args:
            store_client (store_client): store client to read from
            transformer (transformer): transformer to transform to and from datraframe
        """
        self.store_client = store_client
        self.transformer = transformer
        self.buffer = Queue()
        self.nr_records_written = 0


    def __eq__(self, other):
        """
        Implementation of equality method

        Args:
            other (store manager): object to compare to

        Returns:
            boolean: True if self and other are equal
        """
        if not hasattr(other, 'store_client'):
            return False
        if not hasattr(other, 'transformer'):
            return False
        if self.store_client == other.store_client and \
            self.transformer == other.transformer:
            return True
        else:
            return False


    def set_index(self, index):
        """
        Set index field of store

        Args:
            index (str): name of field to be used as index
        """
        self.store_client.set_index(index)


    def put(self, df):
        """
        Put data in the store. Uses buffer so can try again if the store
        is not available.

        Args:
            df (dataframe): data to put into store

        Raises:
            Exception: if form of input data does not match data in the store
        """

        records = self.transformer.to_records(df)
        if self.store_client._check_record_form(records):
            self.buffer.put(records, block=False)
        else:
            raise Exception



        for _ in range(self.buffer.qsize()):
            records = self.buffer.get(block=False)
            try:
                self.store_client.put(records)
                self.nr_records_written += len(records)
            except Exception:
                self.buffer.put(records, block=False)
    


    def get(self, start, stop):
        """
        Get data from the store

        Args:
            start (datetime): start time
            stop (datetime): stop time

        Raises:
            ReadStoreError: if error occured during reading from the store

        Returns:
            dataframe: dataframe of all records with index field between start time and stop time
        """
        try:
            records = self.store_client.get(start, stop)
            return self.transformer.to_df(records, self.store_client.get_index())
        except:
            raise ReadStoreError

    def get_all(self):
        """
        Get all data in store

        Raises:
            ReadStoreError: if error occured during reading from the store

        Returns:
            dataframe: dataframe of all records in store
        """
        try:
            records = self.store_client.get_all()
            return self.transformer.to_df(records, self.store_client.get_index())
        except:
            raise ReadStoreError


    def remove(self, start, stop):
        """
        Remove records from the store with index between start time and stop time

        Args:
            start (datetime): start time
            stop (datetime): stop time
        """
        self.store_client.remove(start, stop)
    
    def delete_files(self):
        """_summary_
        """
        self.store_client.delete_files()


    def get_first(self):
        """
        Gets the record with the smallest index from the store

        Raises:
            ReadStoreError: if error occured during reading from the store

        Returns:
            dataframe: first record from store
        """
        try:
            record = self.store_client.get_first()
            return self.transformer.to_df(record, self.store_client.get_index())
        except:
            raise ReadStoreError


    def get_last(self):
        """
        Gets the record with the biggest index from the store

        Raises:
            ReadStoreError: if error occured during reading from the store

        Returns:
            dataframe: last record from store
        """
        try:
            record = self.store_client.get_last()
            return self.transformer.to_df(record, self.store_client.get_index())
        except:
            raise ReadStoreError



class Store_Client(ABC):
    """
    Abstract base class for a store client. A store client manages operations on a store
    """

    def __init__(self, index: str, database: str, file: str) -> None:
        """
        Initialization

        Args:
            index (str): name of index field
            database (str): name of database
            file (str): name of file
        """
        self.index = index
        self.database = database
        self.file = file




    @staticmethod
    def _get_min_record(records: list, field: str) -> list:
        """
        Get record with smallest index from record list

        Args:
            records (list of dicts): records to search in
            field (str): name of index field

        Returns:
            list: list with one item being the minimum record
        """
        index_values = [r[field] for r in records]
        smallest_index = index_values.index(min(index_values))
        return [records[smallest_index]]

    @staticmethod
    def _get_max_record(records: list, field: str) -> list:
        """
        Get record with biggest index from record list

        Args:
            records (list of dicts): records to search in
            field (str): name of index field

        Returns:
            list: list with one item being the maximum record
        """        
        index_values = [r[field] for r in records]
        smallest_index = index_values.index(max(index_values))
        return [records[smallest_index]]


    def _check_record_form(self, records_to_check) -> bool:
        """
        Check if form of record corresponds to store. Types, fields and
        index are checked.

        Args:
            record_to_check (dict): record to check

        Returns:
            bool: True if form corresponds
        """

        try:
            existing_record = self.get_first()[0]
        except:
            return True
        form_is_equal = True
        for i1, i2 in zip(existing_record.items(), records_to_check[0].items()):
            key1 = i1[0]
            key2 = i2[0]
            type1 = type(i1[1])
            type2 = type(i2[1])
            if key1 != key2:
                form_is_equal = False
            if type1 != type2:
                if type1 == datetime.datetime and type2 == pd.Timestamp:
                    pass
                elif type1 == datetime.timedelta and type2 == pd.Timedelta:
                    pass
                elif type1 == float and type2 == np.float64:
                    pass
                else:
                    form_is_equal = False
            form_is_equal
        return form_is_equal


    def get_file(self) -> str:
        """
        Get name of file in the store

        Returns:
            str: filename
        """
        return self.file


    def get_database(self) -> str:
        """
        Get name of database in the store

        Returns:
            str: database name
        """
        return self.file


    def set_file(self, file: str) -> None:
        """
        Sets the name of the file in the store

        Args:
            file (str): new name of file
        """
        self.file = file


    def set_index(self, index: str) -> None:
        """
        Set name of the index field

        Args:
            index (str): new name of the index
        """
        self.index = index


    def get_index(self) -> str:
        """
        Get the name of the index field

        Returns:
            str: name of the index field
        """
        return self.index
    

    @abstractmethod
    def get_existing_files(self) -> list:
        """
        Abstract method to get existing files in database
        """
        pass


    @abstractmethod
    def delete_files(self):
        """
        Abstract method to delete all filed in the database
        """
        pass

    @abstractmethod
    def put(self, records):
        """
        Abstract method to put records in store

        Args:
            records (list): records to put in store
        """
        pass

    @abstractmethod
    def get(self, start: datetime.datetime, stop: datetime.datetime) -> list:
        """
        Abstract methot to get records with index greater than start and smaller than stop

        Args:
            start (datetime.datetime): start time
            stop (datetime.datetime): stop time

        Returns:
            list: list of records
        """
        pass

    @abstractmethod
    def get_all(self) -> list:
        """
        Abstract method to get all records from the store

        Returns:
            list: list of records
        """
        pass

    @abstractmethod
    def remove(self, start: datetime.datetime, stop: datetime.datetime):
        """
        Abstract method to remove records from the store with index greater than start
        and index smaller than stop

        Args:
            start (datetime.datetime): start time
            stop (datetime.datetime): stop time
        """
        pass

    @abstractmethod
    def get_first(self) -> list:
        """
        Abstract method to get record with smallest index from the store

        Returns:
            list: list of records
        """
        pass

    @abstractmethod
    def get_last(self) -> list:
        """
        Abstract method to get record with largest index from the store

        Returns:
            list: list of records
        """
        pass


class In_Memory_Store_Client(Store_Client):
    """
    Implementation of in memory store client. This client keeps all the data in memory

    """

    def __init__(self, index: str) -> None:
        """
        Initialization

        Args:
            index (str): name of index field
            inject_faults (bool, optional): Wether or not to inject faults in the instance.
            Is only used for testing and should be removed. Defaults to False.
        """
        self.records = []
        super().__init__(index, None, None)


    def __eq__(self, other: Store_Client) -> bool:
        """
        Implementation of equality method

        Args:
            other (Store_Client): object to compare to

        Returns:
            bool: True if self and other are equal
        """
        if self.index == other.index:
            return True



    def _get_record_flags(self, records: list, start: datetime.datetime, stop: datetime.datetime) -> list:
        """
        Get mask to indicate which record has an index greater than start and lower then stop

        Args:
            records (list): records to check
            start (datetime.datetime): start time
            stop (datetime.datetime): stop time

        Returns:
            list: mask
        """
        flags = []
        for record in records:
            if record[self.index] >= start and record[self.index] <= stop:
                flags.append(True)
            else:
                flags.append(False)
        return flags


    def get_existing_files(self) -> None:
        """
        Get existing files

        Returns:
            None: no files available for in memory store
        """
        return None

    
    def put(self, records: list) -> None:
        """
        Put records in the store

        Args:
            records (list): records to store
        """
        self.records += records


    def get(self, start: datetime.datetime, stop: datetime.datetime) -> list:
        """
        Get records with index greater than start and smaller than stop

        Args:
            start (datetime.datetime): start time
            stop (datetime.datetime): stop time

        Returns:
            list: records found
        """
        flags = self._get_record_flags(self.records, start, stop)
        selected_records = list(compress(self.records, flags))
        return selected_records


    def get_all(self) -> list:
        """
        Get all records from store

        Returns:
            list: records
        """
        return self.records


    def remove(self, start: datetime.datetime, stop: datetime.datetime) -> None:
        """
        Remove records with index greater than start and smaller than stop

        Args:
            start (datetime.datetime): start time
            stop (datetime.datetime): stop time
        """
        flags = self._get_record_flags(self.records, start, stop)
        inverted_flags = [not f for f in flags]
        self.records = list(compress(self.records, inverted_flags))

    def delete_files(self) -> None:
        """
        Delete all files and hence all records
        """
        self.records = []


    def get_first(self) -> list:
        """
        Get record with smallest index

        Returns:
            list: record
        """
        return self._get_min_record(self.records, self.index)


    def get_last(self):
        """
        Get record with largest index

        Returns:
            list: record
        """
        return self._get_max_record(self.records, self.index)




class Mongo_Store_Client(Store_Client):


    def __init__(self, client: MongoClient, index: str, database: str, collection: str):
        """
        Initialization

        Args:
            client (MongoClient): client to access MongoDb
            index (str): name of index field
            database (str): name of database
            collection (str): name of collection
        """
        self.client = client
        self.local_timezone = pytz.timezone('Europe/Amsterdam')
        self.store_timezone = pytz.UTC
        self.parser = Mongo_Record_Parser(self.local_timezone, self.store_timezone)
        super().__init__(index, database, collection)

    def __eq__(self, other: Store_Client) -> bool :
        """
        Implementation of equality method

        Args:
            other (Store_Client): object to compare to

        Returns:
            bool: True if self is considered equal to other
        """
        if not hasattr(other, 'client'):
            return False
        if not hasattr(other, 'local_timezone'):
            return False
        if not hasattr(other, 'store_timezone'):
            return False
        if not hasattr(other, 'parser'):
            return False
        if not hasattr(other, 'index'):
            return False
        if not hasattr(other, 'database'):
            return False
        if not hasattr(other, 'file'):
            return False        
        if self.client == other.client and \
            self.local_timezone == other.local_timezone and \
                self.store_timezone == other.store_timezone and \
                    self.parser == other.parser and \
                        self.index == other.index and \
                            self.database == other.database and \
                                self.file == other.file:
            return True
        else:
            return False


    def get_database(self) -> str:
        """
        Get name of database

        Returns:
            str: name of database
        """
        return self.database

    def get_collection(self) -> str:
        """
        Get name of collection

        Returns:
            str: collection name
        """
        return self.file


    def get_existing_files(self) -> list:
        """
        Get all files in the database

        Returns:
            list: list of database names
        """
        return self._get_existing_collections()




    def _generate_filter(self, start: datetime.datetime = None, stop: datetime.datetime = None) -> dict:
        """
        Generate filter based in start and stop time

        Args:
            start (datetime.datetime, optional): start time. Defaults to None.
            stop (datetime.datetime, optional): stop time. Defaults to None.

        Returns:
            dict: representing filter
        """
        start = self.parser.parse_value_to_store(start)
        stop = self.parser.parse_value_to_store(stop)


        if (start == None and stop == None) or self.index == None:
            filter = None
        else:
            if start == None:
                filter = { self.index: { '$lte' : stop } }
            elif stop == None:
                filter = { self.index: { '$gte' : start } }
            else:
                filter = { self.index: { '$lte' : stop, '$gte' : start} }

        return filter

    def _get_existing_collections(self) -> list:
        """
        Get names of existing collections

        Returns:
            list: names of collections
        """
        return self.client[self.get_database()].collection_names()

    @staticmethod
    def _get_records_from_cursor(cursor: pymongo.cursor):
        """
        Get records from pymongo cursor

        Args:
            cursor (pymongo.cursor): _description_

        Returns:
            _type_: _description_
        """
        records = [c for c in cursor]
        for record in records:
            record.pop('_id')
        return records

   
    def put(self, records):
        records = self.parser.parse_records_to_store(records)
        self.client[self.get_database()][self.get_collection()].insert_many(records)
    

    def get(self, start, stop):
        filter = self._generate_filter(start=start, stop=stop)
        cursor = self.client[self.get_database()][self.get_collection()].find(filter=filter)
        records =  self._get_records_from_cursor(cursor)
        return self.parser.parse_records_from_store(records)


    def get_all(self):
        filter = self._generate_filter(start=None, stop=None)
        cursor = self.client[self.get_database()][self.get_collection()].find(filter=filter)
        records =  self._get_records_from_cursor(cursor)
        return self.parser.parse_records_from_store(records)


    def remove(self, start, stop):
        filter = self._generate_filter(start=start, stop=stop)
        self.client[self.get_database()][self.get_collection()].remove(filter=filter)

    def delete_files(self):
        self.client[self.get_database()][self.get_collection()].drop()



    def get_first(self):
        cursor = [self.client[self.get_database()][self.get_collection()].find_one(sort=[(self.index, 1)])]
        records =  self._get_records_from_cursor(cursor)
        return self.parser.parse_records_from_store(records)

    def get_last(self):
        cursor = [self.client[self.get_database()][self.get_collection()].find_one(sort=[(self.index, -1)])]
        records =  self._get_records_from_cursor(cursor)
        return self.parser.parse_records_from_store(records)




class Influx_Store_Client(Store_Client):


    def __init__(self, client: InfluxDBClient, index: str, bucket: str, measurement: str):
        """
        Initialization

        Args:
            client (MongoClient): client to access MongoDb
            index (str): name of index field
            database (str): name of database
            collection (str): name of collection
        """
        self.client = client
        self.parser = Influx_Record_Parser()
        super().__init__(index, bucket, measurement)


    def __eq__(self, other: Store_Client) -> bool :
        """
        DUMMY IMPLEMENTATION, NEEDS UPDATE
        Implementation of equality method

        Args:
            other (Store_Client): object to compare to

        Returns:
            bool: True if self is considered equal to other
        """
        return True

    def _check_record_form(self, records_to_check):
        return True


    def get_bucket(self) -> str:
        """
        Get name of database

        Returns:
            str: name of database
        """
        return self.database

    def get_measurement(self) -> str:
        """
        Get name of collection

        Returns:
            str: collection name
        """
        return self.file


    def get_existing_files(self) -> list:
        """
        Get all files in the database

        Returns:
            list: list of database names
        """
        return self._get_existing_measurements()




    def _generate_query(self, start: datetime.datetime = None, stop: datetime.datetime = None) -> str:
        """
        Generate filter based in start and stop time

        Args:
            start (datetime.datetime, optional): start time. Defaults to None.
            stop (datetime.datetime, optional): stop time. Defaults to None.

        Returns:
            dict: representing filter
        """

        # issues with timezone patched with adding to time queries
        
        start_corrected = start - datetime.timedelta(hours=1)
        stop_corrected = stop - datetime.timedelta(hours=1)
        start_str = start_corrected.strftime("%Y-%m-%dT%H:%M:%SZ")
        stop_str = stop_corrected.astimezone().strftime("%Y-%m-%dT%H:%M:%SZ")

        if (start == None and stop == None) or self.index == None:
            filter = None
        else:
            if start == None:
                range = f'stop: {stop_str}'
            elif stop == None:
                range = f'start: {start_str}'
            else:
                range = f'start: {start_str}, stop: {stop_str}'
            filter = f'''from(bucket:"{self.get_bucket()}")
                        |> range({range})    
                        |> filter(fn: (r) => r["_measurement"] == "{self.get_measurement()}")
                        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                        |> drop(columns: ["_measurement", "result", "_start", "_stop", "host", "tag", "topic"])
                        '''

        return filter


    def _get_existing_measurements(self) -> list:
        """
        DUMMY IMPLEMENTATION
        Get names of existing collections

        Returns:
            list: names of collections
        """
        return [self.get_measurement()]


   
    def put(self, records):
        records = self.parser.parse_records_to_store(records)
        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=self.get_bucket(), record=records, data_frame_measurement_name=self.get_measurement())
    

    def get(self, start, stop):
        query = self._generate_query(start=start, stop=stop)
        query_api = self.client.query_api()
        df = query_api.query_data_frame(query)
        records = df.to_dict('records')
        return self.parser.parse_records_from_store(records)


    def get_all(self):
        stop = datetime.now()
        start = stop - datetime.timedelta(years=10)
        return self.get(start=start, stop=stop)


    def remove(self, start, stop):
        pass
    
    
    def delete_files(self):
        pass



    def get_first(self):
        stop = datetime.now()
        start = stop - datetime.timedelta(weeks=1)
        query = self._generate_query(start=start, stop=stop)
        query = query + '|> first(column: "_time")'
        query_api = self.client.query_api()
        df = query_api.query_data_frame(query)

    def get_last(self):
        stop = datetime.datetime.now()
        start = stop - datetime.timedelta(weeks=1)
        query = self._generate_query(start=start, stop=stop)
        query = query + '|> last(column: "_time")'
        query_api = self.client.query_api()
        df = query_api.query_data_frame(query)




class CSV_Store_Client(Store_Client):

    def __init__(self, index, directory, filename):
        self.filename = filename
        self.local_timezone = pytz.timezone('Europe/Amsterdam')
        self.store_timezone = pytz.UTC
        self.parser = CSV_Record_Parser(self.local_timezone, self.store_timezone)
        super().__init__(index, directory, filename)

    def __eq__(self, other):
        if not hasattr(other, 'local_timezone'):
            return False
        if not hasattr(other, 'store_timezone'):
            return False
        if not hasattr(other, 'parser'):
            return False
        if not hasattr(other, 'index'):
            return False
        if not hasattr(other, 'database'):
            return False
        if not hasattr(other, 'file'):
            return False                                                
        if self.local_timezone == other.local_timezone and \
            self.store_timezone == other.store_timezone and \
                self.parser == other.parser and \
                    self.index == other.index and \
                        self.database == other.database and \
                            self.file == other.file:
            return True
        else:
            return False


    def get_existing_files(self):
        return self.get_existing_filenames()

    def get_directory(self):
        return self.database

    def get_file(self):
        return self.file

    def get_full_file_name(self):
        return os.path.join(self.get_directory(), self.get_file())




    def _get_record_flags(self, records, start, stop):

        start = self.parser.parse_value_to_store(start)
        stop = self.parser.parse_value_to_store(stop)

        flags = []
        for record in records:
            if record[self.index] >= start and record[self.index] <= stop:
                flags.append(True)
            else:
                flags.append(False)

        return flags



    def get_existing_filenames(self):
        filenames = []
        for item in os.listdir(self.get_directory()):
            if os.path.isfile(os.path.join(self.get_directory(), item)):
                filenames.append(os.path.splitext(item)[0])
        return filenames


    def put(self, records):
        if not os.path.isdir(self.get_directory()):
            os.mkdir(self.get_directory())
        file_exists = os.path.isfile(self.get_full_file_name())
        keys = records[0].keys()
        with open(self.get_full_file_name(), 'a+', newline='') as f:
            writer = csv.DictWriter(f, keys)
            if not file_exists:
                writer.writeheader()
            records = self.parser.parse_records_to_store(records)
            writer.writerows(records)


    def get(self, start, stop):
        records = self.get_all()
        flags = self._get_record_flags(records, start, stop)
        selected_records = list(compress(records, flags))
        return selected_records


    def get_all(self):
        records = []
        with open(self.get_full_file_name(), newline='') as f:
            reader = csv.DictReader(f)
            records = [record for record in reader]
            records = self.parser.parse_records_from_store(records)
        return records
    

    def remove(self, start, stop):
        records = self.get_all()
        flags = self._get_record_flags(records, start, stop)
        inverted_flags = [not f for f in flags]
        selected_records = list(compress(records, inverted_flags))
        os.remove(self.get_full_file_name())
        self.put(selected_records)



    def get_first(self):
        records = self.get_all()
        return self._get_min_record(records, self.index)


    def get_last(self):
        records = self.get_all()
        return self._get_max_record(records, self.index)
    
    def delete_files(self):
        filename = self.get_full_file_name()
        if os.path.exists(filename):
            os.remove(filename)


class Zero_Store_Client(Store_Client):

    def __init__(self, fields, index):
        self.fields = fields
        self.nr_records = 100
        self.local_timezone = pytz.timezone('Europe/Amsterdam')
        self.first_time = self.local_timezone.localize(datetime.datetime(2018, 1, 1, 0, 0))
        super().__init__(index, None, None)

    def _generate_record(self, time):
        record = {}
        record[self.get_index()] = time
        for field in self.fields:
                record[field] = 0
        return record

    def get_existing_files(self):
        return None
    
    def delete_files(self):
        pass

    def put(self, records):
        pass

    def remove(self, start, stop):
        pass

    def get(self, start, stop):
        records = []
        for i in range(self.nr_records):
            _time = start + (stop - start) * i / self.nr_records
            records.append(self._generate_record(_time))
        return records
    
    def get_all(self):
        now = self.local_timezone.localize(datetime.datetime.now())
        return self.get(self.first_time, now)
 
    def get_first(self):
        return [self._generate_record(self.first_time)]


    def get_last(self):
        now = self.local_timezone.localize(datetime.datetime.now())
        return [self._generate_record(now)]




class Store_Client_Decorator(Store_Client):

    def __init__(self, store_client=None):
        self.store_client = store_client
    
    def set_store_client(self, store_client):
        self.store_client = store_client
    
    def put(self, records):
        self.store_client.put(records)

    def get(self, start, stop):
        return self.store_client.get(start, stop)
    
    def get_existing_files(self):
        return self.store_client.get_existing_files()

    def get_all(self):
        return self.store_client.get_all()

    def get_first(self):
        return self.store_client.get_first()

    def get_last(self):
        return self.store_client.get_last()

    def set_file(self, file):
        self.store_client.set_file(file)

    def get_file(self):
        return self.store_client.get_file()
    
    def delete_files(self):
        return self.store_client.delete_files()
    
    def get_index(self):
        return self.store_client.get_index()

    def set_index(self, index):
        self.store_client.set_index(index)
    
    def remove(self, start, stop):
        self.store_client.remove(start, stop)
    
    def get_database(self):
        return self.store_client.get_database()

class Distributed_Store_Client_Decorator(Store_Client_Decorator):

    def __init__(self, field, id_generator=None, id_reversor=None, fast_search=True):
        self.field = field
        self.id_generator = id_generator
        self.id_reversor = id_reversor
        self.fast_search = fast_search
        super().__init__()

    def __eq__(self, other):
        if not hasattr(other, 'field'):
            return False
        if not hasattr(other, 'id_generator'):
            return False
        if not hasattr(other, 'id_reversor'):
            return False
        if not hasattr(other, 'fast_search'):
            return False
        if not hasattr(other, 'store_client'):
            return False                                                        
        if self.field == other.field and \
            self.id_generator.__code__.co_code == other.id_generator.__code__.co_code and \
                self.id_reversor.__code__.co_code == other.id_reversor.__code__.co_code and \
                    self.fast_search == other.fast_search and \
                        self.store_client == other.store_client:
            return True
        else:
            return False


    @staticmethod
    def get_suffix(id):
        return f'_{id}'
    
    def set_field(self, field):
        self.field = field

    def get_field(self):
        return self.field

    def _filename_matches_pattern(self, filename):
        id = filename.replace(self.get_file() + '_', '')
        try:
            self.id_reversor(id)
            return True
        except:
            return False
    
    def _get_id_from_filename(self, filename):
        if self._filename_matches_pattern(filename):
            return filename[len(self.get_file())+1:]
        else:
            return None
    
    def _get_id(self, value):
        return self.id_generator(value)
    
    def _reverse_id(self, id):
        return self.id_reversor(id)


    def _try_fast_search(self):
        return self.get_field() == self.get_index() and self.fast_search


    def _get_valid_filenames(self):
        filenames = self.get_existing_files()
        valid_filenames = []
        for filename in filenames:
            if self._filename_matches_pattern(filename):
                valid_filenames.append(filename)
        return valid_filenames     


    def _get_extreme_filenames(self):
        filenames = self.get_existing_files()
        id_values, valid_filenames = [], []
        for filename in filenames:
            if self._filename_matches_pattern(filename):
                id_values.append(self._reverse_id(self._get_id_from_filename(filename)))
                valid_filenames.append(filename)
        min_index = id_values.index(min(id_values))
        max_index = id_values.index(max(id_values))
        return {'first': valid_filenames[min_index], 'last': valid_filenames[max_index]}


    def _get_first_filename(self):
        return self._get_extreme_filenames()['first']


    def _get_last_filename(self):
        return self._get_extreme_filenames()['last']


    def _get_filenames_between(self, start, stop):
        filenames = self.get_existing_files()
        valid_filenames = []
        for filename in filenames:
            if self._filename_matches_pattern(filename):
                id_value = self._reverse_id(self._get_id_from_filename(filename))
                if id_value >= self.id_generator(start) and id_value <= self.id_generator(stop):
                    valid_filenames.append(filename)
        return valid_filenames




    def put(self, records):
        ids = []
        separated_records = []
        for record in records:
            id = self._get_id(record[self.field])
            if id in ids:
                index = ids.index(id)
                separated_records[index].append(record)
            else:
                ids.append(id)
                separated_records.append([record])
        
        filename = self.get_file()
        for id, _records in zip(ids, separated_records):
            self.set_file(filename + self.get_suffix(id))
            try:
                super().put(_records)
            except Exception as e:
                raise e
            finally:
                self.set_file(filename)

 

    def _do_for_files(self, _filenames):
        def decorate(method):
            @wraps(method)
            def wrapper(*args):
                records = []
                base_filename = self.get_file()
                filenames = [_filenames] if not isinstance(_filenames, list) else _filenames
                for filename in filenames:
                    self.set_file(filename)
                    result = method(*args)
                    if result is not None:
                        records += method(*args)
                    self.set_file(base_filename)
                if len(records) > 0:
                    return records
                else:
                    return None  
            return wrapper
        return decorate



    def get(self, start, stop):
        if self._try_fast_search():
            return self._do_for_files(self._get_filenames_between(start, stop))(self.store_client.get)(start, stop)
        else:
            return self._do_for_files(self._get_valid_filenames())(self.store_client.get)(start, stop)


    def get_all(self):
        return self._do_for_files(self._get_valid_filenames())(self.store_client.get_all)()


    def remove(self, start, stop):

        if self._try_fast_search():
            return self._do_for_files(self._get_filenames_between(start, stop))(self.store_client.remove)(start, stop)
        else:
            return self._do_for_files(self._get_valid_filenames())(self.store_client.remove)(start, stop)


    def delete_files(self):
        self._do_for_files(self._get_valid_filenames())(self.store_client.delete_files)()



    def get_first(self):
        if self._try_fast_search():
            return self._do_for_files(self._get_first_filename())(self.store_client.get_first)()
        else:
            records = self._do_for_files(self._get_valid_filenames())(self.store_client.get_first)()
            return self._get_min_record(records, self.get_index())

 
    def get_last(self):
        if self._try_fast_search():
            return self._do_for_files(self._get_last_filename())(self.store_client.get_last)()
        else:
            records = self._do_for_files(self._get_valid_filenames())(self.store_client.get_last)()
            return self._get_max_record(records, self.get_index())




class No_Duplicates_Store_Client_Decorator(Store_Client_Decorator):

    def __init__(self, field, resolution):
        self.field = field
        self.resolution = resolution
        super().__init__()

    def __eq__(self, other):
        if not hasattr(other, 'field'):
            return False
        if not hasattr(other, 'resolution'):
            return False
        if not hasattr(other, 'store_client'):
            return False                                
        if self.field == other.field and \
            self.resolution == other.resolution and \
                self.store_client == other.store_client:
            return True
        else:
            return False


    def _are_within_resolution(self, r1, r2):
        return abs(r1[self.field] - r2[self.field]) <= self.resolution



    def _get_unique_records(self, records):
        flags = [True for _ in records]
        for i, outer_record in enumerate(records):
            if flags[i]:
                for n, inner_record in enumerate(records):
                    if self._are_within_resolution(outer_record, inner_record) and (i != n):
                        flags[n] = False
        unique_records = list(compress(records, flags))
        return unique_records



    def put(self, records):
        unique_records = self._get_unique_records(records)
        super().put(unique_records)

    def get(self, start, stop):
        records = super().get(start, stop)
        unique_records = self._get_unique_records(records)
        return unique_records

    def get_all(self):
        records = super().get_all()
        unique_records = self._get_unique_records(records)
        return unique_records

    def remove(self, start, stop):
        super().remove(start, stop)

    def delete_files(self):
        super().delete_files()


    def get_first(self):
        super().get_first()

    def get_last(self):
        super().get_last()



class Lazy_Loader_Store_Client_Decorator(Store_Client_Decorator):
    """
    Store client decorator that enables lazy get operations on a store

    """

    def __init__(self, lazy_interval=datetime.timedelta(seconds=5)):
        """
        Initialization

        Args:
            lazy_interval (timedelta, optional): _description_. Defaults to datetime.timedelta(seconds=5).
        """
        self.lazy_interval = lazy_interval
        self.execution_register = []
        super().__init__()


    def __eq__(self, other):
        """
        Implementation of equality method

        Args:
            other (Store_Client_Decorator): object to compare to

        Returns:
            boolean: True if self and other are equal
        """
        if not hasattr(other, 'lazy_interval'):
            return False
        if not hasattr(other, 'store_client'):
            return False
        if self.lazy_interval == other.lazy_interval and \
            self.store_client == other.store_client:
            return True
        return False


    def _find_in_register(self, name, args):
        """
        Find similar operations in execution register. Equallity is checked on name and
        arguments of operation.

        Args:
            name (string): name of operation
            args (list): list of arguments used for operation

        Returns:
            dict: found operations
        """
        operations = []
        for operation in self.execution_register:
            if operation['name'] == name and operation['args'] == args:
                operations.append(operation)
        return operations



    def _add_to_register(self, name, args, time, result):
        """
        Add operation to execution register

        Args:
            name (string): name
            args (list): arguments
            time (datetime): time on which operation is executed
            result (): retrieved data in operation
        """
        self.execution_register.append(
            {
                'name': name,
                'args': args,
                'time': time,
                'result': result
            }
        )

    def _remove_from_register(self, name, args):
        """
        Remove operations from execution register

        Args:
            name (string): name of operation
            args (list): arguments of operation
        """
        for i, operation in enumerate(self.execution_register):
            if operation['name'] == name and operation['args'] == args:
                self.execution_register.pop(i)



    def _get_best_result_from_register(self, name, args, time):
        """
        Search for operations in execution register that are equal to specified operation and
        are recently executed

        Args:
            name (string): name
            args (list): arguments
            time (datetime): time at which operation should be executed

        Returns:
            dict: representing operation
        """
        operations = self._find_in_register(name, args)
        logger.debug(f'searching for previous operation {name} with args {args} at time {time}')
        result = None
        for operation in operations:
            if (time - operation['time']) < self.lazy_interval:
                if result == None:
                    result = operation
                elif result['time'] < operation['time']:
                    result = operation
        logger.debug(f'result found: {result!=None}')
        return result



    def do_lazy(method):
        """
        Decorator function to perform a lazy operator

        Args:
            method (method): method on which to apply decorator
        """
        def inner(self, *args):
            now = datetime.datetime.now()
            name = method.__name__
            best_operation_from_register = self._get_best_result_from_register(name, args, now)
            if best_operation_from_register is None:
                result = method(self, *args)
                self._remove_from_register(name, args)
                self._add_to_register(name, args, now, result)
            else:
                result = best_operation_from_register['result']
            return result
        return inner
    

    @do_lazy
    def get(self, start, stop):
        """
        Get data from store with lazy decorator

        Args:
            start (datetime): start time
            stop (datetime): stop time

        Returns:
            dataframe: data from store
        """
        return super().get(start, stop)

    @do_lazy
    def get_all(self):
        """
        Get all records from store zith lazy decorator

        Returns:
            dataframe: data from store
        """
        return super().get_all()

    @do_lazy
    def get_first(self):
        """
        Get first record from store zith lazy decorator

        Returns:
            dataframe: first record from store
        """
        return super().get_first()

    @do_lazy
    def get_last(self):
        """
        Get last record from store zith lazy decorator

        Returns:
            dataframe: last record from store
        """
        return super().get_last()





class Transformer():

    def __init__(self):
        pass

    def __eq__(self, other):
        return True

    def to_records(self, df):
        return df.reset_index().to_dict('records')

    def to_df(self, records, index_field):
        records = [records] if not isinstance(records, list) else records
        df = pd.DataFrame.from_records(records)
        df.set_index(index_field, inplace=True)
        return df



class UnitTransformer():

    def __init__(self):
        pass

    def __eq__(self, other):
        return True

    def to_records(self, df):
        return df

    def to_df(self, records, index_field):
        return records


class Record_Parser(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def __eq__(self, other):
        return True

    @abstractmethod
    def parse_records_to_store(self, records):
        pass

    @abstractmethod
    def parse_records_from_store(self, records):
        pass

    @abstractmethod
    def parse_value_to_store(self, value):
        pass

    @abstractmethod
    def parse_value_from_store(self, value):
        pass


    def _parse_time_value(self, value, source_timezone, target_timezone):
        if value.tzinfo is None:
            localized_value = source_timezone.localize(value)
        else:
            localized_value = value
        converted_value = localized_value.astimezone(target_timezone)
        return converted_value


    def _parse_time_delta_to_store(self, value):
        if type(value) is pd.Timedelta:
            return str(value.to_pytimedelta().total_seconds()) + 's'
        elif type(value) is datetime.timedelta:
            return str(value.total_seconds()) + 's'
        raise Exception


    def _parse_records_by_value(self, records, value_parser):
        parsed_records = []
        logger.debug(f'parsing column {records[0].items()}')
        for record in records:
            parsed_record = {}
            for key, value in record.items():
                parsed_record[key] = value_parser(value)
            parsed_records.append(parsed_record)
        return parsed_records




class Influx_Record_Parser(Record_Parser):

    def __init__(self):
        pass

    def __eq__(self, other):
        return True

    def parse_records_to_store(self, records):
        return records

    def parse_records_from_store(self, records):
        return records

    def parse_value_to_store(self, value):
        return value

    def parse_value_from_store(self, value):
        return value





class Mongo_Record_Parser(Record_Parser):

    def __init__(self, local_timezone, store_timezone):
        self.local_timezone = local_timezone
        self.store_timezone = store_timezone


    #def parse_records_from_store(self, records):
    #    return self._parse_records_by_value(records, self.parse_value_from_store)


    def __eq__(self, other):
        if self.local_timezone == other.local_timezone:
            if self.store_timezone == other.store_timezone:
                return True
        return False



    def _get_column(self, records, key):
        column_records = []
        for record in records:
            column_records.append({key: record[key]})
        return column_records


    def _get_column_types(self, records, key):
        column_types = []
        for record in records:
            column_types.append(type(record[key]))
        return column_types
    
    def _replace_column(self, records, column_records):
        if len(records) == 0:
            return column_records
        for record, column_record in zip(records, column_records):
            key, value = next(iter(column_record.items()))
            record[key] = value
        return records
    
    def _get_record_keys(self, records):
        keys = []
        for key, _ in records[0].items():
            keys.append(key)
        return keys



    def parse_records_from_store(self, records):
        if len(records) == 0:
            return records
        
        parsed_records = []
        
        for key in self._get_record_keys(records):
            column = self._get_column(records, key)
            logger.debug(f'column found: name {key}, size {len(column)} by {len(column[0])}')
            parsed_column = self._parse_records_by_value(column, self.parse_value_from_store)
            column_types = set(self._get_column_types(parsed_column, key))

            if datetime.date in column_types and datetime.datetime in column_types:
                date_preventing_parser = lambda v: self.parse_value_from_store(v, prevent_date=True)
                parsed_column = self._parse_records_by_value(column, date_preventing_parser)
            
            parsed_records = self._replace_column(parsed_records, parsed_column)
        
        logger.debug(f'finished parsing...')
        
        return parsed_records
            

    def parse_records_to_store(self, records):
        return self._parse_records_by_value(records, self.parse_value_to_store)



    def parse_value_from_store(self, value, prevent_date=False):
        try:
            if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0:
                if prevent_date == False:
                    parsed_value = value.date()
                    return parsed_value
        except:
            pass
        try:
            parsed_value = self._parse_time_value(value, self.store_timezone, self.local_timezone)
            return parsed_value
        except:
            pass
        try:
            seconds = pytimeparse.parse(value)
            return datetime.timedelta(seconds=seconds)
        except:
            pass
        return value


    def parse_value_to_store(self, value):
        try:
            return self._parse_time_delta_to_store(value)
        except:
            pass
        if type(value) is datetime.date and not isinstance(value, pd.Timestamp):
            return datetime.datetime.combine(value, datetime.datetime.min.time())
        try:
            return self._parse_time_value(value, self.local_timezone, self.store_timezone)
        except:
            pass
        return value





class CSV_Record_Parser(Record_Parser):

    def __init__(self, local_timezone, store_timezone):
        self.local_timezone = local_timezone
        self.store_timezone = store_timezone

    def __eq__(self, other):
        if self.local_timezone == other.local_timezone:
            if self.store_timezone == other.store_timezone:
                return True
        return False



    def parse_records_from_store(self, records):
        return self._parse_records_by_value(records, self.parse_value_from_store)

    def parse_records_to_store(self, records):
        return self._parse_records_by_value(records, self.parse_value_to_store)


    def parse_value_from_store(self, value):
        try:
            return float(value)
        except:
            pass
        try:
            return datetime.date.fromisoformat(value)
        except:
            pass
        try:
            seconds = pytimeparse.parse(value)
            return datetime.timedelta(seconds=seconds)
        except:
            pass
        try:
            value = dateutil.parser.parse(value)
            return self._parse_time_value(value, self.store_timezone, self.local_timezone)
        except:
            pass
        return value
    
    def parse_value_to_store(self, value):
        try:
            return self._parse_time_delta_to_store(value)
        except:
            pass
        try:
            return self._parse_time_value(value, self.local_timezone, self.store_timezone)
        except:
            pass
        return value



class Store_Factory_Mixin():

    @staticmethod
    def date_distributed_decorator(index):
        return Distributed_Store_Client_Decorator(
                index,
                id_generator=lambda x: x.date(),
                id_reversor=lambda x: datetime.date.fromisoformat(x),
            )


    @staticmethod
    def decimal_distributed_decorator(index):
        return Distributed_Store_Client_Decorator(
                index,
                id_generator=lambda x: int(x * 10),
                id_reversor=lambda x: float(x) / 10,
            )


    @staticmethod
    def lazy_decorator_from_seconds(seconds):
        return Lazy_Loader_Store_Client_Decorator(lazy_interval=datetime.timedelta(seconds=seconds))





    @staticmethod
    def create(client, transformer, decorators=None):
        if decorators is None:
            return Store_Manager(client, transformer)
        if len(decorators) == 0:
            return Store_Manager(client, transformer)
        decorators = [decorators] if not isinstance(decorators, list) else decorators
        last_decorator = decorators[-1]
        last_decorator.set_store_client(client)
        for decorator in decorators[:-1]:
            decorator.set_store_client(last_decorator)
            last_decorator = decorator
        return Store_Manager(last_decorator, transformer)



class Store_Factory(Store_Factory_Mixin):

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'type': {'type': 'string'}
        }
 

    def create_from_config(self, config_store, section):

        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)


        if params['type'] == 'MONGODB':
            return Mongo_Store_Factory().create_from_config(config_store, section)
        elif params['type'] == 'CSV':
            return CSV_Store_Factory().create_from_config(config_store, section)
        elif params['type'] == 'zero':
            return Zero_Store_Factory().create_from_config(config_store, section)
        elif params['type'] == 'INFLUXDB':
            return Influx_Store_Factory().create_from_config(config_store, section)            
        else:
            pass





class Zero_Store_Factory(Store_Factory_Mixin):

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'index': {'type': 'string', 'default': 'a'},
            'fields': {'type': 'string_list', 'default': ['a', 'b']},
        }

   

    def create_from_config(self, config_store, section):

        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        client = Zero_Store_Client(params['fields'], params['index'])
        return self.create(client, Transformer())      







class CSV_Store_Factory(Store_Factory_Mixin):

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'index': {'type': 'string'},
            'directory': {'type': 'string'},
            'filename': {'type': 'string'},
            'distributed': {'type': 'bool', 'default': False},
            'distributor': {'type': 'string', 'default': 'date'},     
            'lazy': {'type': 'bool', 'default': False},
            'lazy_seconds': {'type': 'int', 'default': 5},
        }

   

    def create_from_config(self, config_store, section):

        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)


        client = CSV_Store_Client(params['index'], params['directory'], params['filename'])
        decorators = []
        if params['lazy']:
            decorators.append(self.lazy_decorator_from_seconds(params['lazy_seconds']))
        if params['distributed'] and params['distributor'] == 'date':
            decorators.append(self.date_distributed_decorator(params['index']))
        elif params['distributed'] and params['distributor'] == 'decimal':
            decorators.append(self.decimal_distributed_decorator(params['index']))
        return self.create(client, Transformer(), decorators=decorators)      





class Mongo_Store_Factory(Store_Factory_Mixin):

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'url': {'type': 'string'},
            'index': {'type': 'string'},
            'database': {'type': 'string'},
            'collection': {'type': 'string'},
            'distributed': {'type': 'bool', 'default': False},
            'distributor': {'type': 'string', 'default': 'date'},            
            'lazy': {'type': 'bool', 'default': False},
            'lazy_seconds': {'type': 'int', 'default': 5},         
        }

   

    def create_from_config(self, config_store, section):

        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        mongoclient = MongoClient(params['url'])
        client = Mongo_Store_Client(mongoclient, params['index'], params['database'], params['collection'])
        decorators = []
        if params['lazy']:
            decorators.append(self.lazy_decorator_from_seconds(params['lazy_seconds']))
        if params['distributed'] and params['distributor'] == 'date':
            decorators.append(self.date_distributed_decorator(params['index']))
        elif params['distributed'] and params['distributor'] == 'decimal':
            decorators.append(self.decimal_distributed_decorator(params['index']))
        return self.create(client, Transformer(), decorators=decorators)



class Influx_Store_Factory(Store_Factory_Mixin):

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'url': {'type': 'string'},
            'index': {'type': 'string', 'default': '_time'},
            'bucket': {'type': 'string'},
            'organization': {'type': 'string'},
            'token': {'type': 'string'},
            'measurement': {'type': 'string'},
        }

   

    def create_from_config(self, config_store, section):

        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        influxclient = InfluxDBClient(
            url=params['url'],
            token=params['token'],
            org=params['organization']
            )
        client = Influx_Store_Client(influxclient, params['index'], params['bucket'], params['measurement'])
        decorators = []
        return self.create(client, Transformer(), decorators=decorators)




class Store_Register():

    def __init__(self, stores=[]):
        self.stores = stores

    def register(self, store):
        if store not in self.stores:
            self.stores.append(store)
            return self.stores[-1]
        else:
            i = self.stores.index(store)
            return self.stores[i]

