import os
import datetime
import random
import time
import uuid
import json
import pickle
import json
from queue import Queue


import pytest
import pytz
import pandas as pd
import numpy as np





class Dataframe_Generator():

    _CONSISTENT_FORM = {'a': 'datetime', 'b': 'date', 'c': 'datetime', 'd': 'str', 'e': 'float', 'f': 'timedelta'}
    _TIME_ZONE = pytz.timezone('Europe/Amsterdam')

    def __init__(self, nr_records=None, days_back=None, form=_CONSISTENT_FORM):
        self.nr_records = nr_records
        self.days_back = days_back
        self.form = form


    def set_days_back(self, days_back):
        self.days_back = days_back
    
    def set_nr_records(self, nr_records):
        self.nr_records = nr_records

    def randomize_form(self, index_type='random'):
        nr_columns = random.randint(2, 10)
        form = {}
        if index_type == 'random':
            form[str(uuid.uuid4())] = random.choice(['datetime', 'date', 'float'])
        else:
            form[str(uuid.uuid4())] = index_type
        for _ in range(nr_columns - 1):
            key = str(uuid.uuid4())
            _type = random.choice(['datetime', 'date', 'str', 'float', 'timedelta'])
            form[key] = _type
        self.form = form


    def make_form_consistent(self):
        self.form = self._CONSISTENT_FORM

    def get_form(self):
        return self.form

    def generate(self):

        nr_records = random.randint(1,200) if self.nr_records is None else self.nr_records
        days_back = random.randint(5,100) if self.days_back is None else self.days_back
        form = self.form
        

        records = []
        for _ in range(nr_records):
            record = {}
            for field, field_type in form.items():
                if field_type == 'datetime':
                    start_time = datetime.datetime.now(self._TIME_ZONE) - datetime.timedelta(days=days_back)
                    stop_time = datetime.datetime.now(self._TIME_ZONE)
                    record[field] = start_time + (stop_time - start_time) * random.random()
                elif field_type == 'date':
                    start_time = datetime.date.today() - datetime.timedelta(days=days_back)
                    stop_time = datetime.date.today()
                    record[field] = start_time + datetime.timedelta(days=int(days_back*random.random()))
                elif field_type == 'str':
                    record[field] = str(uuid.uuid4())
                elif field_type == 'timedelta':
                    days = random.randint(-100, 100)
                    hours = random.randint(0, 23)
                    minutes = random.randint(0, 59)
                    seconds = random.randint(0, 59)
                    record[field] = datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
                else:
                    record[field] = random.random()
            records.append(record)

        df = pd.DataFrame.from_records(records)
        df.set_index(df.columns[0], inplace=True)
        return df




