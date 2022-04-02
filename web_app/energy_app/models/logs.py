from abc import ABC, abstractmethod
import datetime
from unittest import result
import pandas as pd
import logging
import math
from functools import reduce
import uuid


from services import inputs
from tools import web_app_tools
from web_app.energy_app.models.basic import Model
logger = logging.getLogger('web_app_moddels')





class Request_Parser(ABC):
    @abstractmethod
    def parse(self, request):
        return {'args': [], 'kwargs': {}}

class Day_Request_Parser(Request_Parser):
    
    def parse(self, request):
        to_date = lambda ds: datetime.datetime.strptime(ds, "%Y-%m-%d").date()
        requested_date = request.args.get('date', default=datetime.date.today(), type=to_date)
        return {'args': [requested_date], 'kwargs': {}}



class Last_N_Days_Request_Parser(Request_Parser):
    
    def __init__(self, n_days=30):
        self.n_days = n_days
    
    def parse(self, request):
        stop = datetime.datetime.now()
        start = stop - datetime.timedelta(days=self.n_days)
        return {'args': [start, stop], 'kwargs': {}}




class Processor(ABC):
    @abstractmethod
    def process(self, df):
        pass

class Idle_Processor(Processor):
    
    def process(self, df):
        return df



class PV_Consumption_Processor(Processor):

    def process(self, df):


        df.rename(columns = {
            'actual_elec_delivered':'from_PV',
            'actual_elec_used': 'from_grid',
            'actual_elec_returned': 'to_grid',
            'total_elec_delivered': 'from_PV_cum'
            },
            inplace = True)

        df['from_PV'] = df['from_PV'] / 1000
        df['from_PV_cum'] = df['from_PV_cum'] / 1000


        df['to_consumers'] = df['from_PV'] + df['from_grid'] - df['to_grid']
        df['from_grid_cum'] = df['elec_used_t1'] + df['elec_used_t2']
        df['to_grid_cum'] = df['elec_returned_t1'] + df['elec_returned_t2']

        df.drop(columns=['elec_used_t1', 'elec_used_t2', 'elec_returned_t1', 'elec_returned_t2',
            'actual_tariff'], inplace=True)
        
        return df



class Totals_PV_Consumption_Processor(Processor):

    def process(self, df):


        df.rename(columns = {
            'actual_elec_delivered':'from_PV',
            'actual_elec_used': 'from_grid',
            'actual_elec_returned': 'to_grid',
            'total_elec_delivered': 'from_PV_cum'
            },
            inplace = True)

        df['from_PV'] = df['from_PV'] / 1000
        df['from_PV_cum'] = df['from_PV_cum'] / 1000
        df['from_PV_cum'] = df['from_PV_cum'].clip(lower=0)


        df['to_consumers'] = df['from_PV'] + df['from_grid'] - df['to_grid']
        df['from_grid_cum'] = df['elec_used_t1'] + df['elec_used_t2']
        df['to_grid_cum'] = df['elec_returned_t1'] + df['elec_returned_t2']

        df.drop(columns=['elec_used_t1', 'elec_used_t2', 'elec_returned_t1', 'elec_returned_t2',
            'actual_tariff', 'from_PV'], inplace=True)
        
        return df





class Gas_Consumption_Processor(Processor):

   
    def process(self, df):
        s = df['gas_used']
        s.sort_index(inplace=True)
        s_diff = s.diff() / (s.index.to_series().diff().dt.total_seconds() / 3600)
        s_diff.dropna(inplace=True)
        df = s_diff.to_frame(name='gas_used')
        return df







class Log_Data_Model(Model):

    def __init__(
        self,
        _inputs,
        title,
        fields=None,
        unit='',
        x_format='time', 
        processor=Idle_Processor(),
        request_parser=Day_Request_Parser()
        ):


        self.inputs = _inputs
        self.request_parser = request_parser
        self.processor = processor
        self.fields = fields
        self.unit = unit
        self.x_format = x_format
        super().__init__(title)


    def get_inputs(self, parsed_request):
        dfs = []
        args = parsed_request['args']
        kwargs = parsed_request['kwargs']
        for input in self.inputs:
            try:
                dfs.append(input.get(*args, **kwargs))
            except:
                pass
        return dfs


    def merge_inputs(self, dfs):
        df = reduce(lambda left,right: pd.merge(left, right, how='outer', left_index=True, right_index=True), dfs)
        try:
            df.interpolate(inplace=True)
            df.fillna(method='bfill', inplace=True)
            df.dropna(inplace=True)
        except:
            df = df.interpolate(method='bfill').interpolate(method='ffill')
        return df


    def generate_result(self, df):

        result = {}
        result['title'] = self.title
        result['x_labels'] = list(df.index)
        result['last_updated'] = max(df.index)
        result['series'] = []
        result['id'] = uuid.uuid1()
        result['unit'] = self.unit
        result['x_format'] = self.x_format

        if self.fields is None:
            fields = df.columns
        else:
            fields = self.fields

        for field in fields:
            result['series'].append(
                {
                    'label': field,
                    'data': list(df[field].values),
                }
            )
        
        return result


    def get(self, request):

        parsed_request = self.request_parser.parse(request)
        dfs = self.get_inputs(parsed_request)
        df = self.merge_inputs(dfs)
        df = self.processor.process(df)
        result = self.generate_result(df)

        return result

    def post(self, request):
        pass

