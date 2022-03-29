from abc import ABC, abstractmethod
import datetime
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
        pass

class Day_Request_Parser(Request_Parser):
    def parse(self, request):
        to_date = lambda ds: datetime.datetime.strptime(ds, "%Y-%m-%d").date()
        requested_date = request.args.get('date', default=datetime.date.today(), type=to_date)
        return requested_date



class Processor(ABC):
    @abstractmethod
    def process(self, dfs):
        pass


class Field_Picker(Processor):

    def __init__(self, series, unit):
        self.series = series
        self.unit = unit

    def process(self, dfs):

        #start_time = min(dsmr_log.index.to_pydatetime())
        #stop_time = max(dsmr_log.index.to_pydatetime())
        #log = log[(log.index > start_time) & (log.index < stop_time)]

        df = reduce(lambda left,right: pd.merge(left, right, how='outer', left_index=True, right_index=True), dfs)
        df.interpolate(inplace=True)
        df.fillna(method='bfill', inplace=True)
        df.dropna(inplace=True)

        result = {}
        result['x_labels'] = list(df.index)
        result['last_updated'] = max(df.index)
        result['series'] = []
        result['id'] = uuid.uuid1()
        result['unit'] = self.unit

        for serie in self.series:
            result['series'].append(
                {
                    'label': serie,
                    'data': list(df[serie].values),
                }
            )
        
        return result


class PV_Consumption_Processor(Processor):

    def __init__(self, PV_label='Solar Power', cons_label='Consumption'):
        self.PV_label = PV_label
        self.cons_label = cons_label

    def process(self, dfs):

        #start_time = min(dsmr_log.index.to_pydatetime())
        #stop_time = max(dsmr_log.index.to_pydatetime())
        #log = log[(log.index > start_time) & (log.index < stop_time)]

        df = reduce(lambda left,right: pd.merge(left, right, how='outer', left_index=True, right_index=True), dfs)
        df.interpolate(inplace=True)
        df.fillna(method='bfill', inplace=True)
        df.dropna(inplace=True)


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


        #self.filter_bins = 10
        #self.downsample_rate = 5
        #df['from_PV_filtered'] = df['from_PV'].rolling(window=self.filter_bins).mean().fillna(method='bfill')
        #df['to_consumers_filtered'] = df['to_consumers'].rolling(window=self.filter_bins).mean().fillna(method='bfill')

        df.drop(columns=['elec_used_t1', 'elec_used_t2', 'elec_returned_t1', 'elec_returned_t2',
            'actual_tariff'], inplace=True)



        result = {}
        result['x_labels'] = list(df.index)
        result['last_updated'] = max(df.index)
        result['series'] = []
        result['unit'] = 'kWh'
        result['id'] = uuid.uuid1()

        result['series'].append(
            {
                'label': self.PV_label,
                'data': list(df['from_PV'].values),
            }
        )
        
        result['series'].append(
            {
                'label': self.cons_label,
                'data': list(df['to_consumers'].values),
            }
        )


        return result





class Day_Data_Model(Model):

    def __init__(self, _inputs, title, processor, request_parser=Day_Request_Parser()):


        self.inputs = _inputs
        self.request_parser = request_parser
        self.processor = processor
        super().__init__(title)


    def get_inputs(self, request):
        to_date = lambda ds: datetime.datetime.strptime(ds, "%Y-%m-%d").date()
        requested_date = request.args.get('date', default=datetime.date.today(), type=to_date)
        results = []
        for input in self.inputs:
            try:
                df = input.input.get(requested_date)
                df = df[input['field']]
                results.append[df]
            except:
                pass
        return results




    def get(self, request):

        parsed_request = self.request_parser.parse(request)
        dfs = []
        for input in self.inputs:
            try:
                dfs.append(input.get(parsed_request))
            except:
                pass
        

        result = self.processor.process(dfs)


        dfs = self.get_inputs(request)
        result['title'] = self.title

        return result

    def post(self, request):
        pass

