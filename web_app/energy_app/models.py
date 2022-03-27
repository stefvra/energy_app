from abc import ABC, abstractmethod
from concurrent.futures import process
import datetime
from multiprocessing.sharedctypes import Value
from turtle import color
from flask import render_template, url_for, redirect, request
import pandas as pd
import logging
import math
from pandas.api.types import is_numeric_dtype
from functools import reduce


from services import inputs
from tools import web_app_tools
logger = logging.getLogger('web_app_moddels')


class Model(ABC):

    def __init__(self, title):
        self.title = title

    @abstractmethod
    def get(self, request):
        pass

    @abstractmethod
    def post(self, request):
        pass



class Totals_Data_Model(Model):

    def __init__(self, title):
        super().__init__(title)


    @abstractmethod
    def get_inputs(self, request):
        pass


    def get(self, request):
        input_data = self.get_inputs(request)
        for i in input_data:
            self.drop_non_numericals(i)
            self.rename_df(i)
        result = self.get_data(request, input_data)
        result = self.complete_result(request, result)
        return result

    def complete_result(self, request, result):
        result['title'] = self.title
        result['title'] = f'Totals for (add correct title)'
        return result

    @staticmethod
    def drop_non_numericals(df):
        if df is not None:
            for column in df.columns:
                if not is_numeric_dtype(df[column]):
                    df.drop(labels=column, axis=1, inplace=True)

    @staticmethod
    def rename_df(df):
        df.rename(columns = {
            'actual_elec_delivered':'from_PV',
            'actual_elec_used': 'from_grid',
            'actual_elec_returned': 'to_grid',
            'total_elec_delivered': 'from_PV_cum'
            },
            inplace = True)



    def get_data(self, request, input_data):

        df_pv = input_data['pv']
        df_dsmr = input_data['dsmr']

        dsmr_data_available = df_dsmr is not None
        pv_data_available = df_pv is not None


        if pv_data_available:
            pv_start = df_pv.iloc[0]
            pv_stop = df_pv.iloc[-1]
            from_PV_today = (pv_stop['from_PV_cum'] - pv_start['from_PV_cum']) / 1000
            PV_last_updated = pv_stop.index.to_pydatetime()[-1]
        else:
            from_PV_today = 'N/A'
            PV_last_updated = 'N/A'

        if dsmr_data_available:
            dsmr_start = df_dsmr.iloc[0]
            dsmr_stop = df_dsmr.iloc[-1]
            from_grid_today = dsmr_stop['elec_used_t1'] + dsmr_stop['elec_used_t2'] - dsmr_start['elec_used_t1'] - dsmr_start['elec_used_t2']
            to_grid_today = dsmr_stop['elec_returned_t1'] + dsmr_stop['elec_returned_t2'] - dsmr_start['elec_returned_t1'] - dsmr_start['elec_returned_t2']
            today_actual_cost = self.cost_calculator.calculate(from_grid=from_grid_today, to_grid=to_grid_today)
            dsmr_last_updated = dsmr_stop.index.to_pydatetime()[-1]
        else:
            from_grid_today = 'N/A'
            to_grid_today = 'N/A'
            today_actual_cost = 'N/A'
            dsmr_last_updated = 'N/A'
        
        if pv_data_available and dsmr_data_available:
            from_PV_to_consumers_today = from_PV_today - to_grid_today
            today_cost_without_PV = self.cost_calculator.calculate(
                from_grid=from_PV_today - to_grid_today + from_grid_today,
                to_grid=0,
            )
            today_profit = today_cost_without_PV - today_actual_cost
            pv_dsmr_last_updated = max(PV_last_updated, dsmr_last_updated)
        else:
            from_PV_to_consumers_today = 'N/A'
            today_profit = 'N/A'
            pv_dsmr_last_updated = 'N/A'


        data = []


        data.append(
            {
            'title': 'Electricity Consumed',
            'color': 'bg-danger',
            'icon': 'fa-plug',
            'unit': 'kWh',
            'value': from_grid_today,
            'last_updated': dsmr_last_updated
            }
        )

        data.append(
            {
            'title': 'Electricity Returned',
            'color': 'bg-primary',
            'icon': 'fa-plug',
            'unit': 'kWh',
            'value': to_grid_today,
            'last_updated': dsmr_last_updated
            }
        )

        data.append(
            {
            'title': 'Direct Consumption',
            'color': 'bg-primary',
            'icon': 'fa-plug',
            'unit': 'kWh',
            'value': from_PV_to_consumers_today,
            'last_updated': pv_dsmr_last_updated
            }
        )

        data.append(
            {
            'title': 'Solar Energy',
            'color': 'bg-success',
            'icon': 'fa-sun',
            'unit': 'kWh',
            'value': from_PV_today,
            'last_updated': PV_last_updated
            }
        )

        data.append(
            {
            'title': 'Cost',
            'color': 'bg-secondary',
            'icon': 'fa-eur',
            'unit': '€',
            'value': today_actual_cost,
            'last_updated': dsmr_last_updated
            }
        )

        data.append(
            {
            'title': 'Profit',
            'color': 'bg-secondary',
            'icon': 'fa-eur',
            'unit': '€',
            'value': today_profit,
            'last_updated': pv_dsmr_last_updated
            }
        )

        results = {'data': data}
        results['title'] = self.title
        return results


    def post(self, request):
        pass




class Day_Totals_Data_Model(Totals_Data_Model):

    def __init__(self, dsmr_store, pv_store, title):
        self.dsmr_input = inputs.Store_Get_Day(dsmr_store)
        self.pv_input = inputs.Store_Get_Day(pv_store)
        self.cost_calculator = web_app_tools.Cost_Calculator()
        super().__init__(title)



    def get_inputs(self, request):
        to_date = lambda ds: datetime.datetime.strptime(ds, "%Y-%m-%d").date()
        requested_date = request.args.get('date', default=datetime.date.today(), type=to_date)

        inputs = {}
        try:
            inputs['dsmr'] = self.dsmr_input.get(requested_date)
        except:
            inputs['dsmr'] = None
        
        try:
            inputs['pv'] = self.pv_input.get(requested_date)
        except:
            inputs['pv'] = None

        return inputs






class Ref_Totals_Data_Model(Totals_Data_Model):

    def __init__(self, dsmr_store, pv_store, ref_date, title):

        self.ref_date = ref_date


        self.last_dsmr_input = inputs.Store_Get_Last(dsmr_store)
        self.last_pv_input = inputs.Store_Get_Last(pv_store)
        self.dsmr_input = inputs.Store_Get(dsmr_store)
        self.pv_input = inputs.Store_Get(pv_store)

        self.cost_calculator = web_app_tools.Cost_Calculator()
        self.states = {}

        super().__init__(title)



    def get_inputs(self, request):

        ref_start_time = datetime.datetime.combine(self.ref_date, datetime.time.min)
        #ref_start_time = local_timezone.localize(ref_start_time)
        ref_stop_time = ref_start_time + datetime.timedelta(hours=1)


        inputs = {}
        try:
            last_dsmr_log = self.last_dsmr_input.get()
            if 'ref_dsmr_log' not in self.states:
                self.states['ref_dsmr_log'] = self.dsmr_input.get(ref_start_time, ref_stop_time)
            inputs['dsmr'] = pd.concat([self.states['ref_dsmr_log'], last_dsmr_log])
        except:
            inputs['dsmr'] = None
        
        try:
            last_pv_log = self.last_pv_input.get()
            if 'ref_pv_log' not in self.states:
                self.states['ref_pv_log'] = self.pv_input.get(ref_start_time, ref_stop_time)
            inputs['pv'] = pd.concat([self.states['ref_pv_log'], last_pv_log])
        except:
            inputs['pv'] = None

        return inputs







class Realtime_Data_Model(Model):

    def __init__(self, dsmr_store, pv_store, title):
        self.dsmr_input = inputs.Store_Get_Last(dsmr_store)
        self.pv_input = inputs.Store_Get_Last(pv_store)
        super().__init__(title)



    def get(self, request):
        logger.debug(f'starting get for realtime data...')

        result = {}
        result['title'] = self.title
        result['from_pv'] = {}
        result['from_grid'] = {}
        result['to_consumers'] = {}

        now = datetime.datetime.now()

        try:
            pv_log = self.pv_input.get()
            result['from_pv']['value'] = float(pv_log['actual_elec_delivered'])
            result['from_pv']['last_updated'] = pv_log.index.to_pydatetime()[0]
        except:
            result['from_pv']['value'] = 'N/A'
            result['from_pv']['last_updated'] = 'N/A'

        try:
            dsmr_log = self.dsmr_input.get()
            result['from_grid']['value'] = float(dsmr_log['actual_elec_used']) * 1000 - float(dsmr_log['actual_elec_returned']) * 1000
            result['from_grid']['last_updated'] = dsmr_log.index.to_pydatetime()[0]
            result['to_consumers']['value'] = result['from_pv']['value'] + result['from_grid']['value']
            result['to_consumers']['last_updated'] = max(result['from_pv']['last_updated'], result['from_grid']['last_updated'])
        except:
            result['to_consumers']['value'] = 'N/A'
            result['to_consumers']['last_updated'] = 'N/A'
            result['from_grid']['last_updated'] = 'N/A'
            result['from_grid']['value'] = 'N/A'


        return result

    def post(self, request):
        pass






class Date_Buttons_Data_Model(Model):

    def __init__(self, title):
        super().__init__(title)


    def get(self, request):

        to_date = lambda ds: datetime.datetime.strptime(ds, "%Y-%m-%d").date()
        requested_date = request.args.get('date', default=datetime.date.today(), type=to_date)

        result = {}
        result['title'] = self.title


        result['url_routing'] = {
            'url_for_next_day': url_for('main', date=requested_date + datetime.timedelta(days=1)),
            'url_for_previous_day': url_for('main', date=requested_date - datetime.timedelta(days=1)),
            'url_for_today': url_for('main')
        }

        return result

    def post(self, request):
        pass



class Agent_Data_Model(Model):

    def __init__(self, client, title):
        self.client = client
        super().__init__(title)


    def get(self, request):

        result = {}

        result['states'] = self.client.get_states()
        result['config'] = self.client.get_config()
        result['title'] = self.title

        return result
    

    def parse_form(self, form):
        parsed_form = {}
        for key, value in form.items():
            try:
                parsed_form[key] = float(value)
            except ValueError:
                parsed_form[key] = value
        return parsed_form



    def post(self, request):
        config = self.parse_form(request.form)
        self.client.set_config(config)



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

    def __init__(self, series):
        self.series = series

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

        for serie in self.series:
            result['series'].append(
                {
                    'label': serie,
                    'data': df[serie],
                    'color': '',
                }
            )
        
        return result


class PV_Consumption_Processor(Processor):

    def __init__(self, PV_label='Solar Power', PV_color='', cons_label='Consumption', cons_color=''):
        self.PV_label = PV_label
        self.PV_color = PV_color
        self.cons_label = cons_label
        self.cons_color = cons_color        

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

        result['series'].append(
            {
                'label': self.PV_label,
                'data': df['from_PV'],
                'color': self.PV_color,
            }
        )
        
        result['series'].append(
            {
                'label': self.cons_label,
                'data': df['to_consumers'],
                'color': self.cons_color,
            }
        )


        return result





class Day_Data_Model(Model):

    def __init__(self, inputs, title, processor, request_parser=Day_Request_Parser()):


        self.inputs = inputs
        self.request_parser = request_parser
        self.processor = processor
        super().__init__(title)


    def get_inputs(self, request):
        to_date = lambda ds: datetime.datetime.strptime(ds, "%Y-%m-%d").date()
        requested_date = request.args.get('date', default=datetime.date.today(), type=to_date)
        results = []
        for input in inputs:
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



class Summarized_Data_Model(Model):

    def __init__(self, dsmr_store, pv_store, title):


        self.dsmr_input = inputs.Store_Get_All(dsmr_store)
        self.pv_input = inputs.Store_Get_All(pv_store)
        self.get_dsmr_input = self.dsmr_input.get
        self.get_pv_input = self.pv_input.get

        self.cost_calculator = web_app_tools.Cost_Calculator()
        self.filter_bins = 10
        self.downsample_rate = 5
        super().__init__(title)


    def get(self, request):

        to_date = lambda ds: datetime.datetime.strptime(ds, "%Y-%m-%d").date()
        requested_date = request.args.get('date', default=datetime.date.today(), type=to_date)
        pv_log = self.get_pv_input(requested_date)
        dsmr_log = self.get_dsmr_input(requested_date)

        #def process_logs(pv_log, dsmr_log, ma_filter_bins, downsample_rate=5):
        #start_time = max(min(dsmr_log.index.to_pydatetime()), min(pv_log.index.to_pydatetime()))
        #stop_time = min(max(dsmr_log.index.to_pydatetime()), max(pv_log.index.to_pydatetime()))
        
        start_time = min(dsmr_log.index.to_pydatetime())
        stop_time = max(dsmr_log.index.to_pydatetime())

        log = pd.merge(dsmr_log, pv_log, how='outer', left_index=True, right_index=True)

        log = log[(log.index > start_time) & (log.index < stop_time)]

        for column in log.columns:
            if log[column].dtype != 'float64':
                log.drop(labels=column, axis=1, inplace=True)
        
        log.interpolate(inplace=True)
        log.fillna(method='bfill', inplace=True)

        
        #log = log.resample('1min').mean()

        log.rename(columns = {
            'actual_elec_delivered':'from_PV',
            'actual_elec_used': 'from_grid',
            'actual_elec_returned': 'to_grid',
            'total_elec_delivered': 'from_PV_cum'
            },
            inplace = True)

        log['from_PV'] = log['from_PV'] / 1000
        log['from_PV_cum'] = log['from_PV_cum'] / 1000


        log['to_consumers'] = log['from_PV'] + log['from_grid'] - log['to_grid']
        log['from_grid_cum'] = log['elec_used_t1'] + log['elec_used_t2']
        log['to_grid_cum'] = log['elec_returned_t1'] + log['elec_returned_t2']


        log['from_PV_filtered'] = log['from_PV'].rolling(window=self.filter_bins).mean().fillna(method='bfill')
        log['to_consumers_filtered'] = log['to_consumers'].rolling(window=self.filter_bins).mean().fillna(method='bfill')

        log.drop(columns=['elec_used_t1', 'elec_used_t2', 'elec_returned_t1', 'elec_returned_t2',
            'actual_tariff'], inplace=True)

        #log = log.iloc[::-1][::self.downsample_rate][::-1]
        log.dropna(inplace=True)


        from_grid_today = log['from_grid_cum'].iloc[-1] - log['from_grid_cum'].iloc[0]
        to_grid_today = log['to_grid_cum'].iloc[-1] - log['to_grid_cum'].iloc[0]
        from_PV_today = log['from_PV_cum'].iloc[-1] - log['from_PV_cum'].iloc[0]
        from_PV_to_consumers_today = from_PV_today - to_grid_today


        result = {}
        result['title'] = self.title
        result['log'] = {
            'time': list(log['to_consumers'].index),
            'solar_power': {'value': list(log['from_PV_filtered'].to_numpy()), 'last_updated': log.index[-1]},
            'consumption': {'value': list(log['to_consumers_filtered'].values), 'last_updated': log['to_consumers'].index[-1]}
            }

        return result

    def post(self, request):
        pass
