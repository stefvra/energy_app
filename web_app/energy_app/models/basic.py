from abc import ABC, abstractmethod
from concurrent.futures import process
import datetime
from flask import render_template, url_for, redirect, request
import pandas as pd
import logging
import math



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
        except:
            result['from_grid']['last_updated'] = 'N/A'
            result['from_grid']['value'] = 'N/A'

        try:
            result['to_consumers']['value'] = result['from_pv']['value'] + result['from_grid']['value']
            result['to_consumers']['last_updated'] = max(result['from_pv']['last_updated'], result['from_grid']['last_updated'])
        except:
            result['to_consumers']['value'] = 'N/A'
            result['to_consumers']['last_updated'] = 'N/A'



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
