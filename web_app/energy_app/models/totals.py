from abc import ABC, abstractmethod
import datetime
import pandas as pd
import logging
import math
from pandas.api.types import is_numeric_dtype


from services import inputs
from tools import web_app_tools
from web_app.energy_app.models.basic import Model
logger = logging.getLogger('web_app_moddels')



class Totals_Data_Model(Model):

    def __init__(self, title):
        super().__init__(title)


    @abstractmethod
    def get_inputs(self, request):
        pass


    def get(self, request):
        input_data = self.get_inputs(request)
        for keys, value in input_data.items():
            try:
                self.drop_non_numericals(value)
                self.rename_df(value)
            except:
                pass
        result = self.get_data(request, input_data)
        result = self.complete_result(request, result)
        return result

    def complete_result(self, request, result):
        result['title'] = self.title
        result['title'] = f'Totals for (add correct title)'
        return result

    @staticmethod
    def drop_non_numericals(df):
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
            pv_start = df_pv.sort_index().iloc[0]
            pv_stop = df_pv.sort_index().iloc[-1]
            from_PV_today = (pv_stop['from_PV_cum'] - pv_start['from_PV_cum']) / 1000
            PV_last_updated = df_pv.index.to_pydatetime()[-1]
        else:
            from_PV_today = 'N/A'
            PV_last_updated = 'N/A'

        if dsmr_data_available:
            dsmr_start = df_dsmr.sort_index().iloc[0]
            dsmr_stop = df_dsmr.sort_index().iloc[-1]
            from_grid_today = dsmr_stop['elec_used_t1'] + dsmr_stop['elec_used_t2'] - dsmr_start['elec_used_t1'] - dsmr_start['elec_used_t2']
            to_grid_today = dsmr_stop['elec_returned_t1'] + dsmr_stop['elec_returned_t2'] - dsmr_start['elec_returned_t1'] - dsmr_start['elec_returned_t2']
            gas_used_today = (dsmr_stop['gas_used'] - dsmr_start['gas_used']) * 11.6
            today_actual_cost = self.cost_calculator.calculate(from_grid=from_grid_today, to_grid=to_grid_today)
            dsmr_last_updated = df_dsmr.index.to_pydatetime()[-1]
            gas_cost_today = gas_used_today * .12
        else:
            from_grid_today = 'N/A'
            to_grid_today = 'N/A'
            gas_used_today = 'N/A'
            today_actual_cost = 'N/A'
            gas_cost_today = 'N/A'
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
            'title': 'From Grid',
            'color': 'bg-danger',
            'icon': 'fa-plug',
            'unit': 'kWh',
            'value': from_grid_today,
            'last_updated': dsmr_last_updated
            }
        )

        data.append(
            {
            'title': 'To Grid',
            'color': 'bg-primary',
            'icon': 'fa-plug',
            'unit': 'kWh',
            'value': to_grid_today,
            'last_updated': dsmr_last_updated
            }
        )

        data.append(
            {
            'title': 'Direct Consumption from PV',
            'color': 'bg-primary',
            'icon': 'fa-plug',
            'unit': 'kWh',
            'value': from_PV_to_consumers_today,
            'last_updated': pv_dsmr_last_updated
            }
        )

        data.append(
            {
            'title': 'From PV',
            'color': 'bg-success',
            'icon': 'fa-sun',
            'unit': 'kWh',
            'value': from_PV_today,
            'last_updated': PV_last_updated
            }
        )


        data.append(
            {
            'title': 'Gas Used',
            'color': 'bg-warning',
            'icon': 'fa-fire',
            'unit': 'kWh',
            'value': gas_used_today,
            'last_updated': dsmr_last_updated
            }
        )

        data.append(
            {
            'title': 'Electricity Cost',
            'color': 'bg-secondary',
            'icon': 'fa-eur',
            'unit': '€',
            'value': today_actual_cost,
            'last_updated': dsmr_last_updated
            }
        )

        """
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
        """

        data.append(
            {
            'title': 'Gas Cost',
            'color': 'bg-primary',
            'icon': 'fa-eur',
            'unit': '€',
            'value': gas_cost_today,
            'last_updated': dsmr_last_updated
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

        _inputs = {}
        try:
            _inputs['dsmr'] = self.dsmr_input.get(requested_date)
        except:
            _inputs['dsmr'] = None
        
        try:
            _inputs['pv'] = self.pv_input.get(requested_date)
        except:
            _inputs['pv'] = None

        return _inputs






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


        _inputs = {}
        try:
            last_dsmr_log = self.last_dsmr_input.get()
            if 'ref_dsmr_log' not in self.states:
                self.states['ref_dsmr_log'] = self.dsmr_input.get(ref_start_time, ref_stop_time)
            _inputs['dsmr'] = pd.concat([self.states['ref_dsmr_log'], last_dsmr_log])
        except:
            _inputs['dsmr'] = None
        
        try:
            last_pv_log = self.last_pv_input.get()
            if 'ref_pv_log' not in self.states:
                self.states['ref_pv_log'] = self.pv_input.get(ref_start_time, ref_stop_time)
            _inputs['pv'] = pd.concat([self.states['ref_pv_log'], last_pv_log])
        except:
            _inputs['pv'] = None

        return _inputs



