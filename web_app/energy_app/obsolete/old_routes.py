from flask import render_template, url_for, redirect, request
from energy_app import app
import logging

import dateutil
import numpy as np
import pandas as pd
import datetime
import sys, os
import pytz



import endpoints.stores as stores
from tools import tools
import endpoints.stores as stores

logger = logging.getLogger('web_app')
local_timezone = pytz.timezone('Europe/Amsterdam')



def calculate_cost(from_grid=0, to_grid=0, from_grid_cost=0, to_grid_cost=0):
  return from_grid * from_grid_cost + to_grid * to_grid_cost


def datetime_to_str(time, format='%d-%m-%y %H:%M'):
  if isinstance(time, list):
    return [t.strftime(format) for t in time]
  else:
    return time.strftime(format)

def to_date(dateString): 
    return datetime.datetime.strptime(dateString, "%Y-%m-%d").date()


def process_logs(pv_log, dsmr_log, ma_filter_bins, downsample_rate=5):

  
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

  log = log.resample('1min').mean()

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


  log['from_PV_filtered'] = log['from_PV'].rolling(window=ma_filter_bins).mean().fillna(method='bfill')
  log['to_consumers_filtered'] = log['to_consumers'].rolling(window=ma_filter_bins).mean().fillna(method='bfill')

  log.drop(columns=['elec_used_t1', 'elec_used_t2', 'elec_returned_t1', 'elec_returned_t2',
       'actual_tariff'], inplace=True)

  log = log.iloc[::-1][::downsample_rate][::-1]



  return log



def get_realtime_data(pv_log, dsmr_log, max_timediff=60):


  result = {}
  result['from_PV'] = {}
  result['from_grid'] = {}
  result['to_consumers'] = {}

  result['from_PV']['value'] = pv_log['actual_elec_delivered'].iloc[-1] / 1000
  result['from_PV']['last_updated'] = pv_log.index[-1]

  result['from_grid']['value'] = dsmr_log['actual_elec_used'].iloc[-1] - dsmr_log['actual_elec_returned'].iloc[-1]
  result['from_grid']['last_updated'] = dsmr_log.index[-1]

  result['to_consumers']['value'] = result['from_PV']['value'] + result['from_grid']['value']
  # what to do when logging is broke??
  result['to_consumers']['last_updated'] = max(result['from_PV']['last_updated'], result['from_grid']['last_updated'])
  

  return result




config_file = tools.get_config_file(production_state=True)
config_store = tools.Config_Store(filename=config_file)
store_factory = stores.Store_Factory()

ACTIVATE_CONTROLLER = config_store.get('controller', key='activate')
if ACTIVATE_CONTROLLER:
    controller_states_store = tools.Default_Section_Config_Store('states', filename=config_store.get('controller', key='state_file'))


dsmr_store = store_factory.create_from_config(config_store, 'dsmr_store')
summarized_dsmr_store = store_factory.create_from_config(config_store, 'summarized_dsmr_store')
try:
  pv_store = store_factory.create_from_config(config_store, 'pv_store')
  summarized_pv_store = store_factory.create_from_config(config_store, 'summarized_pv_store')
except:
  pv_store_client = stores.Zero_Store_Client(['actual_elec_delivered', 'day_elec_delivered', 'total_elec_delivered'], 'device_time')
  pv_store = store_factory.create_manager(pv_store_client, stores.Transformer())



str_format = '{0:.2f}'
ma_filter_bins = 10




@app.route('/')
def main():


  data = {}


  ref_date = config_store.get('web_app', 'ref_date')
  consumption_cost = config_store.get('web_app', 'consumption_cost')
  injection_cost = config_store.get('web_app', 'injection_cost')

  data['config'] = config_store.get('web_app')


  requested_date = request.args.get('date', default=datetime.date.today(), type=to_date)
  data['url_routing'] = {
    'url_for_next_day': url_for('main', date=requested_date + datetime.timedelta(days=1)),
    'url_for_previous_day': url_for('main', date=requested_date - datetime.timedelta(days=1)),
    'url_for_today': url_for('main')
  }



  app.logger.debug(f'controller state: {ACTIVATE_CONTROLLER}')
  if ACTIVATE_CONTROLLER:
    controller_config = config_store.get('controller')
    controller_states = controller_states_store.get()
  else:
    controller_config = None
    controller_states = None

  # Get data of requested date
  start_time = datetime.datetime.combine(requested_date, datetime.time.min) - datetime.timedelta(minutes=2)
  start_time = local_timezone.localize(start_time)
  stop_time = start_time + datetime.timedelta(hours=24)
  now = datetime.datetime.now()
  dsmr_log = dsmr_store.get(start_time, stop_time)
  app.logger.debug(f'duration for dsmr log fetching: {datetime.datetime.now()-now}')
  now = datetime.datetime.now()
  pv_log = pv_store.get(start_time, stop_time)
  app.logger.debug(f'duration for fronius log fetching: {datetime.datetime.now()-now}')

  # merge and process data
  now = datetime.datetime.now()
  logger.debug(dsmr_log.dtypes)
  logger.debug(pv_log.dtypes)
  log = process_logs(pv_log, dsmr_log, ma_filter_bins)
  app.logger.debug(log.columns)
  app.logger.info(f'duration for data processing: {datetime.datetime.now()-now}')

  #get realtime data
  realtime_data = get_realtime_data(pv_store.get_last(), dsmr_store.get_last())




  # get reference data


  ref_start_time = datetime.datetime.combine(ref_date, datetime.time.min)
  ref_start_time = local_timezone.localize(ref_start_time)
  ref_stop_time = ref_start_time + datetime.timedelta(hours=1)
  
  try:
    ref_dsmr_log = dsmr_store.get(ref_start_time, ref_stop_time)
    ref_pv_log = pv_store.get(ref_start_time, ref_stop_time)
    ref_log = process_logs(ref_pv_log, ref_dsmr_log, 1)
    from_grid_total = log['from_grid_cum'].iloc[-1] - ref_log['from_grid_cum'].iloc[-1]
    to_grid_total = log['to_grid_cum'].iloc[-1] - ref_log['to_grid_cum'].iloc[-1]
    from_PV_total = log['from_PV_cum'].iloc[-1] - ref_log['from_PV_cum'].iloc[-1]
    from_PV_to_consumers_total = from_PV_total - to_grid_total

    total_actual_cost = calculate_cost(
      from_grid=from_grid_total,
      to_grid=to_grid_total,
      from_grid_cost=consumption_cost,
      to_grid_cost=injection_cost
      )

    total_cost_without_PV = calculate_cost(
      from_grid=from_PV_total - to_grid_total + from_grid_total,
      to_grid=0,
      from_grid_cost=consumption_cost,
      to_grid_cost=injection_cost
      )
    data['total'] = {}
    data['total']['elec'] = {
      'consumed_from_grid': 
      {'value': str_format.format(from_grid_total),
      'last_updated': datetime_to_str(log.index[-1])
      },
      'returned_to_grid': 
      {'value': str_format.format(to_grid_total),
      'last_updated': datetime_to_str(log.index[-1])
      },
      'generated_from_PV': 
      {'value': str_format.format(from_PV_total),
      'last_updated': datetime_to_str(log.index[-1])
      },
      'consumed_from_PV': 
      {'value': str_format.format(from_PV_to_consumers_total),
      'last_updated': datetime_to_str(log.index[-1])
      },        
      'cost': 
      {'value': str_format.format(total_actual_cost),
      'last_updated': datetime_to_str(log.index[-1])
      },        
      'profit': 
      {'value': str_format.format(total_cost_without_PV - total_actual_cost),
      'last_updated': datetime_to_str(log.index[-1])
      }        
    }

  except:
    data['config']['show_total'] = False

  

  from_grid_today = log['from_grid_cum'].iloc[-1] - log['from_grid_cum'].iloc[0]
  to_grid_today = log['to_grid_cum'].iloc[-1] - log['to_grid_cum'].iloc[0]
  from_PV_today = log['from_PV_cum'].iloc[-1] - log['from_PV_cum'].iloc[0]
  from_PV_to_consumers_today = from_PV_today - to_grid_today

  today_actual_cost = calculate_cost(
    from_grid=from_grid_today,
    to_grid=to_grid_today,
    from_grid_cost=consumption_cost,
    to_grid_cost=injection_cost
    )

  today_cost_without_PV = calculate_cost(
    from_grid=from_PV_today - to_grid_today + from_grid_today,
    to_grid=0,
    from_grid_cost=consumption_cost,
    to_grid_cost=injection_cost
    )

  
  


  data['controller'] = {
    'active': ACTIVATE_CONTROLLER,
    'config': controller_config,
    'states': str(controller_states)
    }
  data['realtime'] = {
    'solar_power':
      {'value': str_format.format(realtime_data['from_PV']['value'] * 1000),
      'last_updated': datetime_to_str(realtime_data['from_PV']['last_updated'])},
    'consumption':
      {'value': str_format.format(realtime_data['to_consumers']['value'] * 1000),
      'last_updated': datetime_to_str(realtime_data['to_consumers']['last_updated'])},
    }
  data['log'] = {
    'time': datetime_to_str(log['to_consumers'].index, format='%H:%M'),
    'solar_power':
      {'value': list(log['from_PV_filtered'].to_numpy()),
      'last_updated': datetime_to_str(log.index[-1])},
    'consumption':
      {'value': list(log['to_consumers_filtered'].values),
      'last_updated': datetime_to_str(log['to_consumers'].index[-1])},
    }
  data['today'] = {
    'elec':
      {'consumed_from_grid': 
        {'value': str_format.format(from_grid_today),
        'last_updated': datetime_to_str(log.index[-1])
        },
        'returned_to_grid': 
        {'value': str_format.format(to_grid_today),
        'last_updated': datetime_to_str(log.index[-1])
        },
        'consumed_from_PV': 
        {'value': str_format.format(from_PV_to_consumers_today),
        'last_updated': datetime_to_str(log.index[-1])
        },
        'generated_from_PV': 
        {'value': str_format.format(from_PV_today),
        'last_updated': datetime_to_str(log.index[-1])
        },
        'cost': 
        {'value': str_format.format(today_actual_cost),
        'last_updated': datetime_to_str(log.index[-1])
        },        
        'profit': 
        {'value': str_format.format(today_cost_without_PV - today_actual_cost),
        'last_updated': datetime_to_str(log.index[-1])
        },    
      },
    }
  data['gas'] = {
    'used':
      {'value': str_format.format(log['gas_used'][-1]),
       'last_updated': datetime_to_str(log.index[-1])
      },
    'log':
      {'value': list(log['gas_used'].diff().to_numpy()[1:]),
       'last_updated': datetime_to_str(log.index[-1]),
       'time': datetime_to_str(log.index[1:], format='%d-%m')
      }
    }
  
  data = []
  for app in apps:
    data. append(app.generate_data())
  return render_template('dashboard.html', data = data)




@app.route('/', methods=['POST'])
def main_post():
    config_store.add('controller', request.form)
    return redirect(url_for('main'))
