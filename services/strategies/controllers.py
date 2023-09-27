import asyncio
import logging
import io
import pytz
import datetime

from tools import tools
from . import basic

logger = logging.getLogger('strategies')




class Control_Strategy(basic.Strategy):

    def __init__(self,
        mode='controller',
        upper_treshold_power=700,
        deadband_power=200,
        charging_power=1400,
        min_seconds_between_state_change=5,
        moving_average_seconds=600,
        ):

        config = {
            'mode': mode,
            'upper_treshold_power': upper_treshold_power,
            'deadband_power': deadband_power,
            'charging_power': charging_power,
            'min_seconds_between_state_change': min_seconds_between_state_change,
            'moving_average_seconds': moving_average_seconds
        }
        
        states = {
            'charging': False,
            'time_of_last_state_change': None,
            'power7': None,
            'excess_power': 0
        }
        
        super().__init__(states=states, config=config)
 

    
    def now_localized(self):
        now = datetime.datetime.now()
        now_localized = pytz.timezone('Europe/Brussels').localize(now)
        return now_localized



    async def _execute(self, inputs, commands):

        # read inputs
        self.states['power7'] = await inputs[0].get_async()

        now = self.now_localized()
        ma_filter_start_time = now - datetime.timedelta(seconds=self.config['moving_average_seconds'])
        df_dsmr = await inputs[1].get_async(ma_filter_start_time, now)


        # process input data
        excess_power = (df_dsmr['actual_elec_returned'] - df_dsmr['actual_elec_used']).mean() * 1000
        excess_power += self.states['charging'] * self.config['charging_power']
        self.states['excess_power'] = excess_power


        # execute control logic
        # check if enough time has passed since last state change to invoke a new state change
        if self.states['time_of_last_state_change'] is not None:
            time_since_last_state_change = now - self.states['time_of_last_state_change']
            seconds_since_last_state_change = time_since_last_state_change.total_seconds()
        else:
            seconds_since_last_state_change = None

        if seconds_since_last_state_change is None:
            enough_time_has_passed = True
        else:
            enough_time_has_passed = seconds_since_last_state_change > self.config['min_seconds_between_state_change']

        # update control signal
        if enough_time_has_passed:
            if excess_power > self.config['upper_treshold_power']:
                if self.states['charging'] == False:
                    self.states['time_of_last_state_change'] = self.now_localized()
                    self.states['charging'] = True
            elif excess_power < (self.config['upper_treshold_power'] - self.config['deadband_power']):
                if self.states['charging'] == True:
                    self.states['time_of_last_state_change'] = self.now_localized()
                    self.states['charging'] = False


        # check if it is set manually
        if self.config['mode'] == 'on':
            self.states['charging'] = True
        elif self.config['mode'] == 'off':
            self.states['charging'] = False
        
        # set output
        await commands[0].execute(self.states['charging'])



