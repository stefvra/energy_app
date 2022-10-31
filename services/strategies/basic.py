import asyncio
import logging
import io
import pytz
import datetime

from tools import tools


logger = logging.getLogger('strategies')



class Strategy():

    def __init__(self, states={}, config={}):
        self.states = states
        self.config = config



    async def execute(self, inputs, commands):
        self.states['time_of_last_execution'] = datetime.datetime.now()
        await self._execute(inputs, commands)
    

    def get_config(self):
        return self.config
    
    def get_states(self):
        return self.states


    def set_config(self, config):
        self.config = {**self.config, **config}



class Serial_Read_Strategy(Strategy):

    def __init__(self, reader):
        self.reader = reader
        super().__init__()
    
    async def _execute(self, inputs, commands):
        await self.reader._read_from_serial()




class Log_Strategy(Strategy):

    def __init__(self):
        states = {'records_logged': 0}
        super().__init__(states=states)

    async def _execute(self, inputs, commands):
        df = await inputs[0].get_async()
        if df is not None:
            for command in commands:
                print("*"*250)
                print(f"executing command {command}")
                print("*"*250)
                await command.execute(df)
            self.states['records_logged'] += df.shape[0]
        


