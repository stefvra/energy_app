import asyncio
import logging
import io
import datetime
from tools import tools


logger = logging.getLogger('commands')



class Command():
    
    def __init__(self):
        pass

    async def execute():
        pass



class Store_Put_Command(Command):

    def __init__(self, store):
        self.store = store
        super().__init__()
    
    async def execute(self, df):
        self.store.put(df)


class Store_Remove_Command(Command):

    def __init__(self, store):
        self.store = store
        super().__init__()
    
    async def execute(self, start, stop):
        self.store.remove(start=start, stop=stop)


class GPIO_Command(Command):

    def __init__(self, GPIO_Output):
        self.GPIO_Output = GPIO_Output
        super().__init__()
    
    async def execute(self, mode):
        self.GPIO_Output.set_mode(mode)