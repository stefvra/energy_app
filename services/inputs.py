import asyncio
import logging
import io
import datetime
from tools import tools


logger = logging.getLogger('inputs')


class Input():
    
    def __init__(self):
        pass

    async def get(self):
        pass

    def get(self):
        pass


class Store_Get(Input):

    def __init__(self, store):
        self.store = store
        super().__init__()

    def __eq__(self, other):
        if self.store == other.store:
            return True
        return False

    async def get_async(self, start, stop):
        return self.get(start, stop)

    def get(self, start, stop):
        return self.store.get(start, stop)    



class Store_Get_All(Input):

    def __init__(self, store):
        self.store = store
        super().__init__()

    def __eq__(self, other):
        if self.store == other.store:
            return True
        return False

    async def get_async(self):
        return self.get()

    def get(self):
        return self.store.get_all()



class Store_Get_First(Input):

    def __init__(self, store):
        self.store = store
        super().__init__()

    def __eq__(self, other):
        if self.store == other.store:
            return True
        return False

    async def get_async(self):
        return self.get()

    def get(self):
        return self.store.get_first()


class Store_Get_Last(Input):

    def __init__(self, store):
        self.store = store
        super().__init__()

    def __eq__(self, other):
        if self.store == other.store:
            return True
        return False

    async def get_async(self):
        return self.get()

    def get(self):
        return self.store.get_last()


class Store_Get_Day(Input):

    def __init__(self, store):
        self.store = store
        super().__init__()

    def __eq__(self, other):
        if self.store == other.store:
            return True
        return False

    async def get_async(self, date=datetime.date.today()):
        return self.get(date)

    def get(self, date=datetime.date.today()):
        local_timezone = tools.get_local_timezone()
        start_time = datetime.datetime.combine(date, datetime.time.min) - datetime.timedelta(minutes=2)
        start_time = local_timezone.localize(start_time)
        stop_time = start_time + datetime.timedelta(hours=24)
        return self.store.get(start_time, stop_time)



class Reader_Input(Input):
    
    def __init__(self, reader):
        self.reader = reader
        super().__init__()

    async def get_async(self):
        return await self.reader.async_read()

    def get(self):
        return self.reader.read()



