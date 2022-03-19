import asyncio
import logging
import io
import datetime


logger = logging.getLogger('agents')




class Agent():

    def __init__(self, inputs, commands, event, periodic_strategy, one_time_strategy=None):

        self.inputs = inputs if isinstance(inputs, list) else [inputs]
        self.commands = commands if isinstance(commands, list) else [commands]
        self.event = event
        self.one_time_strategy = one_time_strategy
        self.periodic_strategy = periodic_strategy


    async def run(self):
    
        tasks = []
        tasks.append(asyncio.create_task(self.event.loop()))
        tasks.append(asyncio.create_task(self._run_agent()))
        if self.one_time_strategy is not None:
            tasks.append(asyncio.create_task(self.one_time_strategy.execute(self.inputs, self.commands)))
        await asyncio.gather(*tasks)


    async def _run_agent(self):
        while True:
            
            await self.event.event.wait()
            await self.periodic_strategy.execute(self.inputs, self.commands)
