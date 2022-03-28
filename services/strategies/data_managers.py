
from abc import ABC, abstractmethod
import asyncio
from copy import deepcopy
import logging
import io
import pytz
import datetime
from endpoints.stores import ReadStoreError
import functools

from tools import tools
from . import basic


logger = logging.getLogger('data_manager')

"""
inputs.Store_Get(source_store),
inputs.Store_Get_First(source_store),
inputs.Store_Get_Last(source_store),
inputs.Store_Get_All(source_store),
inputs.Store_Get(target_store),
inputs.Store_Get_First(target_store),
inputs.Store_Get_Last(target_store),
inputs.Store_Get_All(target_store),   


commands.Store_Put_Command(target_store)

"""


"Done, Todo, Faulty"
class Block_State(ABC):
    def __init__(self):
        pass
    @abstractmethod
    async def process(self, inputs, commands, algorithm, block):
        pass
    @abstractmethod
    def is_freshly_processed(self):
        return False

class Closed_Block_State(Block_State):
    def __str__(self):
        return f'closed'
    async def process(self, inputs, commands, algorithm, block):
        logger.debug(f'Not processing {block} that is in closed state... ')
        return
    def is_freshly_processed(self):
        return False


class Done_Block_State(Block_State):
    def __str__(self):
        return f'done'    
    async def process(self, inputs, commands, algorithm, block):
        logger.debug(f'Moving {block} to closed state... ')
        block.change_state(Closed_Block_State())
    def is_freshly_processed(self):
        return True

class Todo_Block_State(Block_State):
    def __str__(self):
        return f'todo'    
    async def process(self, inputs, commands, algorithm, block):
        try:
            logger.debug(f'Processing {block} in todo state... ')
            await algorithm.execute(inputs, commands, block)
            logger.debug(f'{block} processed, moving to done state... ')
            block.change_state(Done_Block_State())
        except:
            logger.debug(f'{block} not processed, moving to faulty state... ')
            block.change_state(Faulty_Block_State())
    def is_freshly_processed(self):
        return False

class Faulty_Block_State(Block_State):
    def __str__(self):
        return f'faulty'    
    async def process(self, inputs, commands, algorithm, block):
        try:
            logger.debug(f'Processing {block} in faulty state... ')
            await algorithm.execute(inputs, commands, block)
            logger.debug(f'{block} processed, moving to done state... ')
            block.change_state(Done_Block_State())
        except:
            logger.debug(f'{block} not processed, staying to faulty state... ')
            block.change_state(Faulty_Block_State())
    def is_freshly_processed(self):
        return False


@functools.total_ordering
class Block:

    def __init__(self, start=None, end=None):
        self.start = start
        self.end = end
        self.state = Todo_Block_State()

    def get_start(self):
        return self.start

    def get_end(self):
        return self.end

    def __eq__(self, other):
        if self.start == other.start:
            if self.end == other.end:
                return True
        return False

    def __lt__(self, other):
        if self.start < other.start:
            return True
        return False

    def __str__(self):
        return f'Block object, start: {self.start}, end: {self.end}, state: {self.state}'


    async def process(self, inputs, commands, algorithm):
        await self.state.process(inputs, commands, algorithm, self)

    def change_state(self, state):
        self.state = state
        logger.debug(f'changed state of block {self} to {state}')


    def is_freshly_processed(self):
        return self.state.is_freshly_processed()



  
class Block_Processing_Strategy(basic.Strategy):
    
    def __init__(
            self,
            algorithm,
            blocks_to_process=None,
            block_length_minutes=10,
            blocks_to_send_over_socket=5
            ):


        self.time_zone = pytz.timezone('Europe/Amsterdam')
        self.algorithm = algorithm
        config = {
            'blocks_to_process': blocks_to_process,
            'block_length_minutes': block_length_minutes,
            'blocks_to_send_over_socket': blocks_to_send_over_socket
        }        
        states = {
            'blocks': [],
        }        
        super().__init__(states=states, config=config)


    def get_states(self):
        blocks_to_send_over_socket = self.config['blocks_to_send_over_socket']
        _states = deepcopy(self.states)
        _states['blocks'] = _states['blocks'][0:blocks_to_send_over_socket]
        return _states

    def _get_edge(self, ref_time, earliest_time):
        dt = datetime.timedelta(minutes=self.config['block_length_minutes'])
        t = self.time_zone.localize(datetime.datetime.combine(datetime.date.today(), datetime.time.min))
        while t < ref_time:
            t += dt
        t -= dt
        yield t

        while t > earliest_time:
            t -= dt
            yield t


    async def update_blocklist(self, first_time):


        now = self.time_zone.localize(datetime.datetime.now())
        edge_generator = self._get_edge(now, first_time)
        stopping_edge = edge_generator.__next__()

        while True:
            try:
                starting_edge = edge_generator.__next__()
            except StopIteration:
                logger.debug(f'blocklist updated to {self.states["blocks"]}')
                return True
            block = Block(start=starting_edge, end=stopping_edge)
            if block not in self.states['blocks']:
                self.states['blocks'].append(block)

            stopping_edge = starting_edge
        




    async def _execute(self, inputs, commands):

        first_time = inputs[1].get().index[0]
        await self.update_blocklist(first_time)
        self.states['blocks'].sort(reverse=True)
        target_store_name = '_'.join([
            commands[0].store.store_client.get_database(),
            commands[0].store.store_client.get_file()
        ])
        logger.debug(f'Executing block processing strategy for target store {target_store_name}')
        logger.debug(f' Complete block list: {self.states["blocks"]}')

        blocks_processed = 0
        for block in self.states['blocks']:
            logger.debug(f'{blocks_processed} blocks processed...')
            try:
                await block.process(inputs, commands, self.algorithm)
                logger.debug(f'Block {block} successfully processed...')
                if block.is_freshly_processed():
                    blocks_processed += 1
            except Exception as e:
                logger.debug(f'Block {block} failed to process with exception {e}')
                pass
            if blocks_processed == self.config['blocks_to_process']:
                return

 



class Algorithm():

    def __init__(self, columns=None):
        self.columns = columns

    def get_columns(self, df):
        if self.columns is None:
            return df.columns
        else:
            return self.columns

   
    def execute(self, inputs, commands, block):
        return True



class Diff_Algorithm(Algorithm):

    def __init__(self, columns=None, sorted=True):
        self.sorted = sorted
        super().__init__(columns=columns)
    
    async def execute(self, inputs, command, block):

        logger.debug('executing diff algorithm...')
        df = inputs[0].get(block.get_start(), block.get_end())
        try:
            inputs[4].get(block.get_start(), block.get_end())
            return
        except ReadStoreError:
            pass

        if self.sorted:
            df.sort_index(inplace=True)
        diff_df = df[self.get_columns(df)].iloc[[0,-1]].diff().iloc[[-1]]
        diff_df.index = [block.get_start() + (block.get_end() - block.get_start()) / 2]
        diff_df.index.name = df.index.name
        await command[0].execute(diff_df)


class Mean_Algorithm(Algorithm):

    def __init__(self, columns=None):
        super().__init__(columns=columns)
    

    async def execute(self, inputs, command, block):

        logger.debug('executing mean algorithm...')
        df = inputs[0].get(block.get_start(), block.get_end())

        try:
            inputs[4].get(block.get_start(), block.get_end())
            return
        except ReadStoreError:
            pass

        mean_df = df[self.get_columns(df)].mean().to_frame().transpose()
        mean_df.index = [block.get_start() + (block.get_end() - block.get_start()) / 2]
        mean_df.index.name = df.index.name
        await command[0].execute(mean_df)




class Archive_Algorithm(Algorithm):

    def __init__(self, columns=None):
        super().__init__(columns=columns)
    
    async def execute(self, input, command, block):

        df = input.get(block['start'], block['end'])
        mean_df = df[self.get_columns(df)].mean().to_frame().transpose()
        mean_df.index = [block['start'] + (block['end'] - block['start']) / 2]
        mean_df.index.name = df.index.name

        await command.execute(mean_df)
        return True


