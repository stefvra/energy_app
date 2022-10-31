from abc import ABC, abstractmethod
import logging
import services.agents as agents
import services.commands as commands
import services.events as events
import services.inputs as inputs
import services.strategies.controllers as controllers
import services.strategies.data_managers as data_managers
import services.strategies.basic as basic_strategies
import services.strategies.connection_handlers as connection_handlers
import endpoints.stores as stores
import endpoints.mqtt as mqtt
import endpoints.readers as readers
import endpoints.outputs as outputs
from tools import tools
from tools.factory_tools import Param_Parser


logger = logging.getLogger('service_factories')





class Service_Factory(ABC):

    def __init__(self, param_register={}):
        self.param_parser = Param_Parser()
        basic_param_register = {
            'activate': {'type': 'bool', 'default': True},
            'requestable': {'type': 'bool', 'default': False},
            'port': {'type': 'int', 'default': 0}
        }
        self.param_register = {**basic_param_register, **param_register}
    
    def parse_params(self, config_store, section):
        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)
        return params
    

    def get_connection_handler_strategy(self, params, strategy):
        if params['requestable']:
            return Connection_Handler_Strategy_Factory().create(params['port'], strategy)
        else:
            return None
    
    def is_not_activated(self, params):
        if not params['activate']:
            return True
        return False


    @abstractmethod
    def create(self):
        pass






class Serial_Reader_Factory(Service_Factory):

    def __init__(self):
        super().__init__()

    def create(self, reader, loop_time=.1):
        event = events.Periodic_Event(loop_period=loop_time)
        strategy = basic_strategies.Serial_Read_Strategy(reader)
        return [agents.Agent(None, None, event, strategy)]




class Logger_Factory(Service_Factory):

    def __init__(self):
        param_register = {
            'period': {'type': 'numeric'},
            'store': {'type': 'string'},
            'store2': {'type': 'string', 'default': 'na'},
            'mqtt_service': {'type': 'string', 'default': 'na'},            
            'reader': {'type': 'string'}                    
        }
        super().__init__(param_register=param_register)

    def create(self, event, reader, stores, strategy, connection_handler_strategy=None):
        _commands = []
        for store in stores:
            if hasattr(store, 'time_field'):
                store.time_field = reader.time_field
            _commands.append(commands.Store_Put_Command(store))
        _inputs = [inputs.Reader_Input(reader)]


        _agents = []
        _agents.append(agents.Agent(_inputs, _commands, event, strategy, one_time_strategy=connection_handler_strategy))
        
        if isinstance(reader, readers.DSMR_Reader):
            _agents +=Serial_Reader_Factory().create(reader)

        return _agents
    

    def create_from_config(self, config_store, section):

        params = self.parse_params(config_store, section)


        if self.is_not_activated(params):
            return None

        store = [stores.Store_Factory().create_from_config(config_store, params['store'])]
        if params['store2'] != 'na':
            store.append(stores.Store_Factory().create_from_config(config_store, params['store2']))
        if params['mqtt_service'] != 'na':
            store.append(mqtt.MQTT_Endpoint_factory().create_from_config(config_store, params['mqtt_service']))
        reader = readers.Reader_Factory().create_from_config(config_store, params['reader'])
        event = events.Periodic_Event(loop_period=params['period'])
        strategy = basic_strategies.Log_Strategy()
        connection_handler_strategy = self.get_connection_handler_strategy(params, strategy)

        return self.create(event, reader, store, strategy, connection_handler_strategy=connection_handler_strategy)



class Data_Manager_Factory(Service_Factory):
    
    def __init__(self):
        param_register = {
            'period': {'type': 'numeric'},
            'source_store': {'type': 'string'},
            'target_store': {'type': 'string'},
            'type': {'type': 'string'},
            'block_length_minutes': {'type': 'int', 'default': 10},
            'blocks_to_process': {'type': 'int', 'default': 10},
        }
        super().__init__(param_register=param_register)

    def create(self, event, strategy, source_store, target_store, connection_handler_strategy=None):
        _inputs = [
            inputs.Store_Get(source_store),
            inputs.Store_Get_First(source_store),
            inputs.Store_Get_Last(source_store),
            inputs.Store_Get_All(source_store),
            inputs.Store_Get(target_store),
            inputs.Store_Get_First(target_store),
            inputs.Store_Get_Last(target_store),
            inputs.Store_Get_All(target_store),            
            ]
        _commands = [commands.Store_Put_Command(target_store)]
        return [agents.Agent(_inputs, _commands, event, strategy, one_time_strategy=connection_handler_strategy)]


    def create_from_config(self, config_store, section):
        
        params = self.parse_params(config_store, section)


        if self.is_not_activated(params):
            return None

        event = events.Periodic_Event(loop_period=params['period'])
        source_store = stores.Store_Factory().create_from_config(config_store, params['source_store'])
        target_store = stores.Store_Factory().create_from_config(config_store, params['target_store'])
        if params['type'] == 'mean':
            algorithm = data_managers.Mean_Algorithm()
        elif params['type'] == 'diff':
            algorithm = data_managers.Diff_Algorithm()
        
        strategy = data_managers.Block_Processing_Strategy(
            algorithm,
            blocks_to_process=params['blocks_to_process'],
            block_length_minutes=params['block_length_minutes']
            )
        
        connection_handler_strategy = self.get_connection_handler_strategy(params, strategy)


        summarizer = self.create(event, strategy, source_store, target_store, connection_handler_strategy=connection_handler_strategy)
        return summarizer




class Controller_Factory(Service_Factory):
    
    def __init__(self):
        param_register = {
            'period': {'type': 'numeric'},
            'store': {'type': 'string'},
            'reader': {'type': 'string'},
            'output': {'type': 'string'},   
            'mode': {'type': 'string', 'default': 'controlled'},
            'upper_treshold_power': {'type': 'int', 'default': 700},
            'deadband_power': {'type': 'int', 'default': 200},
            'charging_power': {'type': 'int', 'default': 1400},
            'min_seconds_between_state_change': {'type': 'int', 'default': 10},
            'moving_average_seconds': {'type': 'int', 'default': 600},
        }

        self.controller_setting_keys = [
            'mode',
            'upper_treshold_power',
            'deadband_power',
            'charging_power',
            'min_seconds_between_state_change',
            'moving_average_seconds',
        ]

        super().__init__(param_register=param_register)


    def create(
        self,
        event,
        store,
        reader,
        output,
        strategy,
        connection_handler_strategy=None,
        ):

        _inputs = [
            inputs.Reader_Input(reader),
            inputs.Store_Get(store),
        ]
        _commands = [commands.GPIO_Command(output)]

        _agents = []
        _agents.append(agents.Agent(_inputs, _commands, event, strategy, one_time_strategy=connection_handler_strategy))

        return _agents
    


    def create_from_config(self, config_store, section):

        params = self.parse_params(config_store, section)
        controller_settings = {k: params[k] for k in self.controller_setting_keys}

        if self.is_not_activated(params):
            return None

        event = events.Periodic_Event(loop_period=params['period'])
        store = stores.Store_Factory().create_from_config(config_store, params['store'])
        reader = readers.Reader_Factory().create_from_config(config_store, params['reader'])
        output = outputs.Output_Factory().create_from_config(config_store, params['output'])
        strategy = controllers.Control_Strategy(**controller_settings)
        connection_handler_strategy = self.get_connection_handler_strategy(params, strategy)


        controller_agents = self.create(
            event,
            store,
            reader,
            output,
            strategy,
            connection_handler_strategy=connection_handler_strategy,
            )

        return controller_agents




class Connection_Handler_Strategy_Factory():

    def __init__(self):
        pass

    def create(self, port, strategy):
        request_register = {
            'get_config': lambda x: strategy.get_config(),
            'set_config': lambda x: strategy.set_config(x),
            'get_states': lambda x: strategy.get_states()
            }
        return connection_handlers.Connection_Handler_Strategy(port, request_register=request_register)
