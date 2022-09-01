from tools.factory_tools import Param_Parser
import web_app.energy_app.controllers as controllers
import web_app.energy_app.models as models
from endpoints import agent_clients, stores
from services import inputs
from tools import tools
from tools import web_app_tools






class Dashboard_Controller_Factory():

    def __init__(self):
        self.param_parser = Param_Parser()
        self.store_register = stores.Store_Register()
        self.template = 'dashboard.html'
        self.param_register = {
            'subtype': {'type': 'string'},
            'activate': {'type': 'bool', 'default': True}
        }

        self.model_register = {
            'realtime': ['realtime_labels.html', Realtime_Model_Factory],
            'day_graph': ['graph.html', Day_Graph_Model_Factory],
            'day_totals': ['cumulative_labels.html', Day_Totals_Model_Factory],
            'date_buttons': ['date_buttons.html', Date_Buttons_Model_Factory],
            'totals': ['cumulative_labels.html', Totals_Model_Factory],
            'client': ['client.html', Client_Model_Factory],
            'summarized_graph': ['graph.html', Totals_Graph_Model_Factory]

        }

    def create(self, models, templates, main_template):
        return controllers.Dashboard_Controller(models, templates, main_template)
    

    def create_from_config(self, config_store):

        config_sections = config_store.find_config('type', value='web_app_section')

        models = []
        templates = []
        for model_key, _ in config_sections.items():
            params = config_store.get(model_key)
            params = self.param_parser.add_defaults(params, self.param_register)
            self.param_parser.check_types(params, self.param_register)
            subtype = params['subtype']
            activate = params['activate']

            if activate:

                default_template = self.model_register[subtype][0]
                template = params['template'] if 'template' in params else default_template
            
                factory = self.model_register[subtype][1]()
                models.append(factory.create_from_config(
                    config_store,
                    model_key,
                    store_register=self.store_register
                    )
                )
                templates.append(template)

        return self.create(models, templates, self.template)




class Realtime_Model_Factory():

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'pv_store': {'type': 'string'},
            'dsmr_store': {'type': 'string'}, 
            'activate': {'type': 'bool', 'default': True},
            'title': {'type': 'string', 'default': 'Realtime'}

        }

    def create(self, dsmr_store, pv_store, title):
        return models.basic.Realtime_Data_Model(dsmr_store, pv_store, title)
    

    def create_from_config(self, config_store, section, store_register=None):


        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        if not params['activate']:
            return None

        dsmr_store = stores.Store_Factory().create_from_config(config_store, params['dsmr_store'])
        pv_store = stores.Store_Factory().create_from_config(config_store, params['pv_store'])
        if store_register is not None:
            dsmr_store = store_register.register(dsmr_store)
            pv_store = store_register.register(pv_store)

        return self.create(dsmr_store, pv_store, params['title'])



class Day_Graph_Model_Factory():

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'stores': {'type': 'string_list'}, 
            'activate': {'type': 'bool', 'default': True},
            'title': {'type': 'string', 'default': 'Day Graph'},
            'processor': {'type': 'string', 'default': 'field_picker'},
            'fields': {'type': 'string_list', 'default': ''},
            'unit': {'type': 'string', 'default': ''}            
        }

    def create(self, _stores, fields, unit, title, processor):
        
        _inputs = [inputs.Store_Get_Day(store) for store in _stores]
        return models.logs.Log_Data_Model(_inputs, title, fields=fields, unit=unit, processor=processor)
    

    def create_from_config(self, config_store, section, store_register=None):


        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        if not params['activate']:
            return None

        _stores = []
        
        for store_name in params['stores']:
            store = stores.Store_Factory().create_from_config(config_store, store_name)
            if store_register is not None:
                store = store_register.register(store)
            _stores.append(store)


        if params['processor'] == 'elec':
            processor = models.logs.PV_Consumption_Processor()
            fields = ['to_consumers', 'from_PV']
            unit = 'kWh'
        elif params['processor'] == 'gas':
            processor = models.logs.Gas_Consumption_Processor()
            fields = ['gas_used']
            unit = 'kW'
        else:
            processor = models.logs.Idle_Processor()
            fields = params['fields']
            unit = params['unit']
        return self.create(_stores, fields, unit, params['title'], processor)





class Totals_Graph_Model_Factory():

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'stores': {'type': 'string_list'}, 
            'activate': {'type': 'bool', 'default': True},
            'title': {'type': 'string', 'default': 'Day Graph'},
            'processor': {'type': 'string', 'default': 'field_picker'},
            'fields': {'type': 'string_list', 'default': ''},
            'unit': {'type': 'string', 'default': ''},
            'elec_from_grid_kwh_cost': {'type': 'numeric', 'default': 0},
            'elec_to_grid_kwh_cost': {'type': 'numeric', 'default': 0},
            'gas_kwh_cost': {'type': 'numeric', 'default': 0}                
        }

    def create(self, _stores, fields, unit, title, processor):
        
        _inputs = [inputs.Store_Get(store) for store in _stores]
        request_parser = models.logs.Last_N_Days_Request_Parser(n_days=365)
        return models.logs.Log_Data_Model(
            _inputs,
            title,
            fields=fields,
            unit=unit,
            x_format='date',
            processor=processor,
            request_parser=request_parser
            )
    

    def create_from_config(self, config_store, section, store_register=None):


        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        if not params['activate']:
            return None

        _stores = []
        
        for store_name in params['stores']:
            store = stores.Store_Factory().create_from_config(config_store, store_name)
            if store_register is not None:
                store = store_register.register(store)
            _stores.append(store)


        elec_cost_calculator = web_app_tools.Elec_Cost_Calculator(
            params['elec_from_grid_kwh_cost'],
            params['elec_to_grid_kwh_cost']
            )
        gas_cost_calculator = web_app_tools.Gas_Cost_Calculator(params['gas_kwh_cost'])        


        if params['processor'] == 'elec':
            processor = models.logs.Totals_PV_Consumption_Processor()
            fields = ['from_PV_cum', 'to_grid_cum', 'from_grid_cum']
            unit = 'kWh'
        elif params['processor'] == 'cost':
            processor = models.logs.Cost_Processor(elec_cost_calculator, gas_cost_calculator)
            fields = ['elec_cost', 'gas_cost']
            unit = '€'
        elif params['processor'] == 'gas':
            processor = models.logs.Scaled_Idle_Processor(11.2)
            fields = ['gas_used']
            unit = 'kWh'

        return self.create(_stores, fields, unit, params['title'], processor)






class Day_Totals_Model_Factory():

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'pv_store': {'type': 'string'},
            'dsmr_store': {'type': 'string'}, 
            'activate': {'type': 'bool', 'default': True},
            'title': {'type': 'string', 'default': 'Day total'},
            'elec_from_grid_kwh_cost': {'type': 'numeric', 'default': 0},
            'elec_to_grid_kwh_cost': {'type': 'numeric', 'default': 0},
            'gas_kwh_cost': {'type': 'numeric', 'default': 0}
        }

    def create(self, dsmr_store, pv_store, title, elec_cost_calculator, gas_cost_calculator):
        return models.totals.Day_Totals_Data_Model(dsmr_store, pv_store, title, elec_cost_calculator, gas_cost_calculator)
    

    def create_from_config(self, config_store, section, store_register=None):


        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        if not params['activate']:
            return None

        dsmr_store = stores.Store_Factory().create_from_config(config_store, params['dsmr_store'])
        pv_store = stores.Store_Factory().create_from_config(config_store, params['pv_store'])
        elec_cost_calculator = web_app_tools.Elec_Cost_Calculator(
            params['elec_from_grid_kwh_cost'],
            params['elec_to_grid_kwh_cost']
            )
        gas_cost_calculator = web_app_tools.Gas_Cost_Calculator(params['gas_kwh_cost'])



        if store_register is not None:
            dsmr_store = store_register.register(dsmr_store)
            pv_store = store_register.register(pv_store)
        return self.create(dsmr_store, pv_store, params['title'], elec_cost_calculator, gas_cost_calculator)


class Totals_Model_Factory():

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'pv_store': {'type': 'string'},
            'dsmr_store': {'type': 'string'}, 
            'activate': {'type': 'bool', 'default': True},
            'ref_date': {'type': 'datetime', 'default': ''},
            'title': {'type': 'string', 'default': 'Totals'},
            'elec_from_grid_kwh_cost': {'type': 'numeric', 'default': 0},
            'elec_to_grid_kwh_cost': {'type': 'numeric', 'default': 0},
            'gas_kwh_cost': {'type': 'numeric', 'default': 0}                        
        }

    def create(self, dsmr_store, pv_store, ref_date, title, elec_cost_calculator, gas_cost_calculator):
        return models.totals.Ref_Totals_Data_Model(dsmr_store, pv_store, ref_date, title, elec_cost_calculator, gas_cost_calculator)
    

    def create_from_config(self, config_store, section, store_register=None):


        params = config_store.get(section)
        print(params)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        if not params['activate']:
            return None

        dsmr_store = stores.Store_Factory().create_from_config(config_store, params['dsmr_store'])
        pv_store = stores.Store_Factory().create_from_config(config_store, params['pv_store'])
        elec_cost_calculator = web_app_tools.Elec_Cost_Calculator(
            params['elec_from_grid_kwh_cost'],
            params['elec_to_grid_kwh_cost']
            )
        gas_cost_calculator = web_app_tools.Gas_Cost_Calculator(params['gas_kwh_cost'])        
        if store_register is not None:
            dsmr_store = store_register.register(dsmr_store)
            pv_store = store_register.register(pv_store)
        return self.create(dsmr_store, pv_store, params['ref_date'], params['title'], elec_cost_calculator, gas_cost_calculator)


class Date_Buttons_Model_Factory():

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'activate': {'type': 'bool', 'default': True},
            'title': {'type': 'string', 'default': 'Select Date'}            
        }

    def create(self, title):
        return models.basic.Date_Buttons_Data_Model(title)
    

    def create_from_config(self, config_store, section, store_register=None):


        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        if not params['activate']:
            return None

        return self.create(params['title'])



class Client_Model_Factory():

    def __init__(self):
        self.param_parser = Param_Parser()
        self.param_register = {
            'activate': {'type': 'bool', 'default': True},
            'server_port': {'type': 'int'}, 
            'server_host': {'type': 'string', 'default': 'localhost'},
            'title': {'type': 'string', 'default': 'Client'}            

        }

    def create(self, host, port, title):
        client = agent_clients.Agent_Client(host=host, port=port)
        return models.basic.Agent_Data_Model(client, title)
    

    def create_from_config(self, config_store, section, store_register=None):


        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        if not params['activate']:
            return None

        
        host = params['server_host']
        port = params['server_port']
        title = params['title']



        return self.create(host, port, title)


