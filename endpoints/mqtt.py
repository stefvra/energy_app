from datetime import datetime, timedelta
import paho.mqtt.client as mqtt_client
from tools.factory_tools import Param_Parser
import logging

logger = logging.getLogger('mqtt')

class MQTT_Endpoint():
    
    def __init__(self, topic, server, port, user, pwd):
        self.client = mqtt_client.Client()
        self.client.username_pw_set(user, password=pwd)
        self.client.connect(server, port=port)
        self.topic = topic

    def __eq__(self, other):
        return True


    def set_index(self, index):
        pass


    def put(self, df):
        logger.debug('starting put to mqtt')
        df = df.select_dtypes(include='number')
        df.index.rename("time", inplace=True)
        df.reset_index(inplace=True)
        df_json = df.to_json(orient="records")
        logger.debug(f'parsing to json done: {df_json}')
        self.client.reconnect()
        self.client.publish(self.topic, payload=df_json)
        logger.debug('message published')


    def get(self, start, stop):
        pass


    def get_all(self):
        pass

    def remove(self, start, stop):
        pass
    
    def delete_files(self):
        pass


    def get_first(self):
        pass


    def get_last(self):
        pass








class MQTT_Endpoint_factory():
    def __init__(self):
        """
            Initialization
        """        
        self.param_parser = Param_Parser()
        self.param_register = {
            'server': {'type': 'string'},
            'user': {'type': 'string'},
            'pwd': {'type': 'string'},
            'port': {'type': 'int'},
            'topic': {'type': 'string'},
        }



    def create_from_config(self, config_store, section):
        """
            Create reader from config store

        Args:
            config_store (config_store): config store that contains configuration
            section (string): applicable section in the config store

        Returns:
            reader: created reader
        """

        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)
        return MQTT_Endpoint(
            params['topic'],
            params['server'],
            params['port'],
            params['user'],
            params['pwd'],
            )





