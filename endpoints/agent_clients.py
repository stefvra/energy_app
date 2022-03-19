import socket
import json
import logging

logger = logging.getLogger('agent_clients')


class Agent_Client():
    """
        Class represents a client to a requestable agent.

    """

    def __init__(self, port=0, host='127.0.0.1', time_out=1):
        """
            Initialisation

        Args:
            port (int, optional): port to connect to. Defaults to 0.
            host (str, optional): host IP to connect to. Defaults to '127.0.0.1'.
            time_out (int, optional): time out value to use for connection. Defaults to 1 second.
        """
        self.port = port
        self.host = host
        self.time_out = time_out
        self.coding = 'utf-8'
        self.buffer_size = 2 * 4096


    def get_states(self):
        """
            Function that requests the states of a requestable agent

        Returns:
            dict: key is state name, value is state value
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect((self.host, self.port))
            s.sendall('get_states'.encode(self.coding))
            data = s.recv(self.buffer_size)
            return json.loads(data)


    def get_config(self):
        """
            Function that requests the configuration of a requestable agent

        Returns:
            dict: key is config name, value is config value
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect((self.host, self.port))
            s.sendall('get_config'.encode(self.coding))
            data = s.recv(self.buffer_size)
            return json.loads(data)

    def set_config(self, config):
        """
            Function sets a configuration parameter to a value

        Args:
            config (dict): key is the config name, value is the config value

        Returns:
            boolean: True if method has ran till end
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect((self.host, self.port))
            request = 'set_config ' + json.dumps(config)
            print(request)
            s.sendall(request.encode(self.coding))
            print(s.recv(self.buffer_size))
            return True





        
