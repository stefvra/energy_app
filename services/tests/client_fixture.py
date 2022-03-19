import asyncio
import datetime
import json
import uuid
import random
import random
import pytest
import itertools





@pytest.fixture(scope='function')
async def client_fixture():
    class Client():

        def __init__(self, port=0, host='127.0.0.1', time_out=1, period=1):
            self.port = port
            self.host = host
            self.time_out = time_out
            self.reader = None
            self.writer = None
            self.requests = [
                {'name': 'get_states', 'payload': lambda: ''},
                {'name': 'get_config', 'payload': lambda: ''},
                {'name': 'set_config', 'payload': self.generate_payload}
            ]
            self.executed_requests = []
            self.coding = 'utf-8'
            self.period = period

        def set_host(self, host):
            self.host = host

        def set_port(self, port):
            self.port = port

        def set_period(self, period):
            self.period = period


        def generate_payload(self):
            key = str(uuid.uuid4())
            value = random.random()
            return {key: value}


        async def open_connection(self):
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)


        async def request_and_receive(self, request):
            self.writer.write(request.encode(self.coding))
            return await self.reader.read(4096)


        def get_request(self):
            for request in itertools.cycle(self.requests):
                name = request['name']
                payload = request['payload']()
                try:
                    raw_payload = json.dumps(payload, default=str)
                except:
                    raw_payload = payload
                raw = f"{name} {raw_payload}"
                yield {'name': name, 'payload': payload, 'raw': raw}


        def parse_response(self, response):
            try:
                parsed_response = json.loads(response)
            except:
                parsed_response = response
            return parsed_response


        async def run(self):

            while True:
                try:
                    await self.open_connection()
                    break
                except ConnectionRefusedError:
                    await asyncio.sleep(1)
                

            request_generator = self.get_request()
            while True:
                
                request = next(request_generator)
                response = await self.request_and_receive(request['raw'])
                response = response.decode(self.coding) 
                parsed_response = self.parse_response(response)

                self.executed_requests.append(
                    {
                        'request': request,
                        'response': {
                            'raw': response,
                            'parsed': parsed_response,
                            },
                        'time': datetime.datetime.now()
                    }
                )
                await asyncio.sleep(1)

        async def teardown(self):
            self.writer.close()
            await self.writer.wait_closed()
        
        def validate_executed_requests(self):
            valid = True
            for r1 in self.executed_requests:
                if r1['request']['name'] == 'set_config':
                    set_config_time = r1['time']
                    set_config_value = r1['request']['payload']
                    for r2 in self.executed_requests:
                        if r2['request']['name'] == 'get_config':
                            get_config_time = r2['time']
                            get_config_value = r2['response']['parsed']  
                            if get_config_time > set_config_time:
                                if len(set_config_value.items() - get_config_value.items()) > 0:
                                    valid = False
            return valid

    
    client = Client()
    yield client
    await client.teardown()

