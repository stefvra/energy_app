import asyncio
import json
from . import basic



class Connection_Handler_Strategy(basic.Strategy):

    def __init__(self, port, request_register={}):
        self.request_register = request_register
        self.port = port
        self.coding = 'utf-8'
        self.buffer_size = 4096
        super().__init__()
    

    def add_request_register(self, request_register):
        self.request_register = {**self.request_register, **request_register}


    def parse_request(self, request):
        splitted_request = request.strip().split(' ', 1)
        command = splitted_request[0]
        if len(splitted_request) > 1:
            payload = json.loads(splitted_request[1])
        else:
            payload = None
        return command, payload


    def execute_request(self, command, payload):
        response = self.request_register[command](payload)
        if response is None:
            response = command
        return response


    async def handle_request(self, reader, writer):
        while True:
            request = await reader.read(self.buffer_size)
            request = request.decode(self.coding) 
            if not request:
                break
            print(request)
            command, payload = self.parse_request(request)
            response = self.execute_request(command, payload)
            if response:
                writer.write(json.dumps(response, default=str).encode(self.coding))
                await writer.drain()
        writer.close()


    async def _execute(self, inputs, commands):


        server = await asyncio.start_server(self.handle_request, '', self.port)

        async with server:
            await server.serve_forever()


