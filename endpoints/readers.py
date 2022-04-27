
from abc import ABC, abstractmethod
import requests
import datetime
import dateutil
import json
import aiohttp
import asyncio
import logging
import random
import pandas as pd
from queue import Queue
import pytz
import numpy as np
from serial.serialutil import SerialException

from tools.factory_tools import Param_Parser
from tools import tools

# try to umport the Raspberry Pi reader classes
# if not available create mock classes

try:
    import busio
    import digitalio
    import board
    import adafruit_mcp3xxx.mcp3008 as MCP
    from adafruit_mcp3xxx.analog_in import AnalogIn
except:
    class busio:
        class SPI:
            def __init__(self, clock=None, MISO=None, MOSI=None):
                pass
    class digitalio:
        class DigitalInOut:
            def __init__(self, a):
                pass
    class board:
        SCK = None
        MISO = None
        MOSI = None
        D5 = None
    class MCP:
        class MCP3008:
            def __init__(sdelf, a, b):
                pass
    class AnalogIn:
        def __init__(self, a, b):
            self.value = random.uniform(0, 1) * 30000





from dsmr_parser import telegram_specifications, obis_references
from dsmr_parser.clients import SERIAL_SETTINGS_V4, AsyncSerialReader
from dsmr_parser.exceptions import ParseError


_reader_timezone = pytz.timezone('Europe/Amsterdam')


logger = logging.getLogger('readers')


class Reader(ABC):
    """Base class that defines a reader. Cannot be used on itself.
    """

    def __init__(self, timezone=_reader_timezone):
        """
            Initialization

        Args:
            timezone (timezone, optional): Timezone to convert dat to. Defaults to _reader_timezone.
        """

        self.timezone = _reader_timezone



    def read(self):
        """
            Template function for reading

        Returns:
            DataFrame: dataframe that has been read in
        """
        return self._read()



    async def async_read(self):
        """
            Template function for reading in asynchronous way

        Returns:
            DataFrame: dataframe that has been read in
        """
        return await self._async_read()
    

    @abstractmethod
    def _read(self):
        """
            Dummy function
        """
        pass

    @abstractmethod
    async def _async_read(self):
        """
            Dummy function
        """
        pass



class HttpReader(Reader):
    """
        Class that manages reading trough http requests
    """
 
    def __init__(self, url, time_out=4, timezone=_reader_timezone):
        """
            Initialization

        Args:
            url (string): url to request for
            time_out (int, optional): timout value. Defaults to 4 seconds.
            timezone (timezone, optional): See base class. Defaults to _reader_timezone.
        """
        self.url = url
        self.time_out = time_out
        super().__init__(timezone)


    def __eq__(self, other):
        """
            Method that checks equallity between readers

        Args:
            other (HttpReader): reader to compare to

        Returns:
            boolean: true if self and other are considered equal
        """
        if self.url == other.url and \
            self.time_out == other.time_out and \
                self.timezone == other.timezone:
            return True
        return False
        



    def _read(self):
        """
            Method that reads from the http server

        Returns:
            DataFrame: DataFrame that is read from the server. localize to reader timezone
        """
        request_time = datetime.datetime.now(self.timezone)
        response = requests.get(self.url, timeout=self.time_out)
        response_time = datetime.datetime.now(self.timezone)
        df = self._log_to_df(response.text, request_time, response_time)
        return df


    async def _async_read(self):
        """
            Method that reads asynchronous from the http server

        Returns:
            DataFrame: DataFrame that is read from the server
        """
        try:
            request_time = datetime.datetime.now(self.timezone)
            timeout = aiohttp.ClientTimeout(total=self.time_out)
            async with aiohttp.ClientSession() as session:
                    async with session.get(self.url, timeout=timeout) as response:
                        html = await response.text()
            response_time = datetime.datetime.now(self.timezone)
            logger.debug('http reader fetched')
            logger.debug(f'request time: {request_time}')
        except:
            return None
        df = self._log_to_df(html, request_time, response_time)
        return df





class Fronius_Reader(HttpReader):
    """
        Class that manages reading from Fronius inverter
    """


    def __init__(self, url, time_out=4, timezone=_reader_timezone):
        """
            Initializes the reader

            Args: see base class
        """


        self.time_field = 'device_time'

        self.fields = {
            'system_request_time': 'datetime',
            'system_response_time': 'datetime',
            'device_time': 'datetime',
            'actual_elec_delivered': 'float',
            'day_elec_delivered': 'float',
            'total_elec_delivered': 'float',
        }

        super().__init__(url, time_out=time_out, timezone=timezone)


    def __eq__(self, other):
        """
            Method that checks equallity between readers

        Args:
            other (Fronius_Reader): reader to compare to

        Returns:
            boolean: true if self and other are equal
        """
        if self.url == other.url and \
            self.time_out == other.time_out and \
                self.timezone == other.timezone and \
                    self.time_field == other.time_field and \
                        self.fields == other.fields:
            return True
        return False




    def _log_to_df(self, log, request_time, response_time):
        """
        Converts fronius invertor response to dataframe

        Args:
            log (string): response from fronius invertor
            request_time (localized datetime): moment that the request was sent out
            response_time (localized datetime): moment that the request was recieved

        Returns:
            DataFrame: Dataframe that is read
        """
        log_dict = json.loads(log)
        if 'PAC' in log_dict['Body']['Data'] and \
            'DAY_ENERGY' in log_dict['Body']['Data'] and \
            'TOTAL_ENERGY' in log_dict['Body']['Data']:
                PAC = log_dict['Body']['Data']['PAC']['Value']
                DAY_ENERGY = log_dict['Body']['Data']['DAY_ENERGY']['Value']
                TOTAL_ENERGY = log_dict['Body']['Data']['TOTAL_ENERGY']['Value']
        else:
            return None


        records = [
            {
                'system_request_time': request_time,
                'system_response_time': response_time,
                'device_time': dateutil.parser.parse(log_dict['Head']['Timestamp']).astimezone(self.timezone),
                'actual_elec_delivered': float(PAC),
                'day_elec_delivered': float(DAY_ENERGY),
                'total_elec_delivered': float(TOTAL_ENERGY),
            }
        ]


        df = pd.DataFrame.from_records(records)
        df.set_index(self.time_field, inplace=True)
        df.index = pd.to_datetime(df.index)
        return df




class OW_Reader(HttpReader):
    """
        Reader for open weather service

    """


    def __init__(self, url, time_out=4, timezone=_reader_timezone):
        """
            Initialization

        Args:
            url (string): request url of the service
            time_out (int, optional): time out for http response. Defaults to 4.
        """


        self.time_field = 'system_request_time'


        self.fields = {
            'system_request_time': 'datetime',
            'system_response_time': 'datetime',
            'forecast': 'str'
        }

        super().__init__(url, time_out=time_out, timezone=timezone)


    def __eq__(self, other):
        """
            Method that checks equallity between readers

        Args:
            other (OW_Reader): reader to compare to

        Returns:
            boolean: true if self and other are equal
        """
        if self.url == other.url and \
            self.time_out == other.time_out and \
                self.timezone == other.timezone and \
                    self.time_field == other.time_field and \
                        self.fields == other.fields:
            return True
        return False





    def _log_to_df(self, log, request_time, response_time):
        """Method that converts open weather log to dataframe

        Args:
            log (json): response of open weather server
            request_time (localized datetime): moment that the request was sent out
            response_time (localized datetime): moment that the request was recieved

        Returns:
            dataframe
        """
        records = [
            {
                'system_request_time': request_time,
                'system_response_time': response_time,
                'forecast': json.dumps(log)
            }
        ]
        df = pd.DataFrame.from_records(records)
        df.set_index(self.time_field, inplace=True)
        df.index = pd.to_datetime(df.index)
        return df





class SMA_Reader(Reader):
    """
        Class that manages reading trough http requests with authentication
    """
 
    def __init__(self, ip, pwd, time_out=(3, 8), timezone=_reader_timezone, conf_file=tools.get_SMA_config_file()):
        """
            Initialization

        Args:
            url (string): url to request for
            time_out (int, optional): timout value. Defaults to 4 seconds.
            timezone (timezone, optional): See base class. Defaults to _reader_timezone.
        """
        self.ip = ip
        self.pwd = pwd
        self.time_out = time_out
        with open(conf_file) as f:
            self.config = json.load(f)
        super().__init__(timezone)



        self.time_field = 'system_response_time'

        self.fields = {
            'system_request_time': 'datetime',
            'system_response_time': 'datetime',
            'actual_elec_delivered': 'float',
            'day_elec_delivered': 'float',
            'total_elec_delivered': 'float',
        }



    def __eq__(self, other):
        """
            Method that checks equallity between readers

        Args:
            other (HttpReader): reader to compare to

        Returns:
            boolean: true if self and other are considered equal
        """
        if self.ip == other.ip and \
            self.time_out == other.time_out and \
                self.timezone == other.timezone and \
                    self.pwd == other.pwd and \
                        self.config == other.config:
            return True
        return False




    def get_query_keys(self):
        query_keys = []
        for item in self.config:
            if item['required'] == 'mandatory' or item['active'] == True:
                if item['key'] is not None:
                    query_keys.append(item['key'])
        return query_keys


    def post(self, url, payload):
        response = requests.post(url, data=payload, verify=False, timeout=self.time_out)
        return response

    async def async_post_to_text(self, url, payload):
        async with aiohttp.ClientSession() as session:
            # comment
            async with session.post(url, data=payload, timeout=self.time_out[1], ssl=False) as response_fut:
                response_text = await response_fut.text()
        return response_text


    def get_login_data(self):
        url = self.ip + '/dyn/login.json'
        payload = "{\"right\":\"usr\",\"pass\":\"" + self.pwd + "\"}"
        return url, payload

    def get_logout_data(self, sid):
        url = self.ip + '/dyn/logout.json?sid=' + sid
        payload = "{}"
        return url, payload


    def get_query_data(self, sid):
        url = self.ip + "/dyn/getValues.json?sid=" + sid
        payload = json.dumps({"destDev": [], "keys": self.get_query_keys()})
        return url, payload

    def _read(self):
        """
            Method that reads from the http server

        Returns:
            DataFrame: DataFrame that is read from the server. localize to reader timezone
        """
        sid = ''
        try:
            request_time = datetime.datetime.now(self.timezone)
            login_response = self.post(*self.get_login_data())
            sid = login_response.json()['result']['sid']
            query_response = self.post(*self.get_query_data(sid))
            response_time = datetime.datetime.now(self.timezone)
            df = self._log_to_df(query_response.text, request_time, response_time)
            return df
        except:
            return None
        finally:
            self.post(*self.get_logout_data(sid))


    async def _async_read(self):
        """
            Method that reads asynchronous from the http server

        Returns:
            DataFrame: DataFrame that is read from the server
        """
        sid = ''
        try:
            request_time = datetime.datetime.now(self.timezone)
            login_response_text = await self.async_post_to_text(*self.get_login_data())
            logger.debug(f'login response: {login_response_text}')
            sid = json.loads(login_response_text)['result']['sid']
            logger.debug(f'login sid: {login_response_text}')
            query_response_text = await self.async_post_to_text(*self.get_query_data(sid))
            logger.debug(f'query response: {query_response_text}')
            response_time = datetime.datetime.now(self.timezone)
            df = self._log_to_df(query_response_text, request_time, response_time)
            logger.debug(f'converted dataframe: {df}')
            return df
        except Exception as e:
            logger.debug(f'exception occured: {e}')
            return None
        finally:
            logout_response_text = await self.async_post_to_text(*self.get_logout_data(sid))
            logger.debug(f'logout response: {logout_response_text}')

    def result_to_dict(self, result):
        d = {}
        keys = {c['key']: c['tag'] for c in self.config}
        temp_dict = list(result.values())[0]
        temp_dict1 = list(temp_dict.values())[0]
        for key, value in temp_dict1.items():
            if key in keys:
                v = value['1'][0]['val']
                d[keys[key]] = v
        return d


    def _log_to_df(self, log, request_time, response_time):
        """
        Converts fronius invertor response to dataframe

        Args:
            log (string): response from fronius invertor
            request_time (localized datetime): moment that the request was sent out
            response_time (localized datetime): moment that the request was recieved

        Returns:
            DataFrame: Dataframe that is read
        """
        log_dict = json.loads(log)
        result_dict = self.result_to_dict(log_dict)

        try:
            records = [
                {
                    'system_request_time': request_time,
                    'system_response_time': response_time,
                    'actual_elec_delivered': float(result_dict['solar_act']),
                    'day_elec_delivered': float(0),
                    'total_elec_delivered': float(result_dict['solar_total']),
                }
            ]
        except:
            return None


        df = pd.DataFrame.from_records(records)
        df.set_index(self.time_field, inplace=True)
        df.index = pd.to_datetime(df.index)
        return df



class DSMR_take_strategy():
    def __init__(self):
        pass

    def __eq__(self, other):
        pass

    def parse(self, df):
        pass


class DSMR_take_all_strategy(DSMR_take_strategy):

    def parse(self, df):
        return df

    def __eq__(self, other):
        if isinstance(other, DSMR_take_all_strategy):
            return True
        return False


class DSMR_take_last_strategy(DSMR_take_strategy):

    def parse(self, df):
        return df.iloc[[-1]]

    def __eq__(self, other):
        if isinstance(other, DSMR_take_last_strategy):
            return True
        return False


class DSMR_take_first_strategy(DSMR_take_strategy):

    def parse(self, df):
        return df.iloc[[0]]

    def __eq__(self, other):
        if isinstance(other, DSMR_take_first_strategy):
            return True
        return False


class DSMR_take_mean_strategy(DSMR_take_strategy):

    def parse(self, df):
        index_name = df.index.name
        s_mean = df.reset_index().apply(lambda x: x.mean())
        df_mean = s_mean.to_frame().T
        df_mean = df_mean.convert_dtypes(convert_integer=False)
        df_mean.set_index(index_name, inplace=True)
        return df_mean

    def __eq__(self, other):
        if isinstance(other, DSMR_take_mean_strategy):
            return True
        return False




class DSMR_Reader(Reader):
    """
        Reader for DSMR port
    """


    def __init__(
        self,
        read_strategy=DSMR_take_mean_strategy(),
        device='/dev/ttyUSB0',
        serial_settings=SERIAL_SETTINGS_V4,
        telegram_specification=telegram_specifications.BELGIUM_FLUVIUS,
        timezone=_reader_timezone
        ):
        """
            Initialization

        Args:
            read_all (bool, optional): Parameter to indicate if all values froml que need to be read. Defaults to False.
            device (str, optional): reference to device. Defaults to '/dev/ttyUSB0'.
            serial_settings (_type_, optional): settings for serial port. Defaults to SERIAL_SETTINGS_V4.
            telegram_specification (_type_, optional): specifications of telegram. Defaults to telegram_specifications.BELGIUM_FLUVIUS.
            timezone (_type_, optional): see base class. Defaults to _reader_timezone.
        """

        self.reader = AsyncSerialReader(
            device=device,
            serial_settings=serial_settings,
            telegram_specification=telegram_specification
            )

        self._buffer = Queue()
        self.read_strategy = read_strategy



        self.time_field = 'device_time'


        self.fields = {
            'device_time': 'datetime',
            'system_time': 'datetime',
            'elec_used_t1': 'float',
            'elec_used_t2': 'float',
            'elec_returned_t1': 'float',
            'elec_returned_t2': 'float',
            'actual_tariff': 'float',
            'actual_elec_used': 'float',
            'actual_elec_returned': 'float',
            'gas_used': 'float',
        }



        super().__init__(timezone=timezone)


    def __eq__(self, other):
        """
            Method that checks equallity between readers

        Args:
            other (DSMR_Reader): reader to compare to

        Returns:
            boolean: true if self and other are equal
        """
        if self.read_strategy == other.read_strategy and \
            self.reader.serial_settings == other.reader.serial_settings and \
                self.reader.serial_settings[self.reader.PORT_KEY] == \
                    other.reader.serial_settings[other.reader.PORT_KEY] and \
                        self.timezone == other.timezone and \
                            self.time_field == other.time_field and \
                                self.fields == other.fields and \
                                    self.reader.telegram_parser.telegram_specification == \
                                        other.reader.telegram_parser.telegram_specification:
            return True
        return False



    # test code to see if event loop can be driven from agent
    async def _read_from_serial(self):
        """
            Read from serial port
        """
        try:
            await self.reader.read(self._buffer)
        
        except ParseError:
            logger.exception('DSMR Parsing Error')
        except ValueError:
            logger.exception('DSMR Parsing Error')
        except SerialException as e:
            logger.exception('Serial error')                




    def _read(self):
        """
            Reads messages from buffer

        Returns:
            Dataframe: read in data
        """
        read_time = datetime.datetime.now(self.timezone)
        messages = []
        for _ in range(self._buffer.qsize()):
            message = self._buffer.get(block=False)
            logger.debug('dsmr reader fetched')
            messages.append(message)
        if not messages:
            return None
        else:
            df = self._log_to_df(messages, read_time)
            return self.read_strategy.parse(df)



    async def _async_read(self):
        """Reads messages from queue in asynchronous way

        Returns:
            Dataframe: read from reader. Datetime index and all localized
            to reader timezone.
        """
        return self._read()



    def _log_to_df(self, messages, read_time):
        """converts dsmr log to dataframe

        Args:
            messages (messages): messages from queue
            read_time (datetime, tocalize to reader timezone): time when read action is done

        Returns:
            dataframe: Datetime index and all localized to reader timezone.
        """
        records = []
        for message in messages:
            logger.warning(message)
            records.append({
                'system_time': read_time,
                'device_time': message[obis_references.P1_MESSAGE_TIMESTAMP].value,
                'elec_used_t1': float(message[obis_references.ELECTRICITY_USED_TARIFF_1].value),
                'elec_used_t2': float(message[obis_references.ELECTRICITY_USED_TARIFF_2].value),
                'elec_returned_t1': float(message[obis_references.ELECTRICITY_DELIVERED_TARIFF_1].value),
                'elec_returned_t2': float(message[obis_references.ELECTRICITY_DELIVERED_TARIFF_2].value),
                'actual_tariff': float(message[obis_references.ELECTRICITY_ACTIVE_TARIFF].value),
                'actual_elec_used': float(message[obis_references.CURRENT_ELECTRICITY_USAGE].value),
                'actual_elec_returned': float(message[obis_references.CURRENT_ELECTRICITY_DELIVERY].value),
                'gas_used': float(message[obis_references.BELGIUM_HOURLY_GAS_METER_READING].value),
                }
            )
        df = pd.DataFrame.from_records(records)
        df.set_index(self.time_field, inplace=True)
        df.index = pd.to_datetime(df.index).tz_convert(self.timezone)
        return df


class MCP3008Reader(Reader):
    """
        Class that manages reading from MCP3008 module
    """

    def __init__(self, channel, cs_pin=board.D5, calibration=[0, 1], timezone=_reader_timezone):
        """
            Initialization

        Args:
            channel (numeric): channel to read in
            cs_pin (int, optional): chipselect pin. Defaults to board.D5.
            calibration (list, optional): _description_. Defaults to [0, 1].
            timezone (_type_, optional): See base class. Defaults to _reader_timezone.
        """

        self.channel = int(channel)
        self.cs_pin = cs_pin
        self.calibration = calibration

        self.spi = busio.SPI(clock=board.SCK, MISO=board.MISO, MOSI=board.MOSI)
        self.cs = digitalio.DigitalInOut(cs_pin)
        self.mcp = MCP.MCP3008(self.spi, self.cs)

        super().__init__(timezone=_reader_timezone)




    def __eq__(self, other):
        """
            Method that checks equallity between readers

        Args:
            other (MCP3008Reader): reader to compare to

        Returns:
            boolean: true if self and other are equal
        """
        if self.channel == other.channel and \
            self.cs_pin == other.cs_pin and \
                self.calibration == other.calibration and \
                        self.timezone == other.timezone:
            return True
        return False



    def _read(self):
        """
            Method that reads in data

        Returns:
            DataFrame: DataFrame that is read
        """

        reading = AnalogIn(self.mcp, self.channel)
        value = self.calibration[0] + reading.value * self.calibration[1]
        return value


    async def _async_read(self):
        """
            Method that reads asynchronous

        Returns: see _read method

        """
        return self.read()






class Reader_Factory():
    """
        Factory class for all reader classes. If needed creation is dispatched
        to other class
    """

    def __init__(self):
        """
            Initialization
        """
        self.param_parser = Param_Parser()
        self.param_register = {
            'type': {'type': 'string'}
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


        if params['type'] == 'FRONIUS':
            return Fronius_Reader_Factory().create_from_config(config_store, section)
        if params['type'] == 'SMA':
            return SMA_Reader_Factory().create_from_config(config_store, section)
        elif params['type'] == 'METEO':
            return OW_Reader_Factory().create_from_config(config_store, section)
        elif params['type'] == 'DSMR':
            return DSMR_Reader_Factory().create_from_config(config_store, section)
        elif params['type'] == 'MCP':
            return MCP3008_Reader_Factory().create_from_config(config_store, section)
        else:
            pass

    def serial_reader_needed(self, config_store, section):
        _type = config_store.get(section, key='type')
        if _type == 'DSMR':
            return True
        else:
            return False





class Fronius_Reader_Factory():
    """
        Factory class for Fronius reader
    """

    def __init__(self):
        """
            Initialization
        """
        self.param_parser = Param_Parser()
        self.param_register = {
            'url': {'type': 'string'},
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
        
        return Fronius_Reader(params['url'])




class SMA_Reader_Factory():
    """
        Factory class for Fronius reader
    """

    def __init__(self):
        """
            Initialization
        """
        self.param_parser = Param_Parser()
        self.param_register = {
            'ip': {'type': 'string'},
            'pwd': {'type': 'string'},
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
        
        return SMA_Reader(params['ip'], params['pwd'])





class OW_Reader_Factory():
    """
        Factory class for OW reader
    """

    def __init__(self):
        """
            Initialization
        """        
        self.param_parser = Param_Parser()
        self.param_register = {
            'url': {'type': 'string'},
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
        return OW_Reader(params['url'])



class DSMR_Reader_Factory():

    def __init__(self):
        """
            Initialization
        """        
        self.param_parser = Param_Parser()
        self.param_register = {
            'device': {'type': 'string'},
            'strategy': {'type': 'string', 'default': 'mean'},
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
        if params['strategy'] == 'mean':
            strategy = DSMR_take_mean_strategy()
        elif params['strategy'] == 'first':
            strategy = DSMR_take_first_strategy()
        elif params['strategy'] == 'last':
            strategy = DSMR_take_last_strategy()
        elif params['strategy'] == 'all':
            strategy = DSMR_take_all_strategy()            


        return DSMR_Reader(device=params['device'], read_strategy=strategy)



class MCP3008_Reader_Factory():
    """
        Factory class for MCP3008 reader
    """

    def __init__(self):
        """
            Initialization
        """        
        self.param_parser = Param_Parser()
        self.param_register = {
            'channel': {'type': 'int'},
            'calibration': {'type': 'int_list', 'default': [0, 1]},        
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
        return MCP3008Reader(params['channel'], calibration=params['calibration'])
        

