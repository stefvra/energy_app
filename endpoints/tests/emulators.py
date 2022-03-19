from decimal import Decimal, ROUND_UP
import os
import random
import json
import time, datetime
import crcmod
import serial
import subprocess
import multiprocessing
import numpy as np
import pytz



class DSMR_Emulator():

    def __init__(
        self,
        with_gas=True,
        with_electricity_returned=True,
        hour_offset=0,
        fault=None,
        client_port='tty_socat_client',
        n_steps=10,
        interval=.1
        ):
        self.with_gas = with_gas
        self.with_electricity_returned=with_electricity_returned
        self.hour_offset=hour_offset
        self.fault=fault
        self.client_port=client_port
        self.n_steps=n_steps        
        self.interval = interval

        self.faults_to_test = ["wrong_timestamp", "wrong_checksum"]
        self.device_port = 'tty_socat_device'



    def start(self):

        cwd = os.getcwd()
        print(cwd)
        print(self.client_port)
        device_port = os.path.join(cwd, self.device_port)
        client_port = os.path.join(cwd, self.client_port)

        cmd = [
            'socat',
            '-d',
            '-d',
            f'PTY,link={device_port},raw,echo=0',
            f'PTY,link={client_port},raw,echo=0'
            ]
        self._socat_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
            )

        self._dsmr_emulator_proc = multiprocessing.Process(
            target=write_messages,
            args=(
                self.device_port,
                self.n_steps,
                self.interval,
                self.with_electricity_returned,
                self.fault,
                self.faults_to_test,
                self.with_gas
                ),
            )


        time.sleep(1)
        self._dsmr_emulator_proc.start()       

    
    def stop(self):
        self._dsmr_emulator_proc.kill()
        self._dsmr_emulator_proc.join()
        time.sleep(1)
        self._socat_proc.terminate()





def round_precision(float_number, fill_count):
    """ Rounds the number for precision. """
    if not isinstance(float_number, Decimal):
        float_number = Decimal(str(float_number))
    rounded = float_number.quantize(Decimal('.001'), rounding=ROUND_UP)
    return str(rounded).zfill(fill_count)



def get_DSMR_data(
    with_electricity_returned,
    fault,
    faults_to_test,
    with_gas


):
    """ Generates 'random' data, but in a way that it keeps incrementing. """
    now = datetime.datetime.now()

    # 1420070400: 01 Jan 2015 00:00:00 GMT
    current_unix_time = time.mktime(now.timetuple())
    second_since = int(current_unix_time - 1420070400)
    electricity_base = second_since * 0.00005  # Averages around 1500/1600 kWh for a year.

    electricity_1 = electricity_base
    electricity_2 = electricity_1 * 0.6  # Consumption during daylight is a bit lower.
    electricity_1_returned = 0
    electricity_2_returned = 0
    gas = electricity_base * 0.3  # Random as well.

    currently_delivered_l1 = random.randint(0, 1500) * 0.001  # kW
    currently_delivered_l2 = random.randint(0, 1500) * 0.001  # kW
    currently_delivered_l3 = random.randint(0, 1500) * 0.001  # kW
    currently_delivered = currently_delivered_l1 + currently_delivered_l2 + currently_delivered_l3
    currently_returned_l1 = 0
    currently_returned_l2 = 0
    currently_returned_l3 = 0
    currently_returned = 0

    # Randomly switch between electricity delivered and returned each 5 seconds for a more realistic graph.
    if with_electricity_returned and second_since % 10 < 5:
        currently_returned = currently_delivered
        currently_returned_l1 = currently_delivered_l1
        currently_returned_l2 = currently_delivered_l2
        currently_returned_l3 = currently_delivered_l3
        currently_delivered_l1 = 0
        currently_delivered_l2 = 0
        currently_delivered_l3 = 0
        currently_delivered = 0

    data = [
        "/XMX5LGBBFFB123456789\r\n",
        "\r\n",
        "1-3:0.2.8(40)\r\n",
    #  "0-0:1.0.0({timestamp}W)\r\n".format(
    #      timestamp=now.strftime('%y%m%d%H%M%S')
    #  ),
        "0-0:96.1.1(FAKEELECID)\r\n",
        "1-0:1.8.1({}*kWh)\r\n".format(round_precision(electricity_1, 10)),
        "1-0:2.8.1({}*kWh)\r\n".format(round_precision(electricity_1_returned, 10)),
        "1-0:1.8.2({}*kWh)\r\n".format(round_precision(electricity_2, 10)),
        "1-0:2.8.2({}*kWh)\r\n".format(round_precision(electricity_2_returned, 10)),
        "0-0:96.14.0(0001)\r\n",  # Should switch high/low tariff, but not used anyway.
        "1-0:1.7.0({}*kW)\r\n".format(round_precision(currently_delivered, 6)),
        "1-0:2.7.0({}*kW)\r\n".format(round_precision(currently_returned, 6)),
        "0-0:96.7.21(00003)\r\n",
        "0-0:96.7.9(00000)\r\n",
        "1-0:99.97.0(0)(0-0:96.7.19)\r\n",
        "1-0:32.32.0(00001)\r\n",
        "1-0:52.32.0(00002)\r\n",
        "1-0:72.32.0(00003)\r\n",
        "1-0:32.36.0(00000)\r\n",
        "1-0:52.36.0(00000)\r\n",
        "1-0:72.36.0(00000)\r\n",
        "0-0:96.13.1()\r\n",
        "0-0:96.13.0()\r\n",
        "1-0:31.7.0(000*A)\r\n",
        "1-0:51.7.0(000*A)\r\n",
        "1-0:71.7.0(001*A)\r\n",
        "1-0:21.7.0({}*kW)\r\n".format(round_precision(currently_delivered_l1, 6)),
        "1-0:41.7.0({}*kW)\r\n".format(round_precision(currently_delivered_l2, 6)),
        "1-0:61.7.0({}*kW)\r\n".format(round_precision(currently_delivered_l3, 6)),
        "1-0:22.7.0({}*kW)\r\n".format(round_precision(currently_returned_l1, 6)),
        "1-0:42.7.0({}*kW)\r\n".format(round_precision(currently_returned_l2, 6)),
        "1-0:62.7.0({}*kW)\r\n".format(round_precision(currently_returned_l3, 6)),
    ]

    if fault == "random":
        fault = random.sample(faults_to_test, 1)[0]


    if fault == "wrong_data":
        data.append("1-0:62.7.0({}kW)\r\n".format(round_precision(currently_returned_l3, 6)))

    if fault == "wrong_ID":
        data.append("1-0:68.9.9({}*kW)\r\n".format(round_precision(0, 6)))

    if fault == "faulty_ID":
        data.append("1-0:62:7:0({}*kW)\r\n".format(round_precision(0, 6)))

    if fault == "missing_ID":
        data = data[:-2]

    if fault == "wrong_timestamp":
        data.append("0-0:1.0.0({timestamp}W)\r\n".format(timestamp=now.strftime('%y%m%d%HRR%MRD%SSS43')))
    else:
        data.append("0-0:1.0.0({timestamp}W)\r\n".format(timestamp=now.strftime('%y%m%d%H%M%S')))



    if with_gas:
        data += [
            "0-1:24.1.0(003)\r\n",
            "0-1:96.1.0(FAKEGASID)\r\n",
            "0-1:24.2.3({}W)({}*m3)\r\n".format(
                now.strftime('%y%m%d%H%M%S'), round_precision(gas, 9)
            ),
        ]

    data += ["!"]
    telegram = "".join(data)

    # Sign the data with CRC as well.
    crc16_function = crcmod.predefined.mkPredefinedCrcFun('crc16')

    unicode_telegram = telegram.encode('ascii')
    calculated_checksum = crc16_function(unicode_telegram)

    hexed_checksum = hex(calculated_checksum)[2:].upper()
    hexed_checksum = '{:0>4}'.format(hexed_checksum)  # Zero any spacing on the left hand size.

    if fault == "wrong_checksum":
        hexed_checksum = hexed_checksum[:-2] + '00'



    return "{}{}".format(telegram, hexed_checksum) + '\r\n'

def write_messages(
    device_port,
    n_steps,
    interval,
    with_electricity_returned,
    fault,
    faults_to_test,
    with_gas
):

    with serial.Serial(port=device_port,
                            baudrate=115200,
                            bytesize=serial.SEVENBITS,
                            parity=serial.PARITY_EVEN,
                            stopbits=serial.STOPBITS_ONE,
                            xonxoff=0,
                            rtscts=0,
                            timeout=20) as ser:
        for _ in range(n_steps):
            time.sleep(interval)
            telegram = get_DSMR_data(
                with_electricity_returned,
                fault,
                faults_to_test,
                with_gas
            )
            te = telegram.encode('utf-8')
            ser.write(te)
                

class Cosem_Emulator():
    def __init__(self, value, unit):
        self.value = value
        self.unit = unit


class DSMR_message_emulator():

    def __init__(self):
        pass

    def get_message(self):

        parser = Cosem_Emulator('0', 'na')
        now = datetime.datetime.now(tz=pytz.UTC)
        time_stamp = Cosem_Emulator(now, 'na')

        message = {
            '\\d-\\d:0\\.2\\.8.+?\\r\\n': parser,
            '\\d-\\d:1\\.0\\.0.+?\\r\\n': time_stamp,
            '\\d-\\d:96\\.1\\.1.+?\\r\\n': parser,
            '\\d-\\d:1\\.8\\.1.+?\\r\\n': parser,
            '\\d-\\d:1\\.8\\.2.+?\\r\\n': parser,
            '\\d-\\d:2\\.8\\.1.+?\\r\\n': parser,
            '\\d-\\d:2\\.8\\.2.+?\\r\\n': parser,
            '\\d-\\d:96\\.14\\.0.+?\\r\\n': parser,
            '\\d-\\d:1\\.7\\.0.+?\\r\\n': parser,
            '\\d-\\d:2\\.7\\.0.+?\\r\\n': parser,
            '96\\.7\\.9.+?\\r\\n': parser,
            '96\\.7\\.21.+?\\r\\n': parser,
            '99\\.97\\.0.+?\\r\\n': parser,
            '\\d-\\d:32\\.32\\.0.+?\\r\\n': parser,
            '\\d-\\d:52\\.32\\.0.+?\\r\\n': parser,
            '\\d-\\d:72\\.32\\.0.+?\\r\\n': parser,
            '\\d-\\d:32\\.36\\.0.+?\\r\\n': parser,
            '\\d-\\d:52\\.36\\.0.+?\\r\\n': parser,
            '\\d-\\d:72\\.36\\.0.+?\\r\\n': parser,
            '\\d-\\d:31\\.7\\.0.+?\\r\\n': parser,
            '\\d-\\d:51\\.7\\.0.+?\\r\\n': parser,
            '\\d-\\d:71\\.7\\.0.+?\\r\\n': parser,
            '\\d-\\d:96\\.13\\.0.+?\\r\\n': parser,
            '\\d-\\d:24\\.1\\.0.+?\\r\\n': parser,
            '\\d-\\d:21\\.7\\.0.+?\\r\\n': parser,
            '\\d-\\d:41\\.7\\.0.+?\\r\\n': parser,
            '\\d-\\d:61\\.7\\.0.+?\\r\\n': parser,
            '\\d-\\d:22\\.7\\.0.+?\\r\\n': parser,
            '\\d-\\d:42\\.7\\.0.+?\\r\\n': parser,
            '\\d-\\d:62\\.7\\.0.+?\\r\\n': parser,
            '\\d-\\d:96\\.1\\.0.+?\\r\\n': parser,
            '\\d-\\d:24\\.2\\.3.+?\\r\\n': parser,
        }

        return message





class Fronius_Emulator():

    def __init__(self, response_time=0):
        self.response_time = response_time



    def _dump_data(self, request):
        now = datetime.datetime.now(pytz.UTC)
        timestr = now.strftime('%Y-%m-%dT%H:%M:%S%z')
        example_timestr = "2021-04-21T08:43:27+02:00"
        response = {
            "Body" : {
                "Data" : {
                    "DAY_ENERGY" : {
                        "Unit" : "Wh",
                        "Value" : 1162.4000000000001
                    },
                    "DeviceStatus" : {
                        "ErrorCode" : 0,
                        "LEDColor" : 2,
                        "LEDState" : 0,
                        "MgmtTimerRemainingTime" : -1,
                        "StateToReset" : False,
                        "StatusCode" : 7
                    },
                    "FAC" : {
                        "Unit" : "Hz",
                        "Value" : 50
                    },
                    "IAC" : {
                        "Unit" : "A",
                        "Value" : 9.6799999999999997
                    },
                    "IDC" : {
                        "Unit" : "A",
                        "Value" : 4.4699999999999998
                    },
                    "PAC" : {
                        "Unit" : "W",
                        "Value" : 2308
                    },
                    "TOTAL_ENERGY" : {
                        "Unit" : "Wh",
                        "Value" : 12906950
                    },
                    "UAC" : {
                        "Unit" : "V",
                        "Value" : 239.5
                    },
                    "UDC" : {
                        "Unit" : "V",
                        "Value" : 536.60000000000002
                    },
                    "YEAR_ENERGY" : {
                        "Unit" : "Wh",
                        "Value" : 1671295.3799999999
                    }
                }
            },
            "Head" : {
                "RequestArguments" : {
                    "DataCollection" : "CommonInverterData",
                    "DeviceClass" : "Inverter",
                    "DeviceId" : "1",
                    "Scope" : "Device"
                },
                "Status" : {
                    "Code" : 0,
                    "Reason" : "",
                    "UserMessage" : ""
                },
                "Timestamp" : timestr
            }
            }

        return json.dumps(response)



    def request_handler(self):
        def f(request):
            time.sleep(self.response_time)
            return self._dump_data(request)
        return f
    

