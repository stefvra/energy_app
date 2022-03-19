import configparser
import os
from dateutil import parser
import datetime
import pytz
import traceback

def print_callstack():
    for line in traceback.format_stack():
        print(line.strip())



log_config_files = {
    'PRODUCTION': 'production.conf',
    'TESTING': 'testing.conf',
}


config_files = {
    'PRODUCTION': 'production.conf',
    'TESTING': 'testing.conf',
}

def get_local_timezone():
    return pytz.timezone('Europe/Amsterdam')



def get_log_config_file(app_state=None):
    if app_state is None:
        if 'ENERGYAPP_STATE' in os.environ:
            app_state = os.getenv('ENERGYAPP_STATE')
        else:
            app_state = 'TESTING'

    return 'config/logging/' + log_config_files[app_state]
    

def get_config_file(production_state=False):
    if 'CONFIG_FILE' in os.environ:
        return(os.getenv('CONFIG_FILE'))
    elif production_state:
        store = Config_Store(filename='config/general/secrets.txt')
        return store.get('config')['dir']
    else:
        return 'config/general/test.conf'



class Config_Store():

    def __init__(self, filename=None, parse_value=True, default_value=None):
        self.filename = filename
        self.parse_value = parse_value
        self.default_value = default_value
        self.parser = configparser.ConfigParser()
        self._read_from_file()

    def _save_to_file(self):
        if self.filename is not None:
            with open(self.filename, 'w') as f:
                self.parser.write(f)
    
    def _read_from_file(self):
        if self.filename is not None:
            self.parser.read(self.filename)

    def _parse_value_at_add(self, value):
        if self.parse_value:
            if isinstance(value, list):
                return ','.join([str(v) for v in value])
            else:
                return str(value)


    def _parse_value(self, value):
        if self.parse_value:
            if ',' in value:
                return [self._parse_value(v) for v in value.split(',')]
            try:
                value = float(value)
                value = int(value) if value.is_integer() else value
                return value
            except Exception:
                pass
            try:
                return parser.parse(value)
            except Exception:
                if value.lower() == 'true':
                    return True
                elif value.lower() == 'false':
                    return False
                elif value.lower() == 'none':
                    return None
                else:
                    return value
        else:
            return value

    def _parse_dict(self, _dict):
        parsed_dict = {}
        for key, value in _dict.items():
            parsed_dict[key] = self._parse_value(value)
        return parsed_dict
 

    def add(self, section, __dict, gentle=False):
        _dict = __dict.copy()
        for key, value in _dict.items():
            _dict[key] = self._parse_value_at_add(value)
        self._read_from_file()
        if section in self.parser:
            if len(self.parser[section]) > 0:
                if gentle:
                    self.parser[section] = {**_dict, **self.parser[section]}
                else:
                    self.parser[section] = {**self.parser[section], **_dict}
            else:
                self.parser[section] = _dict
        else:
            self.parser[section] = _dict
        self._save_to_file()


    def get(self, section, key=None):
        self._read_from_file()
        if key is not None:
            if key in self.parser[section]:
                return self._parse_value(self.parser[section][key])
            else:
                return self.default_value
        else:
            return self._parse_dict(self.parser[section])


    def get_section_keys(self):
        self._read_from_file()
        keys = []
        for k, v in self.parser.items():
            keys.append(k)
        return keys
    

    def find_config(self, key, value=None):
        self._read_from_file()
        result = {}
        for section_key, _ in self.parser.items():
            section_value = self.get(section_key)
            if key in section_value:
                if value is None or section_value[key] == value:
                    result[section_key] = {}
                    result[section_key][key] = section_value[key]
        return result




    def remove_section(self, section):
        self._read_from_file()
        self.parser.remove_section(section)
        self._save_to_file()

    def remove_option(self, section, key):
        self._read_from_file()
        self.parser.remove_option(section, key)
        self._save_to_file()


class Default_Section_Config_Store(Config_Store):

    def __init__(self, default_section, filename=None, parse_value=True, default_value=None):
        self.default_section = default_section
        super().__init__(filename=filename, parse_value=parse_value, default_value=default_value)



    def add(self, _dict, gentle=False):
        super().add(self.default_section, _dict, gentle=gentle)


    def get(self, key=None):
        return super().get(self.default_section, key=key)


    def remove_option(self, key):
        super().remove_option(self.default_section, key)



