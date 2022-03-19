import numbers
import datetime


class Wrong_Parameters_Exception(Exception):
    pass




class Param_Parser():

    def __init__(self, raise_error=True):
        self.raise_error = raise_error
        self.type_register = {
            'bool': self.is_bool,
            'int': self.is_int,
            'string': self.is_string,
            'int_list': self.is_int_list,
            'float': self.is_float,
            'numeric': self.is_numeric,
            'string_list': self.is_string_list,
            'datetime': self.is_datetime
        }

    def raise_error_or_return_boolean(self, value, name='unknown'):
        if not value:
            if self.raise_error:
                raise Wrong_Parameters_Exception(f'failed on parameter {name}')
            else:
                return False
        return True


    def check_types(self, var, param_register):
        for key, info in param_register.items():
            _type = info['type']
            if key not in var:
                return self.raise_error_or_return_boolean(False, name=key)
            self.type_register[_type](var[key], name=key)
        return True


    def add_defaults(self, var, param_register):
        updated_var = var.copy()
        for key, info in param_register.items():
            if key not in updated_var:
                if 'default' in info:
                    updated_var[key] = info['default']
        return updated_var


    def parse(self, var, param_register):
        updated_params = self.add_defaults(var, param_register)
        self.check_types(updated_params, param_register)
        return updated_params


    def is_bool(self, var, name='unknown'):
        result = isinstance(var, bool)
        return self.raise_error_or_return_boolean(result, name=name)


    def is_datetime(self, var, name='unknown'):
        result = isinstance(var, datetime.datetime)
        return self.raise_error_or_return_boolean(result, name=name)


    def is_int(self, var, name='unknown'):
        result = isinstance(var, int)
        return self.raise_error_or_return_boolean(result, name=name)


    def is_float(self, var, name='unknown'):
        result = isinstance(var, float)
        return self.raise_error_or_return_boolean(result, name=name)


    def is_numeric(self, var, name='unknown'):
        result = isinstance(var, numbers.Number)
        return self.raise_error_or_return_boolean(result, name=name)


    def is_int_list(self, var, name='unknown'):
        if not isinstance(var, list):
            return self.raise_error_or_return_boolean(False, name=name)
        for i in var:
            if not isinstance(i, int):
                return self.raise_error_or_return_boolean(False, name=name)
        return True


    def is_string_list(self, var, name='unknown'):
        if not isinstance(var, list):
            return self.raise_error_or_return_boolean(False, name=name)
        for i in var:
            if not isinstance(i, str):
                return self.raise_error_or_return_boolean(False, name=name)
        return True



    def is_string(self, var, name='unknown'):
        result = isinstance(var, str)
        return self.raise_error_or_return_boolean(result, name=name)
