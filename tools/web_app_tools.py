import numbers
import datetime
import pandas as pd


class Dict_Parser():

    def __init__(self, numeric_format='{0:.2f}', datetime_format='%d-%m-%y %H:%M'):
        self.parse_register = [
            [numbers.Number, self.parse_numeric],
            [datetime.datetime, self.parse_datetime],
            [pd.Timestamp, self.parse_datetime]
        ]
        self.numeric_format = numeric_format
        self.datetime_format = datetime_format

    def parse_numeric(self, value):
        return self.numeric_format.format(value)

    def parse_value(self, value):
        for _type, parser in self.parse_register:
            if isinstance(value, _type):
                return parser(value)
        return value
    
    def parse_list(self, _list):
        parsed_list = []
        for v in _list:
            parsed_list.append(self.parse_value(v))
        return parsed_list



    def parse_dict(self, _dict):
        parsed_dict = _dict.copy()
        for k,v in parsed_dict.items():        
            if isinstance(v, dict):
                parsed_dict[k] = self.parse_dict(v)
            if isinstance(v, list):
                parsed_dict[k] = self.parse_list(v)
            else:
                parsed_dict[k] = self.parse_value(v)
        return parsed_dict
                

    def parse_datetime(self, time):
        return time.strftime(self.datetime_format)



def datetime_to_str(time, format='%d-%m-%y %H:%M'):
  if isinstance(time, list):
    return [t.strftime(format) for t in time]
  else:
    return time.strftime(format)







class Cost_Calculator():
    
    def __init__(self, from_grid_cost=.3, to_grid_cost=-.03):
        self.from_grid_cost = from_grid_cost
        self.to_grid_cost = to_grid_cost

    def calculate(self, from_grid, to_grid):
        return from_grid * self.from_grid_cost + to_grid * self.to_grid_cost