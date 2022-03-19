import logging
from tools.factory_tools import Param_Parser

logger = logging.getLogger('outputs')


# try to import Raspberry PI GPIO library. If not possible (eg for testing)
# a mock will be loaded
try:
    import RPi.GPIO as GPIO
except:
    import Mock.GPIO as GPIO


class GPIO_Output():
    """
        Class for managing output to Raspberry PI GPIO
    """

    def __init__(self, pins=[1], enable_mode=True):
        """
            Initialisation

        Args:
            pins (list of integers, optional): pin numbers. Defaults to [1].
            enable_mode (bool, optional): pinmode if output is set to enable. Defaults to True.
        """
        self.pins = pins if type(pins) is list else [pins]
        self.pins = [int(p) for p in self.pins]
        self.enable_mode = enable_mode
        self.mode = not enable_mode
        GPIO.setmode(GPIO.BCM)
        for pin in self.pins:
            GPIO.setup(pin,GPIO.OUT)
            GPIO.output(pin, self.mode)

    def enable(self):
        """
            Set outputs to enable mode
        """
        for pin in self.pins:
            GPIO.output(pin, self.enable_mode)
        self.mode = self.enable_mode

    def disable(self):
        """
            Set outputs to disable mode
        """
        for pin in self.pins:
            GPIO.output(pin, not self.enable_mode)
        self.mode = not self.enable_mode

    def set_mode(self, mode):
        """
            Set outputs to defined mode

        Args:
            mode (boolean): mode to set outputs to
        """
        if mode:
            self.enable()
        else:
            self.disable()

    def is_enabled(self):
        """
            Check if outputs are enabled

        Returns:
            boolean: True if outputs are enabled, False otherzise
        """
        return self.mode == self.enable_mode

    def toggle(self):
        """
            Toggle mode of outputs
        """
        if self.is_enabled():
            self.disable()
            self.mode = not self.enable_mode
        else:
            self.enable()
            self.mode = self.enable_mode




class Output_Factory():
    """
        Factory class for GPIO_Output
    """

    def __init__(self):
        """
            Initialization
        """
        self.param_parser = Param_Parser()
        self.param_register = {
            'pins': {'type': 'int_list', 'default': [1]},
            'enable_mode': {'type': 'bool', 'default': False}
        }



    def create_from_config(self, config_store, section):
        """
            Create output from config store

        Args:
            config_store (config_store): config store that holds the configuration data
            section (string): name of section in the config store

        Returns:
            GPIO_Output: created output
        """

        params = config_store.get(section)
        params = self.param_parser.add_defaults(params, self.param_register)
        self.param_parser.check_types(params, self.param_register)

        return GPIO_Output(
            pins=params['pins'],
            enable_mode=params['enable_mode']
            )