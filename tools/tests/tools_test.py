import pytest
import random
import logging
import logging.config
from tools import tools
import configparser


logging.config.fileConfig(tools.get_log_config_file())
logger = logging.getLogger('tester')

config_filename = 'fixtures/conf.ini'


operations = ['add', 'remove_option', 'remove_section' 'add_gentle']
sections = ['section' + str(i) for i in range(10)]
settings = ['setting' + str(i) for i in range(10)]
values = ['value' + str(i) for i in range(3)] + [['1', '2', '3'], ['a', 'b', 'c']]



def test_config_store():
    cs = tools.Config_Store(filename=config_filename)
    for _ in range(50):
        operation = random.choice(operations)
        section = random.choice(sections)
        setting = random.choice(settings)
        value = random.choice(values)
        if operation == 'add':
            cs.add(section, {setting: value})
            assert cs.get(section, setting) == value
        elif operation == 'remove_option':
            try:
                cs.remove_option(section, key=setting)
            except (KeyError, configparser.NoSectionError):
                continue
            assert (setting not in cs.get(section))
        elif operation == 'add_gentle':
            try:
                initial_value = cs.get(section, key=setting)
                key_existed = True
            except (KeyError, configparser.NoSectionError):
                key_existed = False
            cs.add(section, {setting: value}, gentle=True)
            if key_existed:
                assert cs.get(section, setting) == initial_value
            else:
                assert cs.get(section, setting) == value


def test_default_section_config_store():
    section = random.choice(sections)
    cs = tools.Default_Section_Config_Store(section, filename=config_filename)
    for _ in range(50):
        operation = random.choice(operations)
        setting = random.choice(settings)
        value = random.choice(values)
        if operation == 'add':
            cs.add({setting: value})
            assert cs.get(setting) == value
        elif operation == 'remove_option':
            try:
                cs.remove_option(key=setting)
            except (KeyError, configparser.NoSectionError):
                continue
            assert (setting not in cs.get())
        elif operation == 'add_gentle':
            try:
                initial_value = cs.get(key=setting)
                key_existed = True
            except (KeyError, configparser.NoSectionError):
                key_existed = False
            cs.add({setting: value}, gentle=True)
            if key_existed:
                assert cs.get(setting) == initial_value
            else:
                assert cs.get(setting) == value


