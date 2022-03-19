
from endpoints.outputs import Output_Factory
from tools import tools


output_factory = Output_Factory()
config_store = tools.Config_Store(filename=tools.get_config_file())
mock_GPIO_Output = output_factory.create_from_config(config_store, 'GPIO_output')


def test_GPIO_Output_init():
    assert mock_GPIO_Output.is_enabled() == False


def test_GPIO_Output_enable():
    mock_GPIO_Output.enable()
    assert mock_GPIO_Output.is_enabled() == True

def test_GPIO_Output_disable():
    mock_GPIO_Output.disable()
    assert mock_GPIO_Output.is_enabled() == False

def test_GPIO_Output_toggle():
    initial_state = mock_GPIO_Output.is_enabled()
    mock_GPIO_Output.toggle()
    final_state = mock_GPIO_Output.is_enabled()
    assert initial_state != final_state