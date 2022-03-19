from flask import render_template, url_for, redirect, request
from energy_app import app
import logging

from tools import tools
from web_app.energy_app.factories import Dashboard_Controller_Factory

logger = logging.getLogger('web_app')
config_file = tools.get_config_file(production_state=True)
config_store = tools.Config_Store(filename=config_file)
controller_factory = Dashboard_Controller_Factory()
controller = controller_factory.create_from_config(config_store)


@app.route('/')
def main():
  return controller.control_get(request)


@app.route('/', methods=['POST'])
def main_post():
  return controller.control_post(request)

