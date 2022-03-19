from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
import sys
import logging.config

sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))


from tools import tools
logging.config.fileConfig(tools.get_log_config_file())

app = Flask(__name__)


from energy_app import routes
