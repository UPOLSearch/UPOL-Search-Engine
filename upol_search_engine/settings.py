import configparser
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = ROOT_DIR

CONFIG = configparser.ConfigParser()
config_path = os.path.join(CONFIG_DIR, 'config.ini')
default_config_path = os.path.join(CONFIG_DIR, 'config-default.ini')

if os.path.isfile(config_path):
    CONFIG.read(config_path)
else:
    CONFIG.read(default_config_path)
