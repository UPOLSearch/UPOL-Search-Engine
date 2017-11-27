import configparser
import os
from pathlib import Path

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = ROOT_DIR

CONFIG = configparser.ConfigParser()
config_path = '/etc/upol_search_engine/config.ini'
default_config_path = os.path.join(CONFIG_DIR, 'config-default.ini')

if os.path.isfile(config_path):
    CONFIG.read(config_path)
else:
    CONFIG.read(default_config_path)

version = '0.7-dev'
project_url = 'https://github.com/UPOLSearch/UPOL-Search-Engine'
user_agent = 'Mozilla/5.0 (compatible; UPOL-Search-Engine{0}; {1})'.format(version, project_url)
