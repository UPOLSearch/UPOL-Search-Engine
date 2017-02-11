import configparser
import os

from upol_crawler.utils import urls

CRAWLER_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CRAWLER_DIR)
CONFIG_DIR = ROOT_DIR
CPROFILE_DIR = os.path.join(ROOT_DIR, 'cprofile/')

CONFIG = configparser.ConfigParser()
config_path = os.path.join(CONFIG_DIR, 'config.ini')
default_config_path = os.path.join(CONFIG_DIR, 'config-default.ini')

if os.path.isfile(config_path):
    CONFIG.read(config_path)
else:
    CONFIG.read(default_config_path)

DOMAIN_REGEX = urls.generate_regex(CONFIG.get('Settings', 'limit_domain'))
SEED_FILE = os.path.join(CONFIG_DIR, 'seed.txt')

DATABASE_NAME = urls.domain(CONFIG.get('Settings', 'limit_domain')).replace('.', '-')
