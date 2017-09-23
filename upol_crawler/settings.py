import configparser
import os

from upol_crawler.utils import urls

CRAWLER_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CRAWLER_DIR)
CONFIG_DIR = ROOT_DIR

CONFIG = configparser.ConfigParser()
config_path = os.path.join(CONFIG_DIR, 'config.ini')
CONFIG.read(config_path)

BLACKLIST_FILE = os.path.join(CONFIG_DIR, 'blacklist.txt')

DOMAIN_REGEX = urls.generate_regex(CONFIG.get('Settings', 'limit_domain'))
MAX_DEPTH = CONFIG.getint('Settings', 'max_depth')
VERIFY_SSL = CONFIG.getboolean('Settings', 'verify_ssl')
DELAY_BETWEEN_FEEDING = CONFIG.getint('Settings', 'delay_between_feeding')
CONNECT_MAX_TIMEOUT = CONFIG.getfloat('Settings', 'connect_max_timeout')
READ_MAX_TIMEOUT = CONFIG.getfloat('Settings', 'read_max_timeout')
TASK_FREQUENCY = CONFIG.get('Settings', 'task_frequency')
FREQUENCY_PER_SERVER = CONFIG.getfloat('Settings', 'frequency_per_server')
DB_BATCH_SIZE = CONFIG.getint('Database', 'db_batch_size')
DB_SERVER = CONFIG.get('Database', 'db_server')
DB_NAME = str(urls.domain(CONFIG.get('Settings', 'limit_domain')).replace('.', '-'))
DB_PORT = CONFIG.getint('Database', 'db_port')

def load_external_settings(limit_domain,
                           max_depth,
                           verify_ssl,
                           delay_between_feeding,
                           connect_max_timeout,
                           read_max_timeout,
                           task_frequency,
                           frequency_per_server,
                           db_batch_size,
                           db_server,
                           db_port,
                           blacklist):
    CONFIG['Info'] = {}
    CONFIG['Info']['version'] = '0.4-dev'
    CONFIG['Info']['project_url'] = 'https://github.com/UPOLSearch/UPOL-Crawler'
    CONFIG['Info']['user_agent'] = 'Mozilla/5.0 (compatible; UPOL-Crawler/%(version)s; %(project_url)s)'

    CONFIG['Settings'] = {}
    CONFIG['Settings']['limit_domain'] = str(limit_domain)
    CONFIG['Settings']['max_depth'] = str(max_depth)
    CONFIG['Settings']['verify_ssl'] = str(verify_ssl)
    CONFIG['Settings']['delay_between_feeding'] = str(delay_between_feeding)
    CONFIG['Settings']['connect_max_timeout'] = str(connect_max_timeout)
    CONFIG['Settings']['read_max_timeout'] = str(read_max_timeout)
    CONFIG['Settings']['task_frequency'] = str(task_frequency)
    CONFIG['Settings']['frequency_per_server'] = str(frequency_per_server)

    CONFIG['Database'] = {}
    CONFIG['Database']['db_batch_size'] = str(db_batch_size)
    CONFIG['Database']['db_server'] = str(db_server)
    CONFIG['Database']['db_port'] = str(db_port)

    with open(config_path, 'w') as configfile:
        CONFIG.write(configfile)

    blacklist = urls.load_urls_from_text(blacklist)

    with open(BLACKLIST_FILE, 'w') as blacklistfile:
        blacklistfile.writelines("\n".join(blacklist))
