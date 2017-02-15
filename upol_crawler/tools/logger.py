import logging
import os

from upol_crawler.settings import *


def universal_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    logs_path = os.path.join(ROOT_DIR, 'logs/')
    os.makedirs(logs_path, exist_ok=True)

    handler = logging.FileHandler(os.path.join(logs_path, name + '.log'), 'a')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
