import os
import logging

from upol_crawler.settings import *


def universal_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    handler = logging.FileHandler(os.path.join(os.path.join(ROOT_DIR, 'logs/'), name + '.log'), 'a')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
