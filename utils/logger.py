import logging


def get_logger(name):
    logging.basicConfig(level=logging.INFO, format='%(levelname)-%(message)')
    logger = logging.getLogger(name)
    return logger