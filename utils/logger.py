import logging


def get_logger(name, file_name):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s- %(levelname)s-%(message)s', filemode='a', filename=file_name)
    logger = logging.getLogger(name)
    return logger