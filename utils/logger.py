import logging


def get_logger(name):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s- %(levelname)s-%(message)s', filemode='a', filename='logs/modbus_client.log')
    logger = logging.getLogger(name)
    return logger