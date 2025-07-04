from iot_gateway.modubs_tcp import ModbusClientHandler
from iot_gateway.nmea_client import NmeaHandler
from iot_gateway.mqtt_publisher import MQTTPublisher
from configs import config
import os
import sys
from datetime import datetime
import time
import asyncio
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from utils.logger import get_logger
#-------------loggers-----------------
logger = get_logger("iot_gatway_logger", file_name='logs/iot_gatway.log')
logger.info("Starting iot_gatway......")

#-------------Clients-----------------
modbus_client = ModbusClientHandler(host=config.MODBUS_HOST, port=config.MODUBS_PORT, unit_id=config.MODBUS_UNIT_ID)
nmea_client=NmeaHandler(config.NMEA_PORT,config.NMEA_HOST, send_valid_data=True)
publisher = MQTTPublisher(
        host=config.MQTT_BROKER_HOST,
        port=config.MQTT_BROKER_PORT,
        username=config.MQTT_BROKER_USERNAME,
        password=config.MQTT_BROKER_PASSWORD
    )
last_timestamp=[time.time()]


#----------polling loops-----------
async def poll_temerature_sensors():
    """
    An async function that polls temperature sensors data via Modbus protocolt and publish the valid data to a 
    Mqtt broker.
    
    """
    while True:
        try:
            for reading in modbus_client.read_temperature_sensor():
                if reading and publisher.publish(reading):
                    last_timestamp[0]=time.time()
            await asyncio.sleep(2)
        except Exception as e:
            error_message = "Error in reading temerature sensors, please check modbus protocol logs"
            print(error_message)
            logger.error(f"{error_message}: {e}")
            await asyncio.sleep(2)


async def poll_rot_sensor():
    """
    An async function that polls ROT sensor data via NMEA protocolt and publish the valid data to a 
    Mqtt broker.
    
    """
    while True:
        try:
            for reading in nmea_client.get_ROT_readings():
                if reading and publisher.publish(reading):
                    last_timestamp[0]=time.time()
            await asyncio.sleep(2)
        except Exception as e:
            error_message = "Error in reading ROT sensor, please check NMEA protocol logs"
            print(error_message)
            logger.error(f"{error_message}: {e}")

            await asyncio.sleep(2)

async def calculate_timeout(timeout:int = 60 *config.MAX_ELAPSED_TIME):
        """
        A function to monitor the elabsed time since the last successful  data publish.
        Its useful to shut down the system and log it if the elapsed time exceeds timout

        Args:
        ------
        timeout(int): Maximum allowed seconds of inactivity
        
        """
        while True:
            await asyncio.sleep(60)
            elapsed = time.time() - last_timestamp[0]
            if elapsed > timeout:
                logger.error(f"The mqtt publisher has not recieved data for {timeout/60} minutes, so the connection will be closed.")
                sys.exit(1)


async def main():
    await asyncio.gather(poll_rot_sensor(),
                         poll_temerature_sensors(),
                         calculate_timeout())        



if __name__== "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Shutting down due to KeyboardInterrupt.")