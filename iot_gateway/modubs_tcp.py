from pyModbusTCP.client import ModbusClient
import time
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from utils.logger import get_logger
from configs import config,mb_registers_mapping

logger = get_logger(__name__)
class ModbusClientHandler:
    """
    A Simple wrapper to handle Modbus TCP Communiction.

    Params:
    - Host(str): The Modbus Server host name.
    - port(int): The Modbus port.
    - unit_id(int): The device id.
    - layer(str): The communication layer (default=TCP)
    """
    def __init__(self,  host: str, port:int,unit_id:int, layer:str="TCP" ):
        self.host=host
        self.port = port
        self.layer=layer # Might be used in the future
        self.unit_id = unit_id
        self.connect() #Automatic attempt to connect

    def connect(self) -> None:
        """
        A method to establish connection to the Modbus Server

        Returns:
            None
        """
        try:
            self.cnx=ModbusClient(host=self.host, port=self.port, unit_id=self.unit_id, timeout=0.1)
            if not self.cnx.open():
                logger.error("Unable to connect to Modbus Server")
            else:
                logger.info(f"Connection to Modbus Server on host '{self.host}' port '{self.port}' is successful")
        except Exception as e:
            logger.error(f"Error in connecting to Modbus server {e}")

    def read_registers(self, register_address:int, number_of_registers:int) -> None:
        """
        A Method to read Modbus registers

        Params:
        - register_address(int): The Starting address of the register to read from.
        - number_of_registers(int): The number of registers to read.
        Returns:
            None
        """

        self.values=self.cnx.read_holding_registers(register_address, number_of_registers) #function code 3 (Read Multiple Holding Register) Hex0x03
        if not self.values: # in case connection was lost during reading process
            self.connect()
            return
        print(f"Read values are:{self.values}")
    
    def parse_readings(self) -> None:
        """
        A Method to parse and map the readings from the Modbus Server.

        Returns:
            None
        """
        if not self.values:
            self.connect() # in case connection was lost during parsing process
            return
        
        for index, value in enumerate(self.values):
            sensor_name=mb_registers_mapping.mapping.get(index, "Unknown Sensor")
            print(f"Sensor {sensor_name} is {value} C")

client = ModbusClientHandler(host=config.MODBUS_HOST, port=config.MODUBS_PORT, unit_id=config.MODBUS_UNIT_ID)




while True:
    client.read_registers(0,4)
    client.parse_readings()
    time.sleep(2)