from pyModbusTCP.client import ModbusClient
import time
import sys
import os
from datetime import datetime
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from utils.logger import get_logger
from configs import config,mb_registers_mapping

logger = get_logger("modbus_logger", file_name='logs/modbus_client.log')
iot_logger = get_logger("iot_gatway_logger", file_name='logs/iot_gatway.log')

class ModbusClientHandler:
    """
    A Simple wrapper to handle Modbus TCP Communiction.

    Attributes:
    - Host(str): The Modbus Server host name.
    - port(int): The Modbus port.
    - unit_id(int): The device id.
    - layer(str): The communication layer (default=TCP)
    """
    def __init__(self, host: str, port:int, unit_id:int, layer:str="TCP" ):
        self.host=host
        self.port = port
        self.layer=layer # Might be used in the future
        self.unit_id = unit_id
        self.connect() #Automatic attempt to connect
        self.previous_values = [None]*4

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
                iot_logger.info(f"Connection to Modbus Server on host '{self.host}' port '{self.port}' is successful")
                logger.info(f"Connection to Modbus Server on host '{self.host}' port '{self.port}' is successful")
        except Exception as e:
            logger.error(f"Error in connecting to Modbus server {e}")

    def read_registers(self, register_address:int, number_of_registers:int) -> None:
        """
        A Method to read Modbus registers

        Attributes:
        - register_address(int): The Starting address of the register to read from.
        - number_of_registers(int): The number of registers to read.
        Returns:
            None
        """

        self.values=self.cnx.read_holding_registers(register_address, number_of_registers) #function code 3 (Read Multiple Holding Register) Hex0x03
        if not self.values: # in case connection was lost during reading process
            self.connect()
            return
        # print(f"Read values are:{self.values}")
    
    def parse_readings(self) -> None:
        """
        A Method to parse and map the readings from the Modbus Server.

        Returns:
            None
        """
        if not self.values:
            self.connect() # in case connection was lost during parsing process
            return
        datapoints=[]
        for index, value in enumerate(self.values):
            sensor_name=mb_registers_mapping.mapping.get(index, "Unknown Sensor")
            previous_value=self.previous_values[index]
            datapoint = Datapoint(sensor_name, value, previous_value)
            # print(f"Sensor {sensor_name} is {value} C")
            self.previous_values[index] = datapoint
            datapoints.append(datapoint)
        return datapoints
    def read_temperature_sensor(self):
        self.read_registers(0,4)
        return self.parse_readings()
   


class Datapoint:
    """
    A helper class the represents a single sensor reading and its previous value
    
    Attributes:
    
    - sensor(str): the name of the sensor
    - value(float): The current value of the sensor reading.
    - previous_datapoint(Datapoint): The previous datapoint object for comparison.
    - send_point(bool): A flag if the data point meets the criteria to be sent to the broker.
    - timestamp(datetime): The timestamp of the current reading.

    """
    def __init__(self, sensor:str, value:float, previous_datapoint)->None:
        """
        Initialize a Datapoint object

        Attributes:
        - sensor(str): Sensor name
        - value(float): Sensor reading value
        - previous_datapoint(Datapoint): previous datapoint object
        
        """
        self.sensor = sensor
        self.value = value
        self.previous_datapoint:Datapoint=previous_datapoint
        self.changed = True  
        self.send_point = True
        self.timestamp=datetime.now()
        self.evaluate_datapoint()

    def evaluate_datapoint(self):
        """
        Evaluate if the current reading differs from the previous one based on comparison criteria

        If the temperature change is plus minus 1 degrees (MIN_TEMPERATURE_CHANGE) and the time difference is less that 5 mins MIN_ELAPSED_TIME 
        the data point will be marked as not for sending.

        """
        if not self.previous_datapoint: # The first data points (no previous) will be None
            return
        value_delta = abs(self.value - self.previous_datapoint.value)
        time_delta = (self.timestamp - self.previous_datapoint.timestamp).total_seconds() /60
        if value_delta <= config.MIN_TEMPERATURE_CHANGE and time_delta < config.MIN_ELAPSED_TIME:
            self.changed=False
            self.send_point=False 

    def __str__(self):
        date = self.timestamp.strftime("%Y-%m-%d")
        time = self.timestamp.strftime("%H:%M")
        status = "Valid" if self!= "V" else "Invalid"
        return f"{self.value} C, {status}, {date} at {time} UTC"

