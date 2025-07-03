from pyModbusTCP.client import ModbusClient
import time
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from configs import config,mb_registers_mapping
class MODBUS_CLIENT:
    def __init__(self, ip:str, host: str, port:int,unit_id:int, layer="TCP", ):
        self.ip=ip
        self.host=host
        self.port = port
        self.layer=layer
        self.unit_id = unit_id
        self.connect() #will connect automatically.
    def connect(self):
        self.cnx=ModbusClient(host=self.host, port=self.port, unit_id=self.unit_id, auto_open=True)
    def read_registers(self, register_address:int, number_of_registers:int):
        self.values=self.cnx.read_holding_registers(register_address, number_of_registers) #function code 3 (Read Multiple Holding Register) 
        print(f"Read values are:{self.values}")
        return self.values
    
    def parse_readings(self):

        for index, value in enumerate(self.values):
            print(f"Sensor {mb_registers_mapping.mapping[index]} is {value} C")



client = MODBUS_CLIENT(ip = config.MODBUS_IP ,host=config.MODBUS_HOST, port=config.MODUBS_PORT, unit_id=config.MODBUS_UNIT_ID)




while True:
    client.read_registers(0,4)
    client.parse_readings()
    time.sleep(2)