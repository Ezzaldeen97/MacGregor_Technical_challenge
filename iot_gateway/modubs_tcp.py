from pyModbusTCP.client import ModbusClient
import time
class MODBUS_CLIENT:
    def __init__(self, ip:str, host: str, port:int, layer="TCP", unit_id=1):
        self.ip=ip
        self.host=host
        self.port = port
        self.layer=layer
        self.unit_id = unit_id
        self.connect()
    def connect(self):
        self.cnx=ModbusClient(host=self.host, port=self.port, unit_id=self.unit_id, auto_open=True)
    def read_registers(self, register_address, number_of_registers):
        return self.cnx.read_holding_registers(register_address, number_of_registers) #function code 3 (Read Multiple Holding Register) Hex0x03

client = MODBUS_CLIENT(ip = "127.0.0.1" ,host="localhost", port=8889, unit_id=1)
while True:
    print(client.read_registers(0,4))
    time.sleep(2)