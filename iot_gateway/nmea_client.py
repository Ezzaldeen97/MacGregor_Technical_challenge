import socket
import sys
import os
HOST = "localhost"
PORT = 8888
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from utils.logger import get_logger
from configs import config,mb_registers_mapping

logger = get_logger(__name__)
class NmeaHandler:
    
    def __init__(self, port:int, host:str):
        self.port = port
        self.host = host
        self.connect()

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        print("Connected to NMEA server.")

    def read_nmea_data(self):
        self.sentence = self.sock.recv(1024).decode('ascii')
        return self.sentence
    def parse_data(self):
        try:
            if not self.sentence.startswith('$') or '*' not in self.sentence:
                logger.error("Invalid NMEA format")
                return
            else:
                print(self.sentence)
                data, checksum = self.sentence.split("*")
                fields= data.split(",")
                talker_id = fields[0][1:3]
                sentence_type= fields[0][3:]
                rot_value= fields[1]
                status= fields[2]
                return {
                    "talker_id": talker_id,
                    "sentence_type":sentence_type,
                    "rate_of_turn_value": rot_value,
                    "Status":status,
                    "checksum":checksum}

        except Exception as e:
            logger.error(f"Failed to parse nmea message {e}")



        

nmea_client=NmeaHandler(PORT, HOST)
while True:
    nmea_client.read_nmea_data()
    print(nmea_client.parse_data())