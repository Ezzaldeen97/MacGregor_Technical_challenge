import socket
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from configs import config
from utils.logger import get_logger


logger = get_logger(__name__, file_name= 'logs/nmea_client.log')
class NmeaHandler:
    """
    A handler class for connecting to NMEA TCP stream, reading and parsing

    Params:
    --------
    - port(int): Port Number of the NMEA server.
    - host (str): Host address of the NMEA server

    """
    
    def __init__(self, port:int, host:str) -> None:
        self.port = port
        self.host = host
        self.connect()

    def connect(self) -> None:
        """
        Attempts to establish a TCP connection to the NMEA (server)"

        Returns:
        --------
        - None
        
        """
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            logger.info(f"Connection to NMEA Websocket on host '{self.host}' port '{self.port}' is successful")
        except socket.timeout:
            logger.error("Connection times out")
        except Exception as e:

            logger.error(f"Failed to connect to NMEA Websocket: {e}")

    def read_nmea_data(self) -> str:
        """
        Reads a raw NMEA sentence.
        
        Returns:
        --------
        - data (str) | None : Decoded Nmea sentence (if successful) otherwise returns None
        
        """
        try:
            data= self.sock.recv(1024).decode('ascii') #1 kilobyte
            if not data:
                logger.error("Socket Closed by server")
            else:
                return data
            
        except Exception as e:
            logger.error(f"Unexpected error:{e}")


    def parse_data(self, sentence:str)->dict:
        """
        Parses a given NMEA sentence
        
        Params:
        --------
        - sentence(str): a NMEa statement that follows MG predefined format

        Returns:
        --------
        - dict | None: Parsed message as dictionary if sucessful, otherwise None

        
        """

        try:
            if not sentence.startswith('$') or '*' not in sentence:
                logger.error(f"Invalid NMEA format {sentence}")
                return
            else:
                data, checksum = sentence.split("*")
                fields= data.split(",")
                talker_id = fields[0][1:3]
                sentence_type= fields[0][3:]
                rot_value= fields[1]
                status= fields[2]
                return {
                    "talker_id": talker_id,
                    "sentence_type":sentence_type,
                    "rate_of_turn_value": rot_value,
                    "status":status,
                    "checksum":checksum}
        except IndexError:
            logger.error("Error in index the nmea sentence")
        except Exception as e:
            logger.error(f"Failed to parse nmea message {e}")       

nmea_client=NmeaHandler(config.NMEA_PORT,config.NMEA_HOST)
while True:
    sentence=nmea_client.read_nmea_data()
    print(nmea_client.parse_data(sentence))