import socket
import sys
import os
from datetime import datetime
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
        self.previous_datapoint=None
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
                rot_value= float(fields[1])
                status= fields[2]

                datapoint = Datapoint_Nmea(talker_id, sentence_type, rot_value, status, checksum,self.previous_datapoint)
                self.previous_datapoint=datapoint
                return datapoint
        except IndexError:
            logger.error("Error in index the nmea sentence")
        except Exception as e:
            logger.error(f"Failed to parse nmea message {e}")       


class Datapoint_Nmea:
    def __init__(self,talker_id, sentence_type, rot_value, status, checksum, previous_datapoint)->None:
        """
        Initialize a Datapoint object

        Attributes:
        - sensor(str): Sensor name
        - value(float): Sensor reading value
        - previous_datapoint(Datapoint): previous datapoint object
        
        """
        self.talker_id = talker_id
        self.sentence_type = sentence_type
        self.previous_datapoint:Datapoint_Nmea=previous_datapoint
        self.rot_value=rot_value
        self.status=status
        self.checksum=checksum
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
        value_delta = abs(self.rot_value - self.previous_datapoint.rot_value)
        time_delta = (self.timestamp - self.previous_datapoint.timestamp).total_seconds() /60
        if value_delta <= config.MIN_ROT_CHANGE and time_delta < config.MIN_ELAPSED_TIME:
            self.changed=False
            self.send_point=False 

nmea_client=NmeaHandler(config.NMEA_PORT,config.NMEA_HOST)
while True:
    sentence=nmea_client.read_nmea_data()
    nmea_client.parse_data(sentence)
