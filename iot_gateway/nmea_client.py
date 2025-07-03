import socket
import sys
import os
import operator
from functools import reduce
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
    - send_valid_data (bool): A flag to allow the handler to send only the data that is tagged with "Valid"

    """
    
    def __init__(self, port:int, host:str,send_valid_data:bool=False) -> None:
        self.port = port
        self.host = host
        self.previous_datapoint=None
        self.connect()
        self.buffer=""
        self.send_valid_data=send_valid_data
        if self.send_valid_data:
            logger.info("Only Valid signals will be sent to the broker based on user wish")



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
                self.connect()
                logger.error("Socket Closed by server")
            else:
                messages= [f'${part}' for part in data.split('$') if part] # to recosntruct the messages.
                return messages
        except Exception as e:
            logger.error(f"Unexpected error:{e}")


    def parse_data(self, messages:list)->dict:
        """
        Parses a given NMEA sentence
        
        Params:
        --------
        - messages(list): a list of Nmea messages that follows MG predefined format

        Returns:
        --------
        - dict | None: Parsed message as dictionary if sucessful, otherwise None

        
        """
        def calculate_checksum(data: str) -> int:
            """
            A function to calculate the checksum of the message

            Args:
            -----
            - data(str): The raw data string without the "Given" checksum, eg. 
            """
            #https://gist.github.com/MattWoodhead/0bc2b3066796e19a3a350689b43b50ab
            calculated_checksum = reduce(operator.xor, (ord(s) for s in data[1:]), 0) # [1:] to skip the $
            return calculated_checksum

        def parse_signal(message:str):
            """
            A function that encabsulates the handle of parsing a one signal

            Args:
            -----
            -message(str): The one line of a simple Nmea Message. 

            """
            if not message.startswith('$') or '*' not in message:
                    logger.error(f"Invalid NMEA format {message}")
                    return
            else:
                data, checksum = message.split("*")
                calculated_checksum=calculate_checksum(data)
                if int(checksum, base=16) != calculated_checksum:
                    logger.error(f"Checksum mismatch. Received: {int(checksum, base=16)}, Calculated: {calculated_checksum}")
                    return None
                try:
                    fields= data.split(",")
                    talker_id = fields[0][1:3]
                    sentence_type= fields[0][3:]
                    rot_value= float(fields[1])
                    status= fields[2]
                    if status == "V" and self.send_valid_data:
                        return
                    datapoint = Datapoint_Nmea(talker_id, sentence_type, rot_value, status, checksum,self.previous_datapoint)
                    self.previous_datapoint=datapoint
                    return datapoint
                except IndexError:
                    logger.error("Error in index the nmea sentence")
                except Exception as e:
                    logger.error(f"Failed to parse nmea message {e}")     

        datapoints=[]
        for message in messages:
            dp=parse_signal(message)
            if dp:
                datapoints.append(dp)
        return datapoints
  

    def get_ROT_readings(self)->None:
        """
        A main entry point to get ROT readings from the websocket.

        """
        messages=self.read_nmea_data()
        if messages:
            return self.parse_data(messages)
        



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
    def __str__(self):
        date = self.timestamp.strftime("%Y-%m-%d")
        time = self.timestamp.strftime("%H:%M")
        status = "Valid" if self.status != "V" else "Invalid"
        return f"{self.rot_value}, {status}, {date} at {time} UTC"