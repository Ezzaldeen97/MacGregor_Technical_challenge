import paho.mqtt.client as paho
from paho import mqtt
from datetime import datetime
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from utils.logger import get_logger
from .modubs_tcp import Datapoint
from .nmea_client import Datapoint_Nmea
from configs import config, measurements_mapping

logger = get_logger("mqtt_logger", file_name='logs/mqtt_publisher.log')
iot_logger = get_logger("iot_gatway_logger", file_name='logs/iot_gatway.log')


class MQTTPublisher:
    """
    MQTT Publisher class that manages MQTT client conenctions, 
    publishes messages to different topics
    """


    def __init__(self, host: str, port: int, username: str, password: str):
        """
        Initialize the MQTT Publisher client object.

        Args:
        -----
        host(str): MQTT broker hostname
        port(int): MQTT broker port number
        username(str): Username for MQTT broker authentication
        password(str): Password for MQTT broker authentication.



        """
        
        date_str = datetime.now().strftime("%d%m%y")
        client_id = f"ows-challenge-{date_str}"
        self.client = paho.Client(client_id=client_id, protocol=paho.MQTTv5)
        self.client.username_pw_set(username, password) #NOTE: username/password are not required but essential for HiveMQ
        self.client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        try:
            self.client.connect(host, port)
            logger.info("Mqtt publisher it connected")
            iot_logger.info("Mqtt publisher it connected")
        except Exception as e :
            logger.error("Mqtt could not connect.")
            sys.exit(1)
    def on_connect(self,reason_code):
        logger.info(f"Connected Reason code: {reason_code}")

    def on_publish(self, mid, ):
        logger.info(f"Published mid: {mid}")

    def get_topic(self, message:Datapoint|Datapoint_Nmea)->str:
        """
        Determine MQTT topic based on sensor datapoint object
        
        Args:
        ----
        message(Datapoint|Datapoint_Nmea): Message object to publish
        
        Rerunts:
        --------
        str: The topic string of the message
        """
        if isinstance(message, Datapoint_Nmea) :
            return measurements_mapping.measurements_mapping.get("ROT")
        else:
            return measurements_mapping.measurements_mapping.get(message.sensor)

    def publish(self, message: str, qos: int = 1):
        """
        Publish a message to the MQTT broker on appropriate topic

        Args:
        -----
        message(str): The message to publish

        qos(int, optional): Quality of service lever (deafult is 1 ) -> To fullfill the requirement that at least the message needs to be send once
        """
        
        
        self.topic = self.get_topic(message)
        if message.send_point:
            logger.info(f"[Publishing] {message} to topic '{self.topic}'")
            self.client.publish(self.topic, payload=message.__str__(), qos=qos)
            self.client.will_set(self.topic, payload="connection lost", qos=1)
        else:
            logger.info(f"Message {message} with topic {self.topic} cant be published as it didnt change or its invalid")

    def start(self):
        """
        Start the MQTT network loop.
        
        """
        logger.info("Starting MQTT loop")
        self.client.loop_forever()
