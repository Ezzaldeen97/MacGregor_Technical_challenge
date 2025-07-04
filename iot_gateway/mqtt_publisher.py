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
    def __init__(self, host: str, port: int, username: str, password: str):
        date_str = datetime.now().strftime("%d%m%y")
        client_id = f"ows-challenge-{date_str}"
        self.client = paho.Client(client_id=client_id, protocol=paho.MQTTv5)
        self.client.username_pw_set(username, password)
        self.client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish
        try:
            self.client.connect(host, port)
            logger.info("Mqtt publisher it connected")
            iot_logger.info("Mqtt publisher it connected")
        except Exception as e :
            logger.error("Mqtt could not connect.")
            sys.exit(1)
    def on_connect(self,reason_code):
        logger.info(f"[Connected] Reason code: {reason_code}")

    def on_publish(self, mid, ):
        logger.info(f"[Published] mid: {mid}")

    def on_subscribe(self, mid, granted_qos,):
        logger.info(f"[Subscribed] mid: {mid}, QoS: {granted_qos}")
    def get_topic(self, message):
        if isinstance(message, Datapoint_Nmea) :
            return measurements_mapping.measurements_mapping.get("ROT")
        else:
            return measurements_mapping.measurements_mapping.get(message.sensor)

    def on_message(self, msg):
        logger.info(f"[Message] Topic: {msg.topic}, QoS: {msg.qos}, Payload: {msg.payload.decode()}")

    def publish(self, message: str, qos: int = 1):
        self.topic = self.get_topic(message)
        if message.send_point:
            logger.info(f"[Publishing] {message} to topic '{self.topic}'")
            self.client.publish(self.topic, payload=message.__str__(), qos=qos)
            self.client.will_set(self.topic, payload="connection lost", qos=1)
        else:
            logger.info(f"Message {message} with topic {self.topic} cant be published as it didnt change or its invalid")

    def start(self):
        logger.info("Starting MQTT loop")
        self.client.loop_forever()
