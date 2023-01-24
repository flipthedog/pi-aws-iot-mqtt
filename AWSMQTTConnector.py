from awscrt import mqtt, auth
from awsiot import mqtt_connection_builder

import sys
import threading
import time
from uuid import uuid4
import json

import os, sys
import numpy as np

# from aws_helper.py @ https://github.com/aws/aws-iot-device-sdk-python-v2

class AWSMQTTConnector():

    def __init__(self,
        endpoint,
        ca_file = "root-CA.crt",
        cert = "pi.cert.pem",
        key = "pi.private.key",
        client_id = "basicPubSub",
        topic = "home/logger/atm_data",
        region = "us-east-2",
        count = 0):

        current_cwd = "/home/pi/Projects/atm_logger"

        self.endpoint = endpoint
        self.ca_file = current_cwd + "/auth/" + ca_file
        self.cert = current_cwd + "/auth/" + cert
        self.key = current_cwd + "/auth/" + key
        self.client_id = client_id
        self.topic = topic
        self.region = region
        self.sent_count = count
        self.received_count = 0

        credentials_provider = auth.AwsCredentialsProvider.new_default_chain()

        self.received_all_event = threading.Event()

        self.mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=endpoint,
            port=443,
            pri_key_filepath=self.key,
            cert_filepath=self.cert,
            ca_filepath=self.ca_file,
            on_connection_interrupted=self.on_connection_interrupted,
            on_connection_resumed=self.on_connection_resumed,
            client_id=client_id,
            clean_session=False,
            keep_alive_secs=30,)
        
        self.connect_future = self.mqtt_connection.connect()

        try:
            self.connect_future.result()
        except e:
            print("Connection failed")
            print(e)

    def on_connection_interrupted(self, connection, error, **kwargs):
        print("Connection interrupted. error: {}".format(error))

    # Callback when an interrupted connection is re-established.
    def on_connection_resumed(self, connection, return_code, session_present, **kwargs):
        print("Connection resumed. return_code: {} session_present: {}".format(
            return_code, session_present))

        if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
            print("Session did not persist. Resubscribing to existing topics...")
            resubscribe_future, _ = connection.resubscribe_existing_topics()

            # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
            # evaluate result with a callback instead.
            resubscribe_future.add_done_callback(on_resubscribe_complete)
    
    def on_resubscribe_complete(self, resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))
    
    # Callback when the subscribed topic receives a message
    def on_message_received(self, topic, payload, dup, qos, retain, **kwargs):
        print("Received message from topic '{}': {}".format(topic, payload))

        self.received_count += 1
        self.received_all_event.set()

    
    def publish_message(self, msg, topic=None):
        
        if topic is None:
            topic = self.topic
        
        message_json = json.dumps(msg)

        print("Publishing Message", message_json)

        self.mqtt_connection.publish(
            topic=topic,
            payload=message_json,
            qos=mqtt.QoS.AT_LEAST_ONCE)

        # self.received_all_event.wait()

        # print("{} message(s) received.".format(
        #     self.received_count))

    def start_debug_loop(self):

        count = 0

        while True:

            count += 1

            self.publish_message(dict(id=count,
                                 datetime="22-10-27 00:59:25",
                                 IAQ=2,
                                 temperature=3,
                                 humidity=4,
                                 pressure=5,
                                 resistance=6,
                                 eCO2=7,
                                 VOC=8,
                                 CPUt=9))

            print(f"Count: {count}")
            time.sleep(1)
        
    def __exit__(self):
        disconnect_future = self.mqtt_connection.disconnect()
        disconnect_future.result()
    
if __name__ == '__main__':

    # debug
    conn = AWSMQTTConnector()
    conn.start_debug_loop()