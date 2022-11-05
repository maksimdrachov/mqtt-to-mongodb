# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import time as t
import json

from awscrt import mqtt
import sys
import threading
import time
from uuid import uuid4
import json

import pymongo

# Define ENDPOINT, CLIENT_ID, PATH_TO_CERTIFICATE, PATH_TO_PRIVATE_KEY, PATH_TO_AMAZON_ROOT_CA_1, MESSAGE, TOPIC, and RANGE
ENDPOINT = "a2sbt380oaoybi-ats.iot.eu-central-1.amazonaws.com"
CLIENT_ID = "1cb3c75547b39eb2d1017b95ba5677d2f5011a00daf27a863133fcfad0c3e0d6"
PATH_TO_CERTIFICATE = "certificates-subscribe/1cb3c75547b39eb2d1017b95ba5677d2f5011a00daf27a863133fcfad0c3e0d6-certificate.pem.crt"
PATH_TO_PRIVATE_KEY = "certificates-subscribe/1cb3c75547b39eb2d1017b95ba5677d2f5011a00daf27a863133fcfad0c3e0d6-private.pem.key"
PATH_TO_AMAZON_ROOT_CA_1 = "certificates-subscribe/AmazonRootCA1.pem"
MESSAGE = "Hello World"
TOPIC = "test/testing"
RANGE = 3

count = 3

# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    mydict = {"message": str(payload)}
    db_id = mycol.insert_one(mydict)
    print("Added entry to database: {}".format(db_id))
    global received_count
    received_count += 1
    if received_count == count:
        received_all_event.set()

received_count = 0
received_all_event = threading.Event()

# Setup MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["demo"]
mycol = mydb["mqtt"]

# Spin up resources
event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=PATH_TO_CERTIFICATE,
            pri_key_filepath=PATH_TO_PRIVATE_KEY,
            client_bootstrap=client_bootstrap,
            ca_filepath=PATH_TO_AMAZON_ROOT_CA_1,
            client_id=CLIENT_ID,
            clean_session=False,
            keep_alive_secs=6
            )
print("Connecting to {} with client ID '{}'...".format(
        ENDPOINT, CLIENT_ID))
# Make the connect() call
connect_future = mqtt_connection.connect()
# Future.result() waits until a result is available
connect_future.result()
print("Connected!")
# Publish message to server desired number of times.
print("Subscribing to topic {}".format(TOPIC))
subscribe_future, packet_id = mqtt_connection.subscribe(
    topic=TOPIC, 
    qos=mqtt.QoS.AT_LEAST_ONCE,
    callback=on_message_received)

subscribe_result = subscribe_future.result()
print("Subscribed with {}".format(str(subscribe_result['qos'])))

# Wait for all messages to be received
if not received_all_event.is_set():
    print("Waiting for all messages to be received...")

received_all_event.wait()
print("{} message(s) received.".format(received_count))

# Disconnect
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()
print("Disconnected!")