# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""A small program to monitor all Icypaw communication over MQTT."""
import argparse

from icypaw.proto.sparkplug_b_pb2 import Payload
import paho.mqtt.client as mqtt

import logging
import sys

_DEFAULT_TOPIC = 'spBv1.0/#'
_DEFAULT_BROKER = '127.0.0.1'
_DEFAULT_PORT = 1883

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('-t', '--topic', type=str, help="MQTT topic to monitor", default=_DEFAULT_TOPIC)
parser.add_argument('-b', '--broker', type=str, help="MQTT broker address", default=_DEFAULT_BROKER)
parser.add_argument('-p', '--port', type=int, help="MQTT broker port", default=_DEFAULT_PORT)
parser.add_argument('-v', '--verbose', action='count', help="logging verbosity")
args = parser.parse_args()

log_levels = {
    0: logging.INFO,
    1: logging.DEBUG
}

logging.basicConfig(
    level=log_levels[args.verbose] if args.verbose in log_levels else logging.INFO,
    format="\u001b[31m[%(asctime)s] %(levelname)8s [%(name)s.%(funcName)s:%(lineno)d]\u001b[0m %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout
)

log = logging.getLogger(__name__)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    log.info("Connected with result code " + str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(args.topic)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    try:
        payload = Payload()
        payload.ParseFromString(msg.payload)
        log.info(f"topic: {msg.topic}")
        log.info("\u001b[32m*** Start Payload ***\u001b[0m")
        log.info(f"PAYLOAD:\n{payload}")
        log.info("\u001b[32m***  End Payload  ***\u001b[0m")
    except Exception as exc:
        log.error(exc)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(args.broker, port=args.port)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
