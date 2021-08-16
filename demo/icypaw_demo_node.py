# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Demonstrate the basics of creating a node and device."""
import logging
import sys
import argparse
from random import randint

from icypaw.types import Int64, String, UInt64
from icypaw import Metric, icpw_timer, icpw_trigger, icpw_command, ServerNode, ServerDevice, ServerEngine

class DemoDevice(ServerDevice):

    # The upper limit on the random number produced by this device.
    limit = Metric(Int64, initial=10, properties={'Low': 0, 'High': 1000})

    # The last random number returned.
    random_value = Metric(UInt64, read_only=True)

    @limit.net_hook
    def limit(self, new_limit):
        if new_limit < 0:
            return self.limit
        return new_limit

    @icpw_timer(2)
    def update_random_value(self):
        self.random_value = randint(0, self.limit)
        print(f"Metric random_value updated to {self.random_value}")

    @icpw_command
    def ping(self):
        print("ping!")

    @icpw_command(use_template=False)
    def hello(self):
        print("Hello, World!")

class DemoNode(ServerNode):

    def __init__(self, *args, **kwargs):
        kwargs['device_classes'] = [DemoDevice]
        super().__init__(*args, **kwargs)
        self._devices = {}

    @icpw_command
    def debug_print(self, msg: String):
        """Print some message to the local console for debugging purposes."""
        print(msg)

    @icpw_trigger
    def launch_device(self, device_id):
        """Create a new demo device with the given id."""
        print("Creating new device")
        if device_id not in self._devices:
            device = DemoDevice(self.group_id, self.edge_node_id, device_id)
            self._devices[device_id] = device
        self.icpw_register_device(device)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-b', '--address', type=str, help="MQTT broker address", default='127.0.0.1')
    parser.add_argument('-p', '--port', type=int, help="MQTT broker port", default=1883)
    parser.add_argument('-v', '--verbose', action='count', help="Logging verbosity level", default=0)
    args = parser.parse_args()

    log_level = {
        0: logging.WARN,
        1: logging.INFO,
        2: logging.DEBUG
    }.get(args.verbose, logging.DEBUG)
    logging.basicConfig(
        level=log_level,
        format="[%(asctime)s] %(levelname)8s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout
    )

    node = DemoNode('demo_group', 'rand')
    engine = ServerEngine(node)
    print(f"Connecting to {args.address}:{args.port}")
    with engine.connect(args.address, port=args.port):
        print("Launching device")
        node.launch_device('rand0')
        try:
            while True:
                engine.process_events()
                engine.wait_on_event()
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    main()
