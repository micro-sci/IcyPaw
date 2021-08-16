# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""This demo shows how to use arrays as metrics."""
import logging
import sys
import argparse
import time
from random import randint

from icypaw.types import Int64, String, UInt64, Array, Boolean
from icypaw import Metric, icpw_timer, icpw_trigger, icpw_command, ServerNode, ServerDevice, ServerEngine

class DemoNode(ServerNode):

    simple_values = Metric(Array[Int64])

    complex_values = Metric(Array[(Int64, String, Boolean)])

    @simple_values.net_hook
    def simple_values(self, values):
        print(values.to_python())
        sys.stdout.flush()
        return values

    @complex_values.net_hook
    def complex_values(self, values):
        print(values.to_python())
        sys.stdout.flush()
        return values

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

    node = DemoNode('demo_group', 'demo_node')
    engine = ServerEngine(node)
    print(f"Connecting to {args.address}:{args.port}")
    with engine.connect(args.address, port=args.port):
        try:
            while True:
                engine.process_events()
                engine.wait_on_event(time.time() + 1)
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    main()
