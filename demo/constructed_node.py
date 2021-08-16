# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Make a node that is constructed at runtime and therefore has
metrics not known at time of writing."""

import argparse
import logging
import sys

from icypaw.types import Double
from icypaw import Metric, icpw_timer, icpw_trigger, icpw_command, ServerNode, ServerDevice, ServerEngine



def make_demo_node_class():
    name = "DemoNode"
    metrics = {
        'foo': Metric(Double, initial=3.14)
    }
    return make_node_class(name, metrics)


def make_node_class(name, metric_dict):
    """Create a class with runtime-defined metrics"""
    return type(name, (ServerNode,), metric_dict)


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

    NodeClass = make_demo_node_class()
    node = NodeClass('demo_group', 'my_demo')
    engine = ServerEngine(node)
    print(f"Connecting to {args.address}:{args.port}")
    with engine.connect(args.address, port=args.port):
        print("Connected")
        try:
            while True:
                engine.process_events()
                engine.wait_on_event()
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    main()
