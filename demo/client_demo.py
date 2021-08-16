# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Test out the functions of the client interface."""

import argparse
import time
import sys

from icypaw.client import IcypawClient, Event

def main():
    args = get_parser().parse_args()
    address = args.address
    client = IcypawClient(address, connect=True)

    node_name = f'{args.group}/{args.node}/'
    device_name = f'{args.group}/{args.node}/{args.device}'
    print(f"Using node_name={node_name}")
    print(f"Using device_name={device_name}")
    if args.watch:
        if '#' in node_name:
            print("When using --watch, please provide a group and node")
        if args.metric is None:
            print("When using --watch, please provide a metric")
        client.watch(Event.ONLINE | Event.OFFLINE | Event.METRIC_UPDATE,
                     [node_name, device_name])
    else:
        client.monitor(on_event, Event.ONLINE | Event.OFFLINE | Event.METRIC_UPDATE,
                       [node_name, device_name])

    try:
        while True:
            time.sleep(1)
            sys.stdout.flush()
            if args.watch:
                try:
                    nmetric = client.get_endpoint_metric(node_name, args.metric)
                except ValueError:
                    # This could be intentional if the metric is in the device
                    print(f"Node does not have metric {args.metric}")
                else:
                    print(f"{node_name}: {nmetric.name}: {nmetric.value}")
                if '#' not in device_name:
                    try:
                        dmetric = client.get_endpoint_metric(
                            device_name, args.metric)
                    except ValueError:
                        print(f"Device does not have metric {args.metric}")
                    else:
                        print(f"{device_name}: {dmetric.name}: {dmetric.value}")
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()

    # for endpoint in endpoints:
    #     metrics = client.list_metrics(endpoint)
    #     for metric_name, metric_value in metrics.items():
    #         print(f" {metric_name}: {metric_value.value}")

def get_parser():
    parser = argparse.ArgumentParser(description="Exercise features of the Icypaw Client")
    parser.add_argument('--address', help="The address of the broker",
                        default='127.0.0.1')
    parser.add_argument('--group', help='The name of the group', default='group0')
    parser.add_argument('--node', help='The name of the node to monitor',
                        default='#')
    parser.add_argument('--device', help='The name of the device to monitor',
                        default='#')
    parser.add_argument('--watch', help='Use the watch instead of monitor command',
                        action='store_true')
    parser.add_argument('--metric', help='If in watch mode, the metric to repeatedly display')
    return parser

def on_event(event, endpoint, metrics):
    print(f"Event {event}:")
    print(f" Endpoint {endpoint}")
    for metric in metrics:
        print(f" Metric {metric.name}")
        print(f"  value {metric.value}")
    print()

def display_endpoints(endpoints, client):
    for endpoint_name, endpoint in endpoints.items():
        print(endpoint_name)
        for metric_name, metric_value in endpoint.metrics.items():
            print(f" {metric_name}: {metric_value.value}")
        for cmd_name, cmd_value in endpoint.commands.items():
            print(f" {cmd_name}: <command>")
        print(f" Endpoint is {client.get_endpoint_state(endpoint_name).name}")

if __name__ == '__main__':
    main()
