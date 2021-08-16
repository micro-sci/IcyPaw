#! /usr/bin/env python
# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""A small program to send an NCMD or DCMD to an EoN node or device,
respectively. This creates the raw GRPC packets, rather than using the
preferred Client interface."""
import argparse

from paho.mqtt.publish import single
from icypaw.tahu_interface import DataType, set_metric_value, new_payload

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('group_id', type=str, help="group ID of device")
parser.add_argument('node_id', type=str, help="EoN node ID of device")
parser.add_argument('metric', type=str, help="metric to update")
parser.add_argument('new_value', type=str, help="value to set")
parser.add_argument('-d', '--device_id', type=str, help="device ID of device (optional)")
parser.add_argument('-t', '--type', type=str, help="Tahu DataType of metric", default='String')
args = parser.parse_args()

if args.device_id:
    topic = f'spBv1.0/{args.group_id}/DCMD/{args.node_id}/{args.device_id}'
else:
    topic = f'spBv1.0/{args.group_id}/NCMD/{args.node_id}'

m_type = DataType[args.type]
def convert(str_value, datatype):
    if datatype.value in range(1, 9):
        return int(str_value)
    elif datatype.value in (9, 10):
        return float(str_value)
    elif datatype == DataType.Boolean:
        return bool(str_value)
    elif datatype.value in (12, 14):
        return str_value
    else:
        raise ValueError(f"Unsupported type: {datatype}")

payload = new_payload()
metric = payload.metrics.add()
metric.name = args.metric
metric.is_transient = True
metric.datatype = m_type.value
value = convert(args.new_value, m_type)
set_metric_value(value, m_type, metric)

payload_bytes = payload.SerializeToString()

print(f"Sending message on {topic}: {payload}")
single(topic, payload=payload_bytes)
