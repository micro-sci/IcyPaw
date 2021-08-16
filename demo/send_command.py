# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Demonstrate calling a command using the raw GRPC packets, not the
preferred Client interface."""

from paho.mqtt.publish import single

from icypaw.tahu_interface import new_payload, DataType

topic = 'spBv1.0/demo_group/NCMD/rand'
payload = new_payload()
metric = payload.metrics.add()
metric.name = 'debug_print'.encode()
metric.datatype = DataType.Template.value
metric.is_transient = True
metric.template_value.template_ref = 'debug_print'.encode()
arg_metric = metric.template_value.metrics.add()
arg_metric.name = 'msg'.encode()
arg_metric.string_value = "Hello, World".encode()
arg_metric.datatype = DataType.String.value
payload_bytes = payload.SerializeToString()

single(topic, payload=payload_bytes)
