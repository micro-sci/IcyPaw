# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Demonstrate setting a metric using the raw GRPC packets, not the
preferred Client interface."""

from paho.mqtt.publish import single

from icypaw.tahu_interface import new_payload, DataType

topic = 'spBv1.0/demo_group/DCMD/rand/rand0'
payload = new_payload()
metric = payload.metrics.add()
metric.name = 'limit'
#metric.long_value = 18446744073709551615
metric.long_value = 100
metric.datatype = DataType.Int64.value
metric.is_transient = True
payload_bytes = payload.SerializeToString()

single(topic, payload=payload_bytes)
