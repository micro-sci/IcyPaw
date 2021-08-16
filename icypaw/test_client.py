# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

from unittest import TestCase, main, mock

import paho.mqtt.client as mqtt

from .client import IcypawClient, Event
from .tahu_interface import new_payload, new_metric, add_metrics_to_payload, DataType, iterable_to_propertyset
from .exceptions import IcypawException
from . import conventions

class MockClientMixin:
    """Test mix-in that provides an IcypawClient with a mocked underlying MQTT
    client and helper methods for creating Tahu payloads"""

    def setUp(self):
        self.address = 'address'
        with mock.patch.object(mqtt, 'Client'):
            self.client = IcypawClient(self.address, connect=True)

        self.default_scalar_metric_name = 'x'

        self.alias_map = {}

    ##
    # Helper methods
    #

    def trigger_message(self, tahu_payload, method, group_id, node_id, device_id=None):
        """Simulate sending a message to the client, as though from a remote
        node or device."""
        if device_id is not None:
            topic = f'spBv1.0/{group_id}/{method}/{node_id}/{device_id}'
        else:
            topic = f'spBv1.0/{group_id}/{method}/{node_id}'

        message = mock.MagicMock(payload=tahu_payload.SerializeToString(),
                                 topic=topic)
        self.client._on_message(None, None, message)

    def make_nbirth(self, with_null_scalar=False):
        """Create an NBIRTH message suitable for use as the first argument to
        trigger_message."""
        payload = new_payload()

        metrics = []

        scalar_kwargs = {'value': None} if with_null_scalar else {}

        metrics.append(self.make_bdseq_metric())
        metrics.append(self.make_scalar_metric(**scalar_kwargs))
        metrics.append(self.make_template_metric())
        metrics.append(self.make_template_definition())
        metrics.append(self.make_dummy_template_definition())
        metrics.append(self.make_array_metric())

        add_metrics_to_payload(metrics, payload)

        return payload

    def make_dbirth(self):
        """Create a DBIRTH message suitable for use as the first argument to
        trigger_message."""
        payload = new_payload()

        metrics = []

        metrics.append(self.make_scalar_metric())
        metrics.append(self.make_template_metric())

        add_metrics_to_payload(metrics, payload)

        return payload

    def make_ndata(self, x_value=0, is_historical=False, is_transient=False):
        """Create an NDATA message suitable for use as the first argument to
        trigger_message."""
        payload = new_payload()
        metrics = [self.make_scalar_metric(value=x_value, is_historical=is_historical,
                                           is_transient=is_transient)]
        add_metrics_to_payload(metrics, payload)
        return payload

    def make_ddata(self, x_value=0, is_historical=False, is_transient=False):
        """Create a DDATA message suitable for use as the first argument to
        trigger_message."""
        payload = new_payload()

        metrics = [self.make_scalar_metric(value=x_value, is_historical=is_historical,
                                           is_transient=is_transient)]
        add_metrics_to_payload(metrics, payload)
        return payload

    def make_nbirth_with_command(self):
        payload = new_payload()
        metrics = [self.make_command_metric(),
                   self.make_command_definition()]
        add_metrics_to_payload(metrics, payload)
        return payload

    def make_ndeath(self):
        payload = new_payload()
        add_metrics_to_payload([self.make_bdseq_metric()], payload)
        return payload

    def make_bdseq_metric(self, value=0):
        metric = new_metric()
        metric.name = conventions.BDSEQ.encode()
        metric.datatype = DataType.Int64.value
        metric.long_value = value
        return metric

    def make_scalar_metric(self, name=None, value=44,
                           is_historical=False, is_transient=False, properties={}):
        name = name or self.default_scalar_metric_name
        metric = new_metric()
        metric.name = name.encode()
        metric.datatype = DataType.Int64.value
        if value is not None:
            metric.long_value = value
        else:
            metric.is_null = True
        metric.alias = self.get_alias(name)
        if is_historical:
            metric.is_historical = True
        if is_transient:
            metric.is_transient = True
        iterable_to_propertyset(properties, ps=metric.properties)
        return metric

    def make_template_metric(self, name="foo", value={"a": 5, "b": "hello"}):
        metric = new_metric()
        metric.name = name.encode()
        metric.alias = self.get_alias(name)
        metric.datatype = DataType.Template.value
        metric.template_value.template_ref = "foo_t".encode()
        a_metric = metric.template_value.metrics.add()
        a_metric.datatype = DataType.Int64.value
        a_metric.long_value = value['a']
        a_metric.name = 'a'.encode()
        b_metric = metric.template_value.metrics.add()
        b_metric.datatype = DataType.String.value
        b_metric.string_value = value['b'].encode()
        b_metric.name = 'b'.encode()
        return metric

    def make_template_definition(self, name="foo_t"):
        metric = new_metric()
        metric.name = conventions.make_template_definition(name).encode()
        metric.datatype = DataType.Template.value
        metric.template_value.is_definition = True
        a_metric = metric.template_value.metrics.add()
        a_metric.datatype = DataType.Int64.value
        a_metric.name = 'a'.encode()
        b_metric = metric.template_value.metrics.add()
        b_metric.datatype = DataType.String.value
        b_metric.name = 'b'.encode()
        return metric

    def make_dummy_template_definition(self, name="bar_t"):
        metric = new_metric()
        metric.name = conventions.make_template_definition(name).encode()
        metric.datatype = DataType.Template.value
        metric.template_value.is_definition = True
        return metric

    def make_array_metric(self, name='arr', values=[1, 2, 3]):
        metric = new_metric()
        metric.name = name.encode()
        metric.alias = self.get_alias(name)
        metric.datatype = DataType.DataSet.value
        metric.dataset_value.num_of_columns = 1
        metric.dataset_value.types.append(DataType.Int64.value)
        row = metric.dataset_value.rows.add()
        for val in values:
            elem = row.elements.add()
            elem.long_value = val
        return metric

    def make_command_metric(self, name='do_work'):
        metric = new_metric()
        metric.name = conventions.make_command(name).encode()
        metric.alias = self.get_alias(name)
        metric.datatype = DataType.Template.value
        metric.template_value.template_ref = name.encode()
        bool_metric = metric.template_value.metrics.add()
        bool_metric.name = "value".encode()
        bool_metric.datatype = DataType.Boolean.value
        bool_metric.boolean_value = False
        return metric

    def make_command_definition(self, name='do_work'):
        metric = new_metric()
        metric.name = conventions.make_template_definition(name).encode()
        metric.alias = self.get_alias(name)
        metric.datatype = DataType.Template.value
        metric.template_value.is_definition = True
        bool_metric = metric.template_value.metrics.add()
        bool_metric.name = "value".encode()
        bool_metric.datatype = DataType.Boolean.value
        bool_metric.boolean_value = False
        return metric

    def get_alias(self, name):
        """Return the assigned alias for name, creating one if it does not
        exist."""
        if name not in self.alias_map:
            self.alias_map[name] = len(self.alias_map)
        return self.alias_map[name]


class WaitConnectTester(TestCase):
    def test_wait_connect(self):
        """Test that waiting for a connection doesn't lock up."""
        address = 'address'
        with mock.patch.object(mqtt, 'Client'):
            client = IcypawClient(address)
            def connect_side_effect(_address, _port):
                client._client.on_connect(client._client, None, None, None)
                return None
            client._client.connect.side_effect = connect_side_effect
        client.connect(wait=True)
        self.assertTrue(True)


class ReceivedMessageTester(MockClientMixin, TestCase):

    def test_create_node(self):
        """Test creating a node from its birth certificate"""
        nbirth = self.make_nbirth()
        group_id = 'group0'
        node_id = 'node0'
        device_id = None
        self.trigger_message(nbirth, 'NBIRTH', group_id, node_id)

        endpoints = self.client.list_endpoints()
        self.assertEqual(1, len(endpoints))
        endpoint, = endpoints
        self.assertEqual(endpoint.name.group_id, group_id)
        self.assertEqual(endpoint.name.edge_node_id, node_id)
        self.assertEqual(endpoint.name.device_id, device_id)
        self.assertTrue(endpoint.is_online)
        self.assertIn('x', endpoint.metrics)
        self.assertIn('foo', endpoint.metrics)
        self.assertNotIn('bdSeq', endpoint.metrics)
        self.assertIn('arr', endpoint.metrics)

    def test_create_device(self):
        """Test creating a device from its birth certificate."""
        nbirth = self.make_nbirth()
        group_id = 'group0'
        node_id = 'node0'
        device_id = 'device0'
        self.trigger_message(nbirth, 'NBIRTH', group_id, node_id)

        dbirth = self.make_dbirth()
        self.trigger_message(dbirth, 'DBIRTH', group_id, node_id, device_id)

        endpoints = self.client.list_endpoints()
        self.assertEqual(2, len(endpoints))
        endpoint, = [ep for ep in endpoints if ep.is_device]
        self.assertEqual(endpoint.name.group_id, group_id)
        self.assertEqual(endpoint.name.edge_node_id, node_id)
        self.assertEqual(endpoint.name.device_id, device_id)
        self.assertTrue(endpoint.is_online)
        self.assertIn('x', endpoint.metrics)
        self.assertIn('foo', endpoint.metrics)

    def test_update_node(self):
        """Test updating a node from a NDATA message."""
        nbirth = self.make_nbirth()
        group_id = 'group0'
        node_id = 'node0'
        device_id = None
        self.trigger_message(nbirth, 'NBIRTH', group_id, node_id)

        endpoints = self.client.list_endpoints()
        self.assertEqual(1, len(endpoints))
        endpoint, = endpoints
        self.assertEqual(endpoint.name.group_id, group_id)
        self.assertEqual(endpoint.name.edge_node_id, node_id)
        self.assertEqual(endpoint.name.device_id, device_id)
        self.assertTrue(endpoint.is_online)
        self.assertIn('x', endpoint.metrics)
        self.assertEqual(endpoint.metrics['x'].value, 44)
        self.assertFalse(endpoint.metrics['x'].is_fresh)

        ndata = self.make_ndata(x_value=1234)
        self.trigger_message(ndata, 'NDATA', group_id, node_id)

        endpoint, = self.client.list_endpoints()
        self.assertEqual(endpoint.metrics['x'].value, 1234)
        self.assertTrue(endpoint.metrics['x'].is_fresh)

    def test_update_device(self):
        """Test updating a device from a DDATA message."""
        nbirth = self.make_nbirth()
        group_id = 'group0'
        node_id = 'node0'
        device_id = 'device0'
        self.trigger_message(nbirth, 'NBIRTH', group_id, node_id)

        dbirth = self.make_dbirth()
        self.trigger_message(dbirth, 'DBIRTH', group_id, node_id, device_id)

        endpoint, = [ep for ep in self.client.list_endpoints() if ep.is_device]
        self.assertEqual(endpoint.metrics['x'].value, 44)
        self.assertFalse(endpoint.metrics['x'].is_fresh)

        ddata = self.make_ddata(x_value=1234)
        self.trigger_message(ddata, 'DDATA', group_id, node_id, device_id)

        endpoint, = [ep for ep in self.client.list_endpoints() if ep.is_device]
        self.assertEqual(endpoint.metrics['x'].value, 1234)
        self.assertTrue(endpoint.metrics['x'].is_fresh)

    def test_monitor_birth(self):
        """Test monitoring for a birth event."""
        nbirth = self.make_nbirth()
        group_id = 'group0'
        node_id = 'node0'

        called = [False]
        def on_birth(event, endpoint, metrics):
            called[0] = True
            self.assertEqual(event, Event.ONLINE)
            self.assertEqual(f'{group_id}/{node_id}/', str(endpoint))
            self.assertGreater(len(metrics), 0)
        self.client.monitor(on_birth, Event.ONLINE, ['*/*/'])

        self.trigger_message(nbirth, 'NBIRTH', group_id, node_id)

        self.assertTrue(called[0])

    def test_monitor_data(self):
        """Test monitoring for a data event."""
        nbirth = self.make_nbirth()
        group_id = 'group0'
        node_id = 'node0'
        self.trigger_message(nbirth, 'NBIRTH', group_id, node_id)

        called = [False]
        def on_data(event, endpoint, metrics):
            called[0] = True
            self.assertEqual(event, Event.METRIC_UPDATE)
            self.assertEqual(f'{group_id}/{node_id}/', str(endpoint))
            self.assertEqual(1, len(metrics))
            self.assertEqual('x', metrics[0].name)
        self.client.monitor(on_data, Event.METRIC_UPDATE, ['*/*/'])

        ndata = self.make_ndata(x_value=1234)
        self.trigger_message(ndata, 'NDATA', group_id, node_id)

        self.assertTrue(called[0])

    def test_monitor_death(self):
        """Test monitoring for a death event."""
        group_id = 'group0'
        node_id = 'node0'

        self.trigger_message(self.make_nbirth(), 'NBIRTH', group_id, node_id)

        called = [False]
        def on_data(event, endpoint, metrics):
            called[0] = True
            self.assertEqual(event, Event.OFFLINE)
        self.client.monitor(on_data, Event.OFFLINE, ['*/*/'])

        self.trigger_message(self.make_ndeath(), 'NDEATH', group_id, node_id)

        self.assertTrue(called[0])

    def test_send_command(self):
        """Test sending a command to a node."""
        group_id = 'group0'
        node_id = 'node0'

        nbirth = self.make_nbirth_with_command()
        self.trigger_message(nbirth, 'NBIRTH', group_id, node_id)

        self.client._client.publish.reset_mock()
        self.client.call_command(f'{group_id}/{node_id}/', 'do_work')
        self.client._client.publish.assert_called()
        (topic, payload), _kwargs = self.client._client.publish.call_args
        self.assertEqual(topic, f'spBv1.0/{group_id}/NCMD/{node_id}')
        message_payload = new_payload()
        message_payload.ParseFromString(payload)
        self.assertEqual(1, len(message_payload.metrics))
        self.assertEqual(message_payload.metrics[0].name, conventions.make_command('do_work'))

    def test_watch_ndata(self):
        """Test if we can watch NDATA packets, i.e. update the client without
        calling a callback."""
        nbirth = self.make_nbirth()
        group_id = 'group0'
        node_id = 'node0'
        self.trigger_message(nbirth, 'NBIRTH', group_id, node_id)

        self.client.watch(Event.METRIC_UPDATE, ['*/*/'])

        x_value = 1234
        metric = self.client.get_endpoint_metric('group0/node0/',
                                                 self.default_scalar_metric_name)

        self.assertEqual(metric.name, self.default_scalar_metric_name)
        self.assertNotEqual(metric.value, x_value)
        self.assertFalse(metric.is_fresh)

        ndata = self.make_ndata(x_value=x_value)
        self.trigger_message(ndata, 'NDATA', group_id, node_id)
        metric = self.client.get_endpoint_metric('group0/node0/',
                                                 self.default_scalar_metric_name)

        self.assertEqual(metric.name, self.default_scalar_metric_name)
        self.assertEqual(metric.value, x_value)
        # A metric should always be considered fresh when it is
        # received from an NDATA message.
        self.assertTrue(metric.is_fresh)

    def test_watch_ddata(self):
        """Test if we can watch NDATA packets, i.e. update the client without
        calling a callback."""
        nbirth = self.make_nbirth()
        group_id = 'group0'
        node_id = 'node0'
        device_id = 'device0'
        self.trigger_message(nbirth, 'NBIRTH', group_id, node_id)

        dbirth = self.make_dbirth()
        self.trigger_message(dbirth, 'DBIRTH', group_id, node_id, device_id)

        self.client.watch(Event.METRIC_UPDATE, ['*/*/*'])

        x_value = 1234
        metric = self.client.get_endpoint_metric('group0/node0/device0',
                                                 self.default_scalar_metric_name)

        self.assertEqual(metric.name, self.default_scalar_metric_name)
        self.assertNotEqual(metric.value, x_value)
        self.assertFalse(metric.is_fresh)

        ddata = self.make_ddata(x_value=x_value)
        self.trigger_message(ddata, 'DDATA', group_id, node_id, device_id)
        metric = self.client.get_endpoint_metric('group0/node0/device0',
                                                 self.default_scalar_metric_name)

        self.assertEqual(metric.name, self.default_scalar_metric_name)
        self.assertEqual(metric.value, x_value)
        # A metric should always be considered fresh when it is
        # received from an NDATA message.
        self.assertTrue(metric.is_fresh)


class ClientRuntimeCheckingTester(MockClientMixin, TestCase):
    def setUp(self):
        super().setUp()

        # Create an endpoint fixture with some metrics with fun properties
        nbirth = self.make_nbirth()
        property_metrics = [
            self.make_scalar_metric(name="read-only", value=0, properties={'Writable': False}),
            self.make_scalar_metric(name="read-write", value=0, properties={'Writable': True}),
            self.make_scalar_metric(name="digit", value=1, properties={'Writable': True, 'Low': 1, 'High': 9})
        ]
        add_metrics_to_payload(property_metrics, nbirth)
        self._group_id = 'group0'
        self._node_id = 'node0'
        self.trigger_message(nbirth, 'NBIRTH', self._group_id, self._node_id)
        self.endpoint, = self.client.list_endpoints()

    def _assert_ncmd_issued(self, name, value):
        """Helper method to assert that an NCMD was issued setting the metric
        with the given name to the given value.
        """
        self.assertTrue(self.client._client.publish.called)

        # Get last call
        (topic, msg), _ = self.client._client.publish.call_args_list[-1]

        # Assert NCMD was published
        self.assertEqual(topic, f'spBv1.0/{self._group_id}/NCMD/{self._node_id}')

        payload = new_payload()
        payload.ParseFromString(msg)

        # Assert metric was set to given value
        alias = self.alias_map[name]
        for metric in payload.metrics:
            if (metric.HasField('name') and metric.name == name) \
               or (metric.HasField('alias') and metric.alias == alias):
                metric_value = getattr(metric, metric.WhichOneof('value'))
                self.assertEqual(value, metric_value)
                break
        else:
            self.fail(f'No metric {name} in NCMD payload')

    def test_set_writable_metric(self):
        """Test that client runtime checking permits setting read-write metrics"""
        self.client.set_endpoint_metric(self.endpoint.name, 'read-write', 100)
        self._assert_ncmd_issued('read-write', 100)

    def test_raise_on_nonexistent_node(self):
        """Check raising an exception when writing to a metric on a node that
        doesn't exist."""
        fake_name = "bad_group/bad_node"
        with self.assertRaises(IcypawException):
            self.client.set_endpoint_metric(fake_name, 'read-write', 100)

    def test_raise_on_set_read_only_metric(self):
        """Test that client runtime checking will raise when trying to set a read-only metric"""
        with self.assertRaises(IcypawException):
            self.client.set_endpoint_metric(self.endpoint.name, 'read-only', 100)
        self.assertFalse(self.client._client.publish.called)

        # can override with _force=True
        with self.assertWarns(RuntimeWarning):
            self.client.set_endpoint_metric(self.endpoint.name, 'read-only', 100, _force=True)
        self._assert_ncmd_issued('read-only', 100)

    def test_set_metric_read_only_default(self):
        """Test that client runtime checking treats metrics as read-only by default"""
        with self.assertRaises(IcypawException):
            self.client.set_endpoint_metric(self.endpoint.name, 'x', 100)
        self.assertFalse(self.client._client.publish.called)

    def test_raise_on_set_metric_offline(self):
        """Test that client runtime checking will raise when trying to set a metric on an offline endpoint"""
        self.trigger_message(self.make_ndeath(), 'NDEATH', self._group_id, self._node_id)

        with self.assertRaises(IcypawException):
            self.client.set_endpoint_metric(self.endpoint.name, 'read-write', 100)
        self.assertFalse(self.client._client.publish.called)

        # can override with _force=True
        with self.assertWarns(RuntimeWarning):
            self.client.set_endpoint_metric(self.endpoint.name, 'read-write', 100, _force=True)
        self._assert_ncmd_issued('read-write', 100)

    def test_set_metric_in_limits(self):
        """Test that client runtime checking permits setting a metric within specified limits"""
        self.client.set_endpoint_metric(self.endpoint.name, 'digit', 5)
        self._assert_ncmd_issued('digit', 5)

        # upper bound edge case
        self.client.set_endpoint_metric(self.endpoint.name, 'digit', 9)
        self._assert_ncmd_issued('digit', 9)

        # lower bound edge case
        self.client.set_endpoint_metric(self.endpoint.name, 'digit', 1)
        self._assert_ncmd_issued('digit', 1)

    def test_raise_on_set_metric_out_of_bounds(self):
        """Test that client runtime checking raises when setting a metric outside of specified limits"""
        # less than lower bound
        with self.assertRaises(IcypawException):
            self.client.set_endpoint_metric(self.endpoint.name, 'digit', 0)
        self.assertFalse(self.client._client.publish.called)

        # can override with _force=True
        with self.assertWarns(RuntimeWarning):
            self.client.set_endpoint_metric(self.endpoint.name, 'digit', 0, _force=True)
        self._assert_ncmd_issued('digit', 0)

        self.client._client.publish.reset_mock()

        # greater than upper bound
        with self.assertRaises(IcypawException):
            self.client.set_endpoint_metric(self.endpoint.name, 'digit', 10)
        self.assertFalse(self.client._client.publish.called)

        with self.assertWarns(RuntimeWarning):
            self.client.set_endpoint_metric(self.endpoint.name, 'digit', 10, _force=True)
        self._assert_ncmd_issued('digit', 10)

        self.client._client.publish.reset_mock()

    def test_historical_metric(self):
        """Test receiving a metric with is_historical True."""
        group_id = 'group0'
        node_id = 'node0'

        self.trigger_message(self.make_ndata(is_historical=True), 'NDATA', group_id, node_id)
        metric = self.client.get_endpoint_metric(f'{group_id}/{node_id}/',
                                                 self.default_scalar_metric_name)
        self.assertTrue(metric.is_historical)
        self.assertFalse(metric.is_transient)
        self.assertFalse(metric.is_null)

    def test_transient_metric(self):
        """Test receiving a metric with is_transient True."""
        group_id = 'group0'
        node_id = 'node0'

        self.trigger_message(self.make_ndata(is_transient=True), 'NDATA', group_id, node_id)
        metric = self.client.get_endpoint_metric(f'{group_id}/{node_id}/',
                                                 self.default_scalar_metric_name)
        self.assertFalse(metric.is_historical)
        self.assertTrue(metric.is_transient)
        self.assertFalse(metric.is_null)

    def test_null_metric(self):
        """Test receiving a metric with is_null True."""
        group_id = 'group0'
        node_id = 'node0'

        self.trigger_message(self.make_ndata(x_value=None), 'NDATA', group_id, node_id)
        metric = self.client.get_endpoint_metric(f'{group_id}/{node_id}/',
                                                 self.default_scalar_metric_name)
        self.assertFalse(metric.is_historical)
        self.assertFalse(metric.is_transient)
        self.assertTrue(metric.is_null)

    def test_type_of_null_metric_from_nbirth(self):
        """Test retrieving the type of a metric when the metric is null in the birth certificate."""
        group_id = 'group0'
        node_id = 'node0'

        self.trigger_message(self.make_nbirth(with_null_scalar=True), 'NBIRTH', group_id, node_id)
        metric = self.client.get_endpoint_metric(f'{group_id}/{node_id}/',
                                                 self.default_scalar_metric_name)
        self.assertFalse(metric.is_historical)
        self.assertFalse(metric.is_transient)
        self.assertTrue(metric.is_null)

if __name__ == "__main__":
    main()
