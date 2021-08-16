# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Unit tests for the ServerEngine class which handles MQTT
information and TAHU data shuttling for a single node and its
devices."""

import unittest
from unittest import mock
from types import SimpleNamespace
import time
import logging
from contextlib import contextmanager
from concurrent import futures
import random

import paho.mqtt.client as mqtt

from .server_engine import ServerEngine, _logger
from .node import ServerNode
from .device import ServerDevice
from .tahu_interface import Payload
from . import icpw_timer, icpw_trigger, icpw_command, Metric
from .types import Int64, String, convert_to_signed64
from .conventions import make_command, make_template_definition


class ServerEngineTester(unittest.TestCase):
    """Base class for testers."""

    class TestNode(ServerNode):
        pass

    class TestDevice(ServerDevice):
        pass

    def setUp(self):
        # Patch client so we're not connecting to anything
        self.mock_client_patch = mock.patch('paho.mqtt.client.Client')
        self.mock_client = self.mock_client_patch.start()()
        self.mock_client.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS)

        # Patch subscribe.simple so we can control what the last NBIRTH looks like
        mock_message = SimpleNamespace(payload=b'\x12\t\n\x05bdSeqX*')  # magical mystery NBIRTH
        self.mock_subscribe_patch = mock.patch('paho.mqtt.subscribe.simple', return_value=mock_message)
        self.mock_subscribe = self.mock_subscribe_patch.start()

        self.group_id = 'group0'
        self.edge_node_id = 'node0'

        if hasattr(type(self), 'make_TestDevice_type'):
            self.device_cls = type(self).make_TestDevice_type()
        else:
            self.device_cls = type(self).TestDevice

        if hasattr(type(self), 'make_TestNode_type'):
            self.node_cls = type(self).make_TestNode_type()
        else:
            self.node_cls = type(self).TestNode
        self.node = self.node_cls(group_id=self.group_id,
                                  edge_node_id=self.edge_node_id,
                                  device_classes=[self.device_cls])

        self.broker = 'broker-addr'  # Not a valid IP in case we mess up the test
        self.port = 1234  # Not the right port either

    def tearDown(self):
        self.mock_subscribe_patch.stop()
        self.mock_client_patch.stop()

class StartupTester(ServerEngineTester):
    """Test things that are supposed to happen on creating a server."""

    def test_register_queue(self):
        """Test that constructing a ServerEngine registers a queue with the
        node."""

        engine = ServerEngine(self.node)
        self.assertIs(engine._queue, self.node._command_queue)

class ConnectTester(ServerEngineTester):
    """Test things that are supposed to happen on connecting the engine to
    MQTT.

    """

    class TestNode(ServerNode):
        x = Metric(Int64)
        @icpw_command
        def do_stuff(self, u: Int64, v: Int64):
            pass

    def test_connect(self):
        """Test that the proper registration occurs with the MQTT client
        object."""

        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            self.mock_client.connect.assert_called_with(self.broker, port=self.port)

    def test_publish_birth_death(self):
        """Test that the node birth certificate is published upon connecting to
        MQTT.

        """
        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            pass
        self.mock_client.publish.assert_called_once()

        (act_topic, act_payload), _ = self.mock_client.publish.call_args

        birth_topic = f'spBv1.0/{self.group_id}/NBIRTH/{self.edge_node_id}'
        self.assertEqual(act_topic, birth_topic)

        payload = Payload()
        payload.ParseFromString(act_payload)

        act_metrics = {metric.name: metric for metric in payload.metrics}
        self.assertIn('x', act_metrics)
        self.assertIn(make_command('do_stuff'), act_metrics)
        self.assertIn(make_template_definition('do_stuff'), act_metrics)
        self.assertIn('bdSeq', act_metrics)

    def _assert_nbirth_bdseq_is_incremented(self, last_bdseq, expected_bdseq):
        # Prime subscribe mock with a previous NBIRTH
        last_nbirth = SimpleNamespace(payload=_make_bdseq_payload(bdSeq=last_bdseq).SerializeToString())
        self.mock_subscribe.return_value = last_nbirth
        new_node = self.node_cls(group_id=self.group_id,
                                 edge_node_id=self.edge_node_id,
                                 device_classes=[self.device_cls])
        engine = ServerEngine(new_node)
        with engine.connect(self.broker, self.port):
            (_, act_payload), _ = self.mock_client.publish.call_args

            payload = Payload()
            payload.ParseFromString(act_payload)

            act_metrics = {metric.name: metric for metric in payload.metrics}
            self.assertIn('bdSeq', act_metrics)
            self.assertEqual(act_metrics['bdSeq'].long_value, expected_bdseq)

    def test_nbirth_increments_bdseq(self):
        """Test that the server writes the correct bdSeq value to node birth certificates."""

        self._assert_nbirth_bdseq_is_incremented(0, 1)
        self._assert_nbirth_bdseq_is_incremented(41, 42)
        self._assert_nbirth_bdseq_is_incremented(255, 0)

    def test_nbirth_bdseq_is_zero_on_no_last_nbirth(self):
        """Test that when no NBIRTH is retained on the NBIRTH topic, the next
        node birth certificate has bdSeq = 0."""

        # If there's no retained NBIRTH, the call to mqtt.subscribe.simple will time out
        self.mock_subscribe.side_effect = futures.TimeoutError()

        engine = ServerEngine(self.node)
        with suppress_log():
            with engine.connect(self.broker, self.port):
                (_, act_payload), _ = self.mock_client.publish.call_args
                payload = Payload()
                payload.ParseFromString(act_payload)
                act_metrics = {metric.name: metric for metric in payload.metrics}
                self.assertIn('bdSeq', act_metrics)
                self.assertEqual(act_metrics['bdSeq'].long_value, 0)



    def test_subscribe_ncmd(self):
        """Test that the engine subscribes to NCMD messages for its node."""
        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            pass
        self.mock_client.subscribe.assert_called()
        ncmd_published = False
        ncmd_topic = f'spBv1.0/{self.group_id}/NCMD/{self.edge_node_id}'
        for args, kwargs in self.mock_client.subscribe.call_args_list:
            act_topic, = args
            if act_topic == ncmd_topic:
                ncmd_published = True
        self.assertTrue(ncmd_published)

    def test_node_callbacks(self):
        """Test that the node callback methods are called at the appropriate
        time."""
        engine = ServerEngine(self.node)
        self.node.on_connect = mock.MagicMock()
        self.node.on_shutdown = mock.MagicMock()
        self.node.on_disconnect = mock.MagicMock()
        with engine.connect(self.broker, self.port):
            self.node.on_connect.assert_called_once()
            self.node.on_shutdown.assert_not_called()
            self.node.on_disconnect.assert_not_called()
        self.node.on_connect.assert_called_once()
        self.node.on_shutdown.assert_not_called()
        self.node.on_disconnect.assert_called_once()

class NewDeviceTester(ServerEngineTester):
    """Test cases for a node bringing devices up and down."""

    device_id = 'device0'

    class TestNode(ServerNode):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.dev = None

        def on_connect(self, engine):
            self.dev = self.device_classes[0](self.group_id, self.edge_node_id,
                                              NewDeviceTester.device_id)
            self.icpw_register_device(self.dev)

        def unregister_device(self):
            self.icpw_unregister_device(self.dev)

    class TestDevice(ServerDevice):
        dev_metric = Metric(Int64)

        @icpw_command
        def dev_do_stuff(self, x: Int64):
            pass

    def test_start_device(self):
        """Test bringing up a device after bringing up the engine."""
        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            self.mock_client.publish.reset_mock()
            engine.process_events()
            self.mock_client.publish.assert_called()
            birth_args, death_args = self.mock_client.publish.call_args_list
            (dbirth_topic, dbirth_payload), kwargs = birth_args
            self.assertNotEqual(dbirth_payload, '')
            dbirth = Payload()
            dbirth.ParseFromString(dbirth_payload)
            exp_topic = f'spBv1.0/{self.group_id}/DBIRTH/{self.edge_node_id}/{self.device_id}'
            self.assertEqual(dbirth_topic, exp_topic)
            dbirth_metric_names = [metric.name for metric in dbirth.metrics]
            self.assertIn('dev_metric', dbirth_metric_names)
            self.assertIn(make_command('dev_do_stuff'), dbirth_metric_names)

    def test_end_device(self):
        """Test tearing down a device after bringing up the engine."""
        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            engine.process_events()
            self.mock_client.publish.reset_mock()
            self.node.unregister_device()
            engine.process_events()
            self.mock_client.publish.assert_called()
            exp_topic = f'spBv1.0/{self.group_id}/DDEATH/{self.edge_node_id}/{self.device_id}'
            (ddeath_topic, ddeath_payload), kwargs = self.mock_client.publish.call_args
            ddeath = Payload()
            ddeath.ParseFromString(ddeath_payload)
            self.assertEqual(exp_topic, ddeath_topic)
            self.assertEqual(0, len(ddeath.metrics))

class NodeRebirthTester(ServerEngineTester):

    class TestNode(ServerNode):
        node_metric = Metric(Int64)

    def test_rebirth(self):
        """Test issuing a new NBIRTH certificate with changed metrics."""
        exp_value = random.randint(-100, 100)
        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            engine.process_events()
            self.mock_client.publish.reset_mock()
            self.node.node_metric = exp_value
            self.node.icpw_rebirth()
            engine.process_events()
            self.mock_client.publish.assert_called()
            (nbirth_topic, nbirth_payload), kwargs = self.mock_client.publish.call_args
            exp_topic = f'spBv1.0/{self.group_id}/NBIRTH/{self.edge_node_id}'
            self.assertEqual(exp_topic, nbirth_topic)
            nbirth = Payload()
            nbirth.ParseFromString(nbirth_payload)
            # bdseq and node_metric
            self.assertEqual(2, len(nbirth.metrics))
            node_metric = [metric for metric in nbirth.metrics if metric.name == 'node_metric'][0]
            act_value = convert_to_signed64(node_metric.long_value)
            self.assertEqual(exp_value, act_value)

class DynamicMetricTester(ServerEngineTester):
    """Test adding and removing metrics from endpoints."""

    @classmethod
    def make_TestNode_type(cls):
        # If we don't regenerate this class then the metrics we add or
        # delete in a test will stick around between tests.
        class TestNode(ServerNode):
            node_metric = Metric(Int64)

            @icpw_command
            def add_metric(self, name: String):
                self.icpw_add_metric(name, Metric(Int64))

            @icpw_command
            def del_metric(self, name: String):
                self.icpw_del_metric(name)
        return TestNode

    def test_add_metric_to_node(self):
        """Test adding a metric to a node."""

        engine = ServerEngine(self.node)

        topic, payload = self._create_add_metric_command('x')

        with engine.connect(self.broker, self.port):
            engine.process_events()
            self.mock_client.publish.reset_mock()
            # Invoke the add_metric command as though it was called
            # remotely.

            engine.on_ncmd(None, None,
                           SimpleNamespace(topic=topic, payload=payload.SerializeToString()))
            engine.process_events()

            x_metrics = [metric for metric in self.node.tahu_metrics()
                         if metric.name == 'x']
            self.assertEqual(1, len(x_metrics))

            # Assert the engine sent out a new NBIRTH (and only that)
            arg_list = self.mock_client.publish.call_args_list
            self.assertEqual(1, len(arg_list))
            exp_topic = f'spBv1.0/{self.group_id}/NBIRTH/{self.edge_node_id}'
            (act_topic, act_coded_payload), _ = arg_list[0]
            self.assertEqual(exp_topic, act_topic)
            act_payload = Payload()
            act_payload.ParseFromString(act_coded_payload)
            act_metric_names = {metric.name for metric in act_payload.metrics}
            self.assertIn('x', act_metric_names)
            self.assertIn('node_metric', act_metric_names)

    def test_delete_metric_from_node(self):
        """Test removing a metric from a node."""

        # Since the most common case is to remove dynamically added
        # metrics, this is what we test here. It is possible, though
        # not recommended, to remove any metric from the node's class,
        # although removing one from its base classes is not
        # supported.

        engine = ServerEngine(self.node)

        add_topic, add_payload = self._create_add_metric_command('x')
        del_topic, del_payload = self._create_del_metric_command('x')

        with engine.connect(self.broker, self.port):
            engine.process_events()
            self.mock_client.publish.reset_mock()
            # Invoke the add_metric command as though it was called
            # remotely.

            engine.on_ncmd(None, None,
                           SimpleNamespace(topic=add_topic, payload=add_payload.SerializeToString()))
            engine.process_events()

            x_metrics = [metric for metric in self.node.tahu_metrics()
                         if metric.name == 'x']
            self.assertEqual(1, len(x_metrics))
            self.mock_client.publish.reset_mock()

            engine.on_ncmd(None, None,
                           SimpleNamespace(topic=del_topic, payload=del_payload.SerializeToString()))
            engine.process_events()

            x_metrics = [metric for metric in self.node.tahu_metrics()
                         if metric.name == 'x']
            self.assertEqual(0, len(x_metrics))

            # Assert the engine sent out a new NBIRTH (and only that)
            arg_list = self.mock_client.publish.call_args_list
            self.assertEqual(1, len(arg_list))
            exp_topic = f'spBv1.0/{self.group_id}/NBIRTH/{self.edge_node_id}'
            (act_topic, act_coded_payload), _ = arg_list[0]
            self.assertEqual(exp_topic, act_topic)
            act_payload = Payload()
            act_payload.ParseFromString(act_coded_payload)
            act_metric_names = {metric.name for metric in act_payload.metrics}
            self.assertNotIn('x', act_metric_names)

    def test_bdseq_after_changing_metric(self):
        """Make sure the bdSeq is properly incremented each time a metric is
        added or removed."""

        engine = ServerEngine(self.node)

        add_topic, add_payload = self._create_add_metric_command('x')
        del_topic, del_payload = self._create_del_metric_command('x')

        with engine.connect(self.broker, self.port):
            engine.process_events()
            last_bdseq = self._get_last_bdSeq()
            # Invoke the add_metric command as though it was called
            # remotely.

            engine.on_ncmd(None, None,
                           SimpleNamespace(topic=add_topic, payload=add_payload.SerializeToString()))
            engine.process_events()

            this_bdseq = self._get_last_bdSeq()
            self.assertEqual(this_bdseq, last_bdseq)

            engine.on_ncmd(None, None,
                           SimpleNamespace(topic=del_topic, payload=del_payload.SerializeToString()))
            engine.process_events()

            this_bdseq = self._get_last_bdSeq()
            self.assertEqual(this_bdseq, last_bdseq)

    ##
    # Helper methods
    #

    def _create_add_metric_command(self, metric_name):
        """Create topic and payload for calling the add_metric command."""
        cmd_type = self.node.icpw_signature()['commands']['add_metric']
        topic = f'spBv1.0/{self.group_id}/NDATA/{self.edge_node_id}'
        payload = Payload()
        metric = payload.metrics.add()
        metric.name = 'add_metric'.encode()
        icpw_value = cmd_type({'name': metric_name})
        icpw_value.set_in_metric(metric)
        return topic, payload

    def _create_del_metric_command(self, metric_name):
        """Create topic and payload for calling the add_metric command."""
        cmd_type = self.node.icpw_signature()['commands']['del_metric']
        topic = f'spBv1.0/{self.group_id}/NDATA/{self.edge_node_id}'
        payload = Payload()
        metric = payload.metrics.add()
        metric.name = 'del_metric'.encode()
        icpw_value = cmd_type({'name': metric_name})
        icpw_value.set_in_metric(metric)
        return topic, payload

    def _get_last_bdSeq(self):
        """Return the last bdSeq sent by the node."""
        for (topic, coded_payload), _ in reversed(self.mock_client.publish.call_args_list):
            if 'NBIRTH' in topic:
                payload = Payload()
                payload.ParseFromString(coded_payload)
                for metric in payload.metrics:
                    if metric.name == 'bdSeq':
                        return metric.long_value

        raise ValueError("No bdSeq found")


class TimerTester(ServerEngineTester):
    """Test scheduling and rescheduling timers."""

    class TestNode(ServerNode):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._x = 0

        @icpw_timer(0.1)
        def timer(self):
            self._x += 1

    def test_immediate_timer(self):
        """Test that a timer is immediately scheduled by the engine."""
        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            engine.process_events()
        self.assertEqual(self.node._x, 1)

    def test_repeat_timer(self):
        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            engine.process_events()
            time.sleep(0.1)
            engine.process_events()
        self.assertGreaterEqual(self.node._x, 2)

class TriggerTester(ServerEngineTester):

    class TestNode(ServerNode):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._x = None

        @icpw_trigger
        def trigger(self, x):
            self._x = x

    def test_trigger(self):
        """Test that calling a trigger schedules a function for execution."""
        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            engine.process_events()
            self.assertEqual(self.node._x, None)
            exp_value = 5
            self.node.trigger(exp_value)
            engine.process_events()
            self.assertEqual(exp_value, self.node._x)

class RunInTester(ServerEngineTester):

    class TestNode(ServerNode):

        x = Metric(Int64)

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def on_connect(self, engine):
            self.icpw_run_in(0, self.callback)

        def callback(self):
            self.x = 1

    def test_run_in(self):
        """Test using the icpw_run_in function."""
        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            self.mock_client.publish.reset_mock()
            engine.process_events()
            self.assertEqual(self.node.x, 1)
            # Make sure the engine does not reschedule the event. This
            # breaks the abstraction a bit but is hard to otherwise
            # test.
            self.assertEqual(0, len(engine._scheduled_events))

            # Make sure the update to the metric was published.
            self.mock_client.publish.assert_called()
            (act_topic, _act_payload), kwargs = self.mock_client.publish.call_args
            act_payload = Payload()
            act_payload.ParseFromString(_act_payload)
            exp_topic = f'spBv1.0/{self.group_id}/NDATA/{self.edge_node_id}'
            self.assertEqual(exp_topic, act_topic)
            # Don't check the name because we want that to eventually
            # be aliased away.
            self.assertEqual(1, len(act_payload.metrics))
            self.assertEqual(1, act_payload.metrics[0].long_value)

class IncomingTester(ServerEngineTester):
    """Test the engine's handling of incoming DCMD and NCMD messages. Note
    that this only checks what the engine does once it receives these
    messages, not that it is properly set up to receive them."""

    class TestNode(ServerNode):

        x = Metric(Int64)

        def __init__(self, *args, **kwargs):
            kwargs['device_classes'] = [IncomingTester.TestDevice]
            super().__init__(*args, **kwargs)
            self.y = None
            self.device = None
            self.device_id = None

        @icpw_command
        def update_y(self, value: Int64, unit: String = "Hz"):
            self.y = (value, unit)

        def on_connect(self, engine):
            self.device_id = 'device0'
            self.device = IncomingTester.TestDevice(self.group_id, self.edge_node_id,
                                                    self.device_id)
            self.icpw_register_device(self.device)

    class TestDevice(ServerDevice):

        v = Metric(Int64)

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.w = None

        @icpw_command
        def update_w(self, value: Int64, unit: String = "ft"):
            self.w = (value, unit)

    def check_published_metric(self, value, is_node=True):
        """Check that a metric was published with the given value."""

        self.mock_client.publish.assert_called()
        (act_topic, _act_payload), kwargs = self.mock_client.publish.call_args
        act_payload = Payload()
        act_payload.ParseFromString(_act_payload)
        if is_node:
            exp_topic = f'spBv1.0/{self.group_id}/NDATA/{self.edge_node_id}'
        else:
            exp_topic = f'spBv1.0/{self.group_id}/DDATA/{self.edge_node_id}/{self.node.device_id}'
        self.assertEqual(exp_topic, act_topic)
        # Don't check the name because we want that to eventually
        # be aliased away.
        self.assertEqual(1, len(act_payload.metrics))
        self.assertEqual(value, act_payload.metrics[0].long_value)

    def test_ncmd_metric(self):
        """Test that the node receives an NCMD message destined to update a
        metric."""

        topic = f'spBv1.0/{self.group_id}/NDATA/{self.edge_node_id}'
        payload = Payload()
        metric = payload.metrics.add()
        metric.name = 'x'.encode()
        icpw_value = Int64(5)
        icpw_value.set_in_metric(metric)

        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            engine.process_events()
            self.mock_client.publish.reset_mock()


            engine.on_ncmd(None, None,
                           SimpleNamespace(topic=topic, payload=payload.SerializeToString()))
            engine.process_events()

            self.assertEqual(self.node.x, icpw_value.icpw_value)

            self.check_published_metric(icpw_value.icpw_value)

    def test_ncmd_command(self):
        """Test the node receiving an NCMD message that runs a command."""

        cmd_type = self.node.icpw_signature()['commands']['update_y']

        exp_y = 88

        topic = f'spBv1.0/{self.group_id}/NDATA/{self.edge_node_id}'
        payload = Payload()
        metric = payload.metrics.add()
        metric.name = 'update_y'.encode()
        icpw_value = cmd_type({'value': exp_y, 'unit': 'kHz'})
        icpw_value.set_in_metric(metric)

        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            engine.process_events()
            self.mock_client.publish.reset_mock()


            engine.on_ncmd(None, None,
                           SimpleNamespace(topic=topic, payload=payload.SerializeToString()))
            engine.process_events()

            self.assertEqual(self.node.y, (exp_y, 'kHz'))

    def test_dcmd_metric(self):
        """Test a device receiving a DCMD message that updates a metric."""

        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            engine.process_events()
            self.mock_client.publish.reset_mock()

            topic = f'spBv1.0/{self.group_id}/DDATA/{self.edge_node_id}/{self.node.device_id}'
            payload = Payload()
            metric = payload.metrics.add()
            metric.name = 'v'.encode()
            icpw_value = Int64(5)
            icpw_value.set_in_metric(metric)

            engine.on_dcmd(None, None,
                           SimpleNamespace(topic=topic, payload=payload.SerializeToString()))
            engine.process_events()

            self.assertEqual(self.node.device.v, icpw_value.icpw_value)

            self.check_published_metric(icpw_value.icpw_value, is_node=False)

    def test_dcmd_command(self):
        """Test the node receiving an DCMD message that runs a command."""

        exp_w = 144

        engine = ServerEngine(self.node)
        with engine.connect(self.broker, self.port):
            engine.process_events()
            self.mock_client.publish.reset_mock()

            cmd_type = self.node.device.icpw_signature()['commands']['update_w']

            topic = f'spBv1.0/{self.group_id}/DDATA/{self.edge_node_id}/{self.node.device_id}'
            payload = Payload()
            metric = payload.metrics.add()
            metric.name = 'update_w'.encode()
            icpw_value = cmd_type({'value': exp_w, 'unit': 'MHz'})
            icpw_value.set_in_metric(metric)

            engine.on_dcmd(None, None,
                           SimpleNamespace(topic=topic, payload=payload.SerializeToString()))
            engine.process_events()

            self.assertEqual(self.node.device.w, (exp_w, 'MHz'))

class ExtraMetricTester(ServerEngineTester):

    def test_extra_metrics(self):
        """Test attempting to set a non-existant metric in a node."""

        with suppress_log():
            topic = f'spBv1.0/{self.group_id}/NDATA/{self.edge_node_id}'
            payload = Payload()
            metric = payload.metrics.add()
            metric.name = 'x'.encode()
            icpw_value = Int64(5)
            icpw_value.set_in_metric(metric)

            engine = ServerEngine(self.node)
            with engine.connect(self.broker, self.port):
                self.mock_client.publish.reset_mock()

                engine.on_ncmd(None, None,
                               SimpleNamespace(topic=topic, payload=payload.SerializeToString()))

                # Really there's nothing to test for yet. Eventually we'll
                # check that an exception is sent out by the engine.

                self.mock_client.publish.assert_not_called()

##
# Helpers
#

class IgnoreFilter(logging.Filter):
    """A class used to ignore all log entries so that they don't show up
    on the console."""
    def filter(self, record):
        return 0

@contextmanager
def suppress_log():
    log_filter = IgnoreFilter()
    _logger.addFilter(log_filter)
    try:
        yield
    finally:
        _logger.removeFilter(log_filter)

def _make_bdseq_payload(bdSeq=0):
    """Make a quick and minimal NBIRTH pseudo-payload fixture with the given bdSeq"""
    payload = Payload()
    metric = payload.metrics.add()
    metric.name = "bdSeq".encode()
    metric.long_value = bdSeq
    return payload

if __name__ == '__main__':
    unittest.main()
