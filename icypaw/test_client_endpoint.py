# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Test the client_endpoint module which provides a protocol-
independent way of representing Tahu endpoints on the client side."""

import types
import itertools
import unittest
import gc

try:
    from nose.tools import nottest
except ModuleNotFoundError:
    def nottest(x):
        return x

from icypaw.client_endpoint import ClientEndpoint, ClientEndpointName, ClientEndpointMetric, ClientEndpointCommand
from icypaw.tahu_interface import new_payload, new_metric, add_metrics_to_payload, make_timestamp, DataType
import icypaw.types
from icypaw.types import Int64, String, Struct, Array, Field
from icypaw.conventions import make_command, make_template_definition
from icypaw.exceptions import IcypawException

class ClientEndpointNameStringInitTester(unittest.TestCase):

    def test_init_node(self):
        """Test initializing a ClientEndpointName from a node string only."""
        str_input = 'Node0'
        exp_group = None
        exp_node = 'Node0'
        exp_device = None
        self.run_init_test(str_input, exp_group, exp_node, exp_device)

    def test_init_node_device(self):
        """Test initializing a ClientEndpointName from a node/device string."""
        str_input = 'Node0/Dev0'
        exp_group = None
        exp_node = 'Node0'
        exp_device = 'Dev0'
        self.run_init_test(str_input, exp_group, exp_node, exp_device)

    def test_init_group_node_device(self):
        """Test initializing a ClientEndpointName from a group/node/device string."""
        str_input = 'Group0/Node0/Dev0'
        exp_group = 'Group0'
        exp_node = 'Node0'
        exp_device = 'Dev0'
        self.run_init_test(str_input, exp_group, exp_node, exp_device)

    def test_init_group_node(self):
        """Test initializing a ClientEndpointName from a group/node string."""
        str_input = 'Group0/Node0/'
        exp_group = 'Group0'
        exp_node = 'Node0'
        exp_device = None
        self.run_init_test(str_input, exp_group, exp_node, exp_device)

    @nottest
    def run_init_test(self, str_input, exp_group, exp_node, exp_device):
        name = ClientEndpointName(str_input)
        self.assertEqual(exp_group, name.group_id)
        self.assertEqual(exp_node, name.edge_node_id)
        self.assertEqual(exp_device, name.device_id)

class ClientEndpointNameTupleInitTester(unittest.TestCase):

    def test_init_node_device(self):
        """Test initializing a ClientEndpointName from a node, device tuple."""
        exp_group = None
        exp_node = 'Node0'
        exp_device = 'Dev0'
        tpl_input = (exp_node, exp_device)
        self.run_test(tpl_input, exp_group, exp_node, exp_device)

    def test_init_group_node_device(self):
        """Test initializing a ClientEndpointName from a node, device tuple."""
        exp_group = 'Group0'
        exp_node = 'Node0'
        exp_device = 'Dev0'
        tpl_input = (exp_group, exp_node, exp_device)
        self.run_test(tpl_input, exp_group, exp_node, exp_device)

    def test_init_group_node(self):
        """Test initializing a ClientEndpointName from a node, device tuple."""
        exp_group = 'Group0'
        exp_node = 'Node0'
        exp_device = None
        tpl_input = (exp_group, exp_node, exp_device)
        self.run_test(tpl_input, exp_group, exp_node, exp_device)

    @nottest
    def run_test(self, tpl_input, exp_group, exp_node, exp_device):
        name = ClientEndpointName(tpl_input)
        self.assertEqual(exp_group, name.group_id)
        self.assertEqual(exp_node, name.edge_node_id)
        self.assertEqual(exp_device, name.device_id)

class ClientEndpointComponentStringTester(unittest.TestCase):
    """Test the *_str properties of the ClientEndpointName."""

    def test_node(self):
        exp_node = 'Node0'
        name = ClientEndpointName(edge_node_id=exp_node)
        self.assertEqual(exp_node, name.edge_node_id_str)

    def test_node_none(self):
        exp_node = ''
        name = ClientEndpointName(edge_node_id=None)
        self.assertEqual(exp_node, name.edge_node_id_str)

    def test_node_any(self):
        exp_node = '+'
        name = ClientEndpointName(edge_node_id=any)
        self.assertEqual(exp_node, name.edge_node_id_str)

    def test_device(self):
        exp_device = 'Dev0'
        name = ClientEndpointName(device_id=exp_device)
        self.assertEqual(exp_device, name.device_id_str)

    def test_device_none(self):
        exp_device = ''
        name = ClientEndpointName(device_id=None)
        self.assertEqual(exp_device, name.device_id_str)

    def test_device_any(self):
        exp_device = '+'
        name = ClientEndpointName(device_id=any)
        self.assertEqual(exp_device, name.device_id_str)

    def test_group(self):
        exp_group = 'Group0'
        name = ClientEndpointName(group_id=exp_group)
        self.assertEqual(exp_group, name.group_id_str)

    def test_group_none(self):
        exp_group = ''
        name = ClientEndpointName(group_id=None)
        self.assertEqual(exp_group, name.group_id_str)

    def test_group_any(self):
        exp_group = '+'
        name = ClientEndpointName(group_id=any)
        self.assertEqual(exp_group, name.group_id_str)

class ClientEndpointMetricMiscTester(unittest.TestCase):

    def test_is_node(self):
        name = ClientEndpointName(edge_node_id='Node0')
        self.assertTrue(name.is_node)
        self.assertFalse(name.is_device)

    def test_is_device(self):
        name = ClientEndpointName(edge_node_id='Node0', device_id='Dev0')
        self.assertFalse(name.is_node)
        self.assertTrue(name.is_device)

    def test_has_wildcard(self):
        """Test a name testing for a wildcard field."""
        name = ClientEndpointName(group_id='Group0', edge_node_id='Node0', device_id='Dev0')
        self.assertFalse(name.has_wildcard)
        name = ClientEndpointName(group_id=any, edge_node_id='Node0', device_id='Dev0')
        self.assertTrue(name.has_wildcard)
        name = ClientEndpointName(group_id='Group0', edge_node_id=any, device_id='Dev0')
        self.assertTrue(name.has_wildcard)
        name = ClientEndpointName(group_id='Group0', edge_node_id='Node0', device_id=any)
        self.assertTrue(name.has_wildcard)

    def test_str(self):
        """Test conversion of a ClientEndpointName to a string."""
        test_cases = [
            (('Group0', 'Node0', 'Dev0'), 'Group0/Node0/Dev0'),
            ((None, 'Node0', 'Dev0'), '/Node0/Dev0'),
            (('Group0', 'Node0', None), 'Group0/Node0/'),
            ((any, 'Node0', 'Dev0'), '+/Node0/Dev0'),
            (('Group0', any, 'Dev0'), 'Group0/+/Dev0'),
            (('Group0', 'Node0', any), 'Group0/Node0/+'),
        ]

        for init, exp_str in test_cases:
            name = ClientEndpointName(init)
            self.assertEqual(str(name), exp_str)

    def test_match_success(self):
        """Test that two identical names match."""
        name0 = ClientEndpointName('Group0/Node0/Dev0')
        name1 = ClientEndpointName('Group0/Node0/Dev0')
        self.assertTrue(name0.match(name1))

    def test_match_failure(self):
        """Test that two names do not match if they are not equal."""
        name0 = ClientEndpointName('Group0/Node0/Dev0')
        name1 = ClientEndpointName('Group0/Node0/Dev1')
        self.assertFalse(name0.match(name1))

    def test_match_wildcards(self):
        """Test that a name with wildcards can match an appropriate name."""
        pat = ClientEndpointName('+/Node0/Dev0')
        name = ClientEndpointName('Group0/Node0/Dev0')
        self.assertTrue(pat.match(name))

class ClientEndpointTester(unittest.TestCase):

    def setUp(self):
        gc.collect()

        class do_work(Struct):
            """The type of a command called 'do_work'"""
            a = Field(Int64)
            b = Field(String)
        self.do_work_cls = do_work

        class FooTemplate(Struct):
            """The type of a template metric."""
            network_name = 'foo'
            x = Field(Int64)
            y = Field(String)
        self.foo_template_cls = FooTemplate

        self.arr_cls = Array[Int64]

        self.birth = new_payload()
        self.alias_map = {'x': 1, 'foo': 2, 'w': 3, 'do_work': 4, 'arr': 5}

        self.exp_x_value = 42
        self.exp_foo_value = {'x': 7, 'y': 'hello'}
        self.exp_w_value = 'abc'
        self.exp_do_work_value = {'a': 1, 'b': 'xyz'}
        self.exp_arr_value = [1, 2, 3]

        x_metric = self._make_x_metric(self.exp_x_value)
        foo_template = self._make_foo_template()
        foo_metric = self._make_foo_metric(self.exp_foo_value)
        w_metric = self._make_w_metric(self.exp_w_value)
        do_work_template = self._make_do_work_template()
        do_work_metric = self._make_do_work_metric(self.exp_do_work_value)
        arr_metric = self._make_arr_metric(self.exp_arr_value)

        metrics = [x_metric, foo_template, foo_metric, w_metric, do_work_template, do_work_metric,
                   arr_metric]
        add_metrics_to_payload(metrics, self.birth)

    def _make_x_metric(self, value):
        """Create the x metric with the given Int64 value."""
        x_metric = new_metric()
        x_metric.name = 'x'.encode()
        x_metric.alias = self.alias_map['x']
        x_metric.datatype = DataType.Int64.value
        x_metric.long_value = value
        return x_metric

    def _make_foo_template(self):
        foo_template = new_metric()
        foo_template.name = make_template_definition('foo').encode()
        foo_template.datatype = DataType.Template.value
        foo_template.template_value.is_definition = True
        foo_x_template = foo_template.template_value.metrics.add()
        foo_x_template.name = 'x'.encode()
        foo_x_template.datatype = DataType.Int64.value
        foo_y_template = foo_template.template_value.metrics.add()
        foo_y_template.name = 'y'.encode()
        foo_y_template.datatype = DataType.String.value
        return foo_template

    def _make_foo_metric(self, value):
        """Create the foo metric (not its template definition) with the given
        value as a dictionary. Not including one of its fields will
        cause it to not be included in the value.

        """

        foo_metric = new_metric()
        foo_metric.name = 'foo'.encode()
        foo_metric.alias = self.alias_map['foo']
        foo_metric.datatype = DataType.Template.value
        foo_metric.template_value.template_ref = 'foo'.encode()
        if 'x' in value:
            foo_x_metric = foo_metric.template_value.metrics.add()
            foo_x_metric.name = 'x'.encode()
            foo_x_metric.datatype = DataType.Int64.value
            foo_x_metric.long_value = value['x']
        if 'y' in value:
            foo_y_metric = foo_metric.template_value.metrics.add()
            foo_y_metric.name = 'y'.encode()
            foo_y_metric.datatype = DataType.String.value
            foo_y_metric.string_value = value['y'].encode()
        return foo_metric

    def _make_w_metric(self, value):
        """Create the x metric with the given Int64 value."""
        w_metric = new_metric()
        w_metric.name = 'w'.encode()
        w_metric.alias = self.alias_map['w']
        w_metric.datatype = DataType.String.value
        w_metric.string_value = value.encode()
        return w_metric

    def _make_do_work_template(self):
        do_work_metric = new_metric()
        do_work_metric.name = make_template_definition('do_work').encode()
        do_work_metric.alias = self.alias_map['do_work']
        do_work_metric.datatype = DataType.Template.value
        do_work_metric.template_value.is_definition = True
        do_work_a_metric = do_work_metric.template_value.metrics.add()
        do_work_a_metric.name = 'a'.encode()
        do_work_a_metric.datatype = DataType.Int64.value
        do_work_b_metric = do_work_metric.template_value.metrics.add()
        do_work_b_metric.name = 'b'.encode()
        do_work_b_metric.datatype = DataType.String.value
        return do_work_metric

    def _make_do_work_metric(self, value):
        do_work_metric = new_metric()
        do_work_metric.name = make_command('do_work').encode()
        do_work_metric.alias = self.alias_map['do_work']
        do_work_metric.datatype = DataType.Template.value
        do_work_metric.template_value.template_ref = 'do_work'.encode()
        if 'a' in value:
            do_work_a_metric = do_work_metric.template_value.metrics.add()
            do_work_a_metric.name = 'a'.encode()
            do_work_a_metric.datatype = DataType.Int64.value
            do_work_a_metric.long_value = value['a']
        if 'b' in value:
            do_work_b_metric = do_work_metric.template_value.metrics.add()
            do_work_b_metric.name = 'b'.encode()
            do_work_b_metric.datatype = DataType.Int64.value
            do_work_b_metric.string_value = value['b'].encode()
        return do_work_metric

    def _make_arr_metric(self, value, use_name=True):
        arr_metric = new_metric()
        arr_metric.name = 'arr'.encode()
        arr_metric.alias = self.alias_map['arr']
        icypaw_value = self.arr_cls(value)
        icypaw_value.set_in_metric(arr_metric)
        return arr_metric

    def test_create(self):
        """Test creating an endpoint with a birth certificate."""

        endpoint = ClientEndpoint('test', self.birth)
        self.assertEqual(endpoint.commands['do_work'].value.a,
                         self.exp_do_work_value['a'])
        self.assertEqual(endpoint.commands['do_work'].value.b,
                         self.exp_do_work_value['b'])
        self.assertEqual(endpoint.metrics['x'].value,
                         self.exp_x_value)
        self.assertEqual(endpoint.metrics['w'].value,
                         self.exp_w_value)
        self.assertEqual(endpoint.metrics['foo'].value.x,
                         self.exp_foo_value['x'])
        self.assertEqual(endpoint.metrics['foo'].value.y,
                         self.exp_foo_value['y'])
        self.assertEqual(endpoint.metrics['arr'].value,
                         self.exp_arr_value)

    def test_update(self):
        """Test updating with a data or birth message."""
        endpoint = ClientEndpoint('test', self.birth)
        self.assertEqual(endpoint.metrics['w'].value,
                         self.exp_w_value)
        exp_w_value = 'New W Value'
        metric = self._make_w_metric(exp_w_value)
        payload = new_payload()
        add_metrics_to_payload([metric], payload)
        endpoint.update_from_tahu_data(payload)
        self.assertEqual(endpoint.metrics['w'].value, exp_w_value)

    def test_partial_update(self):
        """Test a partial update of a metric."""
        endpoint = ClientEndpoint('test', self.birth)
        self.assertEqual(endpoint.metrics['foo'].value.x,
                         self.exp_foo_value['x'])
        self.assertEqual(endpoint.metrics['foo'].value.y,
                         self.exp_foo_value['y'])
        exp_x = 1234897
        metric = self._make_foo_metric({'x': exp_x})
        payload = new_payload()
        add_metrics_to_payload([metric], payload)
        endpoint.update_from_tahu_data(payload)
        self.assertNotEqual(exp_x, self.exp_foo_value['x'])
        self.assertEqual(endpoint.metrics['foo'].value.x,
                         exp_x)
        self.assertEqual(endpoint.metrics['foo'].value.y,
                         self.exp_foo_value['y'])

    def test_array_update(self):
        """Test updating an endpoint with an array metric."""
        endpoint = ClientEndpoint('test', self.birth)
        self.assertEqual(endpoint.metrics['arr'].value,
                         self.exp_arr_value)
        exp_value = [2, 3, 5, 7, 11]
        payload = new_payload()
        metric = self._make_arr_metric(exp_value, use_name=False)
        add_metrics_to_payload([metric], payload)
        endpoint.update_from_tahu_data(payload)
        self.assertNotEqual(self.exp_arr_value, exp_value)
        self.assertEqual(endpoint.metrics['arr'].value,
                         exp_value)

    def test_death(self):
        """Test an endpoint being brough down by a death certificate."""
        endpoint = ClientEndpoint('test', self.birth)
        self.assertTrue(endpoint.is_online)
        endpoint.update_from_tahu_death(new_payload())
        self.assertFalse(endpoint.is_online)

    def test_rebirth(self):
        """Test an endpoint becoming active after becoming inactive."""
        endpoint = ClientEndpoint('test', self.birth)
        self.assertTrue(endpoint.is_online)
        death_payload = new_payload()
        endpoint.update_from_tahu_death(death_payload)
        self.assertFalse(endpoint.is_online)
        payload = new_payload()
        endpoint.update_from_tahu_data(payload)
        self.assertFalse(endpoint.is_online)
        self.birth.timestamp = death_payload.timestamp + 1
        endpoint.update_from_tahu_birth(self.birth)
        self.assertTrue(endpoint.is_online)

class ClientEndpointLifetimeMixin:
    """Mix-in for client endpoint lifetime test cases"""

    def test_endpoint_is_online_across_lifetime(self):
        """Test that the endpoint correctly reports when it is or is not online over lifetimes."""

        # Should be offline on initialization
        endpoint = ClientEndpoint('test')
        self.assertFalse(endpoint.is_online)

        # Should be online after first birth
        endpoint.update_from_tahu_birth(self.next_birth())
        self.assertTrue(endpoint.is_online)

        # Should be offline after first death
        endpoint.update_from_tahu_death(self.next_death())
        self.assertFalse(endpoint.is_online)

        # Should be online after subsequent births
        endpoint.update_from_tahu_birth(self.next_birth())
        self.assertTrue(endpoint.is_online)

        # Should be offline after subsequent deaths
        endpoint.update_from_tahu_death(self.next_death())
        self.assertFalse(endpoint.is_online)

class ClientEndpointLifetimeByBdSeqTester(ClientEndpointLifetimeMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._next_bdseq = 0

    def next_birth(self):
        birth_cert = _bdseq_payload(self._next_bdseq)
        self._next_death_cert = _bdseq_payload(self._next_bdseq)
        self._next_bdseq = (self._next_bdseq + 1) % 256
        return birth_cert

    def next_death(self):
        return self._next_death_cert

    def test_endpoint_lifetime_across_bdseq_byte_wrap(self):
        """Test that endpoint lifetime reporting is consistent when the bdSeq number overflows a byte."""
        self._next_bdseq = 255
        self.test_endpoint_is_online_across_lifetime()
        self.assertEqual(self._next_bdseq, 1)

class ClientEndpointLifetimeByTimestampTester(ClientEndpointLifetimeMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        # manual timestamps for repeatability
        self._now = make_timestamp()

    def next_birth(self):
        self._now += 100
        return new_payload(timestamp=self._now)

    def next_death(self):
        self._now += 100
        return new_payload(timestamp=self._now)

class ClientEndpointSpecialBooleanBase(unittest.TestCase):
    """Base class for tests testing the is_null, is_transient, and
    is_historical booleans."""

    def setUp(self):
        self.metric_name = "my_metric"
        self.metric_type = Int64
        self.metric_value = self.metric_type(42)
        self.metric_alias = 1
        properties = types.SimpleNamespace(values=[], keys=[])
        self.metric_properties = properties

    def make_metric(self, *, is_null=False, is_transient=False, is_historical=False):
        metric_args = {
            'is_historical': is_historical,
            'is_transient': is_transient,
            'is_null': is_null,
            'is_fresh': True,
        }
        return ClientEndpointMetric(self.metric_name, self.metric_value, self.metric_type,
                                    self.metric_alias,
                                    self.metric_properties, **metric_args)

class ClientEndpointIsNullTester(ClientEndpointSpecialBooleanBase):

    def test_get_is_null(self):
        """Test whether the is_null property of a ClientEndpointMetric returns
        whether a metric is null."""

        for is_null in [True, False]:
            client_metric = self.make_metric(is_null=is_null)
            self.assertEqual(client_metric.is_null, is_null)

    def test_get_value(self):
        """Test retrieving a value that is non-null."""

        client_metric = self.make_metric(is_null=False)
        self.assertEqual(client_metric.value, self.metric_value.to_pseudopython())

    def test_get_null_raises(self):
        """Test failure upon retrieving a value that is null."""

        client_metric = self.make_metric(is_null=True)
        with self.assertRaises(IcypawException):
            client_metric.value

    def test_get_default_value(self):
        """Test getting a default value if the metric is null."""

        client_metric = self.make_metric(is_null=True)
        self.assertEqual(client_metric.get(), None)
        default = "This isn't even the right type but it doesn't matter"
        self.assertEqual(client_metric.get(default), default)

class ClientEndpointIsTransientTester(ClientEndpointSpecialBooleanBase):

    def test_get_is_transient(self):
        """Test whether the is_transient property of a ClientEndpointMetric
        returns whether a metric is transient."""

        for is_transient in [True, False]:
            client_metric = self.make_metric(is_transient=is_transient)
            self.assertEqual(client_metric.is_transient, is_transient)

class ClientEndpointIsHistoricalTester(ClientEndpointSpecialBooleanBase):

    def test_get_is_historical(self):
        """Test whether the is_historical property of a ClientEndpointMetric
        returns whether a metric is historical."""

        for is_historical in [True, False]:
            client_metric = self.make_metric(is_historical=is_historical)
            self.assertEqual(client_metric.is_historical, is_historical)

    def test_use_historical_value_property(self):
        """Tests whether we can access the value of a historical metric with
        the historical_value property."""
        client_metric = self.make_metric(is_historical=True)
        self.assertEqual(client_metric.historical_value, self.metric_value)

    def test_value_property_raises_exception(self):
        """Tests that using the .value property of a metric raises an
        exception if the metric is historical."""
        client_metric = self.make_metric(is_historical=True)
        with self.assertRaises(IcypawException):
            client_metric.value

    def test_historical_and_null(self):
        """Test that retrieving a historical value fails if it is null."""
        client_metric = self.make_metric(is_historical=True, is_null=True)
        with self.assertRaises(IcypawException):
            client_metric.value
        with self.assertRaises(IcypawException):
            client_metric.historical_value

class ClientEndpointSpecialBooleanMetric(unittest.TestCase):

    def test_special_booleans_from_metric(self):
        """Test creating a ClientEndpointMetric from a tahu metric and testing
        the value of the special booleans."""

        for is_null, is_transient, is_historical in itertools.product([True, False], repeat=3):
            metric = new_metric()
            metric.name = 'my_metric'.encode()
            metric.datatype = DataType.Int64.value
            metric.long_value = 42
            metric.is_null = is_null
            metric.is_transient = is_transient
            metric.is_historical = is_historical
            client_metric = ClientEndpointMetric.from_metric(metric, True, {})
            self.assertEqual(client_metric.is_null, is_null)
            self.assertEqual(client_metric.is_transient, is_transient)
            self.assertEqual(client_metric.is_historical, is_historical)

    def test_client_endpoint_command_from_metric(self):
        metric = new_metric()
        metric.name = 'my_metric'.encode()
        metric.datatype = DataType.Int64.value
        metric.long_value = 42
        client_command = ClientEndpointCommand.from_metric(metric)
        self.assertEqual(client_command.name, metric.name)
        self.assertEqual(client_command.value, metric.long_value)


class ClientEndpointPythonTypeTester(unittest.TestCase):

    def test_field(self):
        """Test that the return value for python_type matches the pythontype
        of the underlying type object."""

        class Foo(icypaw.types.Struct):
            pass

        type_list = [
            icypaw.types.UInt32, icypaw.types.Int32, icypaw.types.Int64, icypaw.types.UInt64,
            icypaw.types.Double, icypaw.types.Boolean, icypaw.types.String, Foo,
            icypaw.types.Array[icypaw.types.Int64]]

        properties = types.SimpleNamespace(values=[], keys=[])

        for typ in type_list:
            client_metric = ClientEndpointMetric(
                "name", None, typ, 0, properties,
                is_historical=False, is_transient=False, is_null=True, is_fresh=True)
            self.assertEqual(client_metric.python_type, typ.pythontype)

def _bdseq_payload(bdSeq):
    """Helper to build a payload with the given value as the bdSeq metric"""
    metric = new_metric()
    metric.name = 'bdSeq'.encode()
    metric.datatype = DataType.UInt64.value
    metric.long_value = bdSeq

    payload = new_payload()
    add_metrics_to_payload([metric], payload)
    return payload
