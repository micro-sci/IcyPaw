# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Test metric descriptors."""

import unittest
from random import randint
import threading

from .metric_descriptor import Metric, get_metric_object
from .types import Int64, Historical, Transient
from .exceptions import IcypawException

class SimpleMetricTester(unittest.TestCase):

    def setUp(self):
        self.name = 'my_metric'
        self.type_ = Int64
        class Node:
            my_metric = Metric(self.type_)
        self.node = Node()
        self.exp_value = randint(-10, 10)

    def test_get_set(self):
        self.assertIsInstance(self.node.my_metric, int)
        self.node.my_metric = self.exp_value
        self.assertEqual(self.node.my_metric, self.exp_value)

    def test_get_network(self):
        self.node.my_metric = self.exp_value
        act_value = get_metric_object(self.node, 'my_metric').get_network(self.node)
        self.assertIsInstance(act_value, self.type_)
        self.assertEqual(act_value.icpw_value, self.exp_value)

    def test_set_network(self):
        icpw_exp_value = self.type_(self.exp_value)
        get_metric_object(self.node, 'my_metric').set_network(self.node, icpw_exp_value)
        self.assertEqual(self.node.my_metric, self.exp_value)

    def test_get_name(self):
        self.assertEqual(get_metric_object(self.node, 'my_metric').name, self.name)

class RenamedMetricTester(SimpleMetricTester):

    def setUp(self):
        self.name = 'folder/My Metric'
        self.type_ = Int64
        class Node:
            my_metric = Metric(self.type_, name=self.name)
        self.node = Node()
        self.exp_value = randint(-10, 10)

class NetHookMetricTester(SimpleMetricTester):
    def setUp(self):
        self.name = 'my_metric'
        self.type_ = Int64
        class Node:
            my_metric = Metric(self.type_)

            @my_metric.net_hook
            def my_metric(self, value):
                return value + 1

        self.node = Node()
        self.exp_value = randint(-10, 10)

    def test_get_set(self):
        self.assertIsInstance(self.node.my_metric, int)
        self.node.my_metric = self.exp_value
        self.assertEqual(self.node.my_metric, self.exp_value)

    def test_set_network(self):
        exp_value = self.type_(self.exp_value)
        get_metric_object(self.node, 'my_metric').set_network(self.node, exp_value)
        self.assertEqual(self.node.my_metric, self.exp_value + 1)

class ReadOnlyMetricTester(SimpleMetricTester):

    def setUp(self):
        self.name = 'my_metric'
        self.type_ = Int64
        class Node:
            my_metric = Metric(self.type_, read_only=True)

        self.node = Node()
        self.exp_value = randint(-10, 10)

    def test_set_network(self):
        with self.assertRaises(IcypawException):
            get_metric_object(self.node, 'my_metric').set_network(self.node, self.type_())

class ReadOnlyNetHookTester(unittest.TestCase):

    def test_fail_on_init(self):
        """Test that merely creating a net_hook on a read-only metric fails"""

        with self.assertRaises(IcypawException):
            class Node:
                my_metric = Metric(Int64, read_only=True)

                @my_metric.net_hook
                def my_metric(self, value):
                    pass

class InheritanceMetricTester(SimpleMetricTester):
    """Test that the metric can be accessed when it is a member of a base
    class."""

    def setUp(self):
        self.name = 'my_metric'
        self.type_ = Int64
        class NodeBase:
            my_metric = Metric(self.type_)
        class Node(NodeBase):
            pass
        self.node = Node()
        self.exp_value = randint(-10, 10)

class MultipleInstanceTester(unittest.TestCase):

    def test_multiple_instances(self):
        """Make sure the data is being stored in the instances and not the
        classes."""
        class Node:
            my_metric = Metric(Int64)
        self.node0 = Node()
        self.node1 = Node()

        self.node0.my_metric = 5
        self.node1.my_metric = 7

        self.assertEqual(self.node0.my_metric, 5)
        self.assertEqual(self.node1.my_metric, 7)

class AssignToThread(unittest.TestCase):

    def test_assign_to_thread_read(self):
        """Test whether an exception is raised when a metric is assigned to a
        specific thread and read another."""

        class Node:
            my_metric = Metric(Int64)
        node = Node()

        my_metric = get_metric_object(node, 'my_metric')

        t = threading.Thread(target=my_metric.assign_to_current_thread, args=(node,))
        t.start()
        t.join()

        with self.assertRaises(IcypawException):
            node.my_metric

    def test_assign_to_thread_write(self):
        """Test whether an exception is raised when a metric is assigned to a
        specific thread and read another."""

        class Node:
            my_metric = Metric(Int64)
        node = Node()

        my_metric = get_metric_object(node, 'my_metric')

        t = threading.Thread(target=my_metric.assign_to_current_thread, args=(node,))
        t.start()
        t.join()

        with self.assertRaises(IcypawException):
            node.my_metric.x = 75

class AliasNetHook(unittest.TestCase):

    def test_net_hook_alias(self):
        """Test registering the net_hook under a different name from the
        metric and using that method directly."""

        class Node:
            my_metric = Metric(Int64)

            @my_metric.net_hook
            def update_my_metric(self, value):
                return abs(value)

        node = Node()

        exp_value = 42
        node.my_metric = exp_value

        self.assertEqual(node.my_metric, exp_value)

        # Just make sure the method is callable. In a real Device or
        # Node this would likely have some desirable side-effect.
        self.assertEqual(node.update_my_metric(-exp_value), exp_value)

        my_metric = get_metric_object(node, 'my_metric')

        # Make sure the net_hook is still called in the correct way
        exp_value = 55
        my_metric.set_network(node, Int64(-exp_value))
        self.assertEqual(node.my_metric, exp_value)

class HistoricalTester(unittest.TestCase):

    def test_historical_metric_value(self):
        """Test setting a value to be historical."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = Historical(exp_value)

        self.assertTrue(my_metric.is_historical(node))
        self.assertEqual(node.my_metric, exp_value)

    def test_non_historical_metric_value(self):
        """Test that values not set to historical are not historical."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = exp_value

        self.assertFalse(my_metric.is_historical(node))
        self.assertEqual(node.my_metric, exp_value)

    def test_historical_resets_on_normal_value(self):
        """Test that a historical value becomes unhistorical after setting a
        normal value."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = Historical(exp_value)

        self.assertTrue(my_metric.is_historical(node))
        self.assertEqual(node.my_metric, exp_value)

        node.my_metric = exp_value

        self.assertFalse(my_metric.is_historical(node))
        self.assertEqual(node.my_metric, exp_value)

class TransientTester(unittest.TestCase):

    def test_transient_metric_value(self):
        """Test setting a value to be transient."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = Transient(exp_value)

        self.assertTrue(my_metric.is_transient(node))
        self.assertEqual(node.my_metric, exp_value)

    def test_non_transient_metric_value(self):
        """Test that values not set to transient are not transient."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = exp_value

        self.assertFalse(my_metric.is_transient(node))
        self.assertEqual(node.my_metric, exp_value)

    def test_transient_resets_on_normal_value(self):
        """Test that a transient value becomes untransient after setting a
        normal value."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = Transient(exp_value)

        self.assertTrue(my_metric.is_transient(node))
        self.assertEqual(node.my_metric, exp_value)

        node.my_metric = exp_value

        self.assertFalse(my_metric.is_transient(node))
        self.assertEqual(node.my_metric, exp_value)

class HistoricalTransientTester(unittest.TestCase):

    def test_historical_transient_metric_value(self):
        """Test a metric that is both historical and transient."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = Historical(Transient(exp_value))

        self.assertTrue(my_metric.is_historical)
        self.assertTrue(my_metric.is_transient)

    def test_historical_transient_idempotency(self):
        """"Test that multiple applications of Historical and Transient have
        no additional effect."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = Historical(Transient(Historical(Historical(
            Transient(Transient(exp_value))))))

        self.assertTrue(my_metric.is_historical)
        self.assertTrue(my_metric.is_transient)

class NullTester(unittest.TestCase):

    def test_null_value(self):
        """Test setting a value to be null."""

        class Node:
            my_metric = Metric(Int64)

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = None

        self.assertTrue(my_metric.is_null(node))
        self.assertEqual(node.my_metric, None)

    def test_non_null_value(self):
        """Test that values not set to null are not null."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = exp_value

        self.assertFalse(my_metric.is_null(node))
        self.assertEqual(node.my_metric, exp_value)

    def test_null_resets_on_normal_value(self):
        """Test that a null value is no longer null with a normal value."""

        class Node:
            my_metric = Metric(Int64)

        node = Node()
        my_metric = get_metric_object(node, 'my_metric')
        node.my_metric = None

        self.assertTrue(my_metric.is_null(node))
        self.assertEqual(node.my_metric, None)

        exp_value = 5
        node.my_metric = exp_value

        self.assertFalse(my_metric.is_null(node))
        self.assertEqual(node.my_metric, exp_value)

class TahuMetricNullTester(unittest.TestCase):

    def test_is_null(self):
        """Test that the tahu metric of a null metric is null"""

        class Node:
            my_metric = Metric(Int64)

        node = Node()
        node.my_metric = None
        my_metric = get_metric_object(node, 'my_metric')
        tahu_metric = my_metric.tahu_metric(node)
        self.assertTrue(tahu_metric.is_null)

    def test_null_has_no_value(self):
        """Test that the value that would otherwise exist in a metric does not
        if it is null."""

        class Node:
            my_metric = Metric(Int64)

        node = Node()
        node.my_metric = None
        my_metric = get_metric_object(node, 'my_metric')
        tahu_metric = my_metric.tahu_metric(node)
        self.assertFalse(tahu_metric.HasField('long_value'))

class TahuMetricHistoricalTester(unittest.TestCase):

    def test_is_historical(self):
        """Test that the tahu metric of a historical metric is historical."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        node.my_metric = Historical(exp_value)
        my_metric = get_metric_object(node, 'my_metric')
        tahu_metric = my_metric.tahu_metric(node)
        self.assertTrue(tahu_metric.is_historical)
        self.assertEqual(tahu_metric.long_value, 42)

class TahuMetricTransientTester(unittest.TestCase):

    def test_is_transient(self):
        """Test that the tahu metric of a transient metric is transient."""

        class Node:
            my_metric = Metric(Int64)

        exp_value = 42

        node = Node()
        node.my_metric = Transient(exp_value)
        my_metric = get_metric_object(node, 'my_metric')
        tahu_metric = my_metric.tahu_metric(node)
        self.assertTrue(tahu_metric.is_transient)
        self.assertEqual(tahu_metric.long_value, 42)
