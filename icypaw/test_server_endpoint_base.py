# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Unit tests for the server endpoint base class."""

import unittest
import time
import threading

from .server_endpoint_base import ServerEndpointBase
from .metric_descriptor import Metric, get_metric_object
from .timer_descriptor import icpw_timer
from .trigger_descriptor import icpw_trigger
from .command_descriptor import icpw_command
from .types import Int64, Struct, Field
from .exceptions import IcypawException

GROUPID = 'group0'

class ServerEndpointBaseMetricTester(unittest.TestCase):

    def test_group_id(self):
        """Test the group ID of an endpoint."""
        endpoint = ServerEndpointBase(GROUPID)
        self.assertEqual(endpoint.group_id, GROUPID)

    def test_metric(self):
        """Test constructing an endpoint with a single metric."""
        class Endpoint(ServerEndpointBase):
            x = Metric(Int64)
        foo = Endpoint(GROUPID)
        self.assertEqual({}, foo.icpw_updated_metrics())

        exp_metrics = {'x': Int64()}
        act_metrics = foo.icpw_all_metrics()
        self.assertEqual(exp_metrics, act_metrics)

    def test_metric_update(self):
        """Test getting the update of a metric."""
        class Endpoint(ServerEndpointBase):
            x = Metric(Int64)
        foo = Endpoint(GROUPID)
        prev_value = 0
        new_value = 15
        foo.x = new_value

        exp_metrics = {'x': (Int64(new_value), Int64(prev_value))}
        act_metrics = foo.icpw_updated_metrics()
        self.assertEqual(exp_metrics, act_metrics)

    def test_get_metric(self):
        """Test retrieving the value of a stored metric."""
        class Endpoint(ServerEndpointBase):
            x = Metric(Int64)
        foo = Endpoint(GROUPID)
        exp_value = 7
        foo.x = exp_value
        self.assertEqual(foo.x, exp_value)

    def test_metric_with_net_hook(self):
        """Test using a metric with a net_hook."""
        class Endpoint(ServerEndpointBase):
            x = Metric(Int64)

            @x.net_hook
            def x(self, value):
                return value - 1

        foo = Endpoint(GROUPID)
        exp_value = 42
        metrix_x = get_metric_object(foo, 'x')
        metrix_x.set_network(foo, Int64(exp_value + 1))

        self.assertEqual(foo.x, exp_value)

    def test_struct_metric_with_net_hook(self):
        """Test using a struct (template) metric with a net_hook."""
        class MyStruct(Struct):
            network_name = 'MyStruct'

            y = Field(Int64)

        class Endpoint(ServerEndpointBase):
            x = Metric(MyStruct)

            @x.net_hook
            def x(self, value):
                return value

        foo = Endpoint(GROUPID)
        exp_value = {'y': 42}
        metrix_x = get_metric_object(foo, 'x')
        metrix_x.set_network(foo, MyStruct(exp_value))

        self.assertEqual(foo.x, exp_value)

    def test_metric_update_from_network(self):
        """Test updating a metric with a value that has come from the
        network."""
        class Endpoint(ServerEndpointBase):
            x = Metric(Int64)

        foo = Endpoint(GROUPID)
        exp_value = -40
        foo.icpw_update_metric('x', Int64(exp_value))
        self.assertEqual(exp_value, foo.x)

    def test_read_only_metric(self):
        """Test that a read only metric causes an error when written over the
        network but not locally."""
        class Endpoint(ServerEndpointBase):
            x = Metric(Int64, read_only=True)

        foo = Endpoint(GROUPID)
        exp_value = 1010
        foo.x = exp_value
        self.assertEqual(foo.x, exp_value)

        with self.assertRaises(IcypawException):
            foo.icpw_update_metric('x', Int64(0))

    def test_metric_with_different_name(self):
        """Test a metric with a network name different from its Python
        name."""

        metric_name = 'Metric X'

        class Endpoint(ServerEndpointBase):
            x = Metric(Int64, name=metric_name)

            def __init__(self, *args):
                super().__init__(*args)
                self._get_count = 0

        foo = Endpoint(GROUPID)
        exp_value = -123
        foo.x = exp_value

        exp_value = 0
        foo.icpw_update_metric(metric_name, Int64(exp_value))
        self.assertEqual(foo.x, exp_value)

        with self.assertRaises(IcypawException):
            foo.icpw_update_metric('x', Int64(0))

    def test_add_metric(self):
        """Test adding a metric after creating an endpoint.

        """

        class Endpoint(ServerEndpointBase):
            y = Metric(Int64)
        x = Metric(Int64)
        foo = Endpoint(GROUPID)
        foo.icpw_add_metric('x', x)
        prev_x_value = 0
        new_x_value = 15
        prev_y_value = 0
        new_y_value = 42
        foo.x = new_x_value
        foo.y = new_y_value

        exp_metrics = {'x': (Int64(new_x_value), Int64(prev_x_value)),
                       'y': (Int64(new_y_value), Int64(prev_y_value))}
        act_metrics = foo.icpw_updated_metrics()
        self.assertEqual(exp_metrics, act_metrics)
        self.assertFalse(foo.icpw_is_birth_certificate_fresh)
        foo.icpw_make_birth_certificate_fresh()
        self.assertTrue(foo.icpw_is_birth_certificate_fresh)

    def test_del_metric(self):
        """Test deleting a dynamically-added metric."""

        class Endpoint(ServerEndpointBase):
            y = Metric(Int64)
        x = Metric(Int64)
        foo = Endpoint(GROUPID)
        foo.icpw_add_metric('x', x)
        prev_x_value = 0
        new_x_value = 15
        prev_y_value = 0
        new_y_value = 42
        foo.x = new_x_value
        foo.y = new_y_value

        exp_metrics = {'x': (Int64(new_x_value), Int64(prev_x_value)),
                       'y': (Int64(new_y_value), Int64(prev_y_value))}
        act_metrics = foo.icpw_updated_metrics()
        self.assertEqual(exp_metrics, act_metrics)
        self.assertFalse(foo.icpw_is_birth_certificate_fresh)

        foo.icpw_del_metric('x')
        foo.x = new_y_value  # This will set an instance variable, not a metric

        exp_metrics = {}
        act_metrics = foo.icpw_updated_metrics()
        self.assertEqual(exp_metrics, act_metrics)
        self.assertFalse(foo.icpw_is_birth_certificate_fresh)

    def test_del_metric_with_network_name(self):
        """Test deleting a metric with a different network name."""
        metric_name = 'X name'
        class Endpoint(ServerEndpointBase):
            x = Metric(Int64, name=metric_name)
        foo = Endpoint(GROUPID)
        foo.x = 42
        foo.icpw_del_metric(network_name=metric_name)
        self.assertEqual({}, foo.icpw_updated_metrics())
        with self.assertRaises(AttributeError):
            foo.x

    def test_del_readd_metric(self):
        """Test deleting and re-adding a metric to make sure it leaves no trace."""
        first_value = 5
        second_value = 7

        class Endpoint(ServerEndpointBase):
            x = Metric(Int64)
        foo = Endpoint(GROUPID)
        foo.x = first_value
        foo.icpw_del_metric('x')
        foo.icpw_add_metric('x', Metric(Int64, initial=second_value))
        self.assertEqual(foo.x, second_value)

    def test_metric_properties(self):
        """Test retrieving a metric and its properties from an endpoint."""
        exp_value = 42
        prop_name = 'Prop'
        class Endpoint(ServerEndpointBase):
            x = Metric(Int64, properties={prop_name: exp_value})
        foo = Endpoint(GROUPID)
        act_value = get_metric_object(foo, 'x').properties[prop_name]
        self.assertEqual(exp_value, act_value)


class ServerEndpointBaseTimerTester(unittest.TestCase):

    def setUp(self):
        self.command_queue = MockQueue()
        self.repeat_sec = 5
        class Endpoint(ServerEndpointBase):
            def __init__(self, *args):
                super().__init__(*args)
                self._x = 0

            @icpw_timer(self.repeat_sec)
            def timer(self):
                self._x += 1
        self.cls = Endpoint

    def test_timer_register(self):
        """Test that decorated timers are pushed onto the command queue."""
        foo = self.cls(GROUPID)
        foo.icpw_register_command_queue(self.command_queue)
        self.assertEqual(1, len(self.command_queue._queue))
        item = self.command_queue._queue[0]
        self.assertEqual(item.payload.repeat_sec, self.repeat_sec)
        self.assertEqual(foo._x, 0)
        item.payload.func()
        self.assertEqual(foo._x, 1)

    def test_timer_call(self):
        """Test that a timer can be called like a normal method."""
        foo = self.cls(GROUPID)
        self.assertEqual(foo._x, 0)
        foo.timer()
        self.assertEqual(foo._x, 1)

class ServerEndpointBaseTriggerTester(unittest.TestCase):

    def test_trigger_register(self):
        """Test an endpoint with a trigger."""
        command_queue = MockQueue()
        class Endpoint(ServerEndpointBase):
            def __init__(self, *args):
                super().__init__(*args)
                self._x = 0

            @icpw_trigger
            def trigger(self, x):
                self._x = x
        foo = Endpoint(GROUPID)
        foo.icpw_register_command_queue(command_queue)
        exp_value = 888
        foo.trigger(exp_value)
        self.assertEqual(1, len(command_queue._queue))
        item = command_queue._queue[0]
        self.assertEqual(0, foo._x)
        item.payload.func()
        self.assertEqual(exp_value, foo._x)

class ServerEndpointBaseRunInTester(unittest.TestCase):

    def setUp(self):
        self.delay_sec = 5.0
        self.command_queue = MockQueue()
        class Endpoint(ServerEndpointBase):
            def __init__(self, *args):
                super().__init__(*args)
                self._x = 0
            def ordinary_func(self, inc=1):
                self._x += inc
        self.foo = Endpoint(GROUPID)
        self.foo.icpw_register_command_queue(self.command_queue)

    def test_run_in(self):
        """Test the icpw_run_in method."""
        min_time = time.time()
        self.foo.icpw_run_in(self.delay_sec, self.foo.ordinary_func)
        max_time = time.time()
        self.assertEqual(1, len(self.command_queue._queue))
        item = self.command_queue._queue[0]
        self.assertEqual(0, self.foo._x)
        item.payload.func()
        self.assertEqual(1, self.foo._x)
        self.assertLessEqual(min_time + self.delay_sec, item.time)
        self.assertGreaterEqual(max_time + self.delay_sec, item.time)

    def test_run_in_args(self):
        """Test passing args when running a function on delay."""
        inc = 15
        min_time = time.time()
        self.foo.icpw_run_in(self.delay_sec, self.foo.ordinary_func, inc)
        max_time = time.time()
        self.assertEqual(1, len(self.command_queue._queue))
        item = self.command_queue._queue[0]
        self.assertEqual(0, self.foo._x)
        item.payload.func()
        self.assertEqual(inc, self.foo._x)
        self.assertLessEqual(min_time + self.delay_sec, item.time)
        self.assertGreaterEqual(max_time + self.delay_sec, item.time)

    def test_run_in_kwargs(self):
        """Test passing kwargs when running a function on delay."""
        inc = -2
        min_time = time.time()
        self.foo.icpw_run_in(self.delay_sec, self.foo.ordinary_func, inc=inc)
        max_time = time.time()
        self.assertEqual(1, len(self.command_queue._queue))
        item = self.command_queue._queue[0]
        self.assertEqual(0, self.foo._x)
        item.payload.func()
        self.assertEqual(inc, self.foo._x)
        self.assertLessEqual(min_time + self.delay_sec, item.time)
        self.assertGreaterEqual(max_time + self.delay_sec, item.time)

class ServerEndpointBaseCommandTester(unittest.TestCase):

    def setUp(self):
        class Endpoint(ServerEndpointBase):
            def __init__(self, *args):
                super().__init__(*args)
                self._x = 0

            @icpw_command
            def do_stuff(self, x: Int64):
                self._x = x
        self.cls = Endpoint

    def _get_command_type(self, name):
        return self.cls.__dict__[name].type

    def test_command_network(self):
        """Test running a command received from the network."""
        foo = self.cls(GROUPID)
        cmd_type = self._get_command_type('do_stuff')
        exp_value = -578
        args = cmd_type({'x': exp_value})
        foo.icpw_update_metric('do_stuff', args)
        self.assertEqual(foo._x, exp_value)

    def test_command_local(self):
        """Test running a command locally."""
        foo = self.cls(GROUPID)
        exp_value = -578
        foo.do_stuff(exp_value)
        self.assertEqual(foo._x, exp_value)

    def test_command_different_name(self):
        """Test running a command with a different network name."""
        command_name = 'Command/Do Stuff'
        class EndpointChild(self.cls):
            @icpw_command(name=command_name)
            def do_stuff(self, x: Int64):
                self._x = x
        self.cls = EndpointChild
        foo = self.cls(GROUPID)
        exp_value = 7
        foo.do_stuff(exp_value)
        self.assertEqual(foo._x, exp_value)
        exp_value = -7
        cmd_type = self._get_command_type('do_stuff')
        args = cmd_type({'x': exp_value})
        foo.icpw_update_metric(command_name, args)
        self.assertEqual(exp_value, foo._x)

        with self.assertRaises(IcypawException):
            foo.icpw_update_metric('do_stuff', args)

    def test_command_default_args(self):
        """Test that the type of a command contains default arguments."""
        def_x = 872
        class EndpointChild(self.cls):
            @icpw_command
            def do_stuff(self, x: Int64 = def_x):
                pass
        self.cls = EndpointChild
        cmd_type = self._get_command_type('do_stuff')
        obj = cmd_type()
        self.assertEqual(obj.x, def_x)

class ServerEndpointBaseThreadlockTester(unittest.TestCase):
    """Tests for locking metrics to a specific thread to prevent race conditions."""

    def set_from_new_thread(self, endpoint, metric_name, value):
        t = threading.Thread(target=setattr, args=(endpoint, metric_name, value))
        t.start()
        t.join()

    def test_no_lock(self):
        """Test accessing a metric from another thread without locking
        them."""
        class Endpoint(ServerEndpointBase):
            x = Metric(Int64)
        foo = Endpoint(GROUPID)
        exp_value = 77
        self.set_from_new_thread(foo, 'x', exp_value)
        self.assertEqual(foo.x, exp_value)

    def lock_from_new_thread(self, endpoint):
        t = threading.Thread(target=endpoint.icpw_assign_to_current_thread)
        t.start()
        t.join()

    def test_lock(self):
        """Test setting a metric that was locked in a different thread."""
        class Endpoint(ServerEndpointBase):
            x = Metric(Int64)
        foo = Endpoint(GROUPID)
        self.lock_from_new_thread(foo)
        with self.assertRaises(IcypawException):
            foo.x = 0

class ServerEndpointBaseSignatureTester(unittest.TestCase):

    def test_signature(self):
        """Test getting the types of metrics and commands from an endpoint
        class."""

        y_name = 'Y Metric'

        cmd_name = 'Long Command'

        class Foo(Struct):
            w = Field(Int64)

        class Endpoint(ServerEndpointBase):
            x = Metric(Int64)
            y = Metric(Foo, name=y_name)

            @icpw_command
            def do_stuff(self, x: Int64):
                pass

            @icpw_command(name=cmd_name)
            def other_stuff(self, z: Int64):
                pass

        exp_signature = {
            'metrics': {
                'x': Int64,
                y_name: Foo,
            },
            'commands': {
                'do_stuff': Endpoint.__dict__['do_stuff'].type,
                cmd_name: Endpoint.__dict__['other_stuff'].type,
            }
        }
        act_signature = Endpoint.icpw_signature()

        self.assertEqual(exp_signature, act_signature)

class InheritanceTester(unittest.TestCase):

    def test_inherit_metric(self):
        """Test that an endpoint derived from another endpoint inherits its
        metrics."""

        class Base(ServerEndpointBase):
            x = Metric(Int64)

        class Foo(Base):
            pass

        foo = Foo(GROUPID)
        self.assertEqual(foo.x, Int64().icpw_value)
        self.assertIn('x', foo.icpw_signature()['metrics'])
        self.assertIn('x', foo.icpw_all_metrics())

    def test_inherit_timer(self):
        """Test that a derived endpoint inherits timers."""

        class Base(ServerEndpointBase):
            @icpw_timer(5)
            def timer(self):
                self._x = 0

        class Foo(Base):
            pass

        foo = Foo(GROUPID)

        self.assertTrue(hasattr(foo, 'timer'))

        queue = MockQueue()
        foo.icpw_register_command_queue(queue)

        self.assertEqual(len(queue._queue), 1)

        foo.timer()
        self.assertEqual(0, foo._x)

    def test_inherit_trigger(self):
        """Test that a derived endpoint inherits triggers."""

        class Base(ServerEndpointBase):
            @icpw_trigger
            def trigger(self, x):
                self._x = x

        class Foo(Base):
            pass

        foo = Foo(GROUPID)
        self.assertTrue(hasattr(foo, 'trigger'))

        queue = MockQueue()
        foo.icpw_register_command_queue(queue)

        exp_value = 42

        foo.trigger(exp_value)
        self.assertFalse(hasattr(foo, '_x'))

        queue._queue[0].payload.func()

        self.assertEqual(foo._x, exp_value)

    def test_inherit_command(self):
        """Test that a derived endpoint inherits commands."""

        class Base(ServerEndpointBase):
            @icpw_command
            def do_work(self, x: Int64):
                self._x = x

        class Foo(Base):
            pass

        foo = Foo(GROUPID)

        self.assertIn('do_work', foo.icpw_signature()['commands'])

##
# Helper classes
#

class MockQueue:

    def __init__(self):
        self._queue = []

    def put(self, value):
        self._queue.append(value)

    def get(self):
        return self._queue.pop(0)
