# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Unit tests for the icpw_command decorator."""

import unittest

from .command_descriptor import icpw_command, get_command_object
from .types import Int64, Int32, String, Struct, Field, Boolean
from .exceptions import IcypawException

class CommandDescriptorTester(unittest.TestCase):

    def test_call_name(self):
        class Foo:
            @icpw_command
            def do_stuff(self):
                pass

        foo = Foo()

        self.assertEqual(get_command_object(foo, 'do_stuff').name, 'do_stuff')

    def test_call_local(self):
        """Test creating a command on a method and calling it like a normal
        method."""

        class Foo:
            @icpw_command
            def do_stuff(self, x: Int64):
                self.x = x

        foo = Foo()

        exp_value = 7
        foo.do_stuff(exp_value)
        act_value = foo.x

        self.assertEqual(exp_value, act_value)

    def test_call_local_default_args(self):
        """Test calling a command locally with default arguments."""

        def_y = "abc"

        class Foo:
            @icpw_command
            def do_stuff(self, x: Int64, y: String = def_y):
                self.x = x
                self.y = y

        foo = Foo()

        exp_x = 7
        exp_y = "hello"

        foo.do_stuff(exp_x, y=exp_y)

        act_x = foo.x
        act_y = foo.y

        self.assertEqual(exp_x, act_x)
        self.assertEqual(exp_y, act_y)

        foo.do_stuff(exp_x)

        act_x = foo.x
        act_y = foo.y

        self.assertEqual(exp_x, act_x)
        self.assertEqual(def_y, act_y)

    def test_call_network(self):
        """Test calling a command via the network interface."""

        class Foo:
            @icpw_command
            def do_stuff(self, x: Int64):
                self.x = x

        class do_stuff(Struct):
            network_name = 'do_stuff'
            x = Field(Int64)

        foo = Foo()

        exp_value = -42
        icypaw_arg = do_stuff({'x': exp_value})
        get_command_object(foo, 'do_stuff').run_network(foo, icypaw_arg)

        self.assertEqual(foo.x, exp_value)

    def test_call_network_default(self):
        """Test calling a command via the network interface with a default
        value."""

        def_y = "abc"

        class Foo:
            @icpw_command
            def do_stuff(self, x: Int64, y: String = def_y):
                self.x = x
                self.y = y

        exp_x = 42
        exp_y = "def"

        class do_stuff(Struct):
            network_name = 'do_stuff'
            x = Field(Int64)
            y = Field(String, default=def_y)

        foo = Foo()

        icypaw_arg = do_stuff({'x': exp_x, 'y': exp_y})

        get_command_object(foo, 'do_stuff').run_network(foo, icypaw_arg)

        self.assertEqual(foo.x, exp_x)
        self.assertEqual(foo.y, exp_y)

        icypaw_arg = do_stuff({'x': exp_x})
        get_command_object(foo, 'do_stuff').run_network(foo, icypaw_arg)

        self.assertEqual(foo.x, exp_x)
        self.assertEqual(foo.y, def_y)

    def test_call_bad_args(self):
        """Test calling a command via the network interface with bad
        arguments."""

        class Foo:
            @icpw_command
            def do_stuff(self, x: Int64):
                pass

        class do_stuff(Struct):
            network_name = 'do_stuff'
            x = Field(Int32)

        foo = Foo()

        icypaw_arg = do_stuff({'x': 5})

        with self.assertRaises(IcypawException):
            get_command_object(foo, 'do_stuff').run_network(foo, icypaw_arg)

    def test_scalar_one_arg_local(self):
        """Test calling a command defined using a scalar single argument."""

        class Foo:
            @icpw_command(use_template=False)
            def do_stuff(self, x: Int64):
                self.x = x

        foo = Foo()

        exp_value = 7
        foo.do_stuff(exp_value)
        act_value = foo.x

        self.assertEqual(exp_value, act_value)

    def test_scalar_one_arg_network(self):
        """Test calling a command defined using a scalar single argument."""

        class Foo:
            @icpw_command(use_template=False)
            def do_stuff(self, x: Int64):
                self.x = x

        foo = Foo()

        exp_value = -42
        icypaw_arg = Int64(exp_value)
        get_command_object(foo, 'do_stuff').run_network(foo, icypaw_arg)

        self.assertEqual(foo.x, exp_value)

    def test_scalar_no_args_network(self):
        """Test calling a command defined with no arguments."""

        exp_value = 55

        class Foo:
            @icpw_command(use_template=False)
            def do_stuff(self):
                self.x = exp_value

        foo = Foo()

        icypaw_arg = Boolean(True)
        get_command_object(foo, 'do_stuff').run_network(foo, icypaw_arg)

        self.assertEqual(foo.x, exp_value)
