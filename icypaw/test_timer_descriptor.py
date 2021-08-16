# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Test the icpw_timer decorator."""
import unittest

from .timer_descriptor import icpw_timer, get_timer_object
from .exceptions import IcypawException

class TimerDescriptorTester(unittest.TestCase):

    def setUp(self):
        self.exp_seconds = 2.5

        class Endpoint:
            def __init__(self):
                self.x = None

            @icpw_timer(self.exp_seconds)
            def foo(self):
                self.x = 0

        self.cls = Endpoint

    def test_create_timer(self):
        ep = self.cls()
        act_seconds = get_timer_object(ep, 'foo').seconds
        self.assertEqual(act_seconds, self.exp_seconds)

    def test_call(self):
        """Test calling a timer object directly."""
        ep = self.cls()
        self.assertEqual(ep.x, None)
        ep.foo()
        self.assertEqual(ep.x, 0)

class BadTimerDescriptorTester(unittest.TestCase):

    def test_has_arguments(self):
        """Test that we reject a timer that takes arguments."""
        with self.assertRaises(IcypawException):
            class Endpoint:
                @icpw_timer(1.0)
                def foo(self, x):
                    pass

    def test_zero_time(self):
        """Test that we reject a timer that has a time interval of zero."""
        with self.assertRaises(IcypawException):
            class Endpoint:
                @icpw_timer(0)
                def foo(self):
                    pass

    def test_negative_time(self):
        """Test that we reject a timer that has a negative time interval."""
        with self.assertRaises(IcypawException):
            class Endpoint:
                @icpw_timer(-1)
                def foo(self):
                    pass
