# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Unit tests for the trigger descriptor decorator."""

import time
import unittest

from .trigger_descriptor import icpw_trigger
from .engine_queue import ScheduleQueueItem

class MockQueue:

    def __init__(self):
        self._queue = []

    def put(self, item):
        self._queue.append(item)

    def get(self):
        return self._queue.pop(0)

class TriggerDescriptorTester(unittest.TestCase):

    def test_call(self):
        """Test calling a method decorated with the icpw_trigger decorator."""

        class Endpoint:

            def __init__(self, queue):
                self.queue = queue

            def icpw_enqueue_command(self, item):
                self.queue.put(item)

            @icpw_trigger
            def foo(self, x, y=None):
                self.x = x
                self.y = y

        queue = MockQueue()

        exp_x = 5
        exp_y = 13

        ep = Endpoint(queue)

        before_time = time.time()
        ep.foo(exp_x, y=exp_y)
        after_time = time.time()

        self.assertEqual(1, len(queue._queue))

        item = queue.get()

        self.assertEqual(type(item), ScheduleQueueItem)
        self.assertLessEqual(before_time, item.time)
        self.assertLessEqual(item.time, after_time)

        self.assertFalse(hasattr(ep, 'x'))
        self.assertFalse(hasattr(ep, 'y'))

        item.payload.func()

        self.assertEqual(ep.x, exp_x)
        self.assertEqual(ep.y, exp_y)
