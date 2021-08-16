# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Test features that the ServerNode base class adds on top of the
ServerEndpointBase."""

import unittest

from .server_endpoint_base import ServerEndpointBase
from .node import ServerNode
from .engine_queue import (RegisterDeviceQueueItem, UnregisterDeviceQueueItem,
                           NodeRebirthQueueItem)

class NodeTester(unittest.TestCase):

    def setUp(self):

        class MyDevice(ServerEndpointBase):
            pass

        class MyNode(ServerNode):
            pass

        self.cls = MyNode
        self.dev_cls = MyDevice

        self.exp_group_id = 'group0'
        self.exp_edge_node_id = 'node0'

        self.device_classes = [MyDevice]
        self.node = self.cls(self.exp_group_id, self.exp_edge_node_id, self.device_classes)

    def test_ids(self):
        """Test creating a node object and retrieving its IDs."""

        self.assertEqual(self.node.group_id, self.exp_group_id)
        self.assertEqual(self.node.edge_node_id, self.exp_edge_node_id)

    def test_register_device(self):
        """Test registering a device after setting the command queue."""
        queue = MockQueue()
        self.node.icpw_register_command_queue(queue)
        dev = self.dev_cls(self.exp_group_id)
        self.node.icpw_register_device(dev)
        self.assertEqual(len(queue._queue), 1)
        self.assertTrue(isinstance(queue._queue[0], RegisterDeviceQueueItem))
        self.assertEqual(queue._queue[0].payload.device, dev)
        self.assertEqual(queue._queue[0].payload.node, self.node)

    def test_unregister_device(self):
        """Test unregistering a device after setting the command queue."""
        queue = MockQueue()
        self.node.icpw_register_command_queue(queue)
        dev = self.dev_cls(self.exp_group_id)
        self.node.icpw_unregister_device(dev)
        self.assertEqual(len(queue._queue), 1)
        self.assertTrue(isinstance(queue._queue[0], UnregisterDeviceQueueItem))
        self.assertEqual(queue._queue[0].payload.node, self.node)
        self.assertEqual(queue._queue[0].payload.device, dev)

    def test_preregister_device(self):
        """Test registering a device before setting the command queue."""
        queue = MockQueue()
        dev = self.dev_cls(self.exp_group_id)
        self.node.icpw_register_device(dev)
        self.node.icpw_register_command_queue(queue)
        self.assertEqual(len(queue._queue), 1)
        self.assertTrue(isinstance(queue._queue[0], RegisterDeviceQueueItem))
        self.assertEqual(queue._queue[0].payload.device, dev)
        self.assertEqual(queue._queue[0].payload.node, self.node)

    def test_unpreregister_device(self):
        """Test unregistering a device before setting the command queue."""
        queue = MockQueue()
        dev = self.dev_cls(self.exp_group_id)
        self.node.icpw_register_device(dev)
        self.node.icpw_unregister_device(dev)
        self.node.icpw_register_command_queue(queue)
        self.assertEqual(len(queue._queue), 2)
        self.assertIsInstance(queue._queue[0], RegisterDeviceQueueItem)
        self.assertIsInstance(queue._queue[1], UnregisterDeviceQueueItem)

    def test_register_bad_device(self):
        """Test trying to register a device not on the list."""

        class UnknownDevice(ServerEndpointBase):
            pass

        with self.assertRaises(TypeError):
            self.node.icpw_register_device(UnknownDevice(self.exp_group_id))

    def test_register_nondevice(self):
        """Test trying to register something that isn't a device at all."""
        with self.assertRaises(TypeError):
            self.node.icpw_register_device(1)

    def test_rebirth(self):
        """Test sending a new birth message."""
        queue = MockQueue()
        self.node.icpw_register_command_queue(queue)
        self.node.icpw_rebirth()
        self.assertEqual(len(queue._queue), 1)
        self.assertTrue(isinstance(queue._queue[0], NodeRebirthQueueItem))


##
# Helpers
#

class MockQueue:
    def __init__(self):
        self._queue = []

    def put(self, item):
        self._queue.append(item)

    def get(self):
        return self._queue.pop(0)
