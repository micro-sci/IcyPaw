# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""The Icypaw node base class."""

from .server_endpoint_base import ServerEndpointBase
from .engine_queue import (RegisterDeviceQueueItem, UnregisterDeviceQueueItem,
                           NodeRebirthQueueItem)

class ServerNode(ServerEndpointBase):
    """The base class for node servers. By subclassing from this class and
    annotating metrics and functions, much of the machinery of
    communicating with the Icypaw system is handled automatically."""

    def __init__(self, group_id, edge_node_id, device_classes=None):
        """Initialize properties common to all Nodes on the Icypaw network.

        group_id -- The group this node will belong to.

        edge_node_id -- The unique ID (within this group) for this node.

        device_classes -- A list of class objects (derived from
        ServerDevice) for all devices that may be attached to this
        node.

        """

        super().__init__(group_id)
        self._edge_node_id = edge_node_id
        self._device_classes = tuple(device_classes or ())

        if not all(issubclass(dev_class, ServerEndpointBase) for dev_class in self._device_classes):
            raise TypeError('device_classes must be derived from ServerEndpointBase')

    @property
    def edge_node_id(self):
        return self._edge_node_id

    @property
    def device_classes(self):
        """Return a list of all possible device classes this node may
        activate."""
        return self._device_classes

    def icpw_register_device(self, device):
        """Used by derived classes to tell the engine to start accepting
        incoming data for the given device."""

        if not isinstance(device, self._device_classes):
            raise TypeError(f'Device {device} is not one of the classes given at construction')

        item = RegisterDeviceQueueItem(self, device)
        self.icpw_enqueue_command(item)

    def icpw_unregister_device(self, device):
        """Used by derived classes to tell the engine that a device is
        offline. The device may be brought online again later using
        icpw_register_device."""

        item = UnregisterDeviceQueueItem(self, device)
        self.icpw_enqueue_command(item)

    def icpw_rebirth(self):
        """Issue a new birth certificate with updated values of all metrics."""

        item = NodeRebirthQueueItem()
        self.icpw_enqueue_command(item)

    ##
    # Callbacks used by the engine
    #

    def on_connect(self, engine):
        """Callback called by the engine between connecting and starting the
        event loop."""
        pass

    def on_shutdown(self, engine):
        """Callback called by the engine just before disconnecting when an
        explicit shutdown is caused. This will therefore not always be
        called.

        """
        pass

    def on_disconnect(self, engine):
        """Callback called by the engine just after disconnecting."""
        pass
