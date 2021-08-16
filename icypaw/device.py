# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""The Icypaw device base class."""

from .server_endpoint_base import ServerEndpointBase

class ServerDevice(ServerEndpointBase):
    """The base class for device servers. By subclassing from this class and
    annotating metrics and functions, much of the machinery of
    communicating with the Icypaw system is handled automatically."""

    def __init__(self, group_id, edge_node_id, device_id):
        """Initialize properties common to all Devices on the Icypaw network.

        group_id -- The group this node will belong to.

        edge_node_id -- The unique ID (within this group) for this
        device's node.

        device_id -- The unique ID (among this node's devices) for
        this device.

        """

        super().__init__(group_id)
        self._edge_node_id = edge_node_id
        self._device_id = device_id

    @property
    def edge_node_id(self):
        return self._edge_node_id

    @property
    def device_id(self):
        return self._device_id
