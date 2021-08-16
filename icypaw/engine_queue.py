# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Classes and functions relating to the Queue used to communicate
between Nodes/Devices and the Engine."""

import time

class QueueItem:
    """Base class for items passed to the Engine on its queue."""

    def __init__(self, exec_time, payload):
        self._time = exec_time
        self._payload = payload

    @property
    def time(self):
        return self._time

    @property
    def payload(self):
        return self._payload

    def __lt__(self, other):
        return self.time < other.time

    def __gt__(self, other):
        return self.time > other.time

class ScheduleQueueItem(QueueItem):
    """An item used to communicate to the Engine that we wish to schedule
    the execution of a function."""

    class Payload:
        def __init__(self, func, repeat_sec):
            self.func = func
            self.repeat_sec = repeat_sec

    def __init__(self, func, repeat_sec=None, delay_sec=None, exec_time=None):
        if exec_time is None:
            delay_sec = delay_sec or 0.0
            exec_time = time.time() + delay_sec
        payload = ScheduleQueueItem.Payload(func, repeat_sec)
        super().__init__(exec_time, payload)

class RegisterDeviceQueueItem(QueueItem):
    """An item used to communicate to the Engine that a new device is
    ready to be brought online."""

    class Payload:
        def __init__(self, node, device):
            self.node = node
            self.device = device

    def __init__(self, node, device):
        exec_time = time.time()
        payload = RegisterDeviceQueueItem.Payload(node, device)
        super().__init__(exec_time, payload)

class UnregisterDeviceQueueItem(QueueItem):
    """An item used to communicate to the Engine that a new device is
    ready to be taken offline."""

    class Payload:
        def __init__(self, node, device):
            self.node = node
            self.device = device

    def __init__(self, node, device):
        exec_time = time.time()
        payload = RegisterDeviceQueueItem.Payload(node, device)
        super().__init__(exec_time, payload)

class NodeRebirthQueueItem(QueueItem):
    """An item used to signal to the Engine that the node's NBIRTH
    certificate must be reissued with fresh metrics."""

    class Payload:
        pass

    def __init__(self):
        exec_time = time.time()
        payload = NodeRebirthQueueItem.Payload()
        super().__init__(exec_time, payload)
