# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""The icpw_trigger class and helper functions. This allows other
threads of operation to safely call methods in Node and Device server
endpoints."""

import functools
from .engine_queue import ScheduleQueueItem
from .descriptor import get_object, iter_objects, iter_objects_from_type

class icpw_trigger:
    """A decorator around methods that will schedule their execution in
    another thread. Triggers may be called from the endpoint's thread,
    but they will still be scheduled for later execution.

    """

    ##
    # Initialization
    #

    def __init__(self, func):
        self._func = func

    ##
    # Descriptor protocol methods
    #

    def __get__(self, instance, _owner):
        """Simply return the wrapped function.

        """

        return self._wrap_function(instance)

    ##
    # Private methods
    #

    def _wrap_function(self, instance):
        """Create a function that schedules the execution of the wrapped
        function."""
        def trigger(*args, **kwargs):
            queue_item = ScheduleQueueItem(functools.partial(self._func, instance,
                                                             *args, **kwargs))
            instance.icpw_enqueue_command(queue_item)
        return trigger

def get_trigger_object(inst, name):
    return get_object(inst, name, icpw_trigger)

def iter_trigger_objects(inst):
    return iter_objects(inst, icpw_trigger)

def iter_trigger_objects_from_type(cls):
    return iter_objects_from_type(cls, icpw_trigger)
