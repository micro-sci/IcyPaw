# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Implement a decorator and its backend for methods on a node or
device that are meant to be run at fixed intervals. This decorator
allows the methods to be run as regular methods from code in the
endpoint as well."""

import functools
import inspect
from .descriptor import get_object, iter_objects, iter_objects_from_type
from .exceptions import IcypawException

class icpw_timer:
    """A class following the Python descriptor protocol that wraps a
    periodically-run method."""

    ##
    # Constructor and initialization
    #

    def __init__(self, seconds):
        """Return a decorator for a method to be run at fixed intervals."""

        self._seconds = float(seconds)
        if self._seconds <= 0:
            raise IcypawException(f"Timer must have positive interval, found {self._seconds}")
        self._func = None

    def __call__(self, func):
        """The call actually used as the decorator"""
        self._func = func

        sig = inspect.signature(func)
        for name, param in sig.parameters.items():
            if name == 'self':
                continue
            if param.default == inspect.Parameter.empty:
                raise IcypawException(f"Timer parameter {name} has no default value")

        return self

    ##
    # Public methods
    #

    @property
    def seconds(self):
        """Return the repeat interval in seconds."""
        return self._seconds

    @property
    def function(self):
        """Return the function that was wrapped. It must still be bound to an
        instance before being called."""
        return self._func

    def bind(self, inst):
        """Return the given function bound to an instance."""
        return functools.partial(self._func, inst)

    ##
    # Descriptor protocol methods
    #

    def __get__(self, instance, _owner):
        """Simply return the wrapped function. This allows the implementor of
        an endpoint to call a command as though it were a regular
        Python function and bypass all type checking.

        """

        return self.bind(instance)

def get_timer_object(inst, name):
    """Extract a timer object from an instance, bypassing the normal
    descriptor protocol."""

    return get_object(inst, name, icpw_timer)

def iter_timer_objects(inst):
    return iter_objects(inst, icpw_timer)

def iter_timer_objects_from_type(cls):
    return iter_objects_from_type(cls, icpw_timer)
