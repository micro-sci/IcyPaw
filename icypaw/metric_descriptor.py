# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Implement metric descriptors. These provide a convenient interface
to Node and Device metrics with customizable behavior."""

import threading

from .types import is_icypaw_type_annotation, BooleanWithMetadata
from .exceptions import IcypawException
from .descriptor import get_object, iter_objects, iter_objects_from_type
from . import tahu_interface as ti

class Metric:
    """A class following the Python descriptor protocol that provides easy
    access to a metric. It allows the user to set up hooks to
    customize the external API while automatically providing a simple,
    property-like API internally.

    """

    def __init__(self, type_, name=None, read_only=False, initial=None, properties=None):
        """Create a new Metric in the body of a Node or Device.

        type_ -- The IcypawType for this metric.

        name -- The name this metric will be published as externally. If not
        given, use the same name as the variable this is stored under.

        read_only -- Whether this variable is read-only for clients on the
        network. The metric is always writable within the Node or Device. This
        value is also used to set the ``Writable`` property of this metric,
        which may be overridden by ``properties``

        initial -- An initial value to make all instances of this metric.

        properties -- A dict of properties to associate with this metric.

        """

        if not is_icypaw_type_annotation(type_):
            raise TypeError(f'type {type_} is not an IcypawType')

        self._type = type_
        self._name = str(name) if name is not None else None
        self._owner_name = None
        self._read_only = bool(read_only)
        self._initial = initial

        # Merge default metric properties with arguments
        self._properties = {
            'Writable': not self._read_only
        }
        if properties:
            self._properties.update(properties)

        self._net_hook = None

    ##
    # Public methods
    #

    @property
    def name(self):
        """The full path this metric is known under on the network."""
        return self._name or self._owner_name

    @property
    def owner_name(self):
        """The name this metric is known by internally."""
        return self._owner_name

    @property
    def type(self):
        """The IcypawType subclass representing the type of this metric."""
        return self._type

    @property
    def properties(self):
        """Return a dict of the properties of this metric mapping strings
        to Python types."""
        return dict(self._properties)

    def assign_to_current_thread(self, instance):
        """Make it an error to read or write this metric from any thread other
        than this one."""
        stored_metric = self._get_stored_metric(instance)
        stored_metric.assign_to_current_thread()

    def get(self, instance):
        """Return the IcypawType object stored."""
        return self._get_value(instance)

    def is_historical(self, instance):
        """Return whether the value as currently stored is considered
        historical."""

        return self._get_stored_metric(instance).is_historical

    def is_transient(self, instance):
        """Return whether the value as currently stored is considered
        transient."""

        return self._get_stored_metric(instance).is_transient

    def is_null(self, instance):
        """Return whether the value as currently stored is null."""

        return self._get_value(instance) is None

    def get_network(self, instance):
        """Return the value according to the user's customizations. If a
        get_hook has been provided, use it. Otherwise return the value
        itself. Unlike __get__, this returns the wrapped Icypaw
        object.

        """

        # Note: We eventually will want to add some sort of hook here,
        # but prematurely adding features here was causing problems so
        # it's not implemented.

        icypaw_value = self._get_value(instance)
        return icypaw_value

    def set_network(self, instance, value):
        """Set the value according to the user's customizations. If a net_hook
        has been provided, use it. Otherwise set the value
        directly. This accepts a wrapped IcypawType value but unwraps
        it when being sent to the set hook.

        Additionally, if this Metric has Template type, this will
        merge the value in, unlike the direct setter which will
        overwrite all previous values.

        """

        if self._read_only:
            raise IcypawException(f"Attempting to set read-only metric {self._name}")

        if not isinstance(value, self._type):
            raise IcypawException(f"Metric set with wrong type, expected {self._type} got {type(value)}")

        is_historical = False
        is_transient = False

        if self._net_hook:
            pyvalue = self._net_hook(instance, value.to_pseudopython())
            if pyvalue is None:
                raise IcypawException("net_hook did not return a value")
            icypaw_value = self._type(pyvalue)
            self._set_value(instance, icypaw_value, is_historical, is_transient)
        else:
            self._set_value(instance, value, is_historical, is_transient)

    def tahu_metric(self, instance, with_properties=False):
        """Construct a Tahu Metric representation of this metric."""
        tahu_metric = ti.new_metric()
        tahu_metric.name = str(self.name).encode()

        if not self.is_null(instance):
            self.get(instance).set_in_metric(tahu_metric)
        else:
            tahu_metric.is_null = True

        if self.is_historical(instance):
            tahu_metric.is_historical = True

        if self.is_transient(instance):
            tahu_metric.is_transient = True

        if with_properties:
            self._set_properties(tahu_metric, self._properties)

        return tahu_metric

    def delete_metric(self, instance):
        """Delete this metric and its stored data from the given instance."""
        delattr(type(instance), self._owner_name)
        delattr(instance, self._store_name)

    ##
    # Decorators
    #

    def net_hook(self, func):
        """Define the given function as the network set hook for this metric. If
        not provided, the value is set directly.

        net_hook is expected to be a method in the endpoint class. It must
        return the value it wants set. In this way it differs from Python
        property setters which expect the user to store the value themselves.

        """

        if self._read_only:
            raise IcypawException("net_hook cannot be used for a read-only metric")

        self._net_hook = func
        return self

    ##
    # Descriptor protocol methods
    #

    def __get__(self, instance, owner):
        """Return the value of this metric. This is used locally on the Node
        or Device. Return a value that acts like a native Python
        value.

        """

        if not self.is_null(instance):
            return self._get_value(instance).to_pseudopython()
        else:
            return None

    def __set__(self, instance, value):
        """Set the value of this metric, performing type checking and possible
        conversion. This is used locally on the Node or Device.

        """

        value, is_historical, is_transient, is_null = self._unwrap_user_value(value)

        if is_null:
            icypaw_value = None
        else:
            icypaw_value = self._type(value)
        self._set_value(instance, icypaw_value, is_historical, is_transient)

    def __set_name__(self, owner, name):
        """Set the network name if one has not already been given."""
        self._owner_name = name

        # If the hook has a different name from what this descriptor
        # is registered with, also register the hook as its own method
        # so that it may be used directly.

        if self._net_hook and self._net_hook.__name__ != name:
            setattr(owner, self._net_hook.__name__, self._net_hook)

    ##
    # Private methods
    #

    def _get_value(self, instance):
        """Retrieve the value stored in the instance."""
        stored_metric = self._get_stored_metric(instance)
        return stored_metric.stored_value

    def _set_value(self, instance, value, is_historical, is_transient):
        """Store the given Icypaw value in the instance."""
        stored_metric = self._get_stored_metric(instance)
        stored_metric.stored_value = value
        stored_metric.is_historical = is_historical
        stored_metric.is_transient = is_transient

    def _get_stored_metric(self, instance):
        stored_metric = getattr(instance, self._store_name, None)
        if stored_metric is None:
            stored_metric = StoredMetric(self._type(self._initial))
            setattr(instance, self._store_name, stored_metric)
        return stored_metric

    @property
    def _store_name(self):
        """Return the name we use to store the actual value in the instance."""
        return f'__icypaw_{self._owner_name}__'

    @classmethod
    def _set_properties(cls, tahu_metric, property_dict):
        """Apply properties from a python dict to a tahu metric.

        Certain metric properties may have ICPW semantics which require special
        handling.
        """
        props = ti.PropertyDict(tahu_metric.properties)
        property_dict = property_dict.copy()

        #'Low' and 'High' limits must be of the same type as the metric itself
        for limit_key in ('Low', 'High'):
            value = property_dict.pop(limit_key, None)
            if value is not None:
                props[limit_key] = ti.property_value(value, tahu_metric.datatype)

        # Add all the rest of the metrics
        props.update(property_dict)

    def _unwrap_user_value(self, value):
        """Given a value that may have optional metadata wrapped around it,
        return the value and the value for any metadata.

        return (value, is_historical, is_transient, is_null)

        """

        # Note that we allow null values to be historical or
        # transient, even if I don't have a good use case for this.

        if isinstance(value, BooleanWithMetadata):
            is_historical = value.is_historical
            is_transient = value.is_transient
            value = value.value
        else:
            is_historical = False
            is_transient = False

        is_null = (value is None)

        return value, is_historical, is_transient, is_null

def get_metric_object(inst, name):
    """Extract a metric object from an instance, bypassing the normal
    descriptor protocol."""

    return get_object(inst, name, Metric)

def iter_metric_objects(inst):
    """Iterate over all Metric objects accessible in an instance."""

    return iter_objects(inst, Metric)

def iter_metric_objects_from_type(cls):
    """Iterate over all Metric objects accessible from the give class."""
    return iter_objects_from_type(cls, Metric)

class StoredMetric:
    """A metric along with some metadata stored in the Node object."""

    def __init__(self, value):
        self._value = value
        self._thread_id = None
        self.is_historical = False
        self.is_transient = False
        self.is_null = False

    @property
    def stored_value(self):
        self._check_thread()
        return self._value

    @stored_value.setter
    def stored_value(self, value):
        self._check_thread()
        self._value = value

    def assign_to_current_thread(self):
        """Assign this metric to the current thread. This will ensure that
        future access only occurs from this thread."""
        self._thread_id = threading.get_ident()

    def _check_thread(self):
        """Validate that this thread is being called from the assigned thread
        if one was assigned."""
        if self._thread_id is not None and self._thread_id != threading.get_ident():
            raise IcypawException('Metric called from the wrong thread')
