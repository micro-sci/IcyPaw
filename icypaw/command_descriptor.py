# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Implement a decorator and its backend for commands sent to a node
or device."""

import inspect
import functools

import icypaw.types as types
from .types import (is_icypaw_type_annotation, make_struct, Field,
                    is_icypaw_scalar_type_annotation)
from .exceptions import IcypawException
from .descriptor import get_object, iter_objects, iter_objects_from_type
from . import conventions, tahu_interface as ti

class icpw_command:
    """A class following the Python descriptor protocol that wraps a
    command."""

    ##
    # Constructor and initialization
    #

    def __init__(self, name=None, properties=None, use_template=True):
        """Return a new descriptor that has worked out a type and name for the
        given command."""

        if callable(name):
            func = name
            name = func.__name__
        else:
            func = None
            name = name

        self._func = func
        self._name = name
        self._use_template = use_template
        self._type = self._make_type() if func else None

        # Merge default command properties with arguments
        self._properties = {
            'Writable': True,
            'ICPWCommand': True
        }
        if properties:
            self._properties.update(properties)

    def __call__(self, func):
        """Handle the case where the user provides a name in the decorator."""

        assert self._func is None
        self._func = func
        if self._name is None:
            self._name = func.__name__
        self._type = self._make_type()
        return self

    def _make_type(self):
        """Create the type object representing the arguments to this
        command. If use_template is True, this will always be a
        Template, even with just one argument. This allows the
        argument to be optional. With use_template set to False a
        scalar type will be used representing the lone argument. If
        there are zero arguments, a dummy boolean argument will be
        created.

        """

        if self._use_template:
            icypaw_type = self._make_template_type()
        else:
            icypaw_type = self._make_scalar_type()

        return icypaw_type

    def _make_template_type(self):
        """Return the type of this command, gleaned from its signature, as a
        template."""

        sig = inspect.signature(self._func)

        field_dict = {}

        for field_name, param in sig.parameters.items():
            if field_name == 'self':
                continue
            if param.annotation == inspect.Parameter.empty:
                raise IcypawException(f"Field {field_name} is unannotated in {self._func.__name__}")
            if not is_icypaw_type_annotation(param.annotation):
                raise IcypawException(f"Field {field_name}'s annotation is not an IcypawType")
            field_default = param.default if param.default != inspect.Parameter.empty else None
            field_dict[field_name] = Field(param.annotation, name=field_name, default=field_default)

        icypaw_type = make_struct(self._name, field_dict)

        return icypaw_type

    def _make_scalar_type(self):
        """Return the type of this command as a single scalar. The signature
        must have zero or one parameters.

        Note that having a single argument with a Template type would
        be indistinguishable from having multiple parameters and
        therefore is not allowed.

        """

        sig = inspect.signature(self._func)

        parameters = [param for name, param in sig.parameters.items()
                      if name != 'self']

        if len(parameters) == 0:
            icypaw_type = types.Boolean
        elif len(parameters) == 1:
            param = parameters[0]
            if param.annotation == inspect.Parameter.empty:
                raise IcypawException(f"Argument is unannotated in {self._func.__name__}")
            if not is_icypaw_type_annotation(param.annotation):
                raise IcypawException("Argument's annotation is not an IcypawType")
            if not is_icypaw_scalar_type_annotation(param.annotation):
                raise IcypawException("Argument's annotation cannot be a Struct")
            icypaw_type = param.annotation
        else:
            raise IcypawException(f"Cannot create a non-template signature with {len(parameters)} parameters")

        return icypaw_type


    ##
    # Public methods
    #

    @property
    def name(self):
        """Return the name this command is to go by on the network."""
        return self._name

    @property
    def type(self):
        """Return the argument type for this command."""
        return self._type

    def tahu_metric(self, with_properties=False):
        """Construct a Tahu Metric representation of this metric."""
        tahu_metric = ti.new_metric()
        tahu_metric.name = conventions.make_command(self.name).encode()
        self.type().set_in_metric(tahu_metric)

        if with_properties:
            self._set_properties(tahu_metric, self._properties)

        return tahu_metric

    ##
    # Descriptor protocol methods
    #

    def __get__(self, instance, _owner):
        """Simply return the wrapped function. This allows the implementor of
        an endpoint to call a command as though it were a regular
        Python function and bypass all type checking.

        """

        # I feel like there should be a way to have Python do this
        # binding for us, but this will work nonetheless.
        return functools.partial(self._func, instance)

    ##
    # Internal API methods
    #

    def run_network(self, instance, icypaw_arg):
        """Run this command using the IcypawType argument sent in a metric
        over the network."""

        assert is_icypaw_type_annotation(type(icypaw_arg)), f"{icypaw_arg} is not an Icypaw type"

        arglist = []
        argdict = {}

        if is_icypaw_scalar_type_annotation(type(icypaw_arg)):
            sig = inspect.signature(self._func)
            parameters = [param for name, param in sig.parameters.items()
                          if name != 'self']
            if isinstance(icypaw_arg, types.Boolean) and len(parameters) == 0:
                # This is idiomatic for no arguments.
                pass
            else:
                arglist.append(icypaw_arg.to_python())
        else:
            # Stuff Python values from icypaw_arg into argdict
            for name, icypaw_value in icypaw_arg.icpw_value.items():
                if not isinstance(icypaw_value, self._func.__annotations__[name]):
                    raise IcypawException(f"Argument {name} to command {self._func.__name__} has wrong type")
                argdict[name] = icypaw_value.to_python()

        self._func(instance, *arglist, **argdict)

    ##
    # Private methods
    #

    @classmethod
    def _set_properties(cls, tahu_metric, property_dict):
        """Apply properties from a python dict to a tahu metric.

        Certain command properties may have ICPW semantics which require special
        handling.
        """
        props = ti.PropertyDict(tahu_metric.properties)
        property_dict = property_dict.copy()

        # There currently aren't any ICPW semantics we want enforced for
        # commands, but I'm leaving this here for future development

        # Add all properties
        props.update(property_dict)


def get_command_object(inst, name):
    """Extract a command object from an instance, bypassing the normal
    descriptor protocol."""

    return get_object(inst, name, icpw_command)

def iter_command_objects(inst):
    """Iterate over (name, obj) tuples of commands accessible in this
    instance."""

    return iter_objects(inst, icpw_command)

def iter_command_objects_from_type(cls):
    return iter_objects_from_type(cls, icpw_command)
