# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

from . import conventions
from .exceptions import IcypawException
from .tahu_interface import DataType, read_from_propertyset, is_endpoint_property
from .types import (Field, IcypawType, make_struct, type_from_metric, value_from_metric,
                    merge_with_metric, is_icypaw_type_annotation)


class ClientEndpoint:
    """Store the information about an endpoint independent of the transport protocol."""

    def __init__(self, name, birth_metric=None, templates=None):
        name = ClientEndpointName(name)
        if name.has_wildcard:
            raise ValueError('Endpoint name may not contain wildcards')

        # The full name of this endpoint
        self.name = name

        # A dictionary mapping a command name to ClientEndpointCommand
        self.commands = {}

        # A dictionary mapping a metric name to ClientEndpointMetric
        self.metrics = {}

        # A dictionary mapping endpoint property keys to python values
        self.properties = {}

        # A dictionary mapping a template definition name to its IcypawType.
        self.templates = templates or {}

        self._alias_map = {}

        # Sequence numbers for births and deaths as they are recieved.  Note
        # that bdSeq may not be present in a tahu birth certificate e.g. for
        # DBIRTH or legacy NBIRTH. In this case we fall back to timestamps as a
        # proxy for birth-death sequences
        self._last_birth_seq = None
        self._last_death_seq = None

        if birth_metric is not None:
            self.update_from_tahu_birth(birth_metric, templates=self.templates)

    @property
    def is_online(self):
        """Return whether this endpoint is thought to be online."""
        if self._last_birth_seq is not None:
            # Got a birth cert at some point
            if self._last_death_seq is not None:
                # Got a death cert too, so compare them directly
                # TODO check lifetime sequence consistency (not out-of-order,
                # not comparing a bdSeq to a timestamp, etc)
                return (
                    (self._last_birth_seq > self._last_death_seq)
                    # bdSeq wraps at 256
                    or (self._last_death_seq == 255 and self._last_birth_seq < 255)
                    # Curiously, since we're overloading lifetime sequence
                    # semantics with timestamps, endpoints may be erroneously
                    # reported as online during the first milliseconds of
                    # January 1st, 1970. Time-travelers take note.
                )
            else:
                # No death cert yet, so we're online
                return True
        else:
            # No birth cert, so our actual state is unknown, presumed offline
            return False

    @property
    def is_node(self):
        return self.name.device_id is None

    @property
    def is_device(self):
        return self.name.device_id is not None

    def get_name_from_alias(self, alias):
        return self._alias_map[alias]

    def update_from_tahu_data(self, message):
        """Update the metrics in this Endpoint from the given decoded
        message."""

        for metric in message.metrics:
            metric_name = self._get_metric_name(metric, fill_in=True)

            if metric_name not in self.metrics:
                raise ValueError(f'Metric {metric_name} not registered in endpoint {self.name}')

            self.metrics[metric_name].update_from_tahu(metric)

    def update_from_tahu_birth(self, message, templates=None):
        is_fresh = False

        self.commands = {}
        self.metrics = {}
        self.properties = {}
        command_metrics = {}
        template_metrics = {}

        for metric in message.metrics:
            if conventions.is_template_definition(metric.name):
                name = conventions.make_base_name_from_template_definition(metric.name)
                template_metrics[name] = metric

        self.templates = templates or {}
        self.templates.update(self._make_template_types(template_metrics))

        struct_types = set(self.templates.values())

        for metric in message.metrics:
            if conventions.is_metric(metric.name):
                self.metrics[metric.name] = ClientEndpointMetric.from_metric(
                    metric, is_fresh, struct_types=struct_types)
            if conventions.is_command(metric.name):
                name = conventions.make_base_name_from_command(metric.name)
                if metric.datatype == DataType.Template.value:
                    command_metrics[name] = metric
                else:
                    self.commands[name] = ClientEndpointCommand.from_metric(metric)
            if metric.HasField('alias'):
                self._alias_map[metric.alias] = metric.name
            if is_endpoint_property(metric):
                self.properties[metric.name] = value_from_metric(metric, struct_types=struct_types).to_python()

        for command_name, metric in command_metrics.items():
            template_name = metric.template_value.template_ref
            if template_name not in self.templates:
                raise IcypawException(f'Command {command_name} has unknown type {template_name}')
            self.commands[command_name] = ClientEndpointCommand.from_metrics(
                metric, self.templates[command_name])

        seq = self._get_lifetime_sequence(message)
        self._last_birth_seq = seq if seq is not None else (self._last_birth_seq or 0) + 1

    @classmethod
    def _make_template_types(cls, template_definition_dict):
        """Transform the input template_definitions into IcypawType
        classes. We may have to account for dependencies among the types."""

        ret = {}

        todo = {name: definition for name, definition in template_definition_dict.items()}

        while todo:
            converted = {}
            for name, template_def in todo.items():
                icypaw_type = cls._make_template_type(template_def)
                if icypaw_type is not None:
                    converted[name] = icypaw_type

            if not converted:
                raise IcypawException(f"Could not create types: {list(todo.keys())}")

            for name, icypaw_type in converted.items():
                del todo[name]
                ret[name] = icypaw_type

        assert len(ret) == len(template_definition_dict)

        return ret

    @staticmethod
    def _make_template_type(template_definition):
        """Take a TAHU template definition and create an IcypawType out of
        it."""

        assert conventions.is_template_definition(template_definition.name)

        network_name = conventions.make_base_name_from_template_definition(template_definition.name)

        field_dict = {}

        for metric in template_definition.template_value.metrics:
            field_type = type_from_metric(metric)
            if field_type is None:
                return None
            field_value = field_type()
            field_value.merge_with_metric(metric)
            field_dict[metric.name] = Field(field_type, default=field_value.to_python())

        icypaw_type = make_struct(network_name, field_dict)
        return icypaw_type

    def update_from_tahu_death(self, message):
        seq = self._get_lifetime_sequence(message)
        self._last_death_seq = seq if seq is not None else (self._last_death_seq or 0) + 1

    def _get_lifetime_sequence(self, message):
        """Determine a suitable lifetime sequence number from a tahu birth/death message.

        Ideally this will be the bdSeq metric of the message. Since the bdSeq number
        is not guaranteed to be present, e.g. for node architectures that don't
        support it or all devices, we fall back to using timestamps as a proxy.

        Timestamps should always be present in a Sparkplug-B payload. A returned
        value of ``None`` indicates that the given message was not timestamped for
        whatever reason, and that the caller should track endpoint lifetime
        sequences manually.
        """

        for metric in message.metrics:
            if self._get_metric_name(metric) == 'bdSeq':
                return metric.long_value

        if message.HasField('timestamp'):
            return message.timestamp

    def _get_metric_name(self, metric, fill_in=False):
        """Retrieve the name of this metric. This may involve looking up the
        name in the alias map.

        metric -- A protobuf metric.

        fill_in -- If True fill in the name in the metric if we have
        to look it up in the alias map.

        """

        if metric.HasField('name'):
            metric_name = metric.name
        else:
            if not metric.HasField('alias'):
                raise ValueError('Malformed message has metric with no name and no alias')
            metric_name = self._alias_map[metric.alias]
            if fill_in:
                metric.name = metric_name

        return metric_name

class ClientEndpointName:
    """Store the components of an endpoint's name and handle conversion
    from a number of input styles."""

    def __init__(self, single_arg=None, group_id=None, edge_node_id=None, device_id=None):
        """Create a new endpoint name from either a single argument or the
        components of the name.

        single_arg -- An argument that is parsed into the components
        of this name. See below for format examples.

        group_id -- The name of the group this endpoint belongs to.

        edge_node_id -- The name of the Node.

        device_id -- The name of the device within the node.

        single_arg examples:

        Examples are of the form <Python object> -> (group_id, edge_node_id, device_id)

        # Single string is the Node ID
        'A' -> (None, 'A', None)

        # Two strings connected by a slash are Node ID and Device ID
        'A/B' -> (None, 'A', 'B')

        # Three strings connected by two slashes are Group ID, Node ID, and Device ID
        'A/B/C' -> ('A', 'B', 'C')

        # Two strings with a trailing slash are a Group ID and Node ID
        'A/B/' -> ('A', 'B', None)

        # A two-tuple is a Node ID and Device ID
        ('A', 'B')

        # A three-tuple is a Group ID, Node ID, and Device ID
        ('A', 'B', 'C')

        # Use None for the Device ID to get Group ID and Node ID
        ('A', 'B', None)

        It is not recommended that you mix single_arg with the other
        inputs but if so the *_id inputs take precedence.

        A wildcard may be used in place of any component of the
        name. This allows this name to be used as a search filter. A
        wildcard may be the special `any` value (NOT a string 'any'
        which would be a valid name), or one of the characters [*, #,
        +], all of which are treated identically (this differs from
        the MQTT spec).

        """

        self._group_id = None
        self._edge_node_id = None
        self._device_id = None

        self._parse_argument(single_arg)

        self._group_id = group_id or self._group_id
        self._edge_node_id = edge_node_id or self._edge_node_id
        self._device_id = device_id or self._device_id

        self._group_id = self._validate_and_normalize(self._group_id)
        self._edge_node_id = self._validate_and_normalize(self._edge_node_id)
        self._device_id = self._validate_and_normalize(self._device_id)

    def __str__(self):
        if self._device_id is None:
            string = f"{self.group_id_str}/{self.edge_node_id_str}/"
        else:
            string = f"{self.group_id_str}/{self.edge_node_id_str}/{self.device_id_str}"
        return string

    def __repr__(self):
        return f"ClientEndpointName({self})"

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return (self._group_id == other._group_id
                and self._edge_node_id == other._edge_node_id
                and self._device_id == other._device_id)

    @property
    def is_node(self):
        """Return whether this names a Node endpoint."""
        return self._device_id is None

    @property
    def is_device(self):
        """Return whether this names a Device endpoint."""
        return self._device_id is not None

    @property
    def has_wildcard(self):
        return (self._group_id is any or self._edge_node_id is any or self._device_id is any)

    @property
    def group_id(self):
        """Return the group_id as a string, or None if not provided, or any if
        a wildcard was given."""
        return self._group_id

    @property
    def group_id_str(self):
        if self._group_id is any:
            return '+'
        if self._group_id is None:
            return ''
        return self._group_id

    @property
    def edge_node_id(self):
        """Return the edge_node_id as a string, or None if not provided, or
        any if a wildcard was given."""
        return self._edge_node_id

    @property
    def edge_node_id_str(self):
        if self._edge_node_id is any:
            return '+'
        if self._edge_node_id is None:
            return ''
        return self._edge_node_id

    @property
    def device_id(self):
        """Return the device_id as a string, or None if not provided, or any if
        a wildcard was given."""
        return self._device_id

    @property
    def device_id_str(self):
        if self._device_id is any:
            return '+'
        if self._device_id is None:
            return ''
        return self._device_id

    def match(self, other):
        """Attempt to match the object against this endpoint name. Accepts any
        argument that is accepted by the constructor. Other must be an
        actual endpoint name, with no wildcards, while self may
        obviously have wildcards. If self has no wildcards, then match
        is equivalent to ==.

        """

        other_name = ClientEndpointName(other)
        if other_name.has_wildcard:
            raise ValueError('May only match against concrete endpoint name')
        return self._match_component(self._device_id, other.device_id) and\
            self._match_component(self._edge_node_id, other.edge_node_id) and\
            self._match_component(self._group_id, other.group_id)

    def _match_component(self, this_component, other_component):
        """Return if a component (device/node/group name) matches."""
        if this_component == other_component:
            return True
        if this_component is None or other_component is None:
            return False
        if this_component is any:
            return True
        return False

    def _parse_argument(self, arg):
        if arg is None:
            return

        if all(hasattr(arg, attr) for attr in ['group_id', 'edge_node_id', 'device_id']):
            self._group_id = arg.group_id
            self._edge_node_id = arg.edge_node_id
            self._device_id = arg.device_id
            return

        if isinstance(arg, str):
            arg = arg.strip()
            trailing_slash = arg.endswith('/')
            fields = arg.split('/')
            if trailing_slash:
                fields[-1] = None
            arg = tuple(fields)

        if not isinstance(arg, tuple):
            raise TypeError('ClientEndpointName argument must be a string or tuple')

        if len(arg) == 1:
            self._edge_node_id, = arg
        elif len(arg) == 2:
            self._edge_node_id, self._device_id = arg
        elif len(arg) == 3:
            self._group_id, self._edge_node_id, self._device_id = arg
        else:
            raise ValueError('ClientEndpointName argument must have 1, 2, or 3 components')

    def _validate_and_normalize(self, field):
        """Given one of the name components, check for invalid characters and
        normalize wildcards."""

        if field is None or field is any:
            return field

        wildcards = ['#', '+', '*']

        if field in wildcards:
            return any

        if any(forbidden_char in field for forbidden_char in wildcards):
            raise ValueError('ClientEndpointName component may not contain a wildcard unless it is the only character')

        return field

class ClientEndpointMetricBase:
    """Base class for metric-based classes such as the
    ClientEndpointMetric and ClientEndpointCommand."""

    def __init__(self, name, value, icypaw_type, alias, metric_properties):
        """Create a new metric. This class is intended to be created by the
        framework and passed to the user.

        name -- The string name of the metric.

        value -- The value of the metric. This may be None for a null
        metric, an IcypawType, or a Python value that is convertible
        to the given type.

        icypaw_type -- The type for this metric. If value is not of
        this type, it will be converted internally to it.

        alias -- Another value under which this metric may be known.

        """

        if not is_icypaw_type_annotation(icypaw_type):
            raise TypeError('icypaw_type must be a subclass of IcypawType')

        if value is None:
            icypaw_value = None
        elif not isinstance(value, IcypawType):
            icypaw_value = icypaw_type(value)
        else:
            if not isinstance(value, icypaw_type):
                raise TypeError('value is not the correct type')
            icypaw_value = value

        self._name = str(name)
        # Even though we return the unwrapped Python object to the
        # user, store the Icypaw object internally.
        self._value = icypaw_value
        self._type = icypaw_type
        self._alias = alias
        self._properties = read_from_propertyset(metric_properties)

    @staticmethod
    def _extract_alias(metric):
        if metric.HasField('alias'):
            return metric.alias
        return None

    @property
    def name(self):
        return self._name

    @property
    def value(self):
        return self._value.to_pseudopython()

    @property
    def alias(self):
        return self._alias

    @property
    def icypaw_type(self):
        return self._type

    @property
    def python_type(self):
        return self._type.pythontype

    @property
    def properties(self):
        return self._properties

    @property
    def writable(self):
        return self._properties.get('Writable', False)

    def __str__(self):
        if 'Unit' in self._properties:
            return f"{self.value} {self._properties['Unit']}"
        else:
            return f"{self.value}"



class ClientEndpointMetric(ClientEndpointMetricBase):
    """Represent a metric as a name, value, type, and some belief as to its
    freshness."""

    @classmethod
    def from_metric(cls, metric, is_fresh, struct_types):
        """Construct an EndpointMetric from a tahu Metric.

        metric -- The decoded TAHU metric object.

        is_fresh -- Whether this metric is considered fresh.

        """

        name = metric.name

        icypaw_type = type_from_metric(metric, struct_types=struct_types)
        icypaw_value = value_from_metric(metric, icpw_type=icypaw_type)

        alias = cls._extract_alias(metric)

        return cls(name, icypaw_value, icypaw_type, alias, metric.properties,
                   is_historical=metric.is_historical, is_transient=metric.is_transient,
                   is_null=metric.is_null, is_fresh=is_fresh)

    def __init__(self, *args, is_historical, is_transient, is_null, is_fresh):
        super().__init__(*args)
        self._is_fresh = is_fresh
        self._is_historical = is_historical
        self._is_transient = is_transient
        self._is_null = is_null

    @property
    def is_fresh(self):
        return self._is_fresh

    @property
    def is_historical(self):
        return self._is_historical

    @property
    def is_null(self):
        return self._is_null

    @property
    def is_transient(self):
        return self._is_transient

    @property
    def is_valid(self):
        """Return if this data is not null and current, i.e. if it is not
        historical."""
        return not self.is_null and not self.is_historical

    @property
    def value(self):
        if not self.is_valid:
            raise IcypawException("Test values with is_valid before retrieving from the client")
        return super().value

    @property
    def historical_value(self):
        if not self.is_historical:
            raise IcypawException("historical_value is only valid for historical data")
        if self._is_null:
            raise IcypawException("historical data was null")
        return super().value

    def get(self, default=None):
        """Retrieve the value. If this metric is null, return the default
        value instead."""
        if not self.is_valid:
            return default
        return super().value

    def update_from_tahu(self, tahu_metric):
        """Update the value in place from the given tahu_metric in
        protobuf. Sets this metric to be fresh."""

        self._value = merge_with_metric(self._value, tahu_metric, self._type)
        self._is_fresh = True
        self._is_historical = tahu_metric.is_historical
        self._is_null = tahu_metric.is_null
        self._is_transient = tahu_metric.is_transient


class ClientEndpointCommand(ClientEndpointMetricBase):
    """Represent a command as a name, default value, and type."""

    @classmethod
    def from_metrics(cls, metric, icypaw_type):
        """Create a command from a TAHU metric and a template definition for
        its type."""

        if metric.datatype != DataType.Template.value:
            raise TypeError('For non-template commands, use the from_metric() classmethod')

        name = metric.name

        icypaw_value = icypaw_type()
        icypaw_value.merge_with_metric(metric)

        alias = cls._extract_alias(metric)

        return cls(name, icypaw_value, icypaw_type, alias, metric.properties)

    @classmethod
    def from_metric(cls, metric):
        """Create a command from a TAHU metric that is not a template. While
        the Python Icypaw server classes will not do this, it is allowed by
        the spec and so is supported here."""

        if metric.datatype == DataType.Template.value:
            raise TypeError('For template commands, use the from_metrics() classmethod')

        name = metric.name

        icypaw_type = type_from_metric(metric)
        icypaw_value = value_from_metric(metric)
        icypaw_type = type_from_metric(metric)

        alias = cls._extract_alias(metric)

        return cls(name, icypaw_value, icypaw_type, alias, metric.properties)

def _get_lifetime_sequence(message):
    """Determine a suitable lifetime sequence number from a tahu birth/death message.

    Ideally this will be the bdSeq metric of the message. Since the bdSeq number
    is not guaranteed to be present, e.g. for node architectures that don't
    support it or all devices, we fall back to using timestamps as a proxy.

    Timestamps should always be present in a Sparkplug-B payload. A returned
    value of ``None`` indicates that the given message was not timestamped for
    whatever reason, and that the caller should track endpoint lifetime
    sequences manually.
    """

    for metric in message.metrics:
        if metric.name == 'bdSeq':
            return metric.long_value

    if message.HasField('timestamp'):
        return message.timestamp
