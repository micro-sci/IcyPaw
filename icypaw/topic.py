# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Functions and classes for generically dealing with TAHU topic
strings."""

from .exceptions import TahuInterfaceError

DEFAULT_NAMESPACE = "spBv1.0"

# TODO: Refactor common functionality into a base class.

class NodeTopic:
    """A class representing a topic sent to or from a node.
    """

    def __init__(self, namespace, group_id, message_type, edge_node_id):
        """Create a node message topic from the individual components."""

        self._namespace = validate_topic_component(namespace)
        self._group_id = validate_topic_component(group_id)
        self._message_type = validate_topic_component(message_type).upper()
        self._edge_node_id = validate_topic_component(edge_node_id)

    @property
    def topic(self):
        return f"{self._namespace}/{self._group_id}/{self._message_type}/{self._edge_node_id}"

    @property
    def namespace(self):
        return self._namespace

    @property
    def group_id(self):
        return self._group_id

    @property
    def message_type(self):
        return self._message_type

    @property
    def edge_node_id(self):
        return self._edge_node_id

    @property
    def device_id(self):
        """Having this property provides a uniform interface between Node and
        Device topics."""
        return None

    def match(self, tahu_topic):
        """Return whether or not this Topic matches the TAHU topic string."""
        fields = tahu_topic.split('/')
        if len(fields) != 4:
            return False
        components = [self._namespace, self._group_id, self._message_type, self._edge_node_id]
        return all(self.match_field(fld, comp) for fld, comp in zip(fields, components))

    def match_field(self, tahu_field, component):
        """Return whether the given component matches a field in a TAHU string."""
        if component == '+':
            return True
        return tahu_field == component

    def __repr__(self):
        return self.topic


class DeviceTopic:
    """A class representing a topic sent to or from a device. Can be used to
    construct or de-construct a topic.

    """

    def __init__(self, namespace, group_id, message_type, edge_node_id, device_id):
        """Create a node message topic from the individual components."""

        self._namespace = validate_topic_component(namespace)
        self._group_id = validate_topic_component(group_id)
        self._message_type = validate_topic_component(message_type).upper()
        self._edge_node_id = validate_topic_component(edge_node_id)
        self._device_id = validate_topic_component(device_id)

    @property
    def topic(self):
        return f"{self._namespace}/{self._group_id}/{self._message_type}/{self._edge_node_id}/{self._device_id}"

    @property
    def namespace(self):
        return self._namespace

    @property
    def group_id(self):
        return self._group_id

    @property
    def message_type(self):
        return self._message_type

    @property
    def edge_node_id(self):
        return self._edge_node_id

    @property
    def device_id(self):
        return self._device_id

    def match(self, tahu_topic):
        """Return whether or not this Topic matches the TAHU topic string."""
        fields = tahu_topic.split('/')
        if len(fields) != 5:
            return False
        components = [self._namespace, self._group_id, self._message_type, self._edge_node_id,
                      self._device_id]
        return all(self.match_field(fld, comp) for fld, comp in zip(fields, components))

    def match_field(self, tahu_field, component):
        """Return whether the given component matches a field in a TAHU string."""
        if component == '+':
            return True
        return tahu_field == component

    def __repr__(self):
        return self.topic


class StateTopic:
    """State topics are used by control applications and do not include
    node, device, or even group IDs."""

    def __init__(self, scada_host_id):
        """Create a new topic for the state message, which is the only message
        used by hosts exclusively."""

        self._scada_host_id = validate_topic_component(scada_host_id)

    @property
    def topic(self):
        return "STATE/{self._scada_host_id}"

def parse_topic(topic_string):
    """Return a *Topic class that is the result of parsing the given
    string."""

    # The MQTT spec allows for wildcards only in subscription topics,
    # and even then brokers might not support them. We therefore do
    # not support the parsing of wildcard topics, since no code at the
    # Tahu level should be consuming them.

    fields = topic_string.split('/')

    if len(fields) == 2 and fields[0].upper() == 'STATE':
        return StateTopic(fields[1])

    if len(fields) != 4 and len(fields) != 5:
        raise TahuInterfaceError(f"Topic must have 2, 4, or 5 fields, found {len(fields)}")

    namespace, group_id, message_type, edge_node_id, device_id = (fields + [None])[:5]

    # TODO -- validate topic components
    if device_id is not None:
        return DeviceTopic(namespace, group_id, message_type, edge_node_id, device_id)
    else:
        return NodeTopic(namespace, group_id, message_type, edge_node_id)

def validate_topic_component(component):
    """Make sure one component of a topic has the right type and does not
    contain illegal characters. Return the component, possibly
    converted from another type.

    """

    if isinstance(component, bytes):
        component = component.decode()

    if not isinstance(component, str):
        raise TahuInterfaceError(f"Must provide a string as a topic component, found '{component}")

    # We allow the entire component to be a '+' for filtering
    if component != '+':
        if any(c in component for c in ('#', '/', '+')):
            raise TahuInterfaceError(f"Topic component '{component}' contains one of '#', '/', or '+'")

    return component

def make_topic_string(namespace, group_id, message_type, edge_node_id, device_id=None):
    """Return a topic string for either a Node or Device topic."""
    if namespace is None:
        namespace = DEFAULT_NAMESPACE
    if device_id is None:
        topic_obj = NodeTopic(namespace, group_id, message_type, edge_node_id)
    else:
        topic_obj = DeviceTopic(namespace, group_id, message_type, edge_node_id, device_id)
    return topic_obj.topic
