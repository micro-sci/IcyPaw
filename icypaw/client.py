# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""The interface for clients who wish to communicate with nodes and devices."""

from enum import Enum
import threading
from threading import RLock
from copy import deepcopy
import logging
import warnings

import paho.mqtt.client
import paho.mqtt.subscribe

from .client_endpoint import ClientEndpoint, ClientEndpointName
from .tahu_interface import DEFAULT_NAMESPACE, Payload, TahuClientInterface
from .conventions import is_metric
from .types import IcypawType, IcypawScalarType, Struct, ArrayType, Boolean
from .exceptions import IcypawException
from .topic import DeviceTopic, NodeTopic, parse_topic

_logger = logging.getLogger(__name__)

DEFAULT_PORT = 1883

class IcypawClient:

    # The Quality of Service numbers are provided here as class-level
    # variables and not as method arguments because this exposes an
    # MQTT-specific detail and in general I'm trying to use only
    # concepts, not implementation details, from the transport
    # protocol.
    subscribe_qos = 0
    command_qos = 0

    # Number of seconds between when a birth message is dated and the
    # current time when we will still consider its values to be fresh.
    freshness_time = 10

    def __init__(self, address, port=DEFAULT_PORT, connect=False):
        """Create a client object that facilitates interaction with endpoints.

        address -- The address of the broker to connect to.

        port -- The listening port on the broker.

        """

        # This is unlocked when the connection first goes through to
        # allow the caller to wait for that event.
        self._connect_event = threading.Event()

        self._iface = TahuClientInterface()

        self._client_data = Locked.new(InternalClientData)

        self._address = address
        self._port = port

        # A mapping of topics to the timestamp and serial number of
        # the last seen messages.
        self._seen_messages = {}

        self._client = self._init_client()
        if connect:
            self.connect()

    def connect(self, wait=False):
        """Connect to the broker and start processing messages.

        wait -- If true, do not return until the connection has
        succeeded.

        """

        self._client.connect(self._address, self._port)
        self._client.loop_start()
        if wait:
            self._connect_event.wait()

    def __del__(self):
        # The if-statement prevents exceptions before the
        # initialization of _client from causing another exception in
        # this method.
        if hasattr(self, '_client') and self._client is not None:
            self.disconnect()

    def _init_client(self):
        """Return the initialized MQTT client.

        """

        self._client = paho.mqtt.client.Client()

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.reconnect_delay_set()  # Automatically reconnect

        return self._client

    def disconnect(self):
        """Disconnect from the broker. Call this explicitly to flush out any
        unsent published messages."""
        self._client.loop_stop()
        self._client.disconnect()
        self._client = None

    ##
    # Discovery
    #

    def list_endpoints(self):
        """Return all nodes known to the client at this time.

        Return a list of Endpoint objects.

        """

        with self._client_data as data:
            endpoints = deepcopy(list(data.endpoints.values()))

        return endpoints

    def list_metrics(self, endpoint_name):
        """Return a list of all metrics on the given endpoint.

        """

        endpoint_name = ClientEndpointName(endpoint_name)

        with self._client_data as data:
            if endpoint_name not in data.endpoints:
                raise ValueError(f'No known endpoint {endpoint_name}')
            metrics = deepcopy(data.endpoints[endpoint_name].metrics)

        return metrics

    def list_commands(self, endpoint_name):
        """Return a list of all commands on the given endpoint as well as
        their call signatures."""

        endpoint_name = ClientEndpointName(endpoint_name)

        with self._client_data as data:
            if endpoint_name not in data.endpoints:
                raise ValueError(f'No known endpoint {endpoint_name}')
            commands = deepcopy(data.endpoints[endpoint_name].commands)

        return commands

    ##
    # Endpoint access
    #

    def get_endpoint_metric(self, endpoint_name, metric_name):
        """Return the latest value of a device or node's metric. The metric
        should be checked for freshness.

        """

        endpoint_name = ClientEndpointName(endpoint_name)

        with self._client_data as data:
            if endpoint_name not in data.endpoints:
                raise ValueError(f'No known endpoint {endpoint_name}')
            endpoint = data.endpoints[endpoint_name]
            if metric_name not in endpoint.metrics:
                raise ValueError(f'No metric {metric_name} found in {endpoint_name}')
            return endpoint.metrics[metric_name]

    def set_endpoint_metric(self, endpoint_name, metric_name, value, _force=False):
        """Set the value of a metric on an endpoint. The type must be
        convertible to the type of the metric.

        By default, this will raise an ``IcypawException`` when trying to set
        the value of a metric declared read-only by the server, or when trying
        to set a metric on an endpoint that appears offline.

        Client runtime checking may be disabled by calling this method with
        ``_force=True``. If so, this client will try to set the metric on the
        remote server by issuing an NCMD/DCMD message, but this message may
        still be ignored by the server.
        """

        endpoint_name = ClientEndpointName(endpoint_name)
        with self._client_data as data:
            if endpoint_name not in data.endpoints:
                # Even with _force, we can't continue without an
                # endpoint object. The main issue is we need to know
                # what kind of metric to set.
                raise IcypawException(f"Endpoint `{endpoint_name}' unknown")
            endpoint = deepcopy(data.endpoints[endpoint_name])

        if not endpoint.is_online:
            _raise_by_default(
                f"Attempting to set metric value of offline endpoint `{endpoint_name}'",
                force=_force
            )

        metric = endpoint.metrics[metric_name]

        if not metric.writable:
            _raise_by_default(
                f"Attempting to set value of read-only metric `{metric_name}' of endpoint `{endpoint_name}'",
                force=_force
            )

        # limit checking for scalar types, if available
        if issubclass(metric.icypaw_type, IcypawScalarType):
            low_limit = metric.properties.get('Low', None)
            if low_limit is not None and value < low_limit:
                _raise_by_default(
                    f"Value {value} is less than the lower bound {low_limit} for "
                    f"metric `{metric_name}' of endpoint `{endpoint_name}'",
                    force=_force
                )

            high_limit = metric.properties.get('High', None)
            if high_limit is not None and value > high_limit:
                _raise_by_default(
                    f"Value {value} is greater than the upper bound {high_limit} for "
                    f"metric `{metric_name}' of endpoint `{endpoint_name}'",
                    force=_force
                )

        icypaw_value = metric.icypaw_type(value)
        if isinstance(icypaw_value, Struct):
            sent_value = icypaw_value.make_tahu_template()
        else:
            # sent_value = icypaw_value.value
            sent_value = icypaw_value.to_pseudopython()
        if endpoint_name.is_node:
            tahu_payload, tahu_topic = self._iface.new_ncmd(
                endpoint_name.group_id, endpoint_name.edge_node_id, metric_name,
                sent_value, metric.icypaw_type.datatype
            )
        else:  # device
            tahu_payload, tahu_topic = self._iface.new_dcmd(
                endpoint_name.group_id, endpoint_name.edge_node_id,
                endpoint_name.device_id, metric_name, sent_value,
                metric.icypaw_type.datatype
            )
        self._client.publish(tahu_topic, tahu_payload.SerializeToString())

    def call_command(self, endpoint_name, command_name, *args, _force=False, **kwargs):
        """Issue a command to the given endpoint. Commands do not have return
        values.

        By default, this will raise an ``IcypawException`` when trying to call a
        command on a remote endpoint thought to be offline.

        Runtime safety checking may be disabled by calling this method with
        ``_force=True``. If so, this client will try to call the command on the
        remote server by issuing an NCMD/DCMD message, but this message may
        still be ignored.
        """

        endpoint_name = ClientEndpointName(endpoint_name)
        with self._client_data as data:
            endpoint = deepcopy(data.endpoints[endpoint_name])

        if not endpoint.is_online:
            _raise_by_default(
                f"Attempting to set value of offline endpoint `{endpoint_name}'",
                force=_force
            )

        command = endpoint.commands[command_name]
        icypaw_value = self._make_icypaw_value(command.icypaw_type, *args, **kwargs)
        if isinstance(icypaw_value, Struct):
            value = icypaw_value.make_tahu_template()
        else:
            value = icypaw_value.value
        if endpoint.is_device:
            tahu_payload, tahu_topic = self._iface.new_dcmd(
                endpoint_name.group_id, endpoint_name.edge_node_id, endpoint_name.device_id, command_name, value,
                command.icypaw_type.datatype)
        else:
            tahu_payload, tahu_topic = self._iface.new_ncmd(
                endpoint_name.group_id, endpoint_name.edge_node_id, command_name, value,
                command.icypaw_type.datatype)
        self._client.publish(tahu_topic, tahu_payload.SerializeToString())

    def get_endpoint_state(self, endpoint_name):
        """Return whether the given endpoint is online, offline, or
        nonexistant (i.e. there is no record of it ever being online)."""

        endpoint_name = ClientEndpointName(endpoint_name)

        with self._client_data as data:
            if endpoint_name in data.endpoints:
                if data.endpoints[endpoint_name].is_online:
                    return EndpointState.ONLINE
                else:
                    return EndpointState.OFFLINE
            else:
                return EndpointState.UNKNOWN

    ##
    # Event monitoring
    #

    def monitor(self, callback, events, endpoint_list):
        """Register a callback with this client that is called when an event
        from event_list occurs involving an endpoint in endpoint_list.

        callback -- The function called when an event occurs on an
        endpoint. It is given the event, the endpoint, and another
        event-specific argument with any additional information
        (usually a list of changed metrics).

        events -- Any event or combination of events (with the |
        operator) that trigger this callback.

        endpoint_list -- A list of endpoint names or endpoint name
        patterns for which this callback is called.

        """

        for _endpoint_name in endpoint_list:
            endpoint_name = ClientEndpointName(_endpoint_name)
            for event in events:
                topic = self._make_subscription_topic(endpoint_name, event)
                self._subscribe_user_to(event, endpoint_name, topic, callback)

    def watch(self, events, endpoint_list):
        """Make this client listen for certain update events for endpoints in
        endpoint_list. Either this method or monitor must be called for this
        client object to update metrics as it receives them.

        events -- Any event or combination of events (with the |
        operator) that trigger this callback.

        endpoint_list -- A list of endpoint names or endpoint name
        patterns for which this callback is called.

        """

        self.monitor(None, events, endpoint_list)

    def _make_subscription_topic(self, endpoint_name, event):
        """From the user-provided ClientEndpointName and Event objects produce a
        TOPIC string that the MQTT client can subscribe to."""

        if event == Event.ONLINE:
            message_base = 'BIRTH'
        elif event == Event.OFFLINE:
            message_base = 'DEATH'
        elif event == Event.METRIC_UPDATE:
            message_base = 'DATA'
        else:
            raise ValueError('Unknown Event type')

        def convert_component(comp):
            if comp == any:
                return '+'
            return comp

        if endpoint_name.is_node:
            message_type = f'N{message_base}'
            topic = NodeTopic(DEFAULT_NAMESPACE, endpoint_name.group_id_str, message_type,
                              endpoint_name.edge_node_id_str)
        elif endpoint_name.is_device:
            message_type = f'D{message_base}'
            topic = DeviceTopic(DEFAULT_NAMESPACE, endpoint_name.group_id_str, message_type,
                                endpoint_name.edge_node_id_str, endpoint_name.device_id_str)
        else:
            raise ValueError('Unknown endpoint type')

        return topic

    def _subscribe_user_to(self, event, endpoint_pattern, topic, callback):
        """Subscribe the user to some TAHU topic with a callback."""
        subscription = Subscription(event, endpoint_pattern, callback)
        with self._client_data as data:
            data.user_subscriptions[event].append(subscription)
            data.subscriptions.append((topic, self.subscribe_qos))
        self._client.subscribe(topic.topic, self.subscribe_qos)

    ##
    # Synchronization
    #

    def acquire_locks(self, endpoint_list, timeout=None, priority=None):
        """Request locks from the listed nodes and devices.

        endpoint_list -- A list of strings or tuples identifying nodes
        and devices.

        group_id -- The ID of the default group for the locks. All
        endpoints need not belong to the same group, but those not
        belonging to the default group need to be specified
        individually.

        timeout -- The length of time, in seconds, after which the
        lock is automatically released. If not given, the locking
        node's default value will be used.

        priority -- Assert a given priority to getting locks obtained.

        """


    def release_locks(self, endpoint_list):
        """Release locks acquired through acquire_locks."""

    ##
    # Private methods
    #

    def _on_connect(self, client, userdata, flags, rc):
        try:
            self._connect_event.set()

            # Subscribe to all birth and death certificates
            client.subscribe(f"{DEFAULT_NAMESPACE}/+/NBIRTH/+")
            client.subscribe(f"{DEFAULT_NAMESPACE}/+/NDEATH/+")
            client.subscribe(f"{DEFAULT_NAMESPACE}/+/DBIRTH/+/+")
            client.subscribe(f"{DEFAULT_NAMESPACE}/+/DDEATH/+/+")

            # Subscribe to user-requested events
            with self._client_data as data:
                for topic, qos in data.subscriptions:
                    client.subscribe(topic.topic, qos)
        except Exception as exc:
            # This is run in another thread and exceptions are
            # otherwise silently ignored.
            _logger.exception(exc)
            _logger.error(f"In on_connect: {type(exc).__name__}: {exc}")

    def _on_message(self, client, userdata, message):
        """All messages received from the client are routed through here. We
        then manually dispatch to internal methods, user methods, or both."""

        try:
            self._on_message_no_try(client, userdata, message)
        except Exception as exc:
            _logger.exception(exc)
            _logger.error(f"In handling incoming message: {exc}")

    def _on_message_no_try(self, client, userdata, message):
        if len(message.payload) == 0:
            _logger.debug(f"Dropping empty message on {message.topic}")
            return

        topic = parse_topic(message.topic)

        endpoint_name = ClientEndpointName(topic)

        payload = Payload()
        payload.ParseFromString(message.payload)

        # Try to deduplicate received messages. We receive one message
        # per subscription that covers that message. Note that this is
        # independent of the QOS which governs delivery from the broker.
        if payload.HasField('timestamp') and payload.HasField('seq'):
            fingerprint = (payload.timestamp, payload.seq)
            if self._seen_messages.get(message.topic) == fingerprint:
                return
            self._seen_messages[message.topic] = fingerprint

        # Route to fixed methods in the client
        if topic.message_type in ['NBIRTH', 'DBIRTH']:
            self._on_birth(topic, endpoint_name, payload)
        elif topic.message_type in ['NDEATH', 'DDEATH']:
            self._on_death(topic, endpoint_name, payload)
        elif topic.message_type == 'NDATA' or topic.message_type == 'DDATA':
            self._on_data(topic, endpoint_name, payload)

        if topic.message_type in ['NBIRTH', 'DBIRTH']:
            event = Event.ONLINE
        if topic.message_type in ['NDEATH', 'DDEATH']:
            event = Event.OFFLINE
        if topic.message_type in ['NDATA', 'DDATA']:
            event = Event.METRIC_UPDATE

        self._route_message_to_user(event, topic, endpoint_name, payload)

    def _on_birth(self, topic, endpoint_name, payload):
        """Update or create the endpoint entry based on a birth message coming
        in."""

        if topic.message_type == 'NBIRTH':
            self._iface.register_nbirth(topic.topic, payload)
        else:
            self._iface.register_dbirth(topic.topic, payload)

        with self._client_data as data:
            templates = None
            if endpoint_name.is_device:
                node_name = ClientEndpointName(group_id=endpoint_name.group_id,
                                               edge_node_id=endpoint_name.edge_node_id)
                try:
                    templates = data.endpoints[node_name].templates
                except KeyError as exc:
                    _logger.exception(exc)
                    _logger.error(f"Device {endpoint_name} birth received before node")
            data.endpoints[endpoint_name] = ClientEndpoint(endpoint_name, birth_metric=payload,
                                                           templates=templates)

    def _on_death(self, topic, endpoint_name, payload):
        """Update an endpoint entry based on a death certificate having been
        received."""

        with self._client_data as data:
            if endpoint_name in data.endpoints:
                data.endpoints[endpoint_name].update_from_tahu_death(payload)
            else:
                msg = f'Death certificate received for unknown endpoint {endpoint_name}'
                raise IcypawException(msg)

    def _on_data(self, topic, endpoint_name, payload):
        """Update an endpoint entry based on a data message."""

        with self._client_data as data:
            if endpoint_name in data.endpoints:
                data.endpoints[endpoint_name].update_from_tahu_data(payload)
            else:
                msg = f'Data message received for unknown endpoint {endpoint_name}'
                raise IcypawException(msg)

    def _route_message_to_user(self, event, topic, endpoint_name, payload):
        """Route a message received to any user callbacks that have requested it.

        topic -- The DeviceTopic or NodeTopic object made from the
        topic of this message.

        payload -- The decoded protobuf payload.

        """

        metric_names = [metric.name for metric in payload.metrics if is_metric(metric.name)]

        # Shallow copy the subscriptions so that the user calls don't
        # cause deadlock.
        with self._client_data as data:
            subscriptions = list(data.user_subscriptions[event])
            # Ideally, if one of the metrics does not belong, we
            # should have raised an exception before getting to this
            # point.
            metrics = [metric for name, metric in data.endpoints[endpoint_name].metrics.items()
                       if name in metric_names]

        # Route to the user's callbacks
        for sub in subscriptions:
            sub.call_if(endpoint_name, metrics)

    def _make_icypaw_value(self, icypaw_type, *args, **kwargs):
        """Format the given arguments and keyword arguments into an object of
        the given Icypaw type."""

        assert issubclass(icypaw_type, IcypawType)

        if issubclass(icypaw_type, IcypawScalarType):
            if len(args) == 1:
                return icypaw_type(args[0])
            elif len(args) == 0 and icypaw_type == Boolean:
                # Zero argument commands take a dummy boolean argument.
                return icypaw_type(True)
            else:
                raise TypeError(f'Cannot convert arguments to type {icypaw_type}')
        elif issubclass(icypaw_type, Struct):
            if len(args) != 0:
                # There might be cases where it's fine to use positional arguments.
                raise TypeError('Struct types only support call by keyword')
            return icypaw_type(kwargs)
        elif issubclass(icypaw_type, ArrayType):
            raise RuntimeError('Arrays not yet implemented')
        else:
            raise TypeError('Unimplemented Icypaw type')

class Event(Enum):
    """Different events that the user may monitor. These Events may be
    combined with bitwise operators, resulting in a set of Events. Both
    single Events and sets of events may be queries for membership with
    the `in` operator.

    Do not rely on the exact values of these events as they may change.

    """

    # Event occurs when an endpoint comes online.
    ONLINE = 0x1

    # Event occurs when an endpoint goes offline.
    OFFLINE = 0x2

    # Event occurs when a metric's value changes.
    METRIC_UPDATE = 0x4

    def __iter__(self):
        """Allow iterating over a single event so as to match the behavior of
        sets of events."""
        yield self

    def __contains__(self, other):
        """Allow treating Events as sets of events for the purpose of member
        testing."""
        return self == other

    def __or__(self, other):
        if isinstance(other, set):
            return {self} | other
        elif isinstance(other, Event):
            return {self, other}
        raise TypeError('May only combine Events and sets of Events with or')

    def __ror__(self, other):
        if isinstance(other, set):
            return {self} | other
        elif isinstance(other, Event):
            return {self, other}
        raise TypeError('May only combine Events and sets of Events with or')

    def __xor__(self, other):
        if isinstance(other, set):
            return {self} ^ other
        elif isinstance(other, Event):
            return {self} ^ {other}
        raise TypeError('May only combine Events and sets of Events with xor')

    def __rxor__(self, other):
        if isinstance(other, set):
            return {self} ^ other
        elif isinstance(other, Event):
            return {self} ^ {other}
        raise TypeError('May only combine Events and sets of Events with xor')

    def __and__(self, other):
        if isinstance(other, set):
            return {self} & other
        elif isinstance(other, Event):
            return {self} & {other}
        raise TypeError('May only combine Events and sets of Events with and')

    def __rand__(self, other):
        if isinstance(other, set):
            return {self} & other
        elif isinstance(other, Event):
            return {self} & {other}
        raise TypeError('May only combine Events and sets of Events with and')

class EndpointState(Enum):
    """The possible states of existence of an endpoint (node or
    device). They can either be online, offline (i.e. there is a birth
    certificate with corresponding death certificate), or completely
    unknown."""

    ONLINE = 0
    OFFLINE = 1
    UNKNOWN = 2

class Locked:
    """Wrap an object in a single lock that is temporarily unlocked with a
    context manager."""

    @classmethod
    def new(cls, other_cls, *args, **kwargs):
        """Create a new object and wrap it in this lock."""
        return cls(other_cls(*args, **kwargs))

    def __init__(self, data):
        """Wrap an existing object."""
        self._lock = RLock()
        self._data = data

    def __enter__(self):
        self._lock.acquire()
        return self._data

    def __exit__(self, *args):
        self._lock.release()

class InternalClientData:
    """Data stored in the client that must be made thread-safe."""

    def __init__(self):
        # A list of subscriptions used internally to this client.
        self.subscriptions = []

        # Lists of user-defined subscriptions by event type. These
        # events map directy to message types in TAHU.
        self.user_subscriptions = {
            Event.ONLINE: [],
            Event.OFFLINE: [],
            Event.METRIC_UPDATE: [],
        }

        # A dictionary mapping endpoint names to ClientEndpoint objects.
        self.endpoints = {}

class Subscription:
    """Data class storing data for a user subscription."""

    def __init__(self, event, endpoint_pattern, callback):
        self.event = event
        self.endpoint_pattern = endpoint_pattern
        self.callback = callback

    def call_if(self, endpoint_name, metrics):
        """Call the callback associated with this subscription if the
        subscription matches the endpoint name.

        endpoint_name -- The ClientEndpointName for the message received.

        metrics -- A list of ClientEndpointMetric objects that were
        updated as a result of this message.

        """

        if self.callback and self.endpoint_pattern.match(endpoint_name):
            self.callback(self.event, endpoint_name, metrics)

def _raise_by_default(msg, force=False):
    """Raise an IcypawException with the given message, or warn with the
    same message if called with ``force=True``.

    This can be used to implement client runtime error-checking in a
    user-friendly manner.
    """
    if not force:
        raise IcypawException(msg)
    else:
        warnings.warn(msg, RuntimeWarning)
