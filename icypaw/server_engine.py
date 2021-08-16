# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Icypaw application engine.

The application engine lives above the MQTT client and below the EoN node in the
Icypaw application stack. This layer manages the MQTT client's connection to the
broker, the node's Tahu interface, and the application's state and lifespan.

"""

import logging
from queue import Queue, Empty
from itertools import chain, repeat
from contextlib import contextmanager
import time
from heapq import heappop, heappush
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import paho.mqtt.client as mqtt
from paho.mqtt import subscribe

from icypaw.tahu_interface import (TahuServerInterface, new_metric,
                                   new_payload, add_metrics_to_payload, read_bdseq,
                                   build_endpoint_property)
from icypaw.exceptions import IcypawException
import icypaw.conventions as conventions
from icypaw.engine_queue import ScheduleQueueItem
from icypaw.types import value_from_metric, String
from icypaw.topic import parse_topic
from icypaw import __version__

_logger = logging.getLogger(__name__)

_MQTT_ERRMSG = {
    mqtt.MQTT_ERR_NO_CONN: "Client is not currently connected",
    mqtt.MQTT_ERR_QUEUE_SIZE: "Message is neither queued nor sent",
    '': "Unknown MQTT error"
}

_LAST_BDSEQ_TIMEOUT_S = 1  # TODO make configurable


class ServerEngine:
    """Icypaw application engine class

    The engine object implements an Icypaw application and acts as the central
    object. It powers the application EoN node which in turn supports any number
    of devices.

    Specifically, the application engine handles:
    - The connection to the MQTT broker,
    - Dispatching remote commands to the node, and
    - Publishing node metrics

    A typical Icypaw application defines a set of behaviors in a class extending
    ``IcypawNode``. When the application is run, it initializes and starts the
    engine:

    TODO: Enter running commands here.

    Parameters
    ----------
    node: ``IcypawNode``
        The EoN node that this engine will power.
    tahu_interface: ``TahuServerInterface``, optional
        Optionally, a new Tahu server interface to use. Use this to control the
        group ID, edge node ID, and ``bdSeq`` of the application. If not given,
        a new default ``TahuServerInterface`` will be initialized with
        ``bdSeq=0`` and used in the engine.
    mqtt_client: ``paho.mqtt.client.Client``, optional
        Optionally, a constructed MQTT client to use. This can be used to
        control the MQTT client parameters of the application. If not given, a
        new default ``Client`` will be used.
    """


    def __init__(self, node, tahu_interface=None, mqtt_client=None):
        # The queue of upcoming events.
        self._queue = Queue()

        # A priority queue of upcoming events with the soonest event
        # occupying the first slot.
        self._scheduled_events = []

        # The bdSeq will be overwritten in the connect method.
        self._iface = tahu_interface or TahuServerInterface(group_id=node.group_id,
                                                            edge_node_id=node.edge_node_id,
                                                            bdSeq=0)

        self._mqtt_client = mqtt_client or mqtt.Client()
        self._mqtt_client.enable_logger(_logger)

        self._node = None
        self._nbirth = None
        self._ndeath = None
        self._devices = {}

        self._device_classes = []

        self._register_node(node)

    @property
    def name(self):
        return __package__

    @property
    def description(self):
        return f"{self.name} {__version__}"

    def _register_node(self, node):
        """Set the node powered by this engine & reinitialize.

        This method tells the engine to power the given node, sets up polling
        and dispatch, and builds the Tahu interface for the application.

        This method is called during initialization and in general should not be
        called manually unless you know what you're doing.

        Parameters
        ----------
        node: ``IcypawNode``
            The node to be powered by this engine.
        """

        self._node = node
        self._node.icpw_register_command_queue(self._queue)

        all_tahu_metrics = (self._node.tahu_metrics(with_properties=True)
                            + self._node.tahu_commands(with_properties=True))
        self._iface.set_initial_node_metrics(all_tahu_metrics)

        # Send metrics from the various device classes to the TAHU
        # interface so it can extract the data it needs to create the
        # NBIRTH certificates.
        all_dev_metrics = []
        for dev_class in self._node.device_classes:
            sig = dev_class.icpw_signature()
            iterable = chain(zip(repeat(False), sig['metrics'].items()),
                             zip(repeat(True), sig['commands'].items()))
            for is_command, (name, metric_type) in iterable:
                tahu_metric = new_metric()
                if is_command:
                    tahu_metric.name = conventions.make_command(name).encode()
                else:
                    tahu_metric.name = name.encode()
                metric_obj = metric_type()
                metric_obj.set_in_metric(tahu_metric)
                all_dev_metrics.append(tahu_metric)
        self._iface.register_device_class_metrics(all_dev_metrics)

        # Clear the list of updated metrics
        self._node.icpw_updated_metrics()

        self._device_classes = self._node.device_classes

        # Custom types derived from Struct used by this node or one of
        # the devices.
        self._struct_types = self._node.icpw_types()
        for device_class in self._device_classes:
            self._struct_types.update(device_class.icpw_types())

    @contextmanager
    def connect(self, broker, port=1883):
        """Open a connection to the MQTT broker.

        The state of this connection can be used as a context.

        Typically this should not be called directly; rather, call ``run``,
        which will call this method.

        Parameters
        ----------
        broker : str
            The address of the MQTT broker.
        port : str, optional
            The port used when connecting to the broker.
        """

        self._iface.bdSeq = _get_next_bdseq(self._iface.new_nbirth_topic(), broker, port)

        # Create nbirth and annotate with node endpoint properties
        nbirth = self._iface.new_nbirth()
        node_properties = [
            build_endpoint_property('ICPWServer', String(self.description))
        ]
        add_metrics_to_payload(node_properties, nbirth)

        self._nbirth = {
            'payload': nbirth,
            'topic': self._iface.new_nbirth_topic()
        }

        # Create ndeath and set as LWT
        self._ndeath = {
            'payload': self._iface.new_ndeath(),
            'topic': self._iface.new_ndeath_topic()
        }
        self._mqtt_client.will_set(self._ndeath['topic'],
                                   self._ndeath['payload'].SerializeToString(),
                                   qos=1, retain=True)

        self._mqtt_client.connect(broker, port=port)
        self.on_connect()
        self._node.on_connect(self)
        self._mqtt_client.loop_start()

        # Yield to context, then clean up connection afterward.
        try:
            yield
        except IcypawEngineShutdownException:
            self._node.on_shutdown(self)
            self._mqtt_client.disconnect()
        finally:
            self._mqtt_client.loop_stop()
            self._node.on_disconnect(self)

    def on_connect(self):
        """Hook called after successful connection to the MQTT broker.

        This will publish the node's NBIRTH message, prepare the node's NDEATH
        message & LWT, and subscribe to NCMD messages.
        """
        _logger.info("Successfully connected to broker")

        # Publish NBIRTH
        self.publish(self._nbirth['topic'], self._nbirth['payload'], qos=1, retain=True)

        # Subscribe to NCMD messages for this node
        ncmd_topic = self._iface.new_ncmd_topic()
        _logger.info(f"Subscribing to {ncmd_topic}")
        self._mqtt_client.message_callback_add(ncmd_topic, self.on_ncmd)
        self._mqtt_client.subscribe(ncmd_topic)

    def publish(self, topic, payload, qos=1, retain=False):
        """Publish a Tahu message over MQTT.

        Parameters
        ----------
        topic: str
            The MQTT topic to publish on.
        payload: ``Payload``
            Tahu protobuf payload to be published or a string.
        qos: {0,1,2}, optional
            The MQTT Quality of Service level to use for the message.
        retain: boolean, optional
            If set to ``True``, the message will be set as the retained message
            for the topic.
        """

        # TODO: If the queue is full, try sleeping for a bit then
        # attempting to publish again.

        # TODO: Is QOS 1 the right default number?

        _logger.debug(f"Publishing to {topic}: {payload}")

        if not isinstance(payload, str):
            payload = payload.SerializeToString()

        ret = self._mqtt_client.publish(topic, payload, qos=qos, retain=retain)
        if ret.rc != mqtt.MQTT_ERR_SUCCESS:
            err_msg = _MQTT_ERRMSG[ret.rc if ret.rc in _MQTT_ERRMSG else '']

            if ret.rc == mqtt.MQTT_ERR_NO_CONN:
                # Messages published while connection is dropped will be
                # buffered by the client until reconnected
                _logger.error(err_msg)
            else:
                # Trying to publish while the queue is full results in lost data
                # and should be fatal
                raise IcypawEngineError(err_msg)

    def on_ncmd(self, client, userdata, message):
        """Callback for NCMD topic messages."""
        _logger.info("Received NCMD")
        def ncmd_func():
            try:
                payload = new_payload()
                payload.ParseFromString(message.payload)
                for metric in payload.metrics:
                    icpw_metric = value_from_metric(metric, struct_types=self._struct_types)
                    if metric.HasField('name'):
                        name = metric.name
                    else:
                        name = self._iface.get_node_metric_name(metric.alias)

                    if conventions.is_command(name):
                        name = conventions.make_base_name_from_command(name)

                    self._node.icpw_update_metric(name, icpw_metric)

            except Exception as exc:
                _logger.exception(exc)
                _logger.error(f"In handling NCMD message: {exc}")

        item = ScheduleQueueItem(ncmd_func)
        self._queue.put(item)

    def on_dcmd(self, client, userdata, message):
        """Callback for DCMD topic messages."""
        _logger.info("Received DCMD")

        def dcmd_func():
            try:
                topic = parse_topic(message.topic)
                if topic.device_id not in self._devices:
                    msg = f'Received message for device {topic.device_id} which does not exist'
                    # TODO: When we have true error reporting, we should not
                    # throw an exception here.
                    raise IcypawEngineError(msg)

                device_state = self._devices[topic.device_id]
                device = device_state.device

                if device_state.is_down:
                    _logger.error(f"Received message for down device {topic.device_id}")
                    # TODO: When we have true error reporting, we should report this.
                    return

                payload = new_payload()
                payload.ParseFromString(message.payload)

                for metric in payload.metrics:
                    icpw_metric = value_from_metric(metric, struct_types=self._struct_types)
                    if metric.HasField('name'):
                        name = metric.name
                    else:
                        name = self._iface.get_device_metric_name(device.device_id, metric.alias)

                    if conventions.is_command(name):
                        name = conventions.make_base_name_from_command(name)

                    device.icpw_update_metric(name, icpw_metric)

            except Exception as exc:
                _logger.exception(exc)
                _logger.error(f"In handling DCMD message: {exc}")

        item = ScheduleQueueItem(dcmd_func)
        self._queue.put(item)

    def process_events(self):
        """Process all outstanding events. Returns when it would have to block
        on the incoming event queue. There may still be events in the
        scheduled events queue that it is too soon to execute.

        """

        while True:
            while True:
                if not self._wait_on_queue():
                    break

            if not self._scheduled_events:
                break

            event_time, event_object = self._scheduled_events[0]
            if event_time == 0 or event_time <= time.time():
                heappop(self._scheduled_events)
                self._process_event(event_object)
            else:
                break

    def wait_on_event(self, max_time=None):
        """Wait until an event is ready to be run in the scheduled events
        queue. While waiting, receive incoming events on the incoming event
        queue.

        Returns True if an event was read, false otherwise.
        """

        if self._scheduled_events:
            event_time, _ = self._scheduled_events[0]
            if max_time is not None and event_time > max_time:
                event_time = max_time
        else:
            event_time = max_time
        return self._wait_on_queue(event_time)

    def _wait_on_queue(self, until_time=0):
        """Wait on a new queue item to come in until time.time() >=
        until_time or one item has been pushed.

        until_time -- The time in seconds after the epoch when we must
        give up waiting.

        Creates a new event and puts it on the _scheduled_events
        priority queue.

        Returns True if an event was read, false otherwise.

        """

        if until_time is None:
            # Note: On Windows this might freeze us up forever if no
            # new events come in.
            time_left = None
            block = True
        else:
            time_left = max(0, until_time - time.time())
            block = not (time_left == 0)
        try:
            item = self._queue.get(block=block, timeout=time_left)
        except Empty:
            return False
        event = (item.time, item)
        heappush(self._scheduled_events, event)
        return True

    def _process_event(self, event):
        """Process the given QueueItem according to its type."""
        method_name = f'_process_{type(event).__name__}'
        getattr(self, method_name)(event)

    def _process_ScheduleQueueItem(self, item):
        """Run a command that was previously scheduled to be executed. All
        work in the engine not done in a background thread
        (i.e. receiving MQTT requests) is halted during its
        execution. Upon its completion, the node and all devices are
        checked for updated metrics and appropriate data messages sent
        out. If requested, the item is rescheduled for some point in
        the future. The reschedule point is calculated from the item's
        requested runtime, not the time of processing, to avoid
        accumulated drift.

        """

        try:
            item.payload.func()

            if item.payload.repeat_sec is not None:
                new_time = item.time + item.payload.repeat_sec
                new_item = ScheduleQueueItem(item.payload.func, repeat_sec=item.payload.repeat_sec,
                                             exec_time=new_time)
                heappush(self._scheduled_events, (new_item.time, new_item))
        finally:
            # Even if the payload function puts the endpoint in a bad
            # state, we should accurately reflect that bad state here.
            self._publish_metric_updates()

    def _process_RegisterDeviceQueueItem(self, item):
        """Process a queue item that is the node asking us to register a new
        device. This causes us to emit a birth certificate and set the
        device's internal state to up. A device may be registered even
        if it is already up; this will reissue a birth certificate and
        may be used to add new functionality to the device.

        """

        device = item.payload.device
        device_id = item.payload.device.device_id

        assert item.payload.node is self._node

        if not any(isinstance(item.payload.device, cls) for cls in self._device_classes):
            raise IcypawEngineError('Device not of a class registered with the node')

        # Register the command queue
        device.icpw_register_command_queue(self._queue)
        # Add a device state entry
        if device_id in self._devices:
            device_state = self._devices[device_id]
            device_state.increment_bdSeq()
            device_state.is_up = True
            self._iface.unregister_device(device_id)
        else:
            device_state = DeviceState(item.payload.device)
            # Subscribe to incoming DCMD messages.
            dcmd_topic = self._iface.new_dcmd_topic(device_id)
            self._mqtt_client.message_callback_add(dcmd_topic, self.on_dcmd)
            self._mqtt_client.subscribe(dcmd_topic)
        self._devices[device_id] = device_state
        self._iface.register_device(device_id)

        # Create and register all metrics
        metrics = (item.payload.device.tahu_metrics(with_properties=True)
                   + item.payload.device.tahu_commands(with_properties=True))

        self._iface.set_initial_device_metrics(device_id, metrics)

        # Create a DBIRTH message and topic
        dbirth = self._iface.new_dbirth(device_id)
        dbirth_topic = self._iface.new_dbirth_topic(device_id)

        # Publish the DBIRTH
        self.publish(dbirth_topic, dbirth, qos=1, retain=True)

        # Clear any existing DDEATH
        ddeath_topic = self._iface.new_ddeath_topic(device_id)
        ddeath_payload = ''

        self.publish(ddeath_topic, ddeath_payload, qos=1, retain=True)

    def _process_UnregisterDeviceQueueItem(self, item):
        assert item.payload.node is self._node
        device_id = item.payload.device.device_id
        if item.payload.device.device_id in self._devices:
            device_state = self._devices[item.payload.device.device_id]
            device_state.is_up = False
        else:
            raise IcypawEngineError('Attempt made to unregister a device that does not exist')
        self._devices[device_id] = device_state
        self._iface.unregister_device(device_id)

        # Note: We don't unsubscribe from DCMD messages so that we can
        # in theory emit error messages upon receiving them. Also it
        # makes the code simpler.

        # Create a DDEATH message and topic
        ddeath = self._iface.new_ddeath()
        ddeath_topic = self._iface.new_ddeath_topic(device_id)

        # Publish the DDEATH
        self.publish(ddeath_topic, ddeath, qos=1, retain=True)

    def _process_NodeRebirthQueueItem(self, _item):
        """The node has indicated that it wants to reissue a fresh NBIRTH
        message with new values for all the metrics."""

        self._rebirth_node()

    def _rebirth_node(self):
        """Create a new birth certificate and publish it."""
        all_tahu_metrics = (self._node.tahu_metrics(with_properties=True)
                            + self._node.tahu_commands(with_properties=True))
        for metric in all_tahu_metrics:
            self._iface.set_node_metric(metric, add_if_missing=True)

        # Remove any metrics that have been deleted from the node but
        # not the interface.
        metric_names = {metric.name for metric in all_tahu_metrics}
        removed_metric_names = [name for name in self._iface.list_node_metric_names()
                                if name not in metric_names]
        for name in removed_metric_names:
            self._iface.del_node_metric(name)

        nbirth = self._iface.new_nbirth()
        nbirth_topic = self._iface.new_nbirth_topic()

        self.publish(nbirth_topic, nbirth, qos=1, retain=True)

    def _publish_metric_updates(self):
        """Check the node and all devices and find all updated
        metrics. Publish them. If a birth certificate is stale,
        republish it instead.

        """

        self._publish_node_metric_updates()
        self._publish_device_metric_updates()

    def _publish_node_metric_updates(self):
        """Publish updates to this node's metrics either as an NDATA or NBIRTH
        message."""
        if self._node.icpw_is_birth_certificate_fresh:
            node_payload = self._create_updated_endpoint_metric_payload(self._node)
            if node_payload is not None:
                node_topic = self._iface.new_ndata_topic()
                self.publish(node_topic, node_payload)
        else:
            self._rebirth_node()
            self._node.icpw_make_birth_certificate_fresh()

    def _publish_device_metric_updates(self):
        """Publish updates to all device metrics either as DDATA or DBIRTH
        messages."""
        for device_id, device_state in self._devices.items():
            if device_state.device.icpw_is_birth_certificate_fresh:
                device_payload = self._create_updated_endpoint_metric_payload(device_state.device)
                if device_payload is not None:
                    device_topic = self._iface.new_ddata_topic(device_state.device.device_id)
                    self.publish(device_topic, device_payload)
            else:
                self._rebirth_device(device_id, device_state.device)
                device_state.device.icpw_make_birth_certificate_fresh()

    def _rebirth_device(self, device_id, device):
        """Create a new birth certificate for this device and publish it."""

        all_tahu_metrics = (device.tahu_metrics(with_properties=True)
                            + device.tahu_commands(with_properties=True))
        for metric in all_tahu_metrics:
            self._iface.set_device_metric(device_id, metric)

        dbirth = self._iface.new_dbirth(device_id)
        dbirth_topic = self._iface.new_dbirth_topic(device_id)

        self.publish(dbirth_topic, dbirth, qos=1, retain=True)

    def _create_updated_endpoint_metric_payload(self, endpoint):
        """Go through this endpoint, either a node or device, and extract all
        changed metrics. If any metrics are changed, construct a protobuf
        payload."""

        icpw_metrics = [(name, curr_value) for name, (curr_value, old_value)
                        in endpoint.icpw_updated_metrics().items()]
        if not icpw_metrics:
            return None

        payload = new_payload()
        tahu_metrics = []
        for name, icpw_metric in icpw_metrics:
            tahu_metric = new_metric()
            tahu_metric.name = name.encode()
            icpw_metric.set_in_metric(tahu_metric)
            tahu_metrics.append(tahu_metric)
        add_metrics_to_payload(tahu_metrics, payload)

        return payload

##
# Helper classes
#

class DeviceState:
    """Data class keeping track of the state of a specific device."""
    def __init__(self, device, bdSeq=0, is_up=True):
        self._device = device
        self.bdSeq = bdSeq  # Trigger property setter
        self._is_up = bool(is_up)

    @property
    def device(self):
        return self._device

    @property
    def bdSeq(self):
        return self._bdSeq

    @bdSeq.setter
    def bdSeq(self, value):
        bdSeq = int(value)
        if not (0 <= bdSeq < 256):
            raise ValueError(f'bdSeq of {bdSeq} is not between 0 and 255')
        self._bdSeq = bdSeq

    @property
    def is_up(self):
        return self._is_up

    @is_up.setter
    def is_up(self, value):
        self._is_up = bool(value)

    @property
    def is_down(self):
        return not self._is_up

    @is_down.setter
    def is_down(self, value):
        self._is_up = not value

    def increment_bdSeq(self):
        self._bdSeq += 1
        if self._bdSeq == 256:
            self._bdSeq = 0

def _get_next_bdseq(topic, broker, port, timeout=_LAST_BDSEQ_TIMEOUT_S):
    """Synchronously get the next bdSeq number for a topic."""

    pool = ThreadPoolExecutor(2)
    kwargs = dict(
        msg_count=1,
        retained=True,
        hostname=broker,
        port=port,
    )
    nbirth_future = pool.submit(subscribe.simple, topic, **kwargs)
    try:
        last_nbirth = nbirth_future.result(timeout=timeout)
        payload = new_payload()
        payload.ParseFromString(last_nbirth.payload)
        last_bdseq = read_bdseq(payload)
        if last_bdseq is not None:
            _logger.debug(f"last_bdseq = {last_bdseq}")
            # TODO is byte overflow handled at the tahu interface level?
            return (last_bdseq + 1) % 256
    except TimeoutError:
        # Interpreted as no prior NBIRTH existing
        nbirth_future.cancel()
        _logger.warning("Timed out fetching last birth certificate from the "
                        "broker. If this is not the first time this Icypaw device "
                        "has been run, this may indicate an issue with the broker.")
    finally:
        # On Windows, subscribe.simple can lock up the thread in a way that canceling does not undo. So we let that
        # just hang out while we go on. This is actually fine, because it will finish as soon as we publish our own
        # birth certificate and so will not cause a problem.
        pool.shutdown(wait=False)
    _logger.debug("no last bdseq")
    return 0

##
# Exceptions
#

class IcypawEngineError(IcypawException):
    """An error at the Icypaw engine layer."""
    def __init__(self, message, log_level=logging.ERROR):
        _logger.log(log_level, f"IcypawEngineError: {message}")
        super().__init__(message)


class IcypawEngineStateException(IcypawEngineError):
    """An exception in the state and sequence of the application engine."""
    pass


class IcypawEngineShutdownException(Exception):
    """An exception thrown during engine runtime to signal that the engine is
    shutting down."""
    pass
