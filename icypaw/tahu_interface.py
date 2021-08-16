# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""A wrapper around TAHU to provide some of the niceties."""

import struct
import time
import copy
from enum import Enum
from collections.abc import Iterable, MutableMapping, MutableSequence

from .proto.sparkplug_b_pb2 import Payload
from .exceptions import TahuInterfaceError
from .topic import parse_topic, DEFAULT_NAMESPACE
import icypaw.conventions as conventions

# Bring classes defined underneath of Payload to top level for other
# modules to use.
Template = Payload.Template
Metric = Payload.Metric
DataSet = Payload.DataSet

class DataType(Enum):
    """Values copied from a comment in the Sparkplug B protocol."""
    # Unknown placeholder for future expansion.
    Unknown         = 0

    # Basic Types
    Int8            = 1
    Int16           = 2
    Int32           = 3
    Int64           = 4
    UInt8           = 5
    UInt16          = 6
    UInt32          = 7
    UInt64          = 8
    Float           = 9
    Double          = 10
    Boolean         = 11
    String          = 12
    DateTime        = 13
    Text            = 14

    # Additional Metric Types
    UUID            = 15
    DataSet         = 16
    Bytes           = 17
    File            = 18
    Template        = 19

    # Additional PropertyValue Types
    PropertySet     = 20
    PropertySetList = 21

class TahuClientInterface:
    """A wrapper around TAHU to take care of some details of the protocol
    from a client's point of view."""

    def __init__(self):
        """Create a new client interface for dealing with nodes and
        devices."""

        self._namespace = DEFAULT_NAMESPACE

        # A dictionary of dictionaries that map metric names to aliases.
        self._alias_map = {}

    ##
    # Regsiter nodes and devices
    #

    def register_nbirth(self, topic, nbirth):
        """Register a particular node's birth certificate. This enables this
        interface to create NCMD messages.

        """

        name = self._make_node_name_from_topic(topic)

        self._alias_map[name] = {metric.name: metric.alias for metric in nbirth.metrics}

    def register_dbirth(self, topic, dbirth):
        """Register a particular device's birth certificate. This enables this
        interface to create DCMD messages.

        """

        name = self._make_device_name_from_topic(topic)

        self._alias_map[name] = {metric.name: metric.alias for metric in dbirth.metrics}

    ##
    # Make commands and topics
    #

    def new_ncmd(self, group_id, edge_node_id, cmd, value, datatype):
        """Create a new NCMD payload and topic. Validate that we are using a
        command and value type compatible with the NBIRTH message registered
        earlier.

        cmd -- The name of the command, without the conventional prefix.

        """

        payload = new_payload()

        node_name = self._make_node_name(group_id, edge_node_id)

        metric = payload.metrics.add()
        self._set_name_or_alias(metric, node_name, cmd)
        metric.timestamp = payload.timestamp
        set_metric_value(value, datatype, metric)

        topic = f"{self._namespace}/{group_id}/NCMD/{edge_node_id}"

        return payload, topic

    def new_dcmd(self, group_id, edge_node_id, device_id, cmd, value, datatype):
        """Create a new DCMD payload and topic. Validate that we are using a
        command and value type compatible with the DBIRTH message registered
        earlier.

        cmd -- The name of the command, without the conventional prefix.

        """

        payload = new_payload()

        device_name = self._make_device_name(group_id, edge_node_id, device_id)

        metric = payload.metrics.add()
        self._set_name_or_alias(metric, device_name, cmd)
        metric.timestamp = payload.timestamp
        set_metric_value(value, datatype, metric)

        topic = f"{self._namespace}/{group_id}/DCMD/{edge_node_id}/{device_id}"

        return payload, topic

    ##
    # Private methods
    #

    def _make_node_name_from_topic(self, topic):
        """Create a standard node name to be used as an index."""
        topic_obj = parse_topic(topic)
        return self._make_node_name(topic_obj.group_id, topic_obj.edge_node_id)

    def _make_node_name(self, group_id, edge_node_id):
        return (group_id, edge_node_id)

    def _make_device_name_from_topic(self, topic):
        """Create a standard device name to be used as an index."""
        topic_obj = parse_topic(topic)
        return self._make_device_name(
            topic_obj.group_id, topic_obj.edge_node_id, topic_obj.device_id)

    def _make_device_name(self, group_id, edge_node_id, device_id):
        return (group_id, edge_node_id, device_id)

    def _set_name_or_alias(self, metric, node_name, cmd):
        """Set either the name or alias in this metric. We allow using a name
        if there is no registered birth certificate both for testing and to
        allow the user the flexibility to fire off commands without having
        first listened for a birth certificate."""

        try:
            metric.alias = self._alias_map[node_name][cmd]
        except KeyError:
            full_name = conventions.make_command(cmd)
            metric.name = full_name.encode()

class TahuServerInterface:
    """A wrapper around TAHU (Sparkplug B) to take care of some of the
    details of the protocol. One TahuInterface is necessary per node
    (i.e. it is shared between devices on the same node).

    """

    def __init__(self, group_id=None, edge_node_id=None, bdSeq=None):
        """Create a new interface used by a particular edge node.

        Parameters
        ----------
        group_id: str
            A logical grouping of nodes. If not provided here, a group ID must
            be provided to the methods creating a topic.

        edge_node_id: str
            The label for this edge node. If not provided here, an edge node ID
            must be provided to the methods creating a topic.

        """

        self._namespace = DEFAULT_NAMESPACE
        self._group_id = str(group_id) if group_id else None
        self._edge_node_id = str(edge_node_id) if edge_node_id else None

        # The sequence number for certain messages coming off this
        # node. This is used for *BIRTH, DDEATH, and *DATA messages,
        # although not for *CMD and NDEATH messages.
        self._seq = Seq()

        # Birth/death sequence number used to match up NDEATH notices
        # to NBIRTH notices. It is not used for DDEATH or DBIRTH
        # notices.
        self._bdSeq = bdSeq

        # Keep track of whether a BIRTH message has been issued. Once
        # we produce a BIRTH message, we may not introduce new
        # templates or metrics.
        self._is_born = False

        # The templates used by any of the devices or the node. These
        # are sent out in the birth message.
        self._templates = {}

        # All metrics are kept both for the node and each device. The
        # node is designated by the empty string.
        self._metric_organizers = {'': MetricOrganizer()}

    ##
    # Properties
    #

    @property
    def namespace(self):
        """Return the namespace used by topics. This indicates we are using
        sparkplug B and the version."""
        return self._namespace

    @property
    def group_id(self):
        """Return the group_id used in topics, or None if not set."""
        return self._group_id

    @group_id.setter
    def group_id(self, value):
        if value is None:
            self._group_id = None
        else:
            if '/' in value:
                raise TahuInterfaceError("group_id may not contain a forward slash '/'")
            self._group_id = str(value)

    @property
    def edge_node_id(self):
        """Return the edge_node_id used in topics, or None if not set."""
        return self._edge_node_id

    @edge_node_id.setter
    def edge_node_id(self, value):
        if value is None:
            self._edge_node_id = None
        else:
            if '/' in value:
                raise TahuInterfaceError("edge_node_id may not contain a forward slash '/'")
            self._edge_node_id = str(value)

    @property
    def bdSeq(self):
        """Return the current birth-death sequence number. Increment this
        number whenever the MQTT connection is re-established."""
        return self._bdSeq

    @bdSeq.setter
    def bdSeq(self, value):
        self._bdSeq = int(value) if value is not None else None

    @property
    def seq(self):
        """Return what the next sequence number to be issued is. Note: this
        may not actually be the next sequence number if an NBIRTH is
        re-issued."""

        return self._seq.value

    @property
    def is_born(self):
        """Return whether this node is already born. This is true once a birth
        certificate is created."""
        return self._is_born

    def get_node_metric_alias(self, name):
        """Return the alias for the metric of the given name issued by this
        node."""

        return self._metric_organizers[''].get_alias(name)

    def get_node_metric_name(self, alias):
        """Return the name for the metric of the given alias issued by this node."""
        return self._metric_organizers[''].get_name(alias)

    def get_device_metric_alias(self, device_id, name):
        """Return the alias for the metric of the given name issued by the
        named device."""

        if device_id not in self._metric_organizers:
            raise TahuInterfaceError(f"No device '{device_id}' registered")

        return self._metric_organizers[device_id].get_alias(name)

    def get_device_metric_name(self, device_id, alias):
        """Return the name for the metric of the given alias issued by this node."""
        if device_id not in self._metric_organizers:
            raise TahuInterfaceError(f"No device '{device_id}' registered")

        return self._metric_organizers[device_id].get_name(alias)

    def list_node_metric_names(self):
        """Return a list of the name of all metrics in this node."""
        return [metric.name for metric in self._metric_organizers[''].get_all()]

    def list_device_metric_names(self, device_id):
        """Return a list of the name of all metrics in this node."""
        return [metric.name for metric in self._metric_organizers[device_id].get_all()]

    ##
    # Register and set values
    #

    def register_device(self, name):
        """Register a device. This device will have its own metrics."""

        self._metric_organizers[name] = MetricOrganizer()

    def unregister_device(self, name):
        """Remove a device. Note that this can mess up the templates in the
        NBIRTH message. It's best to register all possible device metrics
        using register_device_class_metrics then only register and unregister
        devices using those metrics."""

        del self._metric_organizers[name]

    def set_initial_node_metrics(self, metrics):
        """Set all metrics for this node. No metrics may be set that were not
        included in this initial call."""

        if self._is_born:
            raise TahuInterfaceError("Cannot set initial metrics after issuing BIRTH message")

        templates = self._metric_organizers[''].set_initial_metrics(metrics)
        self._templates.update(templates)

    def set_node_metric(self, metric, add_if_missing=False):
        """Set a metric used by the node on this interface. This will be sent
        out in the next NDATA message. All metrics must use a name,
        but the interface will replace the name with an alias to save
        space in transmission.

        """

        self._metric_organizers[''].set(metric, add_if_missing=add_if_missing)

    def del_node_metric(self, name):
        """Remove the named metric from this node."""

        self._metric_organizers[''].delete(name)

    def register_device_class_metrics(self, metrics):
        """Set metrics that may appear in a device. This is solely used to
        extract template definitions for use in the next NBIRTH message."""

        # We will create a metric organizer solely for its ability to
        # extract templates. Perhaps this could be refactored in the
        # future.
        org = MetricOrganizer()
        templates = org.set_initial_metrics(metrics)
        self._templates.update(templates)

    def set_initial_device_metrics(self, device, metrics):
        """Set all metrics for a device on this node. No metrics may be set
        that were not included in this initial call.

        """

        templates = self._metric_organizers[device].set_initial_metrics(metrics)
        self._templates.update(templates)

    def set_device_metric(self, device, metric):
        """Set a metric used by the node on this interface. This will be sent
        out in the next DDATA message. All metrics must use a name,
        but the interface will replace the name with an alias to save
        space in transmission.

        """

        self._metric_organizers[device].set(metric)

    ##
    # Construct payloads
    #

    def new_nbirth(self):
        """Return a fully filled-in NBIRTH message. This requires all metrics
        to be initially set.

        """

        self._is_born = True

        payload = new_payload()
        # Note: By my reading of the spec reissuing an NBIRTH will
        # reset the serial number, although it takes some lawyering to
        # work this out.
        payload.seq = self._seq.reset_and_advance()

        timestamp = payload.timestamp

        # Include a bdSeq metric
        self._fill_in_bdseq_metric(payload.metrics.add(), timestamp)

        # Only include metrics for this node, not the devices.
        for metric in self._metric_organizers[''].get_all():
            payload_metric = payload.metrics.add()
            copy_from_protobuf(payload_metric, metric)

        # Include all template definitions, however.
        for name, template in self._templates.items():
            template_metric = payload.metrics.add()
            template_metric.name = f"_types_/{name}".encode()
            template_metric.timestamp = timestamp
            template_metric.datatype = DataType.Template.value
            copy_from_protobuf(template_metric.template_value, template)

        return payload

    def new_dbirth(self, device_id):
        """Return a fully filled-in DBIRTH message payload. This requires all
        metrics for the given device to be set."""

        if not self._is_born:
            raise TahuInterfaceError("First message issued must be an NBIRTH")

        if device_id not in self._metric_organizers:
            raise TahuInterfaceError(f"No device {device_id} registered")

        payload = self.new_seq_payload()

        add_metrics_to_payload(self._metric_organizers[device_id].get_all(), payload)

        return payload

    def new_ndeath(self):
        """Return a fully filled-in NDEATH payload. This requires only a bdSeq
        number. This message is generally given to the MQTT broker to
        be issued in case the node becomes unresponsive.

        """

        # We do not need a timestamp or sequence number so we don't
        # use new_payload()
        payload = Payload()

        metric = payload.metrics.add()
        self._fill_in_bdseq_metric(metric, None)

        return payload

    def new_ddeath(self):
        """Return a fully filled-in DDEATH payload."""

        # Okay, so the spec says both that the DDEATH payload contains
        # only the seq number and also in a different section that the
        # DDEATH message contains no payload at all. So here we
        # provide a mostly-empty payload.

        payload = self.new_seq_payload()
        return payload

    def new_ndata(self):
        """Return a fully filled-in NDATA payload. This contains all of the
        metrics that have been updated since the last NDATA or NBIRTH
        message."""

        if not self._is_born:
            raise TahuInterfaceError("Must issue NBIRTH message before NDATA")

        payload = self.new_seq_payload()

        metrics = self._metric_organizers[''].get_and_commit()
        add_metrics_to_payload(metrics, payload)

        return payload

    def new_ddata(self, device_id):
        """Return a fully filled-in DDATA payload. This contains all of the
        device metrics that have been updated since the last DDATA or
        DBIRTH message.

        """

        # TODO: This check is necessary but not sufficient, since it
        # only checks for an NBIRTH message.
        if not self._is_born:
            raise TahuInterfaceError("Must issue DBIRTH message before DDATA")

        if device_id not in self._metric_organizers:
            raise TahuInterfaceError(f"No device '{device_id}' registered")

        payload = self.new_seq_payload()

        metrics = self._metric_organizers[device_id].get_and_commit()
        add_metrics_to_payload(metrics, payload)

        return payload

    def new_seq_payload(self, timestamp=None):
        """Return a new Payload object mostly not filled in. Likely you will
        not want to use this method directly.

        Parameters
        ----------
        timestamp: int, optional
            If given, this must be the number of milliseconds since the epoch as
            an int. If not given, the current time will be used.
        """

        payload = new_payload(timestamp)
        payload.seq = self._seq.get_and_advance()

        return payload

    ##
    # Construct topics
    #

    def new_nbirth_topic(self, group_id=None, edge_node_id=None):
        """Create a new topic for a node birth NBIRTH message."""
        message_type = 'NBIRTH'
        return self.new_topic(None, group_id, message_type, edge_node_id, None)

    def new_dbirth_topic(self, device_id, group_id=None, edge_node_id=None):
        """Create a new topic for a device's DBIRTH message."""
        message_type = 'DBIRTH'
        # We do not check the device ID against registered devices so
        # that this method can be used to generate topics for other
        # node's devices as well. In case that's useful or something.
        if not device_id:
            raise TahuInterfaceError('Please provide a device_id for the DBIRTH topic')
        return self.new_topic(None, group_id, message_type, edge_node_id, device_id)

    def new_ndeath_topic(self, group_id=None, edge_node_id=None):
        """Create a new topic for a node death NDEATH message."""
        message_type = 'NDEATH'
        return self.new_topic(None, group_id, message_type, edge_node_id, None)

    def new_ddeath_topic(self, device_id, group_id=None, edge_node_id=None):
        """Create a new topic for a device death DDEATH message."""
        message_type = 'DDEATH'
        return self.new_topic(None, group_id, message_type, edge_node_id, device_id)

    def new_ndata_topic(self, group_id=None, edge_node_id=None):
        """Create a new topic for a node data NDATA message."""
        message_type = 'NDATA'
        return self.new_topic(None, group_id, message_type, edge_node_id, None)

    def new_ddata_topic(self, device_id, group_id=None, edge_node_id=None):
        """Create a new topic for a device data DDATA message."""
        message_type = 'DDATA'
        return self.new_topic(None, group_id, message_type, edge_node_id, device_id)

    def new_ncmd_topic(self, group_id=None, edge_node_id=None):
        """Create a new topic for a node command NCMD message."""
        message_type = 'NCMD'
        return self.new_topic(None, group_id, message_type, edge_node_id, None)

    def new_dcmd_topic(self, device_id, group_id=None, edge_node_id=None):
        """Create a new topic for a node command DCMD message."""
        message_type = 'DCMD'
        return self.new_topic(None, group_id, message_type, edge_node_id, device_id)

    def new_state_topic(self, scada_host_id):
        """Create a new topic for a host state message."""
        if not scada_host_id:
            raise TahuInterfaceError("scada_host_id must be provided")
        if '/' in scada_host_id:
            raise TahuInterfaceError("scada_host_id may not contain a forward slash '/'")
        # This does not follow the convention of the node/device messages.
        topic = f"STATE/{scada_host_id}"
        return topic

    def new_topic(self, namespace, group_id, message_type, edge_node_id, device_id):
        """Create a topic from the constituent parts. Prefer to use the
        message-specific new_*_topic methods.

        Any argument except for message_type can be None and will be
        ignored or given a default value as appropriate.

        """

        components = [(namespace, 'namespace'),
                      (group_id, 'group_id'),
                      (message_type, 'message_type'),
                      (edge_node_id, 'edge_node_id'),
                      (device_id, 'device_id')]
        for component, name in components:
            if component is not None and '/' in component:
                raise TahuInterfaceError(f"{name} may not contain a forward slash '/'")

        if namespace is None:
            namespace = self._namespace

        assert namespace is not None

        if group_id is None:
            group_id = self._group_id

        if group_id is None:
            raise TahuInterfaceError('group_id must be provided either in constructor or when creating a topic')

        if edge_node_id is None:
            edge_node_id = self._edge_node_id

        if edge_node_id is None:
            raise TahuInterfaceError('edge_node_id must be provided either in constructor or when creating a topic')

        node_topic = f"{namespace}/{group_id}/{message_type}/{edge_node_id}"

        if device_id is not None:
            topic = f"{node_topic}/{device_id}"
        else:
            topic = node_topic

        return topic

    ##
    # Private methods
    #

    def _fill_in_bdseq_metric(self, metric, timestamp):
        """Fill in all necessary fields for a metric containing the
        bdSeq. Raise an exception if the bdSeq is not set in this instance."""

        if self.bdSeq is None:
            raise TahuInterfaceError('bdSeq not set')

        metric.name = "bdSeq".encode()
        if timestamp is not None:
            metric.timestamp = make_timestamp(timestamp)
        metric.datatype = DataType.UInt64.value
        metric.long_value = self.bdSeq

##
# Helper functions for payloads and metrics
#

def make_timestamp(timestamp=None):
    """Return a properly formatted timestamp. If the argument is None,
    create a timestamp with the current time."""

    if timestamp is not None:
        return timestamp

    # Note that time.time() returns the number of seconds, not
    # accounting for leap seconds, since January 1, 1970 (UTC).
    return int(time.time() * 1000)

def read_timestamp(metric):
    """Read the timestamp from the given metric and convert it to the
    format used by time.time()."""
    return metric.timestamp / 1000.0

def new_payload(timestamp=None):

    """Create a new payload with timestamp."""

    timestamp = make_timestamp(timestamp)

    payload = Payload()
    payload.timestamp = timestamp

    return payload

def new_metric(timestamp=None, properties=None):
    """Create a new metric object, filling in the timestamp with the
    current time if not given."""

    metric = Metric()
    timestamp = make_timestamp(timestamp)
    metric.timestamp = timestamp
    iterable_to_propertyset(properties or {}, metric.properties)
    return metric

def add_metrics_to_payload(metrics, payload):
    """Copy a list of metrics exactly into the metrics section of payload,
    creating the metrics in the process."""

    for metric in metrics:
        payload_metric = payload.metrics.add()
        copy_from_protobuf(payload_metric, metric)

def set_metric_value(value, datatype, metric):
    """Set a value with the given data type into the provided metric."""

    metric.datatype = datatype.value
    set_in_tahu_object(value, datatype, metric)

def metric_property_dict(metric):
    """Build a PropertyDict view for the metric's properties"""
    return PropertyDict(metric.properties)


def set_in_tahu_object(value, datatype, tahu_object):
    """Set a value in some Tahu object, relying on the standard naming
    convention and Python's dynamic lookup. If the datatype is not
    supported by the given object, this will cause an exception.

    """

    # Note: We don't actually check the bounds properly on integers
    # beyond whether they will fit.

    int_types = [DataType.Int8, DataType.Int16, DataType.Int32]
    uint_types = [DataType.UInt8, DataType.UInt16, DataType.UInt32]

    if datatype in int_types:
        value = convert_to_unsigned32(value)
        tahu_object.int_value = value
    elif datatype in uint_types:
        if value < 0:
            raise TahuInterfaceError("Negative number passed for unsigned type")
        value = convert_to_unsigned32(value)
        tahu_object.int_value = value
    elif datatype == DataType.Int64:
        value = convert_to_unsigned64(value)
        tahu_object.long_value = value
    elif datatype == DataType.UInt64:
        if value < 0:
            raise TahuInterfaceError("Negative number passed for unsigned type")
        value = convert_to_unsigned64(value)
        tahu_object.long_value = value
    elif datatype == DataType.Float:
        # This may actually do some conversion, unlike the int fields
        tahu_object.float_value = float(value)
    elif datatype == DataType.Double:
        tahu_object.double_value = float(value)
    elif datatype == DataType.Boolean:
        tahu_object.boolean_value = bool(value)
    elif datatype == DataType.String or datatype == DataType.Text:
        if isinstance(value, bytes):
            tahu_object.string_value = value
        else:
            tahu_object.string_value = str(value).encode()
    elif datatype == DataType.DateTime:
        tahu_object.long_value = value
    elif datatype == DataType.Template:
        # TODO we should maybe type-check tahu_object
        copy_from_protobuf(tahu_object.template_value, value)
    elif datatype == DataType.DataSet:
        if isinstance(value, DataSet):
            copy_from_protobuf(tahu_object.dataset_value, value)
        elif hasattr(value, 'set_in_metric'):
            # We can't check if this is ArrayType as that creates a circular import
            value.set_in_metric(tahu_object)
        else:
            raise TahuInterfaceError(f"Cannot convert {value} to a DataSet")
    elif datatype == DataType.PropertySet:
        copy_from_protobuf(tahu_object.propertyset_value, value)
    elif datatype == DataType.PropertySetList:
        copy_from_protobuf(tahu_object.propertysets_value, value)
    else:
        raise TahuInterfaceError("Yet unsupported type")

def convert_to_unsigned32(value):
    """Convert an integer to 32-bit unsigned."""
    if value > 0xffffffff:
        raise TahuInterfaceError(f"Cannot convert large integer {value} to 32-bit")

    if value > 0:
        return value

    if value < -0x80000000:
        raise TahuInterfaceError(f"Cannot convert small negative integer {value} to 32-bit")

    packed = struct.pack('i', value)
    return struct.unpack('I', packed)[0]

def convert_to_unsigned64(value):
    """Convert an integer to 64-bit unsigned."""
    if value > 0xffffffffffffffff:
        raise TahuInterfaceError(f"Cannot convert large integer {value} to 64-bit")

    if value > 0:
        return value

    if value < -0x8000000000000000:
        raise TahuInterfaceError(f"Cannot convert small negative integer {value} to 64-bit")

    packed = struct.pack('q', value)
    return struct.unpack('Q', packed)[0]

def convert_to_signed32(value):
    """Convert an integer to 32-bit signed."""
    if value > 0xffffffff:
        raise TahuInterfaceError(f"Cannot convert large integer {value} to 32-bit")

    if value < -0x80000000:
        raise TahuInterfaceError(f"Cannot convert small negative integer {value} to 32-bit")

    if value < 0:
        return value

    packed = struct.pack('I', value)
    return struct.unpack('i', packed)[0]

def convert_to_signed64(value):
    """Convert an integer to 64-bit signed."""
    if value > 0xffffffffffffffff:
        raise TahuInterfaceError(f"Cannot convert large integer {value} to 64-bit")

    if value < -0x8000000000000000:
        raise TahuInterfaceError(f"Cannot convert small negative integer {value} to 64-bit")

    if value < 0:
        return value

    packed = struct.pack('Q', value)
    return struct.unpack('q', packed)[0]

def read_bdseq(message):
    """Read the bdSeq number from a TAHU message."""
    for metric in message.metrics:
        if metric.HasField('name') and metric.name == 'bdSeq':
            return metric.long_value
    return None

def copy_from_protobuf(dst, src):
    """Copy the data from src to dst. Make sure that dst is the same type
    as src."""
    if not isinstance(dst, type(src)):
        raise TypeError(f'Cannot copy from {type(src)} into {type(dst)}')
    dst.CopyFrom(src)

class Seq:
    """The sequence number used in certain messages. This is an 8-bit
    unsigned value that wraps to zero when it goes past its limit."""

    def __init__(self):
        self._seq = 0

    @property
    def value(self):
        """Return the current value, do not advance."""
        return self._seq

    def reset(self):
        """Reset the sequence number to zero."""
        self._seq = 0

    def get_and_advance(self):
        """Return the current sequence number and advance the counter."""
        ret = self._seq
        self._advance()
        return ret

    def reset_and_advance(self):
        """Equivalent to calling reset then get_and_advance."""
        self.reset()
        return self.get_and_advance()

    def _advance(self):
        self._seq = (self._seq + 1) & 0xff

class MetricOrganizer:
    """A class that keeps track of metrics for a given device or node."""

    def __init__(self):

        self._metric_class = Payload.Metric
        self._template_class = Payload.Template

        # The current table of the last value for each metric. This
        # includes commands.
        self._metrics = {}

        # Metrics added but not yet moved to the latest metric table.
        self._uncommitted_metrics = []

        self._next_alias = 0

        # A mapping of metric names to alias numbers.
        self._metric_names_to_aliases = {}

        # A dictionary mapping the names of template definitions that
        # have appeared in metrics to template definitions (which by
        # the spec must have their names scrubbed).
        self._template_definitions = {}

        self._committed = False

    @property
    def _metric_aliases_to_names(self):
        """Inverse mapping of metric alias numbers to names.

        This should in principle always be one-to-one, so no data is lost in
        inverting the mapping.
        """
        # TODO computing this can be expensive, we may want to just book-keep this
        return {v: k for k, v in self._metric_names_to_aliases.items()}

    @property
    def template_definitions(self):
        """Return a dictionary of all template definitions used by metrics in
        this organizer."""

        if not self._committed:
            raise TahuInterfaceError('Attempted to retrieve templates before commiting metrics')

        return copy.copy(self._template_definitions)

    def set_initial_metrics(self, metrics):
        """Give this organizer a list of all metrics that will be used by this device or node.

        Return a dict of template definitions derived from the metrics.

        """

        for metric in metrics:
            self._add_metric(metric)

        self._committed = True

        return self._template_definitions

    def set(self, metric, add_if_missing=False):
        """Set a metric. It will be validated before being added to the uncommitted
        list.

        """

        if add_if_missing and \
           metric.HasField('name') and metric.name not in self._metrics:
            self._add_metric(metric)

        self._validate_metric(metric)

        self._uncommitted_metrics.append(metric)

    def delete(self, name):
        """Delete a metric. Silently ignore it if the metric does not exist."""
        if name not in self._metrics:
            return

        del self._metrics[name]

        self._uncommitted_metrics = [metric for metric in self._uncommitted_metrics
                                     if metric.name != name]

    def get_and_commit(self):
        """Return all uncommitted metrics and commit them. The metrics will
        have their names replaced with aliases."""

        rets = [self._copy_with_alias(metric) for metric in self._uncommitted_metrics]

        self._commit_metrics()

        return rets

    def get_all(self):
        """Return all metrics with their current values. All metrics will have
        both a name and alias and are thus suitable for use in a birth
        message."""

        metrics = []

        self._commit_metrics()

        for name, metric in self._metrics.items():
            metric_copy = self._metric_class()
            copy_from_protobuf(metric_copy, metric)
            metric_copy.name = name.encode()
            metric_copy.alias = self._metric_names_to_aliases[name]
            metrics.append(metric_copy)

        return metrics

    def new_metric(self, name):
        """Create a new metric with values from the last metric of the given name."""

        metric = self._metric_class()
        copy_from_protobuf(metric, self._metrics[name])

        return metric

    def get_alias(self, name):
        """Return the alias used for the given metric name."""
        if name not in self._metric_names_to_aliases:
            raise TahuInterfaceError(f"No alias '{name}' registered")
        return self._metric_names_to_aliases[name]

    def get_name(self, alias):
        """Return the name used for the given metric alias."""
        inv_map = self._metric_aliases_to_names
        if alias not in inv_map:
            raise TahuInterfaceError(f"No name for alias '{alias}' registered")
        return inv_map[alias]

    ##
    # Private methods
    #

    def _extract_store_template_definition(self, metric):
        """If this metric uses a template, create a template definition from
        the template instance and store it."""

        if metric.WhichOneof('value') == 'template_value':
            template_def, name = self._make_template_definition(metric.template_value)
            self._template_definitions[name] = template_def

    def _make_template_definition(self, template_instance):
        """Create a template definition from the given
        template_instance. Return the definition and the name used to identify
        it in instances."""

        template_def = self._template_class()
        copy_from_protobuf(template_def, template_instance)
        template_def.is_definition = True

        name = template_instance.template_ref

        template_def.ClearField('template_ref')
        for metric in template_def.metrics:
            # It may not harm anything to keep other types in as well,
            # but we definitely need to preserve data sets as there's
            # no other way to determine the type of the array that is
            # expected.
            if metric.datatype != DataType.DataSet.value:
                metric.ClearField('value')

        return template_def, name

    def _add_metric(self, metric):
        """Add a metric that does not exist to this container."""
        if not metric.HasField('name'):
            raise TahuInterfaceError('Initial metrics must have a name')

        if metric.name in self._metric_names_to_aliases:
            alias = self._metric_names_to_aliases[metric.name]
        else:
            alias = self._next_alias
            self._metric_names_to_aliases[metric.name] = self._next_alias
            self._next_alias += 1

        metric.alias = alias

        self._extract_store_template_definition(metric)

        self._metrics[metric.name] = metric

    def _validate_metric(self, metric):
        """Make sure this is a valid metric by matching it to one of the
        existing metrics."""

        if metric.HasField('name'):
            if metric.name not in self._metrics:
                raise TahuInterfaceError(f'Metric {metric.name} not found in registered metrics')

            model_metric = self._metrics[metric.name]
            if metric.datatype != model_metric.datatype:
                raise TahuInterfaceError(f'Metric {metric.name} has bad data type {metric.datatype}')
        else:
            # NOTE: We don't do more checking when an alias is
            # provided on the assumption that the user used the
            # new_metric() method. We could do more checking here but
            # we can't look up the model metric by name.
            if not metric.HasField('alias'):
                raise TahuInterfaceError('Metric has neither name nor alias provided')

    def _copy_with_alias(self, metric):
        """Create a copy of the metric, but remove the name and fill in the
        alias. This is part of the spec as a space-saving measure."""

        metric_copy = self._metric_class()
        copy_from_protobuf(metric_copy, metric)

        if metric_copy.HasField('name'):
            metric_copy.alias = self._metric_names_to_aliases[metric_copy.name]
            metric_copy.ClearField('name')

        return metric_copy

    def _commit_metrics(self):
        """Move all uncommitted metrics to the metrics table. Additionally, if
        this is the first commit, create the list of template definitions."""
        for metric in self._uncommitted_metrics:
            self._metrics[metric.name] = metric

        self._uncommitted_metrics = []

##
# Converting metrics to Python primitives
#

def read_from_metric(metric):
    """Read the value in a metric and return it as a scalar, dictionary,
    or list, or some nested structure of the above.

    """

    return read_tahu_value(metric.datatype, metric)

def read_from_dataset(dataset):
    """Read the value from a dataset as a list. If there is one column,
    the list will contain scalars. Otherwise it will contain tuples."""
    res = []
    types = [int(type_code) for type_code in dataset.types]
    if dataset.num_of_columns != len(types):
        raise ValueError('Malformed DataSet: num_of_columns does not match length of types')
    for row in dataset.rows:
        if len(row.elements) != dataset.num_of_columns:
            raise ValueError('Malformed DataSet: num_of_columns does not match length of row')
        if len(types) == 1:
            row_value = read_tahu_value(types[0], row.elements[0])
        else:
            row_value = tuple(read_tahu_value(type_code, elem)
                              for type_code, elem in zip(types, row.elements))
        res.append(row_value)
    return res

def read_from_template(template):
    # Note: We make the assumption here that no metrics within a
    # template have an alias. The spec if unclear whether this is
    # allowed, but it is definitely not required and it makes the code
    # a bit more complicated to account for.
    assert all(metric.HasField('name') for metric in template.metrics), "Metric in a template uses an alias"
    return {metric.name: read_from_metric(metric) for metric in template.metrics}

def read_tahu_value(datatype, tahu_object):
    """Read a value from a Tahu object. This relies on DataSet and Metric
    messages having the same naming convention."""
    if datatype == DataType.Int8.value:
        return reformat_int("B", "b", tahu_object.int_value)
    if datatype in [DataType.UInt8.value, DataType.UInt16.value, DataType.UInt32.value]:
        return tahu_object.int_value
    if datatype == DataType.UInt64.value:
        return tahu_object.long_value
    if datatype == DataType.Int16.value:
        return reformat_int("H", "h", tahu_object.int_value)
    if datatype == DataType.Int32.value:
        return reformat_int("I", "i", tahu_object.int_value)
    if datatype == DataType.Int64.value:
        return reformat_int("Q", "q", tahu_object.long_value)
    if datatype == DataType.Float.value:
        return tahu_object.float_value
    if datatype == DataType.Double.value:
        return tahu_object.double_value
    if datatype == DataType.Boolean.value:
        return tahu_object.boolean_value
    if datatype == DataType.String.value or datatype == DataType.Text.value:
        return tahu_object.string_value
    if datatype == DataType.DateTime.value:
        return tahu_object.datetime_value
    if datatype == DataType.DataSet.value:
        # TODO should likewise be type-checking tahu_object
        assert hasattr(tahu_object, 'dataset_value')
        return read_from_dataset(tahu_object.dataset_value)
    if datatype == DataType.Template.value:
        assert hasattr(tahu_object, 'template_value')
        return read_from_template(tahu_object.template_value)
    if datatype == DataType.PropertySet.value:
        assert hasattr(tahu_object, 'propertyset_value')
        return read_from_propertyset(tahu_object.propertyset_value)
    if datatype == DataType.PropertySetList.value:
        assert hasattr(tahu_object, 'propertysets_value')
        return read_from_propertysetlist(tahu_object.propertysets_value)
    assert False, f"Cannot read datatype {datatype}"

def reformat_int(src_format, dst_format, value):
    """Reformat a packed integer. This is used to convert between signed
    and unsigned, not network and host endian (which protobuf is supposed
    to take care of)."""

    return struct.unpack(dst_format, struct.pack(src_format, value))[0]

def read_from_propertyset(ps):
    """Build a native python iterable from a PropertySet.

    If the given PropertySet has keys, the returned iterable will be a dict
    mapping each key to a native python value. If the number of keys does not
    match the number of values, the extra keys or values will be omitted.
    On the other hand, if the given PropertySet does NOT have keys, a list of
    native python values will be returned.
    """
    if _propertyset_is_map(ps):
        return {k: read_tahu_value(v.type, v) for k, v in zip(ps.keys, ps.values)}
    else:
        return [read_tahu_value(v.type, v) for v in ps.values]

def read_from_propertysetlist(psl):
    """Build a native python list from a PropertySetList.

    The returned value will be a list of iterables read by
    ``read_from_propertyset``. The same warnings apply.
    """
    return [read_from_propertyset(ps) for ps in psl.propertyset]

###
# PropertySet tools
#

def is_endpoint_property(metric):
    """Return whether or not the metric is the container for an endpoint property"""
    properties = PropertyDict(metric.properties)
    return properties.get('ICPWEndpointProperty', False)

def build_endpoint_property(key, icpw_value):
    """Construct an endpoint property metric wrapper.

    By convention, endpoint properties are formatted as metrics with the
    reserved property "ICPWEndpointProperty" set to True. Icypaw clients should
    not include metrics with these flags in collections of endpoint metrics.
    """
    prop_metric = new_metric(properties={'ICPWEndpointProperty': True})
    prop_metric.name = key.encode()
    icpw_value.set_in_metric(prop_metric)
    return prop_metric

def _set_in_propertyvalue(value, ps_value):
    """Helper function to set a python value in a PropertyValue.

    If the given value is a PropertyValue, it will be copied directly into the
    destination buffer. If it is a PropertySet or PropertySetList, it will
    likewise be copied into the appropriate field in the destination buffer.

    Otherwise, this method will infer the PropertyValue type from the python
    type of the value. If the value is an iterable, it will be converted into a
    new PropertySet. Otherwise, it will be handled as a scalar type.

    For scalar values, boolean values are inferred to have type Boolean
    unambiguously. Integer values are inferred to have type Int64,
    floating-point values are inferred to have type Double, and string values
    are inferred to have type String.
    """
    if isinstance(value, Payload.PropertyValue):
        copy_from_protobuf(ps_value, value)
    elif isinstance(value, Payload.PropertySetList):
        ps_value.type = DataType.PropertySetList.value
        copy_from_protobuf(ps_value.propertysets_value, value)
    elif isinstance(value, Payload.PropertySet):
        ps_value.type = DataType.PropertySet.value
        copy_from_protobuf(ps_value.propertyset_value, value)
    elif isinstance(value, Iterable) and not isinstance(value, str):
        ps_value.type = DataType.PropertySet.value
        iterable_to_propertyset(value, ps=ps_value.propertyset_value)
    else:
        if isinstance(value, bool):
            datatype = DataType.Boolean
        elif isinstance(value, int):
            datatype = DataType.Int64
        elif isinstance(value, float):
            datatype = DataType.Double
        elif isinstance(value, str):
            datatype = DataType.String
        else:
            raise TahuInterfaceError(f"Unserializable metric property value {value}")

        set_in_tahu_object(value, datatype, ps_value)
        ps_value.type = datatype.value

def iterable_to_propertyset(iterable, ps=None):
    """Build a tahu PropertySet representation of the given iterable.

    We use a PropertySet with no keys to represent non-dict iterables. NOTE that
    while this is valid in the protobuf, it is not supported by the Sparkplug-B
    spec. There is therefore no guarantee that all clients will support
    list-type properties.
    """
    ps = ps or Payload.PropertySet()

    for element in iterable:
        if isinstance(iterable, MutableMapping):
            assert isinstance(element, str), f"Property keys must be strings (got key: {element})"
            ps.keys.append(element.encode())
            element = iterable[element]

        ps_value = ps.values.add()
        _set_in_propertyvalue(element, ps_value)

    return ps

def property_value(value, datatype, p_value=None):
    """Wrap a raw python value in a PropertyValue.

    Unlike other PropertySet functions, this will not attempt to infer the Tahu
    datatype of the value; it must instead be called with an explicit datatype.
    """
    if not isinstance(datatype, DataType):
        datatype = DataType(datatype)

    p_value = p_value or Payload.PropertyValue()
    set_in_tahu_object(value, datatype, p_value)
    p_value.type = datatype.value
    return p_value

def _propertyset_is_map(ps):
    """Helper function to determine if the given PropertySet should be handled
    like a map or like a sequence"""
    return len(ps.keys) > 0 or len(ps.values) == 0

class _PropertyViewMixin:
    """Mix-in for PropertySet view classes"""
    def __init__(self, propertyset):
        self._ps = propertyset

    @property
    def propertyset(self):
        return self._ps

    @classmethod
    def _view_for(cls, ps):
        """Helper method to dispatch the property view subclass (Dict or List)
        to use for a PropertySet."""
        if _propertyset_is_map(ps):
            return PropertyDict(ps)
        else:
            return _PropertyList(ps)

    @classmethod
    def _unwrap_propertyvalue(cls, ps_value):
        if ps_value.type == DataType.PropertySet.value:
            return cls._view_for(ps_value.propertyset_value)
        elif ps_value.type == DataType.PropertySetList.value:
            return [cls._view_for(ps) for ps in ps_value.propertysets_value.propertyset]
        else:
            return read_tahu_value(ps_value.type, ps_value)

class PropertyDict(_PropertyViewMixin, MutableMapping):
    """Mutable dict view to a propertyset"""
    def __init__(self, propertyset):
        super().__init__(propertyset)

        # Internal map of PropertySet keys to PropertyValues
        self._map = dict(zip(propertyset.keys, propertyset.values))

    def __getitem__(self, key):
        return self._unwrap_propertyvalue(self._map[key])

    def __setitem__(self, key, value):
        if key in self._map:
            ps_value = self._map[key]
        else:
            if not isinstance(key, str):
                raise TypeError(f"Property keys must be strings (got key: {key})")
            self._ps.keys.append(key.encode())
            ps_value = self._ps.values.add()
            self._map[key] = ps_value

        _set_in_propertyvalue(value, ps_value)

    def __delitem__(self, key):
        value = self._map[key]
        self._ps.keys.remove(key)
        self._ps.values.remove(value)
        del self._map[key]

    def __iter__(self):
        return iter(self._map)

    def __len__(self):
        return len(self._map)

    def __repr__(self):
        return str({k: v for k, v in self.items()})

class _PropertyList(_PropertyViewMixin, MutableSequence):
    """Mutable list view to an un-keyed propertyset"""
    def __getitem__(self, index):
        return self._unwrap_propertyvalue(self._ps.values[index])

    def __setitem__(self, index, value):
        _set_in_propertyvalue(value, self._ps.values[index])

    def __delitem__(self, index):
        del self._ps.values[index]

    def __iter__(self):
        for ps_value in self._ps.values:
            yield self._unwrap_propertyvalue(ps_value)

    def __len__(self):
        return len(self._ps.values)

    def __repr__(self):
        return str(list(self))

    def insert(self, index, value):
        # the protobuf RepeatedCompositeContainer doesn't natively support insertion
        # so this may be expensive
        index = min(index, len(self))
        next_value = Payload.PropertyValue()
        _set_in_propertyvalue(value, next_value)
        temp_value = self._ps.values.add()
        for ps_value in self._ps.values[index:]:
            copy_from_protobuf(temp_value, ps_value)
            copy_from_protobuf(ps_value, next_value)
            copy_from_protobuf(next_value, temp_value)
