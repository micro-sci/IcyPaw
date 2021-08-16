# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Base class for Node and Device servers."""

from collections import namedtuple
from threading import Lock
import functools

from .engine_queue import ScheduleQueueItem
from .timer_descriptor import iter_timer_objects
from .metric_descriptor import iter_metric_objects, iter_metric_objects_from_type, get_metric_object, Metric
from .command_descriptor import iter_command_objects, iter_command_objects_from_type
from .exceptions import IcypawException

class ServerEndpointBase:
    """Base class for Node and Device servers."""

    ##
    # Constructor and initialization
    #

    def __init__(self, group_id):
        # We communicate with the Engine by means of placing messages
        # on this command queue.
        self._command_queue = None

        # Mutex used mainly to keep from adding items to the buffer
        # while the command queue is being set.
        self._command_queue_lock = Lock()

        # A holding place for commands before the command queue is
        # set. Not used once the command queue is set.
        self._command_queue_buffer = []

        self._group_id = group_id

        # This holds the last value for each metric that was sent
        # out. This is indexed by the internal name, not the network
        # name.
        self._metric_dict = self._initialize_metrics()

        # The birth certificate becomes stale when we add or remove a
        # metric. We start out as initially fresh.
        self._fresh_birth_certificate = True

    def _initialize_metrics(self):
        """Gather all true metrics into a dictionary. This does not include
        any metrics that are treated as commands. The name used here
        is the name used to look up this metric in this Python object,
        not the name of the metric on the network. The latter name can
        be looked up with metric_object.name.

        """

        metric_dict = {name: metric_object.get(self)
                       for name, metric_object in iter_metric_objects(self)}
        return metric_dict

    ##
    # Interface for derived Nodes and Devices
    #

    @property
    def group_id(self):
        """Return the group ID this endpoint will be registered to."""
        return self._group_id

    def icpw_run_in(self, seconds, func, *args, **kwargs):
        """Schedule the execution of a callable for some number of seconds in
        the future.

        """

        wrapped_func = functools.partial(func, *args, **kwargs)
        packet = ScheduleQueueItem(wrapped_func, delay_sec=seconds)
        self.icpw_enqueue_command(packet)

    def icpw_add_metric(self, name, metric):
        """Dynamically add a new metric to this endpoint. This will only
        succeed if no metric exists with the given name.

        NOTE: This adds the metric to the type! There is no way to add
        descriptors to the instance directly. If other endpoints use
        the same type, they will suddenly have this new metric.

        """

        if name in self._metric_dict:
            raise IcypawException(f"Cannot add metric {name} to endpoint: already exists")
        if not isinstance(metric, Metric):
            raise IcypawException(f"Cannot add metric {name} to endpoint: bad type {type(metric)}")
        setattr(type(self), name, metric)
        # This mimics what Python would do as part of the descriptor protocol.
        metric.__set_name__(self, name)
        self._metric_dict[name] = metric.get(self)
        self._fresh_birth_certificate = False

    def icpw_del_metric(self, name=None, network_name=None):
        """Remove a metric from this endpoint.

        name -- The internal name of the metric. If name is given it is used.

        network_name -- The name the metric is known over the
        network. This is the value given in the optional name
        parameter of the constructor. This is used when no name
        argument is provided.

        NOTE: This removes the metric from the type! There is no way to
        add descriptors to the instance directly. If other endpoints
        use the same type, their metric will suddenly disappear.

        """

        if not name and not network_name:
            raise IcypawException("Please provide either a name or network name")

        try:
            if name:
                metric = self._get_metric(name)
            elif network_name:
                metric = self._get_metric_by_name(network_name)
        except Exception as exc:
            name_used = name or network_name
            raise IcypawException(f"Cannot find metric {name_used} to delete: {exc}")

        metric.delete_metric(self)
        del self._metric_dict[metric.owner_name]
        self._fresh_birth_certificate = False

    ##
    # Interface used by Engine
    #

    def tahu_metrics(self, with_properties=False):
        """Return a list of Tahu Metric representations of all endpoint metrics,
        not including commands."""
        return [descriptor.tahu_metric(self, with_properties)
                for _, descriptor in iter_metric_objects(self)]

    def tahu_commands(self, with_properties=False):
        """Return a list of Tahu Metric representations of all endpoint commands."""
        return [descriptor.tahu_metric(with_properties)
                for _, descriptor in iter_command_objects(self)]

    def icpw_update_metric(self, name, icpw_value):
        """Process a new metric that the Engine has determined is for us. This
        handles both metrics and commands.

        icpw_value -- The value in the metric, decoded into an
        IcypawType class.

        Raise an IcypawException if this is a read-only metric or it
        is of the wrong type.

        """

        metric = self._get_metric_by_name(name)

        if metric:
            metric.set_network(self, icpw_value)
            return

        command = self._get_command_by_name(name)
        if command:
            command.run_network(self, icpw_value)
            return

        raise IcypawException(f'No metric or command named `{name}`')

    def icpw_updated_metrics(self):
        """Return a dictionary mapping metrics to (new_value, old_value)
        tuples if those metrics have changed since the last call to
        this method.

        This only returns metrics, not commands, but does return
        "read-only" metrics as they are only read-only from the point
        of view of an external client.

        """

        metric_dict = {}

        for key, old_value in self._metric_dict.items():
            metric_object = self._get_metric(key)
            curr_value = metric_object.get(self)
            if old_value != curr_value:
                metric_dict[metric_object.name] = (curr_value, old_value)
                self._metric_dict[key] = curr_value.copy()

        return metric_dict

    def icpw_all_metrics(self):
        """Return a dictionary mapping metric names to their values as
        IcypawTypes. This triggers any getters for the metrics, making
        it mostly suitable for building birth certificates.

        """

        metric_dict = {}
        for name in self._metric_dict:
            metric_object = self._get_metric(name)
            metric_dict[metric_object.name] = metric_object.get_network(self)
        return metric_dict

    def icpw_register_command_queue(self, queue):
        """Register the server's command queue. We use this queue to push
        external trigger commands. This should only be called from the
        engine and may only be called once.

        """

        with self._command_queue_lock:
            if self._command_queue is not None:
                raise RuntimeError('Command queue may not be set more than once in a node')

            self._command_queue = queue

            for item in self._command_queue_buffer:
                self._command_queue.put(item)

            self._command_queue_buffer = None

        for _name, timer in iter_timer_objects(self):
            item = ScheduleQueueItem(timer.bind(self), repeat_sec=timer.seconds)
            self.icpw_enqueue_command(item)

    def icpw_assign_to_current_thread(self):
        """Make it an error to read or write any metric from a thread other
        than the current one. This should be set from the engine and
        not called by the user.

        """

        for _name, metric in iter_metric_objects(self):
            metric.assign_to_current_thread(self)

    @classmethod
    def icpw_signature(cls):
        """Return a dictionary containing the types for commands and metrics
        stored in this endpoint."""

        signature = {
            'metrics': {},
            'commands': {},
        }

        for name, metric in iter_metric_objects_from_type(cls):
            signature['metrics'][metric.name] = metric.type

        for name, command in iter_command_objects_from_type(cls):
            signature['commands'][command.name] = command.type

        return signature

    @classmethod
    def icpw_types(cls):
        """Return a set of all types used in metrics or commands in this
        endpoint. Some of these may be custom Struct subtypes, and
        others built-in icypaw types.

        """

        types = set()

        for name, metric in iter_metric_objects_from_type(cls):
            types.add(metric.type)

        for name, command in iter_command_objects_from_type(cls):
            types.add(command.type)

        return types

    @property
    def icpw_is_birth_certificate_fresh(self):
        """Return whether the birth certificate (NBIRTH or DBIRTH) is fresh. A
        stale birth certificate will need to be reissued. This is not
        normal but happens when metrics are dynamically added and
        removed.

        """

        return self._fresh_birth_certificate

    def icpw_make_birth_certificate_fresh(self):
        """Set the birth certificate to no longer be stale."""
        self._fresh_birth_certificate = True

    def icpw_enqueue_command(self, action):
        """Method called by various other components like the decorators to
        put an item on the command queue, or in a buffer while we
        await the command queue being set..

        """

        # We do this check outside the mutex for efficiency.
        if self._command_queue is not None:
            self._command_queue.put(action)
        else:
            with self._command_queue_lock:
                # This check avoids the race condition that would
                # otherwise result. This makes our earlier check
                # outside the mutex safe.
                if self._command_queue is not None:
                    self._command_queue.put(action)
                else:
                    self._command_queue_buffer.append(action)

    ##
    # Private methods
    #

    def _get_metric_by_name(self, metric_name):
        """Look up a metric by the name it has on the network, which may
        differ from the name it uses in this class."""
        for name, obj in iter_metric_objects(self):
            if obj.name == metric_name:
                return obj
        return None

    def _get_metric(self, name):
        """Return the Metric object used to wrap access to a metric."""
        return get_metric_object(self, name)

    def _get_metric_value(self, name):
        """Return the IcypawType object stored under the given metric name."""
        metric = self._get_metric(name)
        return metric.get(self)

    def _get_command_by_name(self, command_name):
        """Look up a command by the name it has on the network, which may
        differ form the name it uses in this class."""
        for name, obj in iter_command_objects(self):
            if obj.name == command_name:
                return obj
        return None

EndpointTimer = namedtuple('EndpointTimer', ('function', 'seconds'))
