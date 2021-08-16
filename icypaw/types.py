# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

from datetime import datetime, timezone
from abc import abstractmethod, ABC
from warnings import warn

from dateutil import tz

from .tahu_interface import (set_metric_value, set_in_tahu_object, DataType,
                             Template, DataSet, convert_to_signed32, convert_to_signed64)
from .exceptions import IcypawException

class IcypawType(ABC):
    """Base class for types serialized in the Icypaw API."""

    @abstractmethod
    def set_in_metric(self, metric):
        """Method overridden by child classes to fill in the appropriate value
        field in a Metric object."""
        pass

    @abstractmethod
    def merge_with_metric(self, metric):
        """The subclasses of this type must implement this method to copy the
        value of metric into the current value. For all types but the
        Struct type this will simply overwrite the current value with
        the one in the metric.

        """

        pass

    def set_difference_in_metric(self, metric, other):
        """Impart a more compact representation of this value into the
        metric. This has no effect beyond set_in_metric() for scalar
        values but does for Structs.

        """

        self.set_in_metric(metric)

    @property
    def icpw_value(self):
        """Return a reference to the value stored in this object. This starts
        with icpw because it turns out 'value' is a reasonable Field
        name in a Struct class and this used to interfere.

        """
        return self._value

    def __eq__(self, other):
        """Return whether two IcypawType's are the same. Complicated
        subclasses are encouraged to override this definition.
        """

        # Note: This implies that certain values may compare equal
        # even if their types differ (i.e. Int32 and Int64). I think
        # this is fine.

        if isinstance(other, IcypawType):
            return self._value == other._value
        else:
            return self._value == other

    def __ne__(self, other):
        """Return whether two IcypawType's differ. Complicated
        subclasses are encouraged to override this definition."""
        if isinstance(other, IcypawType):
            return self._value != other._value
        else:
            return self._value != other

    @abstractmethod
    def copy(self):
        """Return a deep copy of this value."""
        pass

    @classmethod
    def type_from_metric(cls, metric):
        """Get the IcypawType class covering the given Tahu ``metric``"""
        tahu_datatype = DataType(metric.datatype)

        # Recursive base case. The subclasses for which this test is
        # insufficient have overridden this method.
        if hasattr(cls, 'datatype') and cls.datatype == tahu_datatype:
            return cls

        # Recurively try each subtype, returning the first that matches
        for subclass in cls.__subclasses__():
            t = subclass.type_from_metric(metric)
            if t is not None:
                return t

        return None

    @abstractmethod
    def to_python(self):
        """Return the Python representation of this value. Unlike the
        .icpw_value property, this works recursively.

        """

        pass

    @abstractmethod
    def to_pseudopython(self):
        """Return an object that can be treated like the Python object
        represented by this value. For scalars this will be the value, while
        for composite types this will be an object that acts like the
        appropriate Python type as long as it is accessed in standard
        ways. This method is much cheaper than to_python."""

        pass

class IcypawScalarType(IcypawType):
    """Base class for scalar Icypaw types. No current use except as a
    convenient means to differentiate scalar from composite types."""

    def copy(self):
        """Return a deep copy of this value."""
        return type(self)(self._value)

    def to_python(self):
        """Return the Python representation of this value."""
        return self._value

    def to_pseudopython(self):
        return self._value

    @property
    def value(self):
        # The .value property conflicted with Struct types which may
        # have happened to have a .value field. I changed .value to
        # .icpw_value which I imagined would conflict less.
        #
        # See also the comments for to_python() and to_pseudopython().
        #
        warn("IcypawType.value is deprecated. Use to_python() or icpw_value instead.",
             DeprecationWarning)
        return self.to_python()


class UInt32(IcypawScalarType):
    """A 32-bit unsigned integer."""
    def __init__(self, value=None):
        if value is None:
            value = 0
        if not (0 <= value <= 0xffffffff):
            raise TypeError(f'Bad value {value} for 64-bit unsigned integer')
        self._value = int(value)

    datatype = DataType.UInt32
    pythontype = int

    def set_in_metric(self, metric):
        set_metric_value(self._value, self.datatype, metric)

    def merge_with_metric(self, metric):
        self._value = metric.int_value

class Int32(IcypawScalarType):
    """A 32-bit signed integer."""
    def __init__(self, value=None):
        if value is None:
            value = 0
        if not (-0x80000000 <= value <= 0x7fffffff):
            raise TypeError(f'Bad value {value} for 32-bit integer')
        self._value = int(value)

    datatype = DataType.Int32
    pythontype = int

    def set_in_metric(self, metric):
        set_metric_value(self._value, self.datatype, metric)

    def merge_with_metric(self, metric):
        self._value = convert_to_signed32(metric.int_value)

    def __int__(self):
        return self._value


class Int64(IcypawScalarType):
    """A 64-bit signed integer."""
    def __init__(self, value=None):
        if value is None:
            value = 0
        if not (-0x8000000000000000 <= value <= 0x7fffffffffffffff):
            raise TypeError(f'Bad value {value} for 64-bit integer')
        self._value = int(value)

    datatype = DataType.Int64
    pythontype = int

    def set_in_metric(self, metric):
        set_metric_value(self._value, self.datatype, metric)

    def merge_with_metric(self, metric):
        self._value = convert_to_signed64(metric.long_value)

    def __int__(self):
        return self._value


class UInt64(IcypawScalarType):
    """A 64-bit unsigned integer."""
    def __init__(self, value=None):
        if value is None:
            value = 0
        if not (0 <= value <= 0xffffffffffffffff):
            raise TypeError(f'Bad value {value} for 64-bit unsigned integer')
        self._value = int(value)

    datatype = DataType.UInt64
    pythontype = int

    def set_in_metric(self, metric):
        set_metric_value(self._value, self.datatype, metric)

    def merge_with_metric(self, metric):
        self._value = metric.long_value

    def __int__(self):
        return self._value


class Double(IcypawScalarType):
    """A double-precision floating point value."""
    def __init__(self, value=None):
        if value is None:
            value = 0.0
        self._value = float(value)

    datatype = DataType.Double
    pythontype = float

    def set_in_metric(self, metric):
        set_metric_value(self._value, self.datatype, metric)

    def merge_with_metric(self, metric):
        self._value = metric.double_value

    def __float__(self):
        return self._value


class Boolean(IcypawScalarType):
    """A boolean variable."""
    def __init__(self, value=None):
        if value is None:
            value = False
        self._value = bool(value)

    datatype = DataType.Boolean
    pythontype = bool

    def set_in_metric(self, metric):
        set_metric_value(self._value, self.datatype, metric)

    def merge_with_metric(self, metric):
        self._value = metric.boolean_value

    def __bool__(self):
        return self._value


class String(IcypawScalarType):
    """A unicode string."""
    def __init__(self, value=None):
        if value is None:
            value = ''
        self._value = str(value)

    datatype = DataType.String
    pythontype = str

    def set_in_metric(self, metric):
        set_metric_value(self._value, self.datatype, metric)

    def merge_with_metric(self, metric):
        value = metric.string_value
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        self._value = value

    def __str__(self):
        return self._value


class DateTime(IcypawScalarType):
    """A datetime value"""
    def __init__(self, value=None):
        if value is None:
            value = datetime.now()
        if not isinstance(value, datetime):
            raise TypeError("Need datetime")
        self._value = value

    datatype = DataType.DateTime
    pythontype = datetime

    def set_in_metric(self, metric):
        set_metric_value(int(self._value.replace(tzinfo=timezone.utc).timestamp() * 1000), self.datatype, metric)

    def merge_with_metric(self, metric):
        if metric.is_null:
            self._value = datetime.utcfromtimestamp(0)
            return
        value = datetime.fromtimestamp(metric.long_value / 1000, tz=tz.tzlocal())
        self._value = value

    def __str__(self):
        return self._value


class Struct(IcypawType):
    """Base class for user-defined aggregate types."""

    datatype = DataType.Template
    pythontype = dict

    # Subclasses must overwrite this with the name of this type as
    # sent over the network.
    network_name = None

    def __init__(self, arg_value_dict=None):
        """Initialize this struct with a dictionary of values."""

        if self.network_name is None:
            # This error would be better at class definition time but
            # that involves writing a metaclass just so we can throw a
            # better exception.
            raise ValueError('Attempting to instantiate a Struct with no network_name')

        arg_value_dict = arg_value_dict or {}
        value_dict = {}

        for typ in self.__class__.__mro__:
            for name in typ.__dict__:
                if name not in value_dict:
                    obj = typ.__dict__[name]
                    if isinstance(obj, Field):
                        assert obj.python_name == name
                        if name not in arg_value_dict:
                            value_dict[name] = obj.type(obj.default)
                        else:
                            # Allowing IcypawType objects in the
                            # initialization dictionary makes other
                            # recursive algorithms in this module
                            # easier.
                            arg_value = arg_value_dict[name]
                            if isinstance(arg_value, obj.type):
                                value_dict[name] = arg_value
                            else:
                                if isinstance(arg_value, IcypawType):
                                    raise TypeError('Attempted to assign the wrong IcypawType')
                                value_dict[name] = obj.type(arg_value)

        # Note: It's important that this be called _value in line with
        # other classes in the IcypawType hierarchy. This allows the
        # value property and __eq__ and __ne__ methods to work.
        self._value = value_dict

    def __getitem__(self, key):
        """Treat this struct like a dict. This is both convenient but also
        allows a Struct to initialize another Struct, which expects a
        dict-like interface for its constructor's argument."""

        return self._value[key]

    def __setitem__(self, key, value):
        self._value[key] = value

    def __contains__(self, key):
        """Return whether a field named key exists in this struct."""
        return key in self._value

    def keys(self):
        return self._value.keys()

    def values(self):
        return self._value.values()

    def items(self):
        return self._value.items()

    def get_field(self, name):
        """Return the Field object corresponding to the given name."""
        for typ in self.__class__.__mro__:
            try:
                return typ.__dict__[name]
            except KeyError:
                pass

        raise KeyError(name)

    def make_tahu_template(self):
        template = Template()
        template.template_ref = self.network_name.encode()

        for name in self._value:
            cur_metric = template.metrics.add()
            cur_metric.name = name.encode()
            self._value[name].set_in_metric(cur_metric)
        return template

    def set_in_metric(self, metric):
        template = self.make_tahu_template()
        set_metric_value(template, self.datatype, metric)

    def set_difference_in_metric(self, metric, other):
        """Set only those fields that differ between this and the other
        instance."""
        template = Template()
        template.template_ref = self.network_name.encode()

        for name in self._value:
            if self._value[name] != other._value[name]:
                cur_metric = template.metrics.add()
                cur_metric.name = name.encode()
                self._value[name].set_difference_in_metric(cur_metric, other._value[name])

        set_metric_value(template, self.datatype, metric)

    @classmethod
    def type_from_metric(cls, metric):
        """Return our own class if we are the particular subclass representing
        this metric. Do not create a new class. Return None if we are
        not the right class.

        """

        # if cls is Struct:
        #     for subclass in cls.__subclasses__():
        #         ret = subclass.type_from_metric(metric)
        #         if ret is not None:
        #             return ret
        #     return None

        if metric.datatype != cls.datatype.value:
            return None

        if metric.template_value.template_ref != cls.network_name:
            return None

        return cls

    def merge_with_metric(self, metric):
        for submetric in metric.template_value.metrics:
            self._value[submetric.name].merge_with_metric(submetric)

    def copy(self):
        """Return a deep copy of this value."""
        copy_value = {name: value.copy() for name, value in self._value.items()}
        return type(self)(copy_value)

    def to_python(self):
        python_value = {name: value.to_python() for name, value in self._value.items()}
        return python_value

    def to_pseudopython(self):
        return self

def make_struct(name, field_dict, network_name=None):
    """Create a new Struct class with annotations from field_dict. If
    network_name is not provided, use name for both name and
    network_name."""

    network_name = network_name or name
    attr_dict = field_dict.copy()
    attr_dict['network_name'] = network_name
    cls = type(name, (Struct,), attr_dict)
    return cls

class Field:
    """Helper class following the descriptor protocol to define fields in
    a Struct."""

    def __init__(self, type_, name=None, default=None):
        """Initialize a new Struct field to be of a certain type with possibly
        a name that is not a legal Python variable name."""

        if not is_icypaw_type_annotation(type_):
            raise TypeError(f'Expected IcypawType, found {type_}')

        self._type = type_
        self._name = str(name) if name is not None else None
        self._owner_name = None
        self.default = default

    @property
    def name(self):
        """Return the name of this field as visible over the network."""
        return self._name or self._owner_name

    @property
    def python_name(self):
        """Return the name of this field used internally in Python. This is
        the same as the name used over the network unless a different name was
        specified in the constructor."""
        return self._owner_name

    @property
    def type(self):
        return self._type

    def __get__(self, instance, owner):
        """Return the unpacked value stored in this Struct field."""
        assert isinstance(instance, Struct)
        value = instance._value[self.python_name]
        if isinstance(value, IcypawScalarType):
            return value.icpw_value
        else:
            return value

    def __set__(self, instance, value):
        """Pack the given value into this field."""
        assert isinstance(instance, Struct)
        new_icypaw_value = self._type(value)
        instance._value[self.python_name] = new_icypaw_value

    def __set_name__(self, owner, name):
        # Note: Since I was confused about this at one time: the
        # network_name is the name of the class as used in
        # Sparkplug. The name here is the name of the field, not the
        # name of the type of the field.
        self._owner_name = name

class TypeAnnotator:
    """Create an annotator that accepts square bracket syntax."""

    def __init__(self, cls):
        self._cls = cls

    def __getitem__(self, item):
        cls_name = f"{self._cls.__name__}[{self.make_name(item)}]"
        cls_base = self._cls
        cls_dict = {"_type": item}
        return self.get_cached_or_make_type(cls_name, cls_base, cls_dict)

    def make_name(self, item):
        if hasattr(item, '__name__'):
            return item.__name__
        try:
            return tuple(self.make_name(x) for x in item)
        except TypeError:
            return str(item)

    def get_cached_or_make_type(self, cls_name, cls_base, cls_dict):
        for subcls in cls_base.__subclasses__():
            if subcls._type == cls_dict['_type']:
                return subcls
        return type(cls_name, (cls_base,), cls_dict)

class ArrayType(IcypawType):
    """A type representing a variable-length array where all elements have
    the same type. Create a subclass with the syntax Array[Type].

    """

    # This is the IcypawType of element of the array. It is filled in
    # automatically.
    _type = None

    def __init__(self, value=None):
        # This is our first opportunity to really check the _type
        # field.
        if not is_icypaw_type_annotation(self._type):
            if not isinstance(self._type, tuple):
                raise TypeError('Type argument to Array must be an IcypawType or a tuple of such')
            if any(not is_icypaw_type_annotation(typ) for typ in self._type):
                raise TypeError('Type argument to Array must be an IcypawType or a tuple of such')

        value = value or []

        self._value = [self._convert_type(v) for v in value]

    datatype = DataType.DataSet
    pythontype = list

    def _convert_type(self, value):
        def convert_single_value(v, t):
            if isinstance(v, t):
                return v
            return t(v)
        if is_icypaw_type_annotation(self._type):
            return convert_single_value(value, self._type)
        elif isinstance(self._type, tuple):
            ret = tuple(convert_single_value(v, typ) for v, typ in zip(value, self._type))
            if len(ret) != len(self._type):
                raise ValueError('Not enough values in array tuple element')
            return ret
        assert False, "Unknown type as Array type; this should have been caught in __init__"

    def __getitem__(self, index):
        res = self._value[index]
        if isinstance(res, tuple):
            return tuple(ele.icpw_value for ele in res)
        else:
            return res.icpw_value

    def set_in_metric(self, metric):
        dataset = DataSet()

        if is_icypaw_scalar_type_annotation(self._type):
            dataset.num_of_columns = 1
            dataset.columns.append(b'')
            dataset.types.append(self._type.datatype.value)
            for item in self._value:
                row = dataset.rows.add()
                elem = row.elements.add()
                set_in_tahu_object(item.icpw_value, self._type.datatype, elem)
        elif isinstance(self._type, tuple):
            dataset.num_of_columns = len(self._type)
            for typ in self._type:
                dataset.columns.append(b'')
                dataset.types.append(typ.datatype.value)
            for item in self._value:
                assert len(item) == len(self._type)
                row = dataset.rows.add()
                for tuple_item, typ in zip(item, self._type):
                    elem = row.elements.add()
                    set_in_tahu_object(tuple_item.icpw_value, typ.datatype, elem)
        else:
            raise TypeError('Unknown or unsupported type as Array type')

        set_metric_value(dataset, self.datatype, metric)

    @classmethod
    def type_from_metric(cls, metric):
        """Override the base class's class method to create a custom Array."""

        if metric.datatype != cls.datatype.value:
            return None

        if cls is ArrayType:
            for subclass in cls.__subclasses__():
                ret = subclass.type_from_metric(metric)
                if ret is not None:
                    return ret

            # There is no existing ArrayType subclass, so we make a new
            # one.
            arg_types = tuple(get_scalar_type_by_datatype(DataType(typ))
                              for typ in metric.dataset_value.types)
            if any(at is None for at in arg_types):
                raise IcypawException('Bad data type in array metric')
            if len(arg_types) == 1:
                arg_type, = arg_types
                return Array[arg_type]
            else:
                return Array[arg_types]
        else:
            assert cls._type is not None

            if is_icypaw_scalar_type_annotation(cls._type):
                if len(metric.dataset_value.types) != 1:
                    return None
                if metric.dataset_value.types[0] != cls._type.datatype.value:
                    return None
            else:
                if len(metric.dataset_value.types) != len(cls._type):
                    return None
                for exp_datatype, act_type in zip(metric.dataset_value.types, cls._type):
                    if exp_datatype != act_type.datatype.value:
                        return None

        return cls

    def merge_with_metric(self, metric):

        array_value = []

        for row in metric.dataset_value.rows:
            # Note: even though DataSetValue messages are not Metrics,
            # they look the same so we can use them in
            # value_from_metric as long as we provide the type.
            if is_icypaw_scalar_type_annotation(self._type):
                value = value_from_metric(row.elements[0], self._type)
            else:
                value = tuple(value_from_metric(elem, icpw_type)
                              for elem, icpw_type in zip(row.elements, self._type))
            array_value.append(value)

        self._value = array_value

    def copy(self):
        """Return a deep copy of this value."""
        if is_icypaw_type_annotation(self._type):
            copy_array = [elem.copy() for elem in self._value]
        else:
            copy_array = [tuple(telem.copy() for telem in elem) for elem in self._value]
        return type(self)(copy_array)

    def to_python(self):
        if is_icypaw_type_annotation(self._type):
            python_array = [elem.to_python() for elem in self._value]
        else:
            python_array = [tuple(telem.to_python() for telem in elem) for elem in self._value]
        return python_array

    def to_pseudopython(self):
        return self

Array = TypeAnnotator(ArrayType)

def merge_with_metric(icpw_value, metric, icpw_type) -> IcypawType:
    """Merge the IcypawType value with the given metric. icypaw_value may
    be None. Returns the merged metric. Unlike the .merge_with_metric
    method of IcypawType objects, this handles None inputs (which
    obviously don't have a .merge_with_metric method).

    icpw_value -- Any IcypawType or None.

    metric -- A decoded Tahu metric.

    icpw_type -- The type this value is expected to be. Differing from
    value_from_metric, this is not optional (to properly handle when
    icypaw_value is None).

    """

    if metric.is_null:
        return None

    if icpw_value is None:
        icpw_value = icpw_type()

    icpw_value.merge_with_metric(metric)
    return icpw_value

def value_from_metric(metric, icpw_type=None, struct_types=None) -> IcypawType:
    """Parse a value from a Tahu metric as an IcypawType

    metric -- A decoded Tahu metric.

    icpw_type -- The type this value is expected to be. If not given,
    the type will be inferred.

    struct_types -- If icpw_type is not given, try these template
    types in addition to the built-in types.

    """

    if icpw_type is None:
        icpw_type = type_from_metric(metric, struct_types=struct_types)

    assert icpw_type is not None

    value = icpw_type()
    value.merge_with_metric(metric)
    return value

def type_from_metric(metric, struct_types=None) -> type:
    """Return an IcypawType subclass that best represents the type of the
    given metric. Raise a TypeError otherwise.

    metric -- A decoded Tahu metric.

    struct_types -- If icpw_type is not given, try these template
    types in addition to the built-in types.

    """

    type_ = None

    if struct_types:
        for struct_type in struct_types:
            type_ = struct_type.type_from_metric(metric)
            if type_:
                break

    if type_ is None:
        type_ = IcypawType.type_from_metric(metric)

    if type_ is None:
        raise TypeError(f"Unsupported IcypawType: {DataType(metric.datatype)}")

    return type_

def is_icypaw_type_annotation(obj):
    """Return if the object is usable as an icypaw type annotation."""
    if isinstance(obj, type) and issubclass(obj, IcypawType):
        return True
    return False

def is_icypaw_scalar_type_annotation(obj):
    """Return if the object is usable as an icypaw scalar type annotation."""
    if isinstance(obj, type) and issubclass(obj, IcypawScalarType):
        return True
    return False

def get_scalar_type_by_datatype(datatype):
    """Return the IcypawScalarType subclass that corresponds to the given
    datatype field. Return None if scalar class is registered by this
    datatype.

    datatype -- An instance of the DataType enumeration.

    """

    if not isinstance(datatype, DataType):
        raise TypeError('Expected DataType instance')

    for cls in IcypawScalarType.__subclasses__():
        if cls.datatype == datatype:
            return cls

    return None

##
# Metadata Wrappers
#

class BooleanWithMetadata:
    """Keep track of whether a value is historical or transient. Note that
    the argument need not be, and in fact often is not, an IcypawType."""

    def __init__(self, value, is_historical=False, is_transient=False):
        self.value = value
        self.is_historical = bool(is_historical)
        self.is_transient = bool(is_transient)

def Historical(value):
    """Function that looks like a class that marks a value as historical
    when setting a metric in an endpoint."""

    if isinstance(value, BooleanWithMetadata):
        value.is_historical = True
    else:
        value = BooleanWithMetadata(value, is_historical=True)

    return value

def Transient(value):
    """Function that looks like a class that marks a value as transient
    when setting a metric in an endpoint."""

    if isinstance(value, BooleanWithMetadata):
        value.is_transient = True
    else:
        value = BooleanWithMetadata(value, is_transient=True)

    return value
