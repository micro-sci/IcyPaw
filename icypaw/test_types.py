# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

from datetime import datetime, timedelta
import unittest
import struct
import itertools
import gc

try:
    from nose.tools import nottest
except ModuleNotFoundError:
    def nottest(x):
        return x

from .types import (UInt32, UInt64, Int32, Int64, Double, Boolean, String,
                    Struct, Array, make_struct, Field, type_from_metric, value_from_metric, DateTime)
from .tahu_interface import Metric, DataType

##
# Support functions
#

def read_from_metric(metric):
    """Read the value in a metric and return it as a scalar or
    dictionary."""
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
    # Note: This might not work in general because Templates may
    # replace metric names with an alias.
    return {metric.name: read_from_metric(metric) for metric in template.metrics}

def read_tahu_value(datatype, tahu_object):
    """Read a value from a Tahu object. This relies on DataSet and Metric
    messages having the same naming convention."""
    if datatype == DataType.Int32.value:
        return reformat_int("I", "i", tahu_object.int_value)
    if datatype == DataType.Int64.value:
        return reformat_int("Q", "q", tahu_object.long_value)
    if datatype == DataType.Double.value:
        return tahu_object.double_value
    if datatype == DataType.Boolean.value:
        return tahu_object.boolean_value
    if datatype == DataType.String.value:
        return tahu_object.string_value
    if datatype == DataType.DateTime.value:
        return datetime.utcfromtimestamp(tahu_object.long_value / 1000)
    if datatype == DataType.DataSet.value:
        assert hasattr(tahu_object, 'dataset_value')
        return read_from_dataset(tahu_object.dataset_value)
    if datatype == DataType.Template.value:
        assert hasattr(tahu_object, 'template_value')
        return read_from_template(tahu_object.template_value)
    assert False, f"Cannot read datatype {datatype}"

def reformat_int(src_format, dst_format, value):
    return struct.unpack(dst_format, struct.pack(src_format, value))[0]

##
# Test classes
#

class Int32Tester(unittest.TestCase):

    def test_value(self):
        """Test retrieving the value of an Int32."""
        exp_value = 42
        icpw_obj = Int32(exp_value)
        self.assertEqual(exp_value, icpw_obj.icpw_value)

    def test_eq(self):
        """Test that two Int32's with the same value compare equal."""
        exp_value = 84
        obj0 = Int32(exp_value)
        obj1 = Int32(exp_value)
        self.assertEqual(obj0, obj1)

    def test_ne(self):
        """Test that two Int32's with the same value compare equal."""
        exp_value = 84
        obj0 = Int32(exp_value)
        obj1 = Int32(2 * exp_value + 1)
        self.assertNotEqual(obj0, obj1)

    def test_set_in_metric(self):
        """Test automatically setting the value field of a Tahu metric."""

        exp_value = 15
        metric = Metric()
        obj = Int32(exp_value)
        obj.set_in_metric(metric)
        self.assertEqual(metric.datatype, DataType.Int32.value)
        self.assertEqual(read_from_metric(metric), exp_value)

    def test_set_in_metric_negative(self):
        """Test setting a negative number in a Tahu metric and reading it
        back."""

        exp_value = -15
        metric = Metric()
        obj = Int32(exp_value)
        obj.set_in_metric(metric)
        self.assertEqual(metric.datatype, DataType.Int32.value)
        self.assertEqual(read_from_metric(metric), exp_value)

class Int64Tester(unittest.TestCase):

    def test_value(self):
        """Test retrieving the value of an Int64."""
        exp_value = 42
        icpw_obj = Int64(exp_value)
        self.assertEqual(exp_value, icpw_obj.icpw_value)

    def test_eq(self):
        """Test that two Int64's with the same value compare equal."""
        exp_value = 84
        obj0 = Int64(exp_value)
        obj1 = Int64(exp_value)
        self.assertEqual(obj0, obj1)

    def test_ne(self):
        """Test that two Int64's with different values are unequal."""
        exp_value = 84
        obj0 = Int64(exp_value)
        obj1 = Int64(2 * exp_value + 1)
        self.assertNotEqual(obj0, obj1)

    def test_set_in_metric(self):
        """Test automatically setting the value field of a Tahu metric."""

        exp_value = 15
        metric = Metric()
        obj = Int64(exp_value)
        obj.set_in_metric(metric)
        self.assertEqual(metric.datatype, DataType.Int64.value)
        self.assertEqual(read_from_metric(metric), exp_value)

    def test_set_in_metric_negative(self):
        """Test setting a negative number in a Tahu metric and reading it
        back."""

        exp_value = -15
        metric = Metric()
        obj = Int64(exp_value)
        obj.set_in_metric(metric)
        self.assertEqual(metric.datatype, DataType.Int64.value)
        self.assertEqual(read_from_metric(metric), exp_value)

class DoubleTester(unittest.TestCase):

    def test_value(self):
        """Test retrieving the value of a Double."""

        exp_value = 3.14
        obj = Double(exp_value)
        self.assertEqual(exp_value, obj.icpw_value)

    def test_eq(self):
        """Test that two Double's with the same value compare equal."""

        exp_value = 123.456
        obj0 = Double(exp_value)
        obj1 = Double(exp_value)
        self.assertEqual(obj0, obj1)

    def test_ne(self):
        """Test that two Double's with the same value compare equal."""

        exp_value = 123.456
        obj0 = Double(exp_value)
        obj1 = Double(exp_value)
        self.assertEqual(obj0, obj1)

    def test_set_in_metric(self):
        """Test setting the value in a Tahu metric."""
        exp_value = -43.5
        obj = Double(exp_value)
        metric = Metric()
        obj.set_in_metric(metric)
        self.assertEqual(DataType.Double.value, metric.datatype)
        self.assertEqual(read_from_metric(metric), exp_value)

class BooleanTester(unittest.TestCase):

    def test_value(self):
        """Test retrieving the value of a Boolean."""
        exp_value = True
        obj = Boolean(exp_value)
        self.assertEqual(exp_value, obj.icpw_value)
        exp_value = False
        obj = Boolean(exp_value)
        self.assertEqual(exp_value, obj.icpw_value)

    def test_eq(self):
        """Test that Boolean's with the same value compare equal."""
        for exp_value in [True, False]:
            obj0 = Boolean(exp_value)
            obj1 = Boolean(exp_value)
            self.assertEqual(obj0, obj1)

    def test_ne(self):
        """Test that Boolean's with different values compare not equal."""
        for exp_value in [True, False]:
            obj0 = Boolean(exp_value)
            obj1 = Boolean(not exp_value)
            self.assertNotEqual(obj0, obj1)

    def test_set_in_metric(self):
        """Test setting the value in a Tahu metric."""
        for exp_value in [True, False]:
            obj = Boolean(exp_value)
            metric = Metric()
            obj.set_in_metric(metric)
            self.assertEqual(read_from_metric(metric), exp_value)

class StringTester(unittest.TestCase):

    def test_value(self):
        """Test retrieving the value of a String."""
        exp_value = "Hello"
        obj = String(exp_value)
        self.assertEqual(exp_value, obj.icpw_value)

    def test_eq(self):
        """Test that String's with the same value compare equal."""
        exp_value = "World"
        obj0 = String(exp_value)
        obj1 = String(exp_value)
        self.assertEqual(obj0, obj1)

    def test_ne(self):
        """Test that String's with different values compare not equal."""
        exp_value = "abc"
        obj0 = String(exp_value)
        obj1 = String("def")
        self.assertNotEqual(obj0, obj1)

    def test_set_in_metric(self):
        """Test setting the value in a Tahu metric."""
        exp_value = "Hello, World"
        obj = String(exp_value)
        metric = Metric()
        obj.set_in_metric(metric)
        self.assertEqual(read_from_metric(metric), exp_value)


class DateTimeTester(unittest.TestCase):

    def test_value(self):
        """Test retrieving the value of a DateTime."""
        exp_value = datetime.now()
        obj = DateTime(exp_value)
        self.assertEqual(exp_value, obj.icpw_value)

    def test_eq(self):
        """Test that DateTime's with the same value compare equal."""
        exp_value = datetime.now()
        obj0 = DateTime(exp_value)
        obj1 = DateTime(exp_value)
        self.assertEqual(obj0, obj1)

    def test_ne(self):
        """Test that DateTime's with different values compare not equal."""
        exp_value = datetime.now()
        different_value = exp_value + timedelta(seconds=1)
        obj0 = DateTime(exp_value)
        obj1 = DateTime(different_value)
        self.assertNotEqual(obj0, obj1)

    def test_set_in_metric(self):
        """Test setting the value in a Tahu metric."""
        exp_value = datetime.now()
        obj = DateTime(exp_value)
        metric = Metric()
        obj.set_in_metric(metric)
        timeerror = read_from_metric(metric) - exp_value
        self.assertLess(timeerror, timedelta(microseconds=1000))  # encoded as millisecond since epoch


class StructTester(unittest.TestCase):

    def test_value(self):
        """Test setting values in a struct and retrieving them."""
        class Foo(Struct):
            network_name = "foo"
            x = Field(Int32)
            y = Field(Int64)
            z = Field(Boolean)
            v = Field(Double)
            w = Field(String)

        exp_x = 32
        exp_y = -64
        exp_z = False
        exp_v = 3.14
        exp_w = "Hello, World"

        foo = Foo()
        foo.x = exp_x
        foo.y = exp_y
        foo.z = exp_z
        foo.v = exp_v
        foo.w = exp_w

        self.assertEqual(foo.x, exp_x)
        self.assertEqual(foo.y, exp_y)
        self.assertEqual(foo.z, exp_z)
        self.assertEqual(foo.v, exp_v)
        self.assertEqual(foo.w, exp_w)

    def test_set_get_via_dict_interface(self):
        """Test setting and retrieving values from a Struct via the dict-like interface."""
        class Foo(Struct):
            network_name = "foo"
            x = Field(Int32)
            y = Field(Int64)
            z = Field(Boolean)
            v = Field(Double)
            w = Field(String)

        exp_x = 32
        exp_y = -64
        exp_z = False
        exp_v = 3.14
        exp_w = "Hello, World"

        foo = Foo()
        foo['x'] = exp_x
        foo['y'] = exp_y
        foo['z'] = exp_z
        foo['v'] = exp_v
        foo['w'] = exp_w

        self.assertEqual(foo['x'], exp_x)
        self.assertEqual(foo['y'], exp_y)
        self.assertEqual(foo['z'], exp_z)
        self.assertEqual(foo['v'], exp_v)
        self.assertEqual(foo['w'], exp_w)

    def test_iterating_via_dict_interface(self):
        """Test iterating over values, keys, and items from a Struct."""
        class Foo(Struct):
            network_name = "foo"
            x = Field(Int32)
            y = Field(Int64)
            z = Field(Boolean)
            v = Field(Double)
            w = Field(String)

        exp_x = 32
        exp_y = -64
        exp_z = False
        exp_v = 3.14
        exp_w = "Hello, World"

        foo = Foo()
        foo['x'] = exp_x
        foo['y'] = exp_y
        foo['z'] = exp_z
        foo['v'] = exp_v
        foo['w'] = exp_w

        act_keys = [key for key in foo.keys()]
        act_values = [value for value in foo.values()]
        act_items = [item for item in foo.items()]

        exp_keys = ['x', 'y', 'z', 'v', 'w']
        exp_values = [exp_x, exp_y, exp_z, exp_v, exp_w]
        exp_items = list(zip(exp_keys, exp_values))

        self.assertEqual(exp_keys, act_keys)
        self.assertEqual(exp_values, act_values)
        self.assertEqual(exp_items, act_items)
        for key in exp_keys:
            self.assertIn(key, foo)

    def test_names(self):
        """Test having fields with long and short names in a Struct."""
        class Foo(Struct):
            network_name = "foo"
            x = Field(Int32, name='Field X')
            y = Field(Int64)

        foo = Foo()

        x_fld = foo.get_field('x')
        y_fld = foo.get_field('y')

        self.assertEqual(x_fld.name, 'Field X')
        self.assertEqual(x_fld.python_name, 'x')
        self.assertEqual(y_fld.name, 'y')
        self.assertEqual(y_fld.python_name, 'y')

    def test_default_value(self):
        """Test creating a Struct with default values."""

        exp_x = 32
        exp_y = -64
        exp_z = False
        exp_v = 3.14
        exp_w = "Hello, World"

        class Foo(Struct):
            network_name = "foo"
            x = Field(Int32, default=exp_x)
            y = Field(Int64, default=exp_y)
            z = Field(Boolean, default=exp_z)
            v = Field(Double, default=exp_v)
            w = Field(String, default=exp_w)

        foo = Foo()

        self.assertEqual(foo.x, exp_x)
        self.assertEqual(foo.y, exp_y)
        self.assertEqual(foo.z, exp_z)
        self.assertEqual(foo.v, exp_v)
        self.assertEqual(foo.w, exp_w)

    def test_constructor_value(self):
        """Test creating a Struct with values passed to the constructor."""

        exp_x = 32
        exp_y = -64
        exp_z = False
        exp_v = 3.14
        exp_w = "Hello, World"

        class Foo(Struct):
            network_name = "foo"
            x = Field(Int32)
            y = Field(Int64)
            z = Field(Boolean)
            v = Field(Double)
            w = Field(String)

        foo = Foo({
            "x": exp_x,
            "y": exp_y,
            "z": exp_z,
            "v": exp_v,
            "w": exp_w,
        })

        self.assertEqual(foo.x, exp_x)
        self.assertEqual(foo.y, exp_y)
        self.assertEqual(foo.z, exp_z)
        self.assertEqual(foo.v, exp_v)
        self.assertEqual(foo.w, exp_w)

    def test_set_in_metric(self):
        """Test setting a Struct into a Metric as a Template value."""

        exp_x = 32
        exp_y = -64
        exp_z = False
        exp_v = 3.14
        exp_w = "Hello, World"

        class Foo(Struct):
            network_name = "foo"
            x = Field(Int32)
            y = Field(Int64)
            z = Field(Boolean)
            v = Field(Double)
            w = Field(String)

        value_dict = {
            "x": exp_x,
            "y": exp_y,
            "z": exp_z,
            "v": exp_v,
            "w": exp_w,
        }

        foo = Foo(value_dict)

        metric = Metric()

        foo.set_in_metric(metric)

        self.assertEqual(metric.datatype, DataType.Template.value)
        self.assertEqual(metric.template_value.template_ref, 'foo')
        unprocessed_metrics = {key for key in value_dict.keys()}
        for cur_metric in metric.template_value.metrics:
            self.assertIn(cur_metric.name, unprocessed_metrics)
            unprocessed_metrics.remove(cur_metric.name)
            exp_value = value_dict[cur_metric.name]
            self.assertEqual(read_from_metric(cur_metric), exp_value)

        self.assertEqual(0, len(unprocessed_metrics))

    def test_set_difference_in_metric(self):
        """Test setting a Struct into a Metric as a Template value by setting
        only those fields that are different from a previous value."""

        exp_x = 32
        exp_y = -64
        exp_z = False
        exp_v = 3.14
        exp_w = "Hello, World"

        class Foo(Struct):
            network_name = "foo"
            x = Field(Int32)
            y = Field(Int64)
            z = Field(Boolean)
            v = Field(Double)
            w = Field(String)

        value_dict = {
            "x": exp_x,
            "y": exp_y,
            "z": exp_z,
            "v": exp_v,
            "w": exp_w,
        }

        diff_dict = {
            "y": -128,
            "w": "Konnichiwa, Minnasama",
        }

        foo = Foo(value_dict)
        bar = Foo(value_dict)

        foo.y = diff_dict['y']
        foo.w = diff_dict['w']

        metric = Metric()

        foo.set_difference_in_metric(metric, bar)

        self.assertEqual(metric.datatype, DataType.Template.value)
        self.assertEqual(metric.template_value.template_ref, 'foo')
        unprocessed_metrics = {key for key in diff_dict.keys()}
        for cur_metric in metric.template_value.metrics:
            self.assertIn(cur_metric.name, unprocessed_metrics)
            unprocessed_metrics.remove(cur_metric.name)
            exp_value = diff_dict[cur_metric.name]
            self.assertEqual(read_from_metric(cur_metric), exp_value)

        self.assertEqual(0, len(unprocessed_metrics))

    def test_nested_struct_value(self):
        """Test setting values for a nested struct."""

        exp_value = 5

        class Bar(Struct):
            network_name = "bar"
            x = Field(Int32)

        class Foo(Struct):
            network_name = "foo"
            w = Field(Bar)

        foo = Foo()
        foo.w.x = exp_value

        self.assertEqual(foo.w.x, exp_value)

    def test_nested_struct_default_value(self):
        """Test setting the default value of a nested struct."""

        exp_value = 7

        class Bar(Struct):
            network_name = "bar"
            x = Field(Int32)

        class Foo(Struct):
            network_name = "foo"
            w = Field(Bar, default={'x': exp_value})

        foo = Foo()

        self.assertEqual(foo.w.x, exp_value)

    def test_nested_struct_set_in_metric(self):
        """Test setting a nested struct in a metric as a template."""

        exp_x = 33
        exp_y = "abc"

        class Bar(Struct):
            network_name = "bar"

            x = Field(Int32, default=exp_x)
            y = Field(String, default=exp_y)

        class Foo(Struct):
            network_name = "foo"

            w = Field(Bar)

        value_dict = {
            'x': exp_x,
            'y': exp_y,
        }

        foo = Foo()

        metric = Metric()

        foo.set_in_metric(metric)

        self.assertEqual(metric.datatype, DataType.Template.value)

        self.assertEqual(metric.datatype, DataType.Template.value)
        self.assertEqual(metric.template_value.template_ref, 'foo')
        self.assertEqual(1, len(metric.template_value.metrics))
        self.assertEqual('w', metric.template_value.metrics[0].name)
        self.assertEqual(metric.template_value.metrics[0].datatype, DataType.Template.value)
        unprocessed_metrics = {key for key in value_dict}

        for cur_metric in metric.template_value.metrics[0].template_value.metrics:
            self.assertIn(cur_metric.name, unprocessed_metrics)
            unprocessed_metrics.remove(cur_metric.name)
            exp_value = value_dict[cur_metric.name]
            self.assertEqual(read_from_metric(cur_metric), exp_value)

        self.assertEqual(0, len(unprocessed_metrics))

    def test_nested_struct_set_difference_in_metric(self):
        """Test setting a nested struct in a metric as a template by setting
        only those fields that are different from a previous value."""

        exp_x = 33
        exp_y = "abc"

        class Bar(Struct):
            network_name = "bar"

            x = Field(Int32, default=exp_x)
            y = Field(String, default=exp_y)

        class Foo(Struct):
            network_name = "foo"

            w = Field(Bar)

        foo = Foo()
        foo2 = Foo()

        foo2.w.x = 383

        metric = Metric()

        foo2.set_difference_in_metric(metric, foo)

        self.assertEqual(metric.datatype, DataType.Template.value)

        self.assertEqual(metric.datatype, DataType.Template.value)
        self.assertEqual(metric.template_value.template_ref, 'foo')
        self.assertEqual(1, len(metric.template_value.metrics))
        self.assertEqual('w', metric.template_value.metrics[0].name)
        self.assertEqual(metric.template_value.metrics[0].datatype, DataType.Template.value)

        self.assertEqual(1, len(metric.template_value.metrics[0].template_value.metrics))
        new_metric = metric.template_value.metrics[0].template_value.metrics[0]
        self.assertEqual(read_from_metric(new_metric), 383)
        self.assertEqual(new_metric.name, "x")
        self.assertEqual(new_metric.datatype, DataType.Int32.value)

    def test_inherit(self):
        """Test whether Structs properly inherit from other Structs."""

        exp_x = 32
        exp_y = -64
        exp_z = False
        exp_v = 3.14
        exp_w = "Hello, World"

        class Foo(Struct):
            network_name = "foo"
            x = Field(Int32, default=exp_x)
            y = Field(Int64, default=exp_y)
            z = Field(Boolean, default=exp_z)

        class Bar(Foo):
            network_name = "bar"
            v = Field(Double, default=exp_v)
            w = Field(String, default=exp_w)

        foo = Foo()
        bar = Bar()

        self.assertTrue(hasattr(foo, 'x'))
        self.assertFalse(hasattr(foo, 'v'))
        self.assertFalse(hasattr(foo, 'w'))

        self.assertEqual(foo.x, exp_x)
        self.assertEqual(foo.y, exp_y)
        self.assertEqual(foo.z, exp_z)

        self.assertEqual(bar.x, exp_x)
        self.assertEqual(bar.y, exp_y)
        self.assertEqual(bar.z, exp_z)
        self.assertEqual(bar.v, exp_v)
        self.assertEqual(bar.w, exp_w)

    def test_multiple_instances(self):
        """Test that multiple instances do not share values in their
        fields."""

        exp_x = 32
        exp_y = -64
        exp_z = False
        exp_v = 3.14
        exp_w = "Hello, World"

        class Foo(Struct):
            network_name = "foo"
            x = Field(Int32, default=exp_x)
            y = Field(Int64, default=exp_y)
            z = Field(Boolean, default=exp_z)
            v = Field(Double, default=exp_v)
            w = Field(String, default=exp_w)

        foo0 = Foo()
        foo1 = Foo()

        foo0.x = 33
        foo0.y = -65
        foo0.z = True
        foo0.v = 1.23
        foo0.w = "Goodbye"

        for var in ['x', 'y', 'z', 'v', 'w']:
            self.assertNotEqual(getattr(foo0, var), getattr(foo1, var))

class ArrayTester(unittest.TestCase):

    def setUp(self):
        int32_value = [-3, -2, -1, 0, 1, 2, 3]
        int64_value = [-281474976710656, 72057594037927936]
        double_value = [-1.5, 0.0, 3.14]
        boolean_value = [False, True]
        string_value = ["Hello", "World"]
        self.exp_values = [(Int32, int32_value),
                           (Int64, int64_value),
                           (Double, double_value),
                           (Boolean, boolean_value),
                           (String, string_value)]

        # Create a list of values that are guaranteed to differ
        # (although not in all places) from exp_values.
        int32_other = [-103, -102, -101, 0, 101, 102, 103]
        int64_other = [55, 72057594037927936]
        double_other = [8000, -578.234, 3.14]
        boolean_other = [True, True]
        string_other = ["konnichiwa", "sekai"]
        self.other_values = [(Int32, int32_other),
                             (Int64, int64_other),
                             (Double, double_other),
                             (Boolean, boolean_other),
                             (String, string_other)]

    def test_array_value(self):
        """Create an Array of IcypawType's and read back the value."""
        for exp_type, exp_value in self.exp_values:
            obj = Array[exp_type](exp_value)
            self.assertEqual(exp_value, obj.icpw_value)

    def test_index_value(self):
        """Test that we can index into Array values."""
        for exp_type, exp_value in self.exp_values:
            obj = Array[exp_type](exp_value)
            for i in range(len(exp_value)):
                self.assertEqual(exp_value[i], obj[i])

    def test_iter_value(self):
        """Test that we can iterate over the values of an Array."""
        for exp_type, exp_value in self.exp_values:
            obj = Array[exp_type](exp_value)
            for ev, ov in itertools.zip_longest(exp_value, obj):
                self.assertEqual(ev, ov)

    def test_index_tuple_value(self):
        """Test retrieving a tuple element of an array by index."""
        exp_values = [(0, 1), (2, 3)]
        obj = Array[(Int64, Int64)](exp_values)
        for i in range(len(exp_values)):
            self.assertEqual(exp_values[i], obj[i])

    def test_iter_tuple_value(self):
        """Test iterating over tuple elements of an array."""
        exp_values = [(0, 1), (2, 3)]
        obj = Array[(Int64, Int64)](exp_values)
        count = 0
        for exp_value, act_value in zip(exp_values, obj):
            self.assertEqual(exp_value, act_value)
            count += 1
        self.assertEqual(count, len(exp_values))

    def test_eq(self):
        """Test that objects compare equal when they have the same
        elements."""
        for exp_type, exp_value in self.exp_values:
            obj0 = Array[exp_type](exp_value)
            obj1 = Array[exp_type](exp_value)
            self.assertEqual(obj0, obj1)

    def test_ne_same_type(self):
        """Test that objects compare unequal, even when some elements are the
        same and the types are the same."""

        for (exp_type, exp_value), (other_type, other_value)\
                in zip(self.exp_values, self.other_values):

            obj0 = Array[exp_type](exp_value)
            obj1 = Array[other_type](other_value)
            self.assertNotEqual(obj0, obj1)

    def test_ne_different_types(self):
        """Test that objects compare unequal when the types differ."""

        # This test mostly exists to make sure the framework doesn't
        # choke on checking different types.

        for other_values in make_list_rotations(self.exp_values):
            for (exp_type, exp_value), (other_type, other_value)\
                    in zip(self.exp_values, other_values):

                self.assertNotEqual(exp_type, other_type)

                obj0 = Array[exp_type](exp_value)
                obj1 = Array[other_type](other_value)
                self.assertNotEqual(obj0, obj1)

    def test_set_in_metric(self):
        """Test that we can set a DataType in a metric and retrieve the value."""
        for exp_type, exp_value in self.exp_values:
            obj = Array[exp_type](exp_value)
            metric = Metric()
            obj.set_in_metric(metric)
            act_value = read_from_metric(metric)
            self.assertEqual(exp_value, act_value)

    def test_reuse_type_object(self):
        """Test creating several arrays with the same type object."""
        cls = Array[Int32]
        foo = cls([1, 2, 3, 4])
        bar = cls([5, 6, 7, 8])
        self.assertNotEqual(foo, bar)

class ArrayTupleTester(unittest.TestCase):

    def setUp(self):
        self.exp_value = [(505, -281474976710656, 3.14, False, "Hello, World"),
                          (1, 72057594037927936, 1.5, True, "abc")]
        self.exp_type = (Int32, Int64, Double, Boolean, String)

        self.other_value = [(-103, 72057594037927936, -578.234, True, "konnichiwa"),
                            (-1, -72057594037927936, 234.234, False, "def")]
        self.other_type = (Int32, Int64, Double, Boolean, String)

    def test_array_value(self):
        """Test creating an array where the elements are tuples and comparing
        its value."""

        obj = Array[self.exp_type](self.exp_value)
        self.assertEqual(self.exp_value, obj.icpw_value)

    def test_eq(self):
        """Test comparing two Arrays over Tuples with the same values."""

        obj0 = Array[self.exp_type](self.exp_value)
        obj1 = Array[self.exp_type](self.exp_value)
        self.assertEqual(obj0, obj1)

    def test_ne(self):
        """Test comparing two Arrays over Tuples with different values."""

        obj0 = Array[self.exp_type](self.exp_value)
        obj1 = Array[self.other_type](self.other_value)
        self.assertNotEqual(obj0, obj1)

    def test_set_in_metric(self):
        """Test setting an Array over Tuples in a metric."""
        obj = Array[self.exp_type](self.exp_value)
        metric = Metric()
        obj.set_in_metric(metric)
        act_value = read_from_metric(metric)
        self.assertEqual(self.exp_value, act_value)

class ArrayStructTester(unittest.TestCase):

    def test_struct_with_array(self):
        """Test creating an Icypaw Struct type that contains an array."""
        class Foo(Struct):
            network_name = "foo"
            x: Array[Int32]
            y: Array[Double, String, Boolean]

        exp_x = [1, 2, 3]
        exp_y = [(1.2, "a", True), (-3.4, "b", False)]

        foo = Foo()
        foo.x = exp_x
        foo.y = exp_y

        self.assertEqual(exp_x, foo.x)
        self.assertEqual(exp_y, foo.y)

    def test_struct_with_array_defaults(self):
        """Test creating an Icypaw Struct type that contains an array with
        default values."""

        exp_x = [1, 2, 3]
        exp_y = [(1.2, "a", True), (-3.4, "b", False)]

        class Foo(Struct):
            network_name = "foo"
            x: Array[Int32] = exp_x
            y: Array[Double, String, Boolean] = exp_y

        foo = Foo()

        self.assertEqual(exp_x, foo.x)
        self.assertEqual(exp_y, foo.y)

    def test_struct_with_array_set_in_metric(self):
        """Test creating an Icypaw Struct type with an array and writing it to
        a metric."""
        class Foo(Struct):
            network_name = "foo"
            x = Field(Array[Int32])
            y = Field(Array[Double, String, Boolean])

        exp_x = [1, 2, 3]
        exp_y = [(1.2, "a", True), (-3.4, "b", False)]
        exp_value = {
            'x': exp_x,
            'y': exp_y,
        }

        foo = Foo()
        foo.x = exp_x
        foo.y = exp_y

        metric = Metric()
        foo.set_in_metric(metric)
        act_value = read_from_metric(metric)
        self.assertEqual(exp_value, act_value)

class ArrayCacheTester(unittest.TestCase):

    def test_array_cache(self):
        cls0 = Array[Int64]
        cls1 = Array[Int64]
        self.assertTrue(cls0 is cls1)

    def test_array_tuple_cache(self):
        cls0 = Array[Int64, String]
        cls1 = Array[Int64, String]
        self.assertTrue(cls0 is cls1)

class StructCreatorTester(unittest.TestCase):
    """Test creating a Struct class programmatically."""

    def setUp(self):
        self.Foo = make_struct('Foo', {'x': Field(Int32), 'y': Field(Double)})

    def test_create(self):
        """Create a Struct class programmatically and read its members."""
        foo = self.Foo()

        exp_x = 5
        exp_y = 3.14

        foo.x = exp_x + 0.01  # Ensure that type conversion is actually taking place.
        foo.y = exp_y

        self.assertEqual(foo.x, exp_x)
        self.assertEqual(foo.y, exp_y)

    def test_eq(self):
        """Create two programmatic Struct classes and test if they are equal."""
        foo = self.Foo()
        bar = self.Foo()

        exp_x = 5
        exp_y = 3.14

        foo.x = exp_x
        foo.y = exp_y

        bar.x = exp_x
        bar.y = exp_y

        self.assertEqual(foo, bar)

    def test_ne(self):
        """Create two programmatic Struct classes and test if they are equal."""
        foo = self.Foo()
        bar = self.Foo()

        foo.x = 5
        foo.y = 3.14

        bar.x = 7
        bar.y = -1.5

        self.assertNotEqual(foo, bar)

    def test_set_in_metric(self):
        """Test programmatically creating a Struct and setting its value in a
        metric."""

        foo = self.Foo()

        exp_x = 5
        exp_y = 3.14

        foo.x = exp_x
        foo.y = exp_y

        metric = Metric()
        foo.set_in_metric(metric)

        self.assertEqual(read_from_metric(metric), foo)

class TypeFromMetricTester(unittest.TestCase):

    def setUp(self):
        # Remove dynamically created subclasses from IcypawType's
        # __subclass__ list. This is not expected to be a problem in
        # actual usage.
        gc.collect()

    def test_int32(self):
        """Test making a metric with an int32 value and determining the
        correct type."""
        exp_class = Int32
        exp_value = -11
        self.run_test(exp_class, exp_value)

    def test_int64(self):
        """Test making a metric with an int64 value and determining the
        correct type."""
        exp_class = Int64
        exp_value = -13
        self.run_test(exp_class, exp_value)

    def test_uint32(self):
        """Test making a metric with an unsigned int32 value and determining the
        correct type."""
        exp_class = UInt32
        exp_value = 11
        self.run_test(exp_class, exp_value)

    def test_uint64(self):
        """Test making a metric with an unsigned int64 value and determining the
        correct type."""
        exp_class = UInt64
        exp_value = 13
        self.run_test(exp_class, exp_value)

    def test_double(self):
        exp_class = Double
        exp_value = 3.14
        self.run_test(exp_class, exp_value)

    def test_boolean(self):
        self.run_test(Boolean, True)

    def test_string(self):
        self.run_test(String, "Hello, World")

    def test_struct(self):
        template_ref = "myref"

        metric = Metric()
        metric.datatype = DataType.Template.value
        metric.template_value.template_ref = template_ref.encode()

        with self.assertRaises(TypeError):
            type_from_metric(metric)

        class Foo(Struct):
            network_name = template_ref

        struct_types = [Foo]

        exp_class = Foo
        act_class = type_from_metric(metric, struct_types=struct_types)
        self.assertEqual(act_class, exp_class)

    def test_array_scalar(self):
        metric = Metric()
        metric.datatype = DataType.DataSet.value
        metric.dataset_value.types.append(DataType.Int64.value)

        exp_class = Array[Int64]
        act_class = type_from_metric(metric)
        self.assertEqual(act_class, exp_class)

    def test_array_tuple(self):
        metric = Metric()
        metric.datatype = DataType.DataSet.value
        metric.dataset_value.types.extend([DataType.Int64.value, DataType.String.value])

        exp_class = Array[Int64, String]
        act_class = type_from_metric(metric)
        self.assertEqual(act_class, exp_class)

    @nottest
    def run_test(self, exp_class, exp_value):
        metric = Metric()

        value = exp_class(exp_value)
        value.set_in_metric(metric)

        act_class = type_from_metric(metric)

        self.assertEqual(exp_class, act_class)

class ValueFromMetricTester(unittest.TestCase):

    def setUp(self):
        gc.collect()

    def test_int32(self):
        """Test creating an Int32 from a metric."""
        self.run_test(Int32(-42))

    def test_int64(self):
        """Test creating an Int64 from a metric."""
        self.run_test(Int64(-502))

    def test_uint32(self):
        """Test creating a UInt32 from a metric."""
        self.run_test(UInt32(42))

    def test_uint64(self):
        """Test creating a UInt32 from a metric."""
        self.run_test(UInt64(502))

    def test_double(self):
        self.run_test(Double(3.14))

    def test_boolean(self):
        self.run_test(Boolean(False))

    def test_string(self):
        self.run_test(String("abc"))

    def test_struct(self):
        class Foo(Struct):
            network_name = "foo"
            x = Field(Int64)
        struct_types = [Foo]
        self.run_test(Foo({'x': 33}), struct_types=struct_types)

    def test_nested_struct(self):
        class Bar(Struct):
            network_name = "bar"
            x = Field(String)

        class Foo(Struct):
            network_name = "foo"
            z = Field(Bar)

        struct_types = [Bar, Foo]

        self.run_test(Foo({'z': {'x': "abc"}}), struct_types=struct_types)

    def test_array(self):
        atype = Array[Int64]
        self.run_test(atype([-2, 1, 5]))

    def test_array_tuples(self):
        atype = Array[Int64, String]
        self.run_test(atype([(-2, "abc"), (1, "z"), (5, "x")]))

    @nottest
    def run_test(self, exp_value, struct_types=None):
        metric = Metric()
        exp_value.set_in_metric(metric)
        act_value = value_from_metric(metric, struct_types=struct_types)
        self.assertEqual(exp_value, act_value)

class MergeWithMetricTester(unittest.TestCase):

    def setUp(self):
        gc.collect()

    def test_int32(self):
        self.run_test(Int32, 1, -2)

    def test_int64(self):
        self.run_test(Int64, 1, -2)

    def test_uint32(self):
        self.run_test(UInt32, 1, 55)

    def test_uint64(self):
        self.run_test(Int64, 82, 999)

    def test_double(self):
        self.run_test(Double, -0.123, 3.14)

    def test_boolean(self):
        self.run_test(Boolean, False, True)

    def test_string(self):
        self.run_test(String, "a", "b")

    def test_struct(self):
        class Foo(Struct):
            network_name = "foo"
            x = Field(Int64)
            y = Field(Int64)
        self.run_test(Foo, {'x': 1, 'y': 2}, {'x': 3, 'y': 4})

    def test_nested_struct(self):
        class Foo(Struct):
            network_name = "foo"
            x = Field(Int64)
            y = Field(Int64)
        class Bar(Struct):
            network_name = "bar"
            w = Field(Foo)
        self.run_test(Bar, {'w': {'x': 1, 'y': 2}}, {'w': {'x': 3, 'y': 4}})

    def test_partial_struct(self):
        """Test merging a metric that does not provide all fields of a
        Struct."""
        class Foo(Struct):
            network_name = "foo"
            x = Field(Int64)
            y = Field(Int64)
        exp_value = Foo({'x': 1, 'y': 2})
        act_value = Foo({'x': 5, 'y': 2})
        metric = Metric()
        metric.datatype = DataType.Template.value
        metric.template_value.template_ref = Foo.network_name.encode()
        x_metric = metric.template_value.metrics.add()
        x_metric.name = "x".encode()
        x_metric.datatype = DataType.Int64.value
        x_metric.long_value = 1
        self.assertNotEqual(exp_value, act_value)
        act_value.merge_with_metric(metric)
        self.assertEqual(exp_value, act_value)

    @nottest
    def run_test(self, icpw_type, start_value, end_value):
        exp_value = icpw_type(end_value)
        act_value = icpw_type(start_value)
        self.assertNotEqual(exp_value, act_value)
        metric = Metric()
        exp_value.set_in_metric(metric)
        act_value.merge_with_metric(metric)
        self.assertEqual(exp_value, act_value)

class ToPythonTester(unittest.TestCase):

    def test_int32(self):
        self.run_scalar_test(Int32, -5)

    def test_uint32(self):
        self.run_scalar_test(UInt32, 55)

    def test_int64(self):
        self.run_scalar_test(Int64, -1000)

    def test_uint64(self):
        self.run_scalar_test(UInt64, 5348)

    def test_double(self):
        self.run_scalar_test(Double, 3.14)

    def test_boolean(self):
        self.run_scalar_test(Boolean, False)
        self.run_scalar_test(Boolean, True)

    def test_string(self):
        self.run_scalar_test(String, "Hello, World")

    @nottest
    def run_scalar_test(self, icpw_type, exp_value):
        icpw_value = icpw_type(exp_value)
        act_value = icpw_value.to_python()
        self.assertEqual(type(act_value), type(exp_value))
        self.assertEqual(act_value, exp_value)

    def test_struct(self):
        class Foo(Struct):
            network_name = 'foo'
            x = Field(Int64)

        class Bar(Struct):
            network_name = 'bar'
            w = Field(Foo)
            v = Field(Int64)

        exp_value = {'v': 1, 'w': {'x': 2}}
        bar = Bar(exp_value)
        act_value = bar.to_python()
        self.assertEqual(type(exp_value), type(act_value))
        self.assertEqual(type(exp_value['w']), type(act_value['w']))
        self.assertEqual(exp_value, act_value)

    def test_array(self):
        t = Array[Int64]
        exp_value = [-1, -2, 0, 5, 7]
        arr = t(exp_value)
        act_value = arr.to_python()
        self.assertEqual(type(exp_value), type(act_value))
        self.assertEqual(exp_value, act_value)
        self.assertTrue(all(type(ex) == type(ac) for ex, ac in zip(exp_value, act_value)))

    def test_array_tuples(self):
        t = Array[Int64, String]
        exp_value = [(0, "a"), (1, "b")]
        arr = t(exp_value)
        act_value = arr.to_python()
        self.assertEqual(type(exp_value), type(act_value))
        self.assertEqual(exp_value, act_value)
        self.assertTrue(all(type(ex) == type(ac) for ex, ac in zip(exp_value, act_value)))

##
# Helper functions
#

def make_list_rotations(original_list, include_original=False):
    """Iterate over rotations of a list."""
    start = 0 if include_original else 1
    for i in range(start, len(original_list)):
        rotated_list = original_list[i:] + original_list[:i + 1]
        yield rotated_list
