# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

import unittest

from . import tahu_interface as ti

class PayloadTester(unittest.TestCase):

    def _make_template_metric(self):
        """Create a metric that uses a template. Return the metric and the
        template definition metric that should result."""

        metric = ti.Metric()
        metric.name = "TestMetric".encode()
        metric.datatype = ti.DataType.Template.value
        metric.template_value.template_ref = "TestTemplate".encode()
        metric.template_value.is_definition = False
        tmetric = metric.template_value.metrics.add()
        tmetric.name = "TestTemplateMetric".encode()
        tmetric.datatype = ti.DataType.UInt64.value
        tmetric.long_value = 42

        template_metric = ti.Metric()
        template_metric.name = "_types_/TestTemplate".encode()
        template_metric.datatype = ti.DataType.Template.value
        template_metric.template_value.is_definition = True
        tmetric = template_metric.template_value.metrics.add()
        tmetric.name = "TestTemplateMetric".encode()
        tmetric.datatype = ti.DataType.UInt64.value

        return metric, template_metric

    def _make_simple_metric(self, value=42):
        """Create and return a simple metric."""
        metric = ti.Metric()
        metric.name = "TestMetric".encode()
        metric.datatype = ti.DataType.Int64.value
        metric.long_value = value

        return metric

    def _compare_metrics(self, exp_metric, act_metric):
        """Use assert* methods to check for equality among the fields of the
        given metrics that we care about."""

        self.assertEqual(exp_metric.HasField('name'), act_metric.HasField('name'))
        if exp_metric.HasField('name'):
            self.assertEqual(exp_metric.name, act_metric.name)
        self.assertEqual(exp_metric.datatype, act_metric.datatype)
        if exp_metric.datatype == ti.DataType.Template.value:
            self._compare_template_instances(exp_metric.template_value, act_metric.template_value)
        else:
            exp_value_field = exp_metric.WhichOneof('value')
            act_value_field = act_metric.WhichOneof('value')
            self.assertEqual(exp_value_field, act_value_field)
            if exp_value_field is not None:
                self.assertEqual(getattr(exp_metric, exp_value_field),
                                 getattr(act_metric, act_value_field))

    def _compare_template_instances(self, exp_template, act_template):
        """Use assert* methods to check for equality among the fields of the
        given templates that we care about."""

        self.assertEqual(exp_template.template_ref, act_template.template_ref)
        self.assertEqual(exp_template.is_definition, act_template.is_definition)
        for exp_metric, act_metric in zip(exp_template.metrics, act_template.metrics):
            self._compare_metrics(exp_metric, act_metric)

    def _make_bdseq_metric(self, bdSeq):
        """Create a metric identical to ones used to convey the birth-death
        sequence."""

        metric = ti.Metric()
        metric.name = "bdSeq".encode()
        metric.datatype = ti.DataType.UInt64.value
        metric.long_value = bdSeq
        return metric

class TestNbirth(PayloadTester):

    def setUp(self):
        self.bdSeq = 42
        self.iface = ti.TahuServerInterface(bdSeq=self.bdSeq)

    def test_nbirth_no_devices_no_templates(self):
        """Test creating an NBIRTH message where we have no devices and no
        templates in use."""

        metric = self._make_simple_metric()

        self.iface.set_initial_node_metrics([metric])

        nbirth = self.iface.new_nbirth()

        self.assertEqual(0, nbirth.seq)
        self.assertTrue(nbirth.HasField('timestamp'))
        self.assertEqual(2, len(nbirth.metrics))

        exp_bdseq_metric = self._make_bdseq_metric(self.bdSeq)
        act_bdseq_metric = nbirth.metrics[0]
        self._compare_metrics(exp_bdseq_metric, act_bdseq_metric)

        act_metric = nbirth.metrics[1]
        self._compare_metrics(metric, act_metric)

    def test_nbirth_no_devices_with_template(self):
        """Test creating an NBIRTH message where we have no devices but a
        template is used."""

        metric, template_metric = self._make_template_metric()

        self.iface.set_initial_node_metrics([metric])

        nbirth = self.iface.new_nbirth()

        bdseq_metric_count = 1
        data_metric_count = 1
        template_metric_count = 1
        total_metric_count = bdseq_metric_count + data_metric_count + template_metric_count

        self.assertEqual(0, nbirth.seq)
        self.assertTrue(nbirth.HasField('timestamp'))
        self.assertEqual(total_metric_count, len(nbirth.metrics))

        exp_bdseq_metric = self._make_bdseq_metric(self.bdSeq)
        act_bdseq_metric = nbirth.metrics[0]
        self._compare_metrics(exp_bdseq_metric, act_bdseq_metric)

        # Test the metric
        act_data_metric = nbirth.metrics[1]
        self._compare_metrics(metric, act_data_metric)

        # Test the template
        act_tmp_metric = nbirth.metrics[2]
        self._compare_metrics(template_metric, act_tmp_metric)

    def test_nbirth_device_with_templates(self):
        """Test creating an NBIRTH message with a device and a template. The
        NBIRTH will not contain the metrics for the device, but it will
        contain its template definitions."""

        device_name = 'dev0'

        self.iface.register_device(device_name)

        metric, template_metric = self._make_template_metric()

        self.iface.set_initial_device_metrics(device_name, [metric])

        nbirth = self.iface.new_nbirth()

        bdseq_metric_count = 1
        data_metric_count = 0
        template_metric_count = 1
        total_metric_count = bdseq_metric_count + data_metric_count + template_metric_count

        self.assertEqual(0, nbirth.seq)
        self.assertTrue(nbirth.HasField('timestamp'))
        self.assertEqual(total_metric_count, len(nbirth.metrics))

        exp_bdseq_metric = self._make_bdseq_metric(self.bdSeq)
        act_bdseq_metric = nbirth.metrics[0]
        self._compare_metrics(exp_bdseq_metric, act_bdseq_metric)

        # Test the template
        act_tmp_metric = nbirth.metrics[1]
        self._compare_metrics(template_metric, act_tmp_metric)

    def test_nbirth_device_class_with_templates(self):
        """Test creating an NBIRTH message with a device that is not yet
        initialized. Instead we are given a list of metrics and must derive
        template definitions from them."""

        metric, template_metric = self._make_template_metric()

        self.iface.register_device_class_metrics([metric])

        nbirth = self.iface.new_nbirth()

        bdseq_metric_count = 1
        data_metric_count = 0
        template_metric_count = 1
        total_metric_count = bdseq_metric_count + data_metric_count + template_metric_count

        self.assertEqual(0, nbirth.seq)
        self.assertTrue(nbirth.HasField('timestamp'))
        self.assertEqual(total_metric_count, len(nbirth.metrics))

        exp_bdseq_metric = self._make_bdseq_metric(self.bdSeq)
        act_bdseq_metric = nbirth.metrics[0]
        self._compare_metrics(exp_bdseq_metric, act_bdseq_metric)

        # Test the template
        act_tmp_metric = nbirth.metrics[1]
        self._compare_metrics(template_metric, act_tmp_metric)

class TestDbirth(PayloadTester):

    def setUp(self):
        self.iface = ti.TahuServerInterface(bdSeq=42)

    def test_dbirth_no_template(self):
        """Create a DBIRTH message for a device with no templates."""
        device_id = 'dev0'
        self.iface.register_device(device_id)
        metric = self._make_simple_metric()
        self.iface.set_initial_device_metrics(device_id, [metric])
        self.iface.new_nbirth()
        dbirth = self.iface.new_dbirth(device_id)

        self.assertEqual(1, dbirth.seq)
        self.assertTrue(dbirth.HasField('timestamp'))
        self.assertEqual(1, len(dbirth.metrics))

        act_metric = dbirth.metrics[0]
        self._compare_metrics(metric, act_metric)

    def test_dbirth_with_template(self):
        """Create a DBIRTH message for a device with a message using a
        template. The template definition is not transmitted in the
        DBIRTH message, unlike the NBIRTH.

        """

        device_id = 'dev0'
        self.iface.register_device(device_id)
        metric, _template_def = self._make_template_metric()
        self.iface.set_initial_device_metrics(device_id, [metric])
        self.iface.new_nbirth()
        dbirth = self.iface.new_dbirth(device_id)

        self.assertEqual(1, dbirth.seq)
        self.assertTrue(dbirth.HasField('timestamp'))
        self.assertEqual(1, len(dbirth.metrics))

        act_metric = dbirth.metrics[0]
        self._compare_metrics(metric, act_metric)

class TestNdeath(PayloadTester):

    def setUp(self):
        self.bdSeq = 42
        self.iface = ti.TahuServerInterface(bdSeq=self.bdSeq)

    def test_ndeath(self):
        """Test creating an NDEATH payload."""
        ndeath = self.iface.new_ndeath()
        metric = self._make_bdseq_metric(self.bdSeq)
        self.assertEqual(1, len(ndeath.metrics))
        self._compare_metrics(metric, ndeath.metrics[0])

class TestDdeath(PayloadTester):

    def setUp(self):
        self.bdSeq = 42
        self.iface = ti.TahuServerInterface(bdSeq=self.bdSeq)

    def test_ddeath(self):
        """Test the DDEATH payload. The spec directly contradicts itself on
        what this should look like."""

        seq = self.iface.seq
        ddeath = self.iface.new_ddeath()
        self.assertNotEqual(seq, self.iface.seq)

        self.assertEqual(seq, ddeath.seq)

class TestNdata(PayloadTester):

    def setUp(self):
        self.iface = ti.TahuServerInterface(bdSeq=0)

    def test_ndata_no_update(self):
        """Test issuing an NDATA message if no updates have occurred."""

        metric = self._make_simple_metric()
        self.iface.set_initial_node_metrics([metric])
        self.iface.new_nbirth()
        exp_seq = self.iface.seq
        ndata = self.iface.new_ndata()

        self.assertNotEqual(exp_seq, self.iface.seq)
        self.assertEqual(exp_seq, ndata.seq)
        self.assertEqual(0, len(ndata.metrics))

    def test_ndata_with_update(self):
        """Test issuing an NDATA message after an update has occurred."""

        metric = self._make_simple_metric()
        self.iface.set_initial_node_metrics([metric])
        self.iface.new_nbirth()

        exp_value = 123
        exp_metric = self._make_simple_metric(exp_value)
        self.iface.set_node_metric(exp_metric)

        exp_seq = self.iface.seq
        ndata = self.iface.new_ndata()

        self.assertNotEqual(exp_seq, self.iface.seq)
        self.assertEqual(exp_seq, ndata.seq)

        act_metric = ndata.metrics[0]
        # exp_metric and act_metric will not be identical because of
        # the use of an alias instead of the name.
        exp_metric.alias = self.iface.get_node_metric_alias(exp_metric.name)
        exp_metric.ClearField('name')

        self._compare_metrics(exp_metric, act_metric)

class TestDdata(PayloadTester):

    def setUp(self):
        self.device_id = 'dev0'
        self.iface = ti.TahuServerInterface(bdSeq=0)
        self.iface.register_device(self.device_id)

    def test_ddata_no_update(self):
        """Test issuing an DDATA message if no updates have occurred."""

        metric = self._make_simple_metric()
        self.iface.set_initial_node_metrics([])
        self.iface.set_initial_device_metrics(self.device_id, [metric])
        self.iface.new_nbirth()
        exp_seq = self.iface.seq
        ddata = self.iface.new_ddata(self.device_id)

        self.assertNotEqual(exp_seq, self.iface.seq)
        self.assertEqual(exp_seq, ddata.seq)
        self.assertEqual(0, len(ddata.metrics))

    def test_ddata_with_update(self):
        """Test issuing an DDATA message after an update has occurred."""

        metric = self._make_simple_metric()
        self.iface.set_initial_node_metrics([])
        self.iface.set_initial_device_metrics(self.device_id, [metric])
        self.iface.new_nbirth()

        exp_value = 123
        exp_metric = self._make_simple_metric(exp_value)
        self.iface.set_device_metric(self.device_id, exp_metric)

        exp_seq = self.iface.seq
        ddata = self.iface.new_ddata(self.device_id)

        self.assertNotEqual(exp_seq, self.iface.seq)
        self.assertEqual(exp_seq, ddata.seq)

        act_metric = ddata.metrics[0]
        # exp_metric and act_metric will not be identical because of
        # the use of an alias instead of the name.
        exp_metric.alias = self.iface.get_device_metric_alias(self.device_id, exp_metric.name)
        exp_metric.ClearField('name')
        self._compare_metrics(exp_metric, act_metric)

class TahuTopicTester(unittest.TestCase):

    def setUp(self):
        self.namespace = "spBv1.0"

    def test_namespace(self):
        """Make sure a default TahuServerInterface uses the correct namespace name."""
        iface = ti.TahuServerInterface()
        self.assertEqual(self.namespace, iface.namespace)

    def test_nbirth_topic_defaults(self):
        """Test creating a topic for an NBIRTH message. Use defaults provided
        to the constructor of the TahuServerInterface."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{def_group_id}/NBIRTH/{def_edge_node_id}"
        act_topic = iface.new_nbirth_topic()

        self.assertEqual(exp_topic, act_topic)

    def test_nbirth_topic_arguments(self):
        """Test creating a topic for an NBIRTH message. Use the arguments to
        the method."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        exp_group_id = 'exp_group_id'
        exp_edge_node_id = 'exp_edge_node_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{exp_group_id}/NBIRTH/{exp_edge_node_id}"
        act_topic = iface.new_nbirth_topic(group_id=exp_group_id, edge_node_id=exp_edge_node_id)

        self.assertEqual(exp_topic, act_topic)

    def test_dbirth_topic_defaults(self):
        """Test creating a topic for a DBIRTH message. Use defaults provided
        to the constructor of the TahuServerInterface."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        device_id = 'test_device_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{def_group_id}/DBIRTH/{def_edge_node_id}/{device_id}"
        act_topic = iface.new_dbirth_topic(device_id)

        self.assertEqual(exp_topic, act_topic)

    def test_dbirth_topic_arguments(self):
        """Test creating a topic for a DBIRTH message. Use the arguments to
        the method."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        exp_group_id = 'exp_group_id'
        exp_edge_node_id = 'exp_edge_node_id'

        device_id = 'test_device_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{exp_group_id}/DBIRTH/{exp_edge_node_id}/{device_id}"
        act_topic = iface.new_dbirth_topic(device_id,
                                           group_id=exp_group_id, edge_node_id=exp_edge_node_id)

        self.assertEqual(exp_topic, act_topic)

    def test_ndeath_topic_defaults(self):
        """Test creating a topic for an NDEATH message. Use defaults provided
        to the constructor of the TahuServerInterface."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{def_group_id}/NDEATH/{def_edge_node_id}"
        act_topic = iface.new_ndeath_topic()

        self.assertEqual(exp_topic, act_topic)

    def test_ndeath_topic_arguments(self):
        """Test creating a topic for an NDEATH message. Use the arguments to
        the method."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        exp_group_id = 'exp_group_id'
        exp_edge_node_id = 'exp_edge_node_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{exp_group_id}/NDEATH/{exp_edge_node_id}"
        act_topic = iface.new_ndeath_topic(group_id=exp_group_id, edge_node_id=exp_edge_node_id)

        self.assertEqual(exp_topic, act_topic)

    def test_ddeath_topic_defaults(self):
        """Test creating a topic for a DDEATH message. Use defaults provided
        to the constructor of the TahuServerInterface."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        device_id = 'test_device_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{def_group_id}/DDEATH/{def_edge_node_id}/{device_id}"
        act_topic = iface.new_ddeath_topic(device_id)

        self.assertEqual(exp_topic, act_topic)

    def test_ddeath_topic_arguments(self):
        """Test creating a topic for a DDEATH message. Use the arguments to
        the method."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        exp_group_id = 'exp_group_id'
        exp_edge_node_id = 'exp_edge_node_id'

        device_id = 'test_device_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{exp_group_id}/DDEATH/{exp_edge_node_id}/{device_id}"
        act_topic = iface.new_ddeath_topic(device_id,
                                           group_id=exp_group_id, edge_node_id=exp_edge_node_id)

        self.assertEqual(exp_topic, act_topic)

    def test_ncmd_topic_defaults(self):
        """Test creating a topic for an NCMD message. Use defaults provided
        to the constructor of the TahuServerInterface."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{def_group_id}/NCMD/{def_edge_node_id}"
        act_topic = iface.new_ncmd_topic()

        self.assertEqual(exp_topic, act_topic)

    def test_ncmd_topic_arguments(self):
        """Test creating a topic for an NCMD message. Use the arguments to
        the method."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        exp_group_id = 'exp_group_id'
        exp_edge_node_id = 'exp_edge_node_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{exp_group_id}/NCMD/{exp_edge_node_id}"
        act_topic = iface.new_ncmd_topic(group_id=exp_group_id, edge_node_id=exp_edge_node_id)

        self.assertEqual(exp_topic, act_topic)

    def test_dcmd_topic_defaults(self):
        """Test creating a topic for a DCMD message. Use defaults provided
        to the constructor of the TahuServerInterface."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        device_id = 'test_device_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{def_group_id}/DCMD/{def_edge_node_id}/{device_id}"
        act_topic = iface.new_dcmd_topic(device_id)

        self.assertEqual(exp_topic, act_topic)

    def test_dcmd_topic_arguments(self):
        """Test creating a topic for a DCMD message. Use the arguments to
        the method."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        exp_group_id = 'exp_group_id'
        exp_edge_node_id = 'exp_edge_node_id'

        device_id = 'test_device_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{exp_group_id}/DCMD/{exp_edge_node_id}/{device_id}"
        act_topic = iface.new_dcmd_topic(device_id,
                                         group_id=exp_group_id, edge_node_id=exp_edge_node_id)

        self.assertEqual(exp_topic, act_topic)

    def test_ndata_topic_defaults(self):
        """Test creating a topic for an NDATA message. Use defaults provided
        to the constructor of the TahuServerInterface."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{def_group_id}/NDATA/{def_edge_node_id}"
        act_topic = iface.new_ndata_topic()

        self.assertEqual(exp_topic, act_topic)

    def test_ndata_topic_arguments(self):
        """Test creating a topic for an NDATA message. Use the arguments to
        the method."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        exp_group_id = 'exp_group_id'
        exp_edge_node_id = 'exp_edge_node_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{exp_group_id}/NDATA/{exp_edge_node_id}"
        act_topic = iface.new_ndata_topic(group_id=exp_group_id, edge_node_id=exp_edge_node_id)

        self.assertEqual(exp_topic, act_topic)


    def test_ddata_topic_defaults(self):
        """Test creating a topic for a DDATA message. Use defaults provided
        to the constructor of the TahuServerInterface."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        device_id = 'test_device_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{def_group_id}/DDATA/{def_edge_node_id}/{device_id}"
        act_topic = iface.new_ddata_topic(device_id)

        self.assertEqual(exp_topic, act_topic)

    def test_ddata_topic_arguments(self):
        """Test creating a topic for a DDATA message. Use the arguments to
        the method."""

        def_group_id = 'def_group_id'
        def_edge_node_id = 'def_edge_node_id'

        exp_group_id = 'exp_group_id'
        exp_edge_node_id = 'exp_edge_node_id'

        device_id = 'test_device_id'

        iface = ti.TahuServerInterface(group_id=def_group_id, edge_node_id=def_edge_node_id)

        exp_topic = f"{self.namespace}/{exp_group_id}/DDATA/{exp_edge_node_id}/{device_id}"
        act_topic = iface.new_ddata_topic(device_id,
                                          group_id=exp_group_id, edge_node_id=exp_edge_node_id)

        self.assertEqual(exp_topic, act_topic)

    def test_state(self):
        """Test creating a topic for a STATE message. This is a very simple
        messsage with a very simple topic."""

        scada_host_id = 'test_scada_host_id'

        iface = ti.TahuServerInterface()

        exp_topic = f"STATE/{scada_host_id}"
        act_topic = iface.new_state_topic(scada_host_id)

        self.assertEqual(exp_topic, act_topic)

class TestMetricOrganizer(unittest.TestCase):

    def test_get_no_template(self):
        """Set a single initial metric, then return it."""

        org = ti.MetricOrganizer()

        name = "TestMetric"
        datatype = ti.DataType.String.value
        value = "test_value"
        metric = ti.Metric()
        metric.name = name.encode()
        metric.datatype = datatype
        metric.string_value = value.encode()

        templates = org.set_initial_metrics([metric])

        self.assertEqual({}, templates)

        act_metric, = org.get_all()

        # Test the name and value.
        self.assertEqual(name, act_metric.name)
        self.assertEqual(value, act_metric.string_value)
        self.assertEqual(datatype, act_metric.datatype)

        # Test if there is an alias.
        self.assertTrue(act_metric.HasField('alias'))

    def test_invalid(self):
        """Test setting an invalid initial metric. This means it was not given
        a name."""

        org = ti.MetricOrganizer()

        datatype = ti.DataType.String.value
        value = "test_value"
        metric = ti.Metric()
        metric.datatype = datatype
        metric.string_value = value.encode()

        with self.assertRaises(ti.TahuInterfaceError):
            org.set_initial_metrics([metric])

    def test_extract_template(self):
        """Test that the program properly extracts a template from a metric in
        the initial commit."""

        org = ti.MetricOrganizer()

        name = "TestMetric"
        datatype = ti.DataType.Template.value
        metric = ti.Metric()
        metric.name = name.encode()
        metric.datatype = datatype

        metric.template_value.template_ref = "TmpRef".encode()
        met = metric.template_value.metrics.add()
        met.name = b"TestTemplateMetric"
        met.datatype = ti.DataType.Int64.value
        met.long_value = 42

        templates = org.set_initial_metrics([metric])

        self.assertEqual(1, len(templates))

        self.assertIn("TmpRef", templates)

        act_template = templates['TmpRef']

        self.assertFalse(act_template.HasField('template_ref'))
        self.assertEqual(act_template.metrics[0].name, "TestTemplateMetric")
        self.assertEqual(act_template.metrics[0].datatype, ti.DataType.Int64.value)
        self.assertFalse(act_template.metrics[0].HasField('long_value'))

    def test_extract_template_of_dataset(self):
        """Test that the program properly extracts a template from a metric in
        the initial commit when the template contains an array type."""

        org = ti.MetricOrganizer()

        name = "TestMetric"
        datatype = ti.DataType.Template.value
        metric = ti.Metric()
        metric.name = name.encode()
        metric.datatype = datatype

        metric.template_value.template_ref = "TmpRef".encode()
        met = metric.template_value.metrics.add()
        met.name = b"TestTemplateMetric"
        met.datatype = ti.DataType.DataSet.value
        met.dataset_value.num_of_columns = 2
        met.dataset_value.columns.append(b"")
        met.dataset_value.columns.append(b"")
        met.dataset_value.types.append(ti.DataType.String.value)
        met.dataset_value.types.append(ti.DataType.Int64.value)

        templates = org.set_initial_metrics([metric])

        self.assertEqual(1, len(templates))

        self.assertIn("TmpRef", templates)

        act_template = templates['TmpRef']

        self.assertFalse(act_template.HasField('template_ref'))
        self.assertEqual(act_template.metrics[0].name, "TestTemplateMetric")
        self.assertEqual(act_template.metrics[0].datatype, ti.DataType.DataSet.value)
        self.assertTrue(act_template.metrics[0].HasField('dataset_value'))
        self.assertEqual(2, act_template.metrics[0].dataset_value.num_of_columns)
        self.assertEqual(ti.DataType.String.value,
                         act_template.metrics[0].dataset_value.types[0])
        self.assertEqual(ti.DataType.Int64.value,
                         act_template.metrics[0].dataset_value.types[1])


class TestClientInterface(unittest.TestCase):

    def test_ncmd(self):
        iface = ti.TahuClientInterface()
        group_id = 'grp0'
        edge_node_id = 'node0'
        cmd = 'Command 0'
        full_cmd = f'command/{cmd}'
        value = 5
        datatype = ti.DataType.Int32
        payload, _topic = iface.new_ncmd(group_id, edge_node_id, cmd, value, datatype)
        metric = payload.metrics[0]
        self.assertEqual(payload.timestamp, metric.timestamp)
        self.assertEqual(full_cmd, metric.name)
        self.assertEqual(datatype.value, metric.datatype)
        self.assertEqual(value, metric.int_value)

    def test_ncmd_topic(self):
        iface = ti.TahuClientInterface()
        group_id = 'grp0'
        edge_node_id = 'node0'
        cmd = 'Command 0'
        value = 5
        datatype = ti.DataType.Int32
        _payload, topic = iface.new_ncmd(group_id, edge_node_id, cmd, value, datatype)
        self.assertEqual(topic, f"spBv1.0/{group_id}/NCMD/{edge_node_id}")

    def test_dcmd(self):
        iface = ti.TahuClientInterface()
        group_id = 'grp0'
        edge_node_id = 'node0'
        device_id = 'dev0'
        cmd = 'Command 0'
        full_cmd = f'command/{cmd}'
        value = 5
        datatype = ti.DataType.Int32
        payload, _topic = iface.new_dcmd(group_id, edge_node_id, device_id, cmd, value, datatype)
        metric = payload.metrics[0]
        self.assertEqual(payload.timestamp, metric.timestamp)
        self.assertEqual(full_cmd, metric.name)
        self.assertEqual(datatype.value, metric.datatype)
        self.assertEqual(value, metric.int_value)

    def test_dcmd_topic(self):
        iface = ti.TahuClientInterface()
        group_id = 'grp0'
        edge_node_id = 'node0'
        device_id = 'dev0'
        cmd = 'Command 0'
        value = 5
        datatype = ti.DataType.Int32
        _payload, topic = iface.new_dcmd(group_id, edge_node_id, device_id, cmd, value, datatype)
        self.assertEqual(topic, f"spBv1.0/{group_id}/DCMD/{edge_node_id}/{device_id}")

class PropertySetTester(unittest.TestCase):
    def _add_value(self, ps, value, datatype, attr):
        v = ps.values.add()
        v.type = datatype.value
        if isinstance(value, str):
            value = value.encode()
        setattr(v, attr, value)

    def _add_propertyset(self, ps):
        v = ps.values.add()
        v.type = ti.DataType.PropertySet.value
        return v.propertyset_value

    def test_dict_to_propertyset(self):
        dict_value = {
            'my_int': 42,
            'my_float': 3.14159,
            'my_bool': False,
            'my_string': 'hello, world!',
            'my_dict': {
                'foo': 0,
                'bar': True
            }
        }
        expected = ti.Payload.PropertySet()
        expected.keys.extend(key.encode() for key in dict_value.keys())

        self._add_value(expected, 42, ti.DataType.Int64, 'long_value')
        self._add_value(expected, 3.14159, ti.DataType.Double, 'double_value')
        self._add_value(expected, False, ti.DataType.Boolean, 'boolean_value')
        self._add_value(expected, 'hello, world!', ti.DataType.String, 'string_value')

        nested_ps = self._add_propertyset(expected)
        nested_ps.keys.extend(key.encode() for key in dict_value['my_dict'].keys())
        self._add_value(nested_ps, 0, ti.DataType.Int64, 'long_value')
        self._add_value(nested_ps, True, ti.DataType.Boolean, 'boolean_value')

        ps = ti.iterable_to_propertyset(dict_value)
        self.assertEqual(expected, ps)

    def test_read_dict_from_propertyset(self):
        expected = {
            'my_int': 42,
            'my_float': 3.14159,
            'my_bool': False,
            'my_string': 'hello, world!',
            'my_dict': {
                'foo': 0,
                'bar': True
            }
        }

        ps = ti.iterable_to_propertyset(expected)  # already covered
        dict_from_ps = ti.read_from_propertyset(ps)
        self.assertDictEqual(expected, dict_from_ps)

    def test_list_to_propertyset(self):
        list_value = [
            'hello',
            {
                'foo': 0,
                'bar': True
            },
            ['hello', 'world']
        ]

        expected = ti.Payload.PropertySet()
        self._add_value(expected, 'hello', ti.DataType.String, 'string_value')
        nested_ps = self._add_propertyset(expected)
        nested_ps.keys.extend(['foo'.encode(), 'bar'.encode()])
        self._add_value(nested_ps, 0, ti.DataType.Int64, 'long_value')
        self._add_value(nested_ps, True, ti.DataType.Boolean, 'boolean_value')
        nested_list = self._add_propertyset(expected)
        self._add_value(nested_list, 'hello', ti.DataType.String, 'string_value')
        self._add_value(nested_list, 'world', ti.DataType.String, 'string_value')

        ps = ti.iterable_to_propertyset(list_value)
        self.assertEqual(expected, ps)

    def test_read_list_from_propertyset(self):
        expected = [
            'hello',
            {
                'foo': 0,
                'bar': True
            },
            ['hello', 'world']
        ]
        ps = ti.iterable_to_propertyset(expected)  # already covered
        list_from_ps = ti.read_from_propertyset(ps)
        self.assertListEqual(expected, list_from_ps)

    def test_read_from_propertysetlist(self):
        expected = [
            {
                'my_int': 42,
                'my_float': 3.14159,
                'my_bool': False,
                'my_string': 'hello, world!',
                'my_dict': {
                    'foo': 0,
                    'bar': True
                }
            },
            [
                'hello',
                {
                    'foo': 0,
                    'bar': True
                },
                ['hello', 'world']
            ]
        ]
        psl = ti.Payload.PropertySetList()
        for py_ps in expected:
            ps = psl.propertyset.add()
            ti.iterable_to_propertyset(py_ps, ps=ps)

        list_from_psl = ti.read_from_propertysetlist(psl)
        self.assertListEqual(expected, list_from_psl)

    def test_property_value_literal(self):
        expected_value = "hello world?"
        expected_type = ti.DataType.String
        expected = ti.Payload.PropertyValue()
        expected.type = expected_type.value
        expected.string_value = expected_value.encode()

        p_value = ti.property_value(expected_value, expected_type)

        self.assertEqual(expected, p_value)

class PropertyDictTester(unittest.TestCase):
    def test_set_raw_scalars(self):
        """Test that PropertyDict sets python scalars correctly in the underlying PropertySet"""
        ps = ti.Payload.PropertySet()
        pdict = ti.PropertyDict(ps)
        expected = {
            'my_int': 42,
            'my_float': 3.14159,
            'my_bool': False,
            'my_string': 'hello, world!'
        }

        for key, value in expected.items():
            pdict[key] = value

        expected_ps = ti.iterable_to_propertyset(expected)
        self.assertEqual(expected_ps, ps)

        # test overwriting values
        pdict['my_bool'] = True
        expected['my_bool'] = True

        expected_ps = ti.iterable_to_propertyset(expected)
        self.assertEqual(expected_ps, ps)


    def test_set_raw_iterables(self):
        """Test that PropertyDict sets python iterables as nested PropertySets in the underlying PropertySet"""
        ps = ti.Payload.PropertySet()
        pdict = ti.PropertyDict(ps)
        expected = {
            'dict': {
                'foo': 0,
                'bar': True,
                'nested_list': ['present day!', 'present time!']
            },
            'list': ['hello', 'world', {'what': 'nested dict'}]
        }

        for key, value in expected.items():
            pdict[key] = value

        expected_ps = ti.iterable_to_propertyset(expected)
        self.assertEqual(expected_ps, ps)

        # test overwriting values
        pdict['list'] = False
        expected['list'] = False

        expected_ps = ti.iterable_to_propertyset(expected)
        self.assertEqual(expected_ps, ps)

    def test_set_propertyvalue(self):
        """Test that PropertyDict sets literal PropertyValues directly in the underlying PropertySet"""
        ps = ti.Payload.PropertySet()
        pdict = ti.PropertyDict(ps)

        expected_value = 99
        expected_type = ti.DataType.Int16

        expected = {
            'literal_value': ti.property_value(expected_value, expected_type)
        }
        pdict['literal_value'] = ti.property_value(expected_value, expected_type)

        expected_ps = ti.iterable_to_propertyset(expected)
        self.assertEqual(expected_ps, ps)
        self.assertEqual(ps.values[0].type, expected_type.value)

    def test_set_propertyset(self):
        """Test that PropertyDict sets literal PropertySets correctly in the underlying PropertySet"""
        ps = ti.Payload.PropertySet()
        pdict = ti.PropertyDict(ps)

        nested_dict = {'key', 'value'}
        expected = {
            'nested dict': ti.iterable_to_propertyset(nested_dict)
        }
        pdict['nested dict'] = ti.iterable_to_propertyset(nested_dict)

        expected_ps = ti.iterable_to_propertyset(expected)
        self.assertEqual(expected_ps, ps)
        self.assertEqual(ps.values[0].type, ti.DataType.PropertySet.value)

    def test_get_scalars(self):
        """Test that scalars are unwrapped from their PropertyValues when accessed from a PropertyDict"""
        expected = {
            'my_int': 42,
            'my_float': 3.14159,
            'my_bool': False,
            'my_string': 'hello, world!'
        }

        ps = ti.iterable_to_propertyset(expected)
        pdict = ti.PropertyDict(ps)
        for key, expected_value in expected.items():
            self.assertEqual(expected_value, pdict[key])

    def test_get_iterable(self):
        """Test that iterables are wrapped in a PropertyDict view when accessed from a PropertyDict"""
        expected = {
            'dict': {
                'foo': 0,
                'bar': True
            },
            'list': ['hello', 'world']
        }

        ps = ti.iterable_to_propertyset(expected)
        pdict = ti.PropertyDict(ps)
        nested_dict = pdict['dict']
        self.assertIsInstance(nested_dict, ti.PropertyDict)
        for key, expected_value in expected['dict'].items():
            self.assertEqual(expected_value, nested_dict[key])

        nested_list = pdict['list']
        self.assertIsInstance(nested_list, ti._PropertyList)
        for idx, expected_value in enumerate(expected['list']):
            self.assertEqual(expected_value, nested_list[idx])

    def test_delete_key(self):
        """Test that keys deleted from a PropertyDict view are reflected in the underlying PropertySet"""
        expected = {
            'a': 8,
            'b': 6,
            'c': 7
        }

        ps = ti.iterable_to_propertyset(expected)
        pdict = ti.PropertyDict(ps)

        del pdict['b']
        self.assertEqual(ps, ti.iterable_to_propertyset({'a': 8, 'c': 7}))
        self.assertNotIn('b', pdict)
        with self.assertRaises(KeyError):
            pdict['b']

        del pdict['c']
        self.assertEqual(ps, ti.iterable_to_propertyset({'a': 8}))
        self.assertNotIn('c', pdict)
        with self.assertRaises(KeyError):
            pdict['c']

        del pdict['a']
        self.assertEqual(ps, ti.Payload.PropertySet())
        self.assertNotIn('a', pdict)
        with self.assertRaises(KeyError):
            pdict['a']

    def test_iter(self):
        """Test that iteration over a PropertyDict view works like iteration over a real dict"""
        expected = {
            'a': 8,
            'b': 6,
            'c': 7
        }

        ps = ti.iterable_to_propertyset(expected)
        pdict = ti.PropertyDict(ps)

        self.assertListEqual(list(expected), list(pdict))

    def test_len(self):
        """Test that the length of a PropertyDict view is reported correctly"""
        ps = ti.Payload.PropertySet()
        pdict = ti.PropertyDict(ps)
        self.assertEqual(len(pdict), 0)

        pdict['a'] = 8
        self.assertEqual(len(pdict), 1)

        pdict['b'] = 6
        self.assertEqual(len(pdict), 2)

        pdict['b'] = 7
        self.assertEqual(len(pdict), 2)

        del pdict['b']
        self.assertEqual(len(pdict), 1)


if __name__ == '__main__':
    unittest.main()
