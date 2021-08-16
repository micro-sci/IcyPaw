# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Keep track of idiosyncratic Icypaw & Sparkplug conventions."""

COMMAND_PREFIX = "command"

TEMPLATE_DEFINITION_PREFIX = '_types_'

# This isn't a convention per se as it comes from the spec but it is
# convenient to have it in this module as this module is meant to be
# self-contained.
BDSEQ = 'bdSeq'

def is_metric(metric_name):
    """Return if the given metric name is a generic metric by
    convention."""

    return not is_bdseq(metric_name) and not is_command(metric_name) and \
        not is_template_definition(metric_name)

def is_bdseq(metric_name):
    """Return whether this metric is the BIRTH-DEATH sequence number."""
    return metric_name == BDSEQ

def is_command(metric_name):
    """Return if the given metric name is a command by convention."""

    fields = metric_name.split('/')
    return fields[0].lower() == COMMAND_PREFIX

def make_command(base_name):
    """Return the name of a metric with the convention that it is a
    command."""

    return '/'.join([COMMAND_PREFIX, base_name])

def make_base_name_from_command(metric_name):
    """Given a conventional command name, return the base portion of the
    name."""
    fields = metric_name.split('/')
    assert fields[0] == COMMAND_PREFIX
    return '/'.join(fields[1:])

def is_template_definition(metric_name):
    """Return if the given metric name is a template definition by
    convention."""

    fields = metric_name.split('/')
    return fields[0].lower() == TEMPLATE_DEFINITION_PREFIX

def make_template_definition(base_name):
    """Return the name of a metric with the convention that it is a
    template definition."""

    return '/'.join([TEMPLATE_DEFINITION_PREFIX, base_name])

def make_base_name_from_template_definition(metric_name):
    """Given a conventional template definition name, return the base name
    portion. This base name may still have hierarchical structure, but
    it only has meaning to the user, not as a convention.

    """

    fields = metric_name.split('/')

    assert fields[0] == TEMPLATE_DEFINITION_PREFIX
    return '/'.join(fields[1:])
