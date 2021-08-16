# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

""" Dynamic package versioning using setuptools_scm """

__version__ = "unknown"

try:
    from pkg_resources import get_distribution, DistributionNotFound
    __version__ = get_distribution('icypaw').version
except DistributionNotFound:
    # package not installed
    try:
        from inspect import getfile, currentframe
        this_file = getfile(currentframe())
        from os.path import abspath
        from setuptools_scm import get_version

        __version__ = get_version(root='..', relative_to=abspath(this_file))
    except Exception:
        pass
