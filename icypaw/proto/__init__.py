# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

""" Compiled protocol buffer interfaces

The contents of this module are compiled from protobuf.
To compile, run `python3 setup.py build_proto`.
"""

# If modules can't be imported, tell the user to recompile them
_IMPORT_ERR_MSG = "(Hint: use `python3 setup.py build_proto` to build or rebuild protobuf modules)"
try:
    from . import sparkplug_b_pb2  # noqa: F401
except ImportError as error:
    # Append hint to error message, in ANSI red
    error.msg = error.msg + f"\n\u001b[31m{_IMPORT_ERR_MSG}\u001b[0m"
    raise error
