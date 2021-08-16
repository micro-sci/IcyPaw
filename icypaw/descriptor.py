# Copyright 2021 National Technology & Engineering Solutions of
# Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
# NTESS, the U.S. Government retains certain rights in this software.

"""Module with common functions to all descriptor modules."""

def get_object(inst, name, exp_type):
    """Extract a descriptor object from an instance, bypassing the normal
    descriptor protocol."""

    for typ in inst.__class__.__mro__:
        try:
            ret = typ.__dict__[name]
            break
        except Exception:
            pass
    else:
        # TODO: Descriptor objects cannot actually be stored in
        # instances, so this code should just go away.
        if name in inst.__dict__:
            ret = inst.__dict__[name]
        else:
            raise ValueError(f"Could not find {name} in {inst}")

    if not isinstance(ret, exp_type):
        if name in inst.__dict__:
            ret = inst.__dict__[name]
        else:
            raise TypeError(f"Requested an object which is not {exp_type.__name__}")

    return ret

def iter_objects(inst, exp_type):
    """Iterate over all descriptor objects accessible from inst. Yields
    (name, obj) tuples."""

    seen = set()

    # Note: The visibility rules here aren't exactly right, but I
    # think it's fine for our purposes. The difference only comes up
    # if the user is dynamically adding things to instances.

    yield from iter_objects_from_type(inst.__class__, exp_type, seen)
    yield from iter_objects_from_instance(inst, exp_type, seen)

def iter_objects_from_type(cls, exp_type, seen=None):
    """Iterate over objects of the given type visible at the class level
    that have not yet been seen."""

    if seen is None:
        seen = set()

    for typ in cls.__mro__:
        for name, obj in typ.__dict__.items():
            if isinstance(obj, exp_type) and name not in seen:
                yield name, obj
                seen.add(name)


def iter_objects_from_instance(inst, exp_type, seen=None):
    """Iterate over objects of the given type visible at the instance
    level that have not yet been seen."""

    if seen is None:
        seen = set()

    for name, obj in inst.__dict__.items():
        if isinstance(obj, exp_type) and name not in seen:
            yield name, obj
            seen.add(name)
