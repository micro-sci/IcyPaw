This project contains a communication interface between various components of
a laboratory (or really any) network.

Why is it called Icypaw?

Icypaw is a convenient and easy to remember name. This project started
life tentatively titled the Internode Communication Protocol Wrapper
(ICPW or Icypaw).

## Installation

Someday this package might live in PyPI, but for now, you'll need to
build a source distribution yourself if you're developing an
application with Icypaw.

### Building

This project uses the [Eclipse Tahu Sparkplug-B protobuf interface][spb-proto],
which you'll need to build from source after cloning this repo:

```console
$ python3 setup.py build_proto
```

You'll probably want to install the project in editable mode:

```console
$ pip install -e .[complete]
```

If you're using Icypaw in another project, you should build a source
distribution and copy it from `dist`:

```console
$ python3 setup.py sdist
```

[spb-proto]: https://github.com/eclipse/tahu/blob/master/sparkplug_b/sparkplug_b.proto
