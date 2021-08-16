This project contains the communication interface between various components of
the ioncontrol system. It is very much a work in progress.

Why is it called Icypaw?

I have worked for the US Government my entire adult life. We code-named
everything. In this case, I started calling this the Internode Communication
Protocol Wrapper (ICPW or Icypaw) so that it would have a name. I'm also partial
to Ion Control Paho Wrapper as a good backronym.

## Installation

Someday this package might live up in the great PyPI in the sky, but for now,
you'll need to [download a release][releases] or build a source distribution
yourself if you're developing an application with Icypaw.

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

[releases]: https://gitlab.sandia.gov/iontraps/tahuinterface/releases
[spb-proto]: https://github.com/eclipse/tahu/blob/master/sparkplug_b/sparkplug_b.proto
