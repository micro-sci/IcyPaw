#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import io
from setuptools import find_packages, setup

# metadata
NAME = 'icypaw'
DESCRIPTION = "Internode Communication Protocal Wrapper (ICPW) server/client tools"
URL = 'https://gitlab.sandia.gov/iontraps/tahuinterface'
AUTHORS = [
    ('Jay Van Der Wall', 'jayvand@sandia.gov'),
    ('Rob Kelly', 'rpkelly@sandia.gov')
]
CLASSIFIERS = [
    # Trove classifiers
    # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
    'Development Status :: 1 - Planning',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python',
    'Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator',
    'Topic :: Software Development :: Libraries :: Application Frameworks',
    'Topic :: System :: Distributed Computing'
]

# oldest python version supported
REQUIRES_PYTHON = '>=3.6.0'

# dependencies
REQUIRES = [
    'paho-mqtt',
    'protobuf',
    'python-dateutil',
]

# installation dependencies
SETUP = [
    'setuptools_scm',
    'protobuf-setuptools'
]

# optional dependencies
EXTRAS = {
    'lint': ['flake8'],
    'test': [
        'nose',
        'nose-timer',
        'coverage',
        'rednose'
    ]
}

# add `complete' target, which will install all extras listed above
EXTRAS['complete'] = list({pkg for req in EXTRAS.values() for pkg in req})

# use README.md for long description
here = os.path.abspath(os.path.dirname(__file__))
try:
    with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION


# Configure setuptools_scm to build the post-release version number
def my_version():
    from setuptools_scm.version import postrelease_version

    return {'version_scheme': postrelease_version}


# wrap it up
setup(
    name=NAME,
    use_scm_version=my_version,
    description=DESCRIPTION,
    long_description=long_description,
    author=', '.join([name for name, _ in AUTHORS]),
    author_email=', '.join([email for _, email in AUTHORS]),
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(),
    install_requires=REQUIRES,
    setup_requires=SETUP,
    extras_require=EXTRAS,
    include_package_data=True,
    classifiers=CLASSIFIERS
)
