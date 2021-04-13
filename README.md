# redisent
Introducing ``redisent``, a Python library which leverages Python [dataclasses](https://docs.python.org/3/library/dataclasses.html) along with [redis-py](https://github.com/andymccurdy/redis-py) for persisting and loading data from Redis.

Under the hood, Python [pickle](https://docs.python.org/3/library/pickle.html) library is used to convert the ``dataclass`` field values in ``byte`` values that can be stored directly in Redis.

[![Documentation Status](https://readthedocs.org/projects/redisent/badge/?version=latest)](https://redisent.readthedocs.io/en/latest/?badge=latest)

## Quick Start

First things first, ``redisent`` needs to be installed. Most often this is done by adding ``redisent`` to any ``requirements.txt`` files and using ``pip`` to install it.

### Normal Installation

```shell
$ pip install redisent

Building wheels for collected packages: redisent
  Building wheel for redisent (setup.py) ... done
  Created wheel for redisent: filename=redisent-0.0.1-py3-none-any.whl size=7839 sha256=73f7efc5992183b7586e67e4c82bd38ac764f92cecc6e43cf1aa6a08e08e6db6
  Stored in directory: /private/var/folders/7d/h7_kc94d4wdf1gz35mwfkr1m0000gn/T/pip-ephem-wheel-cache-4ulkjxrs/wheels/ce/bb/a5/0978695fad1b8bc681a2f10e9eedf3cfb317cfc3f4b77d7bde

Successfully built redisent

Installing collected packages: redisent
  Attempting uninstall: redisent
    Found existing installation: redisent 0.0.1
    Uninstalling redisent-0.0.1:
      Successfully uninstalled redisent-0.0.1

Successfully installed redisent-0.0.1 
```

### Development Installation

For bleeding-edge changes and releases, the ``redisent`` package can be installed using ``pip`` with the URL to this [project on GitHub](https://github.com/synistree/redisent):

```shell
$ pip install git+ssh://git@github.com/synistree/redisent
Collecting git+ssh://****@github.com/synistree/redisent
  Cloning ssh://****@github.com/synistree/redisent to /private/var/folders/7d/h7_kc94d4wdf1gz35mwfkr1m0000gn/T/pip-req-build-f50ebj91
  Running command git clone -q 'ssh://****@github.com/synistree/redisent' /private/var/folders/7d/h7_kc94d4wdf1gz35mwfkr1m0000gn/T/pip-req-build-f50ebj91
```

For anyone who might also be making modifications to ``redisent`` as well as using it with another application virtual environment, the ``pip`` argument ``--editable`` should be used with a locally checked out version of ``redisent``:

```shell
$ git clone https://github.com/synistree/redisent ~/code/redisent
$ cd ~/code/myproject
$ source ./activate
[venv] $ pip install --editable ~/code/redisent
```

This will mean any changes to your local ``redisent`` repository will be realized immediately within the ``myproject`` virtualenv.

## Basic Usage

