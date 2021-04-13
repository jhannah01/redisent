# redisent

Introducing `redisent`, a Python library which leverages Python [dataclasses](https://docs.python.org/3/library/dataclasses.html) along with [redis-py](https://github.com/andymccurdy/redis-py) for persisting and loading data from Redis.

Under the hood, Python [pickle](https://docs.python.org/3/library/pickle.html) library is used to convert the `dataclass` field values in `byte` values that can be stored directly in Redis.

## Build Status

| Branch | Documentation | Build |
|--------|---------------|-------|
| [devel](https://github.com/synistree/redisent/tree/devel) | [![devel Documentation](https://readthedocs.org/projects/redisent/badge/?version=latest)](https://redisent.readthedocs.io/en/latest/?badge=latest) | [![devel](https://travis-ci.com/synistree/redisent.svg?branch=devel)](https://travis-ci.com/synistree/redisent) |
| [master](https://github.com/synistree/redisent/tree/master) | [![stable](https://readthedocs.org/projects/redisent/badge/?version=latest)](https://redisent.readthedocs.io/en/latest/?badge=stable) | [![master](https://travis-ci.com/synistree/redisent.svg?branch=master)](https://travis-ci.com/synistree/redisent)

## Quick Start

First things first, `redisent` needs to be installed. Most often this is done by adding `redisent` to any `requirements.txt` files and using `pip` to install it.

### Normal Installation

For most use-cases, simple use use [pip](https://pip.pypa.io/en/stable/) to install the package from [PyPI](https://pypi.org/).

**NOTE**

As of `Apr 12th 2021`, this package is not yet published on thus `pip install redisent` will fail. Instead use the instructions in the next section to install from Git

```shell
$ pip install redisent

Building wheels for collected packages: redisent
  Building wheel for redisent (setup.py) ... done
  Created wheel for redisent: filename=redisent-0.0.1-py3-none-any.whl size=7839 sha256=73f7efc5992183b7586e67e4c82bd38ac764f92cecc6e43cf1aa6a08e08e6db6
  Stored in directory: /private/var/folders/7d/h7_kc94d4wdf1gz35mwfkr1m0000gn/T/pip-ephem-wheel-cache-4ulkjxrs/wheels/ce/bb/a5/0978695fad1b8bc681a2f10e9eedf3cfb317cfc3f4b77d7bde

Successfully built redisent

Installing collected packages: redisent
  Attempting uninstall: redisent
    Found existing installation: redisent 1.0.3
    Uninstalling redisent-1.0.3:
      Successfully uninstalled redisent-1.0.3

Successfully installed redisent-1.0.3
```

### Install from Git
For any development or testing purposes, installing from Git is a good, simple option:

```shell
$  pip install git+git://github.com/synistree/redisent.git#egg=redisent

Collecting redisent
  Cloning git://github.com/synistree/redisent.git to /tmp/pip-install-055c_6oe/redisent_5c0563b840d845209fbc9ca6d40ce1f2
  Running command git clone -q git://github.com/synistree/redisent.git /tmp/pip-install-055c_6oe/redisent_5c0563b840d845209fbc9ca6d40ce1f2
  Installing build dependencies ... done
  Getting requirements to build wheel ... done
    Preparing wheel metadata ... done

Building wheels for collected packages: redisent
  Building wheel for redisent (PEP 517) ... done
  Created wheel for redisent: filename=redisent-1.0.1-py3-none-any.whl size=11276 sha256=70a1a0fe0313ba57d307ccede65d132d17dee15bb133e94d96e8243d4e14e84a
  Stored in directory: /tmp/pip-ephem-wheel-cache-dihmkpti/wheels/e8/90/68/a3f4e72651fe4e8b00795bd328a5f46127fcb0a88452055fcd
Successfully built redisent
```

#### Development Installation

For bleeding-edge changes and releases, the [devel branch](https://github.com/synistree/redisent/tree/devel) by adding a `@devel` to the end of the repo path in the URI:

```shell
$ pip install git+git://github.com/synistree/redisent.git@devel#egg=redisent
Collecting git+ssh://****@github.com/synistree/redisent
  Cloning ssh://****@github.com/synistree/redisent to /private/var/folders/7d/h7_kc94d4wdf1gz35mwfkr1m0000gn/T/pip-req-build-f50ebj91
  Running command git clone -q 'ssh://****@github.com/synistree/redisent' /private/var/folders/7d/h7_kc94d4wdf1gz35mwfkr1m0000gn/T/pip-req-build-f50ebj91
```

When developing with `redisent`, a local checkout from GitHub is recommented using the `--editable` argument is recommented

```shell

# Checkout repository into "~/code/redisent"
$ cd ~/code
$ git clone git+git://github.com/synistree/redisent.git#egg=redisent --branch master
$ cd redisent

# Create virtualenv (or activate your own)
$ python3.9 -mvenv --prompt 'redisent venv' ~/.myvenv

# Create symlink for activating and load it
$ ln -s ~/.myvenv/bin/activate ./activate
$ source ./activate

# Update pip and install wheel to speed things up a bit
(redisent venv) $ python3 -m pip install -U pip wheel

# Install editable locally checked out repository
(redisent venv) $ pip install --editable ~/code/redisent
```

At this point, [IPython](https://ipython.org/) can be installed and started and the library used directly.

## More Examples

A basic example in the form of a simple reminder entity that can be stored in Redis is used for unit testing. See the [redisent Example documentation](https://redisent.readthedocs.io/en/latest/pages/example.html) for a more indepth explanation with specific examples.
