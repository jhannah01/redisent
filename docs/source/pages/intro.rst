Introduction to redisent
========================

This document will introduce the ``redisent`` Redis library which provides high-level serialization of Python objects to and from Redis.

There are two primary components to the ``redisent`` library:

- :py:class:`~redisent.helpers.RedisentHelper`:

  This class provides the underlying Redis client implementation and, wherever possible, attempts to be agnostic about if the specific instance is being called using ``asyncio`` or not. When ``use_async=True`` is passed to this helper class, the ``aioredis`` package will act as the Redis client. Otherwise the standard ``redis-py`` library is used.


- :py:class:`~redisent.models.RedisEntry`:

  By leveraging `Python dataclasses <https://docs.python.org/3/library/dataclasses.html>`_, this base entity should be subclassed and provide further ``field()`` entries for each attribute that should be mapped to Redis. The object itself will be encoded using ``pickle`` so there is often no need for specific implementations if the ``pickle`` library can encoe the object.
