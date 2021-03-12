from __future__ import annotations

import asyncio
import logging
import pickle

from dataclasses import dataclass, field, fields, asdict
from typing import Mapping, Any, List, Optional, MutableMapping, cast

from redisent import RedisentHelper
from redisent.errors import RedisError

logger = logging.getLogger(__name__)


@dataclass()
class RedisEntry:
    """
    Base dataclass that should be inherited from with additional :py:func:`dataclasses.field` s for each attribute the entry
    will store.

    All ``RedisEntry`` instances must define a unique-to-Redis value for ``redis_id``. If the entry is going to be stored as a hash-map,
    the class must also define a value for ``redis_name``.

    If the ``redis_name`` attribute is set, at a high level, the storing and fetching of values looks like this:

    .. code-block:: ipython

       In [1]: import pickle
          ...: import redis
          ...:
          ...: # Setup some dummy values
          ...: redis_id = 'blah'
          ...: redis_name = 'entry_one'
          ...: ent_values = {'value_one': 1, 'value_two': 2}

       In [2]: # Pickle the dictionary data to bytes
          ...: ent_pickle = pickle.dumps(ent_values)

       In [3]: # Build Redis connection
          ...: conn = redis.StrictRedis('localhost')

       In [4]: # Store them
          ...: conn.hset(redis_id, redis_name, ent_pickle)
       Out[4]: 1

       In [5]: # Fetch back the raw pickled values
          ...: res_raw = conn.hget(redis_id, redis_name)

       In [6]: # Assert they equal the values we set (obviously this should be true)
          ...: assert res_raw == ent_pickle

       In [7]: # Now, decode the response
          ...: res_values = pickle.loads(res_raw)

       In [8]: # And finally assert that "res_values" is the same as the original "ent_values"
          ...: assert res_values == ent_values

       In [9]: res_values
       Out[9]: {'value_one': 1, 'value_two': 2}
    """

    redis_id: str = field(metadata={'redis_field': True})                                   #: Redis ID for this entry
    redis_name: Optional[str] = field(default_factory=str, metadata={'redis_field': True})  #: Optional Redis hashmap name

    def dump(self) -> str:
        """
        Helper for dumping a textual representation of a particular :py:class:`redisent.models.RedisEntry` instance
        """

        dump_out = f'RedisEntry ({type(self).__name__}) for key "{self.redis_id}"'

        if self.redis_name:
            dump_out = f'{dump_out}, hash entry "{self.redis_name}":'

        for attr in self.get_entry_fields(include_redis_fields=False, include_internal_fields=False):
            dump_out = f'{dump_out}\n  => {attr}  \t-> "{getattr(self, attr)}"'

        return dump_out

    @classmethod
    def get_entry_fields(cls, include_redis_fields: bool = False, include_internal_fields: bool = False) -> List[str]:
        """
        Class method used for building a list of strings for each field name, based on the provided filering attributes

        :param include_redis_fields:    if set, include fields with metadata indicating they are Redis-related fields (i.e.
                                        ``redis_id`` or ``redis_name``)
        :param include_internal_fields: if set, include internal fields which are used by ``redisent`` only (any marked
                                        with metadata attribute ``internal_field``)
        """

        flds = []

        for fld in fields(cls):
            is_redis_fld = fld.metadata.get('redis_field', False)
            is_int_fld = fld.metadata.get('internal_field', False)

            if is_redis_fld and not include_redis_fields:
                continue

            if is_int_fld and not include_internal_fields:
                continue

            if not fld.init:
                continue

            flds.append(fld.name)

        return flds

    @property
    def entry_fields(self) -> List[str]:
        """
        Property for returning a list of entry-only related fields

        This method will not include any fields marked with metadata attributes ``redis_field`` or ``internal_field`` and thus
        will only return fields related to this specific entry's dataclass definition
        """

        return self.get_entry_fields(include_redis_fields=False, include_internal_fields=False)

    @property
    def is_hashmap(self) -> bool:
        """
        Property helper to determine if this entry is a hash-map or not

        This is simply determined by if ``redis_name`` is set or not.
        """

        return True if self.redis_name else False

    @classmethod
    def load_dict(cls, redis_id: str, redis_name: str = None, **ent_kwargs) -> RedisEntry:
        """
        Class method for loading a RedisEntry from a provided dictionary of values

        :param redis_id: unique Redis ID for entry
        :param redis_name: unique Redis hashmap name (if entity is stored as a hashmap, this is required)
        :param ent_kwargs: keyword arguments used to build entry values
        """

        if not redis_name:
            if 'redis_name' in ent_kwargs:
                redis_name = ent_kwargs.pop('redis_name')

        ent_fields = cls.get_entry_fields(include_redis_fields=False, include_internal_fields=False)
        cls_kwargs: MutableMapping[str, Any] = {attr: ent_kwargs[attr] for attr in ent_fields if attr in ent_kwargs}

        cls_kwargs['redis_id'] = redis_id
        if redis_name:
            cls_kwargs['redis_name'] = redis_name

        return cls(**cls_kwargs)

    def as_dict(self, include_redis_fields: bool = True, include_internal_fields: bool = False) -> Mapping[str, Any]:
        """
        Return a mapping representing this entry by making use of :py:func:`dataclasses.asdict` along with optionally excluding any
        Redis-related (or internal) fields.

        By default no internal or redis fields (i.e. ``redis_id`` or ``redis_name``) are returned

        :param include_redis_fields:    if set, include fields with metadata indicating they are Redis-related fields (i.e. ``redis_id``
                                        or ``redis_name``)
        :param include_internal_fields: if set, include internal fields which are used by ``redisent`` only (any marked with metadata
                                        attribute ``internal_field``)
        """

        ent_dict = asdict(self)

        if include_redis_fields and include_internal_fields:
            return ent_dict

        flds = self.get_entry_fields(include_redis_fields=include_redis_fields, include_internal_fields=include_internal_fields)
        return {attr: value for attr, value in ent_dict.items() if attr in flds}

    @classmethod
    def decode_entry(cls, entry_bytes, use_redis_id: str = None, use_redis_name: str = None):
        """
        Class method for attempting to build a :py:class:`redisent.models.RedisEntry` instance from the provided ``bytes``
        value ``entry_bytes``

        Under the hood, this makes use of :py:func:`pickle.loads` and :py:class:`redisent.models.RedisEntry.load_dict` to actually
        attempt to build the entry while catching any related exceptions and propagating them as :py:exc:`redisent.errors.RedisError`
        exceptions.
        """

        try:
            ent: MutableMapping[str, Any] = pickle.loads(entry_bytes)

            if isinstance(ent, Mapping):
                redis_id = ent.pop('redis_id', None)
                redis_id = use_redis_id or redis_id

                if not redis_id:
                    raise RedisError('Unable to convert dictionary from Redis into RedisEntry (no value for "redis_id" found)')

                redis_name = ent.pop('redis_name', None)
                redis_name = use_redis_name or redis_name

                return cls.load_dict(redis_id, redis_name=redis_name, **ent)
            elif not isinstance(ent, RedisEntry):
                raise RedisError('Decoded entry is neither a dictionary nor a Mapping')

            return ent
        except RedisError as ex:
            raise ex
        except pickle.PickleError as ex:
            err_message = f'Error decoding entry using pickle: {ex}'
            logger.exception(err_message)
            raise RedisError(err_message, base_exception=ex)
        except Exception as ex:
            err_message = 'General error while attempting to decode possible RedisEntry'
            logger.exception(f'{err_message}: {ex}')
            raise RedisError(err_message, base_exception=ex)

    @classmethod
    def encode_entry(cls, entry: RedisEntry, as_mapping: bool = None) -> bytes:
        """
        Class method for encoding a given :py:class:`redisent.models.RedisEntry` instance as ``bytes`` using
        the :py:func:`pickle.dumps` method.

        :param entry: the :py:class:`redisent.models.RedisEntry` instance to be encoded
        :param as_mapping: if provided, ``entry`` will be treated as a Redis hashmap entry. otherwise, the default behavior
                           is to check :py:attr:`RedisEntry.redis_name`
        """

        if as_mapping is None:
            as_mapping = True if entry.redis_name else False

        try:
            return pickle.dumps(entry.as_dict(include_redis_fields=True, include_internal_fields=False) if as_mapping is True else entry)
        except Exception as ex:
            ent_str = f' (entry name: "{entry.redis_name}")' if entry.redis_name else ''
            raise Exception(f'Error encoding entry for "{entry.redis_id}"{ent_str} using pickle: {ex}')

    def store_sync(self, helper: RedisentHelper) -> bool:
        """
        Blocking / synchronous method for storing this entry in Redis, using the provided :py:class:`redisent.helpers.RedisentHelper` instance.

        This method will do the actual encoding using the :py:func:`RedisEntry.encode_entry` method as well as make use of the provided
        helper :py:func:`redisent.helpers.RedisentHelper.wrapped_redis` (actually, this method makes use of
        the :py:func:`redisent.helpers.RedisentHelper.wrapped_redis_sync`) context manager for storing the entry in Redis.

        .. seealso::
           See also the :py:func:`RedisEntry.store_async` asynchronous method documentation

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used for storing entry
        """

        entry_bytes = self.encode_entry(self)
        op_name = f'set(key="{self.redis_id}")' if not self.redis_name else f'hset(key="{self.redis_id}", name="{self.redis_name}")'

        with helper.wrapped_redis(op_name=op_name) as r_conn:
            if not self.redis_name:
                return True if r_conn.set(self.redis_id, entry_bytes) else False

            return True if r_conn.hset(self.redis_id, self.redis_name, entry_bytes) else False

    async def store_async(self, helper: RedisentHelper) -> bool:
        """
        asyncio / asynchronous method for storing this entry in Redis, using the provided :py:class:`redisent.helpers.RedisentHelper` instance.

        This method will do the actual encoding using the :py:func:`RedisEntry.encode_entry` method as well as make use of the provided
        helper :py:func:`redisent.helpers.RedisentHelper.wrapped_redis` (actually, this method makes use of
        the :py:func:`redisent.helpers.RedisentHelper.wrapped_redis_async`) context manager for storing the entry in Redis.

        .. seealso::
           See also the :py:func:`RedisEntry.store_sync` synchronous method documentation

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used to fetch the entry
        """
        entry_bytes = self.encode_entry(self)
        op_name = f'set(key="{self.redis_id}")' if not self.redis_name else f'hset(key="{self.redis_id}", name="{self.redis_name}")'

        async with helper.wrapped_redis(op_name=op_name) as r_conn:
            if self.redis_name:
                return True if await r_conn.hset(self.redis_id, self.redis_name, entry_bytes) else False

            return True if await r_conn.set(self.redis_id, entry_bytes) else False

    def store(self, helper: RedisentHelper) -> bool:
        """
        A synchronous / asynchronous agnostic wrapper for storing a :py:class:`RedisEntry` instance in Redis

        The corresponding :py:func:`RedisEntry.store_sync` or :py:func:`RedisEntry.store_async` will be called as
        determined be the configured :py:attr:`redisent.helpers.RedisentHelper.redis_pool` type.

        If the helper is async, :py:func:`asyncio.get_event_loop_policy` and :py:func:`asyncio.get_event_loop` methods are used
        to derive or otherwise create a new ``asyncio`` event loop. This event loop will be used to execute the
        :py:func:`RedisEntry.store_async` method.

        .. note::

           There are some caveats with this approach. Namely, if the event loop is stopped or not running, this method might not
           behave quite as expected. Whenever possible, it is best to directly call the correct ``*_sync`` or ``*_async`` method.

        .. seealso::

           The documentation for the synchronous :py:func:`RedisEntry.store_sync` as well as the corresponding asynchronous
           :py:func:`RedisEntry.store_async` method

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used to fetch the entry
        """

        if helper.is_async:
            loop = asyncio.get_event_loop_policy().get_event_loop()
            res = loop.run_until_complete(self.store_async(helper))
            loop.close()
            return res

        return self.store_sync(helper)

    @classmethod
    def fetch_sync(cls, helper: RedisentHelper, redis_id: str, redis_name: str = None) -> RedisEntry:
        """
        Blocking / synchronous method for fetching entries from Redis, using the provided :py:class:`redisent.helpers.RedisentHelper`
        instance.

        This method will do the actual decoding using the :py:func:`RedisEntry.decode_entry` method after fetching the ``bytes`` value
        from Redis using the helper-provided :py:func:`redisent.helpers.RedisentHelper.wrapped_redis` (actually, this method makes use of
        the :py:func:`redisent.helpers.RedisentHelper.wrapped_redis_sync`) context manager for actually fetching from Redis.

        .. seealso::
           See also the :py:func:`RedisEntry.fetch_async` asynchronous method documentation

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used for storing entry
        :param redis_id: unique Redis ID for entry
        :param redis_name: unique Redis hashmap name (if entity is stored as a hashmap, this is required)
        """

        op_name = f'get(key="{redis_id}")' if not redis_name else f'hget(key="{redis_id}", name="{redis_name}")'
        name_str = f' of entry "{redis_name}"' if redis_name else ''

        with helper.wrapped_redis(op_name=op_name) as r_conn:
            entry_bytes = r_conn.get(redis_id) if not redis_name else r_conn.hget(redis_id, redis_name)

        if not entry_bytes:
            raise RedisError(f'Failure during fetch of key "{redis_id}"{name_str}: No data returned')

        return cast(RedisEntry, cls.decode_entry(entry_bytes))

    @classmethod
    async def fetch_async(cls, helper: RedisentHelper, redis_id: str, redis_name: str = None) -> RedisEntry:
        """
        asyncio / asynchronous method for fetching entries from Redis, using the provided :py:class:`redisent.helpers.RedisentHelper`
        instance.

        This method will do the actual decoding using the :py:func:`RedisEntry.decode_entry` method after fetching the ``bytes`` value
        from Redis using the helper-provided :py:func:`redisent.helpers.RedisentHelper.wrapped_redis` (actually, this method makes use of
        the :py:func:`redisent.helpers.RedisentHelper.wrapped_redis_async`) context manager for actually fetching from Redis.

        .. seealso::
           See also the :py:func:`RedisEntry.fetch_sync` synchronous method documentation

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used to fetch the entry
        :param redis_id: unique Redis ID for entry
        :param redis_name: unique Redis hashmap name (if entity is stored as a hashmap, this is required)
        """

        op_name = f'get(key="{redis_id}")' if not redis_name else f'hget(key="{redis_id}", name="{redis_name}")'
        name_str = f' of entry "{redis_name}"' if redis_name else ''

        async with helper.wrapped_redis(op_name=op_name) as r_conn:
            entry_bytes = await (r_conn.get(redis_id) if not redis_name else r_conn.hget(redis_id, redis_name))

        if not entry_bytes:
            raise RedisError(f'Failure during fetch of key "{redis_id}"{name_str}: No data returned')

        return cast(RedisEntry, cls.decode_entry(entry_bytes))

    @classmethod
    def fetch(cls, helper: RedisentHelper, redis_id: str, redis_name: str = None) -> RedisEntry:
        """
        A synchronous / asynchronous agnostic wrapper for fetching entries from redis, using the provided
        :py:class:`redisent.helpers.RedisentHelper`

        The corresponding :py:func:`RedisEntry.fetch_sync` or :py:func:`RedisEntry.fetch_async` will be called as
        determined be the configured :py:attr:`redisent.helpers.RedisentHelper.redis_pool` type.

        If the helper is async, :py:func:`asyncio.get_event_loop_policy` and :py:func:`asyncio.get_event_loop` methods are used
        to derive or otherwise create a new ``asyncio`` event loop. This event loop will be used to execute the
        :py:func:`RedisEntry.fetch_async` method.

        .. note::

           There are some caveats with this approach. Namely, if the event loop is stopped or not running, this method might not
           behave quite as expected. Whenever possible, it is best to directly call the correct ``*_sync`` or ``*_async`` method.

        .. seealso::

           The documentation for the synchronous :py:func:`RedisEntry.fetch_sync` as well as the corresponding asynchronous
           :py:func:`RedisEntry.fetch_async` method

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used to fetch the entry
        :param redis_id: unique Redis ID for entry
        :param redis_name: unique Redis hashmap name (if entity is stored as a hashmap, this is required)
        """

        if helper.is_async:
            loop = asyncio.get_event_loop_policy().get_event_loop()
            res = loop.run_until_complete(cls.fetch_async(helper, redis_id, redis_name=redis_name))
            loop.close()
            return res

        return cls.fetch_sync(helper, redis_id, redis_name=redis_name)

    def delete_sync(self, helper: RedisentHelper, check_exists: bool = True) -> bool:
        """
        Synchronous method responsible for actually deleting a RedisEntry from Redis

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used to delete the entry
        :param check_exists: if set, check first that there is an existing Redis entry for this instance
        """

        if check_exists:
            if not helper.exists_sync(helper, self.redis_id, redis_name=self.redis_name):
                redis_key = '"{self.redis_id}"'
                if self.redis_name:
                    redis_key = f'{redis_key} (redis_name: "{self.redis_name}")'

                logger.warning(f'Request to delete entry {redis_key} failed: No such entry in Redis')
                return False

        op_name = f'hdel("{self.redis_id}", "{self.redis_name}")' if self.redis_name else f'delete("{self.redis_id}")'
        with helper.wrapped_redis(op_name) as r_conn:
            res = r_conn.hdel(self.redis_id, self.redis_name) if self.redis_name else r_conn.delete(self.redis_id)
            return True if res else False

    async def delete_async(self, helper: RedisentHelper, check_exists: bool = True) -> bool:
        """
        Asynchronous method responsible for actually deleting a RedisEntry from Redis

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used to delete the entry
        :param check_exists: if set, check first that there is an existing Redis entry for this instance
        """

        if check_exists:
            if not await helper.exists_async(helper, self.redis_id, redis_name=self.redis_name):
                redis_key = '"{self.redis_id}"'
                if self.redis_name:
                    redis_key = f'{redis_key} (redis_name: "{self.redis_name}")'

                logger.warning(f'Request to delete entry {redis_key} failed: No such entry in Redis')
                return False

        op_name = f'hdel("{self.redis_id}", "{self.redis_name}")' if self.redis_name else f'delete("{self.redis_id}")'
        async with helper.wrapped_redis(op_name) as r_conn:
            if self.redis_name:
                return True if await r_conn.hdel(self.redis_id, self.redis_name) else False

            return True if await r_conn.delete(self.redis_id) else False

    def delete(self, helper: RedisentHelper, check_exists: bool = True) -> bool:
        if helper.is_async:
            loop = asyncio.get_event_loop_policy().get_event_loop()
            res = loop.run_until_complete(self.delete_async(helper, check_exists=check_exists))
            loop.close()
            return res

        return self.delete_sync(helper, check_exists=check_exists)
