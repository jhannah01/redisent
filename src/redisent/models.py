from __future__ import annotations

import logging
import pickle

from dataclasses import dataclass, field, fields, asdict, Field
from tabulate import tabulate
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

    def dump(self, include_redis_fields: bool = True) -> str:
        """
        Helper for dumping a textual representation of a particular :py:class:`redisent.models.RedisEntry` instance
        """

        dump_out = f'RedisEntry ({type(self).__name__}) for key "{self.redis_id}"'

        if self.redis_name:
            dump_out = f'{dump_out}, hash entry "{self.redis_name}":'

        entry_attrs = self.get_entry_fields(include_redis_fields=include_redis_fields, include_internal_fields=False)
        entry_data = [[attr, getattr(self, attr)] for attr in entry_attrs]
        tbl = tabulate(entry_data, headers=['Attribute', 'Value'], tablefmt='presto')

        dump_out += f'\n\n{tbl}'

        return dump_out

    @classmethod
    def get_entry_fields(cls, include_redis_fields: bool = False, include_internal_fields: bool = False) -> Mapping[str, Field]:
        """
        Class method used for building a list of strings for each field name, based on the provided filering attributes

        :param include_redis_fields: if set, include fields with metadata indicating they are Redis-related fields (i.e.
                                     ``redis_id`` or ``redis_name``)
        :param include_internal_fields: if set, include internal fields which are used by ``redisent`` only (any marked
                                        with metadata attribute ``internal_field``)
        """

        flds = {}

        for fld in fields(cls):
            is_redis_fld = fld.metadata.get('redis_field', False)
            is_int_fld = fld.metadata.get('internal_field', False)

            if is_redis_fld and not include_redis_fields:
                continue

            if is_int_fld and not include_internal_fields:
                continue

            if not fld.init:
                continue

            flds[fld.name] = fld

        return flds

    @property
    def entry_fields(self) -> List[str]:
        """
        Property for returning a list of entry-only related fields

        This method will not include any fields marked with metadata attributes ``redis_field`` or ``internal_field`` and thus
        will only return fields related to this specific entry's dataclass definition
        """

        return list(self.get_entry_fields(include_redis_fields=False, include_internal_fields=False).keys())

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

    def store(self, helper: RedisentHelper) -> bool:
        """
        Blocking / synchronous method for storing this entry in Redis, using the provided :py:class:`redisent.helpers.RedisentHelper` instance.

        This method will do the actual encoding using the :py:func:`RedisEntry.encode_entry` method as well as make use of the provided
        helper :py:func:`redisent.helpers.RedisentHelper.wrapped_redis` (actually, this method makes use of
        the :py:func:`redisent.helpers.RedisentHelper.wrapped_redis`) context manager for storing the entry in Redis.

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used for storing entry
        """

        entry_bytes = self.encode_entry(self)
        return helper.set(self.redis_id, entry_bytes, redis_name=self.redis_name)

    @classmethod
    def fetch_all(cls, helper: RedisentHelper, redis_id: str, check_exists: bool = True) -> Mapping[str, RedisEntry]:
        """
        Method for fetching **all** entries for a given hashmap from Redis, using the provided :py:class:`redisent.helpers.RedisentHelper`
        instance.

        Under the hood, this method will call the :py:func:`redisent.helpers.RedisentHelper.keys` method to enumerate all of the hashmap entry keys and
        iteratively fetch them and return a mapping of hash keys to ``RedisEntry`` instances.

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used for fetching entries
        :param redis_id: unique Redis ID for hash map entries
        :param check_exists: if set, check first that there is an existing Redis entry to fetch. If not, a :py:exc:`RedisEntry` is raised
        """

        if check_exists and not helper.exists(redis_id):
            raise RedisError(f'Failed to find entry for "{redis_id}" in Redis while fetching all instances')

        ent_keys = helper.keys(redis_id=redis_id)
        return {e_key: cls.fetch(helper, redis_id=redis_id, redis_name=e_key) for e_key in ent_keys}

    @classmethod
    def fetch(cls, helper: RedisentHelper, redis_id: str, redis_name: str = None, check_exists: bool = True) -> RedisEntry:
        """
        Method for fetching entries from Redis, using the provided :py:class:`redisent.helpers.RedisentHelper`
        instance.

        This method will do the actual decoding using the :py:func:`RedisEntry.decode_entry` method after fetching the ``bytes`` value
        from Redis using the helper-provided :py:func:`redisent.helpers.RedisentHelper.wrapped_redis` (actually, this method makes use of
        the :py:func:`redisent.helpers.RedisentHelper.wrapped_redis`) context manager for actually fetching from Redis.

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used for storing entry
        :param redis_id: unique Redis ID for entry
        :param redis_name: unique Redis hashmap name (if entity is stored as a hashmap, this is required)
        :param check_exists: if set, check first that there is an existing Redis entry to fetch. If not, a :py:exc:`RedisEntry` is raised
        """

        red_ent = f'hash entry for "{redis_name}" in "{redis_id}"' if redis_name else f'entry "{redis_id}"'

        if check_exists and not helper.exists(redis_id, redis_name=redis_name):
            raise RedisError(f'Failed to find {red_ent} was not found in Redis')

        entry_bytes = helper.get(redis_id, redis_name=redis_name)

        if not entry_bytes:
            raise RedisError(f'Failure during fetch of {red_ent}: No data returned')

        return cast(RedisEntry, cls.decode_entry(entry_bytes))

    def delete(self, helper: RedisentHelper, check_exists: bool = True) -> Optional[bool]:
        """
        Method responsible for actually deleting a RedisEntry from Redis

        :param helper: configured instance of :py:class:`redisent.helpers.RedisentHelper` to be used to delete the entry
        :param check_exists: if set, check first that there is an existing Redis entry for this instance
        """

        return helper.delete(self.redis_id, redis_name=self.redis_name, check_exists=check_exists)
