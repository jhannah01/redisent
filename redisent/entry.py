import pickle
from typing import Optional, Union, Mapping, Any, List

from dataclasses import dataclass, field, fields, asdict


@dataclass
class RedisEntry:
    entry_key: str = field(metadata={'field': 'redis_id', 'is_redis_attr': True})

    entry_name: Optional[str] = field(default_factory=str, metadata={'field': 'redis_name', 'is_redis_attr': True})
    serialize: bool = field(default=True, metadata={'field': 'serialize', 'is_redis_attr': True})

    def get_entry_fields(self, include_redis_attributes: bool = True) -> List[str]:
        return [fld.name for fld in fields(self) if include_redis_attributes or not fld.metadata.get('is_redis_attr', False)]

    def as_mapping(self, pickle_result: bool = None, include_redis_attributes: bool = True) -> Union[Mapping[str, Any], bytes]:
        entry_dict = asdict(self)

        ent_value = {attr: entry_dict[attr] for attr in self.get_entry_fields(include_redis_attributes=include_redis_attributes)}

        if pickle_result is None:
            pickle_result = self.serialize

        if not pickle_result:
            return ent_value

        try:
            return pickle.dumps(ent_value)
        except Exception as ex:
            raise Exception(f'Error attempting to encode entry dictionary with pickle: {ex}')

    @classmethod
    def from_mapping(cls, entry: Union[Mapping[str, Any], bytes], use_entry_key: str = None, use_entry_name: str = None) -> RedisEntry:
        if isinstance(entry, bytes):
            try:
                entry = pickle.loads(entry)
            except Exception as ex:
                raise Exception(f'Error attempting to load mapping from bytes with pickle: {ex}')

        entry_key = entry.pop('entry_key') if 'entry_key' in entry else use_entry_key
        if 'redis_key' in entry:
            key = entry.pop('redis_key')

        if 'redis_name' in entry:
            name = entry.pop('redis_name')