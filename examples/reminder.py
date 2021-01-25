from datetime import datetime
from dataclasses import dataclass, field

from redisent import RedisEntry


@dataclass
class Reminder(RedisEntry):
    member_id: str = field(default_factory=str)
    member_name: str = field(default_factory=str)

    channel_id: str = field(default_factory=str)
    channel_name: str = field(default_factory=str)

    provided_when: str = field(default_factory=str)
    content: str = field(default_factory=str)

    trigger_ts: float = field(default_factory=str)
    created_ts: float = field(default_factory=str)

    is_complete: bool = field(default=False, compare=False)

    @property
    def trigger_dt(self) -> datetime:
        """
        Property wrapper for converting the float "trigger_ts" into "datetime"
        """

        return datetime.fromtimestamp(self.trigger_ts)

    @property
    def created_dt(self) -> datetime:
        """
        Property wrapper for converting the float "created_ts" into "datetime"
        """

        return datetime.fromtimestamp(self.created_ts)

    def __post_init__(self) -> None:
        """
        Set the default "is_complete" field based on current timestamp and
        sets "redis_name" based on member and trigger attributes
        """

        self.redis_name = f'{self.member_id}:{self.trigger_ts}'

        if self.trigger_dt < datetime.now():
            self.is_complete = True
