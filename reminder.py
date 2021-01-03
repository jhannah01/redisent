from datetime import datetime
from dataclasses import dataclass, field

from redisent import RedisEntry, RedisentHelper


@dataclass
class Reminder(RedisEntry):
    member_id: str = field(default_factory=str)
    member_name: str = field(default_factory=str)
    channel_id: str = field(default_factory=str)
    channel_name: str = field(default_factory=str)
    provided_when: str = field(default_factory=str)
    trigger_ts: float = field(default_factory=str)
    created_ts: float = field(default_factory=str)
    content: str = field(default_factory=str)
    is_complete: bool = field(default=False)

    @property
    def trigger_dt(self):
        return datetime.fromtimestamp(self.trigger_ts)

    @property
    def created_dt(self):
        return datetime.fromtimestamp(self.created_ts)

    def __post_init__(self):
        dt_now = datetime.now()
        if self.trigger_dt < dt_now:
            self.is_complete = True


rh = RedisentHelper.build('rpi04.synistree.com', use_async=False)
rh_async = RedisentHelper.build('rpi04.synistree.com', use_async=True)

rem = Reminder.fetch(rh, 'reminders', '483182150066110464:1608511502.456233')
