import logging

from typing import TYPE_CHECKING

from redisent.errors import RedisError
from redisent.redis import RedisHelper
from redisent.utils import REDIS_URL, wrapped_redis
from redisent.models import RedisEntry, Reminder


root_logger = logging.getLogger(__name__)
log_ch = logging.StreamHandler()

__version__ = '0.0.1'


def setup_logger():
    log_fmt = logging.Formatter("[%(asctime)s] %(levelname)s in %(module)s: %(message)s")
    log_ch.setFormatter(log_fmt)

    root_logger.addHandler(log_ch)
    root_logger.setLevel(logging.DEBUG)
    root_logger.debug('Logging started for redisent.')


if not TYPE_CHECKING:
    setup_logger()
