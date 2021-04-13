import logging
import os

from typing import TYPE_CHECKING

from redisent.errors import RedisError
from redisent.helpers import RedisentHelper
from redisent.models import RedisEntry
from redisent.common import RedisPrimitiveType, RedisType, RedisPoolType

root_logger = logging.getLogger(__name__)
log_ch = logging.StreamHandler()

__version__ = '1.0.5'
LOG_LEVEL = logging.DEBUG if os.environ.get('REDISENT_DEBUG', False) else logging.CRITICAL


def setup_logger(log_level: int = logging.DEBUG, squelch: bool = False):
    log_fmt = logging.Formatter("[%(asctime)s] %(levelname)s in %(module)s: %(message)s")
    log_ch.setFormatter(log_fmt)
    root_logger.setLevel(log_level)

    if not squelch:
        root_logger.addHandler(log_ch)
        root_logger.debug('Logging started for redisent.')


if os.environ.get('REDISENT_LOGGING', True):
    setup_logger(log_level=LOG_LEVEL, squelch=TYPE_CHECKING)

__all__ = ['RedisError', 'RedisentHelper', 'RedisEntry', 'RedisPrimitiveType', 'RedisType', 'RedisPoolType', 'setup_logger']
