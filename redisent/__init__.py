import logging

from typing import TYPE_CHECKING

from redisent.errors import RedisError
from redisent.helpers import RedisentHelper
from redisent.models import RedisEntry


root_logger = logging.getLogger(__name__)
log_ch = logging.StreamHandler()

__version__ = '0.0.2'
LOG_LEVEL = logging.DEBUG


def setup_logger(log_level: int = logging.DEBUG, squelch: bool = False):
    log_fmt = logging.Formatter("[%(asctime)s] %(levelname)s in %(module)s: %(message)s")
    log_ch.setFormatter(log_fmt)
    root_logger.setLevel(log_level)

    if not squelch:
        root_logger.addHandler(log_ch)
        root_logger.debug('Logging started for redisent.')


setup_logger(log_level=LOG_LEVEL, squelch=TYPE_CHECKING)

__all__ = ['RedisError', 'RedisentHelper', 'RedisEntry', 'setup_logger']
