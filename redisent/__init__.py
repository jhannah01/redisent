import logging

log_fmt = logging.Formatter("[%(asctime)s] %(levelname)s in %(module)s: %(message)s")
log_ch = logging.StreamHandler()
log_ch.setFormatter(log_fmt)

root_logger = logging.getLogger(__name__)
root_logger.addHandler(log_ch)

root_logger.setLevel(logging.DEBUG)
root_logger.debug('Logging started for redisent.')