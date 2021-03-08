import logging

from redis.client import PubSubWorkerThread, PubSub  # type: ignore[attr-defined]
from typing import Callable, MutableMapping, List, Mapping, Optional

from redisent.helpers import RedisentHelper

logger = logging.getLogger(__name__)


class RedisPubSub:
    helper: RedisentHelper

    pub_subs: List[PubSub] = []
    threads: MutableMapping[PubSub, PubSubWorkerThread] = {}

    @property
    def all_patterns(self) -> Mapping[str, PubSub]:
        pattern_map = {}

        for ps in self.pub_subs:
            pattern_map.update({pattern: ps for pattern in ps.patterns})

        return pattern_map

    def __init__(self, helper: RedisentHelper) -> None:
        self.helper = helper

    def subscribe(self, pattern: str, event_handler: Callable[[str], None], use_pubsub: PubSub = None,
                  start_thread: bool = True, sleep_time: float = 0.001) -> PubSub:
        ps_patterns = self.all_patterns

        if pattern in self.all_patterns:
            logger.info(f'Received request to subscribe to "{pattern}" which is already registered')
            return ps_patterns[pattern]

        if not use_pubsub:
            with self.helper.wrapped_redis('pubsub()') as r_conn:
                pubsub: PubSub = r_conn.pubsub()

        else:
            pubsub = use_pubsub

        pubsub.psubscribe(**{pattern: event_handler})

        if pubsub not in self.pub_subs:
            self.pub_subs.append(pubsub)

        if start_thread:
            thread = pubsub.run_in_thread(sleep_time=sleep_time)
            self.threads[pubsub] = thread

        return pubsub

    def start_thread(self, pubsub: PubSub, sleep_time: float = 0.001) -> PubSubWorkerThread:
        if pubsub in self.threads:
            thread = self.threads[pubsub]
            if thread.is_alive():
                logger.warning(f'Received request to start pubsub thread which is already running for "{pubsub.patterns}"')
                return thread

            logger.info(f'Received request to start pubsub thread which already is registered for "{pubsub.patterns}" but not running')
            thread.start()
            return thread

        thread = pubsub.run_in_thread(sleep_time=sleep_time)
        self.threads[pubsub] = thread
        return thread

    def stop_thread(self, pubsub: PubSub) -> bool:
        thread = self.threads.get(pubsub, None)
        if not thread:
            logger.warning(f'Received request to stop pubsub thread for "{pubsub.patterns}" which is not registered')
            return False

        if not thread.is_alive():
            logger.info(f'Received request to stop pubsub thread for "{pubsub.patterns}" which is not running')

        return True

    def get_thread(self, pubsub: PubSub) -> Optional[PubSub]:
        return self.threads.get(pubsub, None)