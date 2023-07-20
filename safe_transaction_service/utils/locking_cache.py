import copyreg
import pickle
from contextlib import AbstractContextManager
from typing import Any, Optional

from django.conf import settings

from redis.exceptions import LockError
from rest_framework.request import Request

from .redis import get_redis


class LockingCache(AbstractContextManager):
    """
    Cache locks so computation inside is guaranteed to only be done once
    """

    CACHE_TIME = 15 * 60  # 15 minutes

    def __init__(
        self,
        request: Request,
        cache_value: str,
        lock_timeout: int = settings.DB_STATEMENT_TIMEOUT // 1000,
    ):
        """
        :param request:
        :param cache_value: Changing value to cache based on that (for example count of elements)
        :param lock_timeout: How long to lock. By default, we use the database timeout
        """
        self.redis = get_redis()
        # TODO Sort and validate query strings
        path_with_query_string = request.get_full_path()
        self.cache_value = cache_value
        self.lock_timeout = lock_timeout
        self.locking_key = f"cache-locks:{path_with_query_string}:{self.cache_value}"
        self.cache_key = f"cache-key:{path_with_query_string}:{self.cache_value}"

        # Encode memoryview for redis
        copyreg.pickle(memoryview, lambda val: (memoryview, (bytes(val),)))

    def get_from_cache(self) -> Optional[Any]:
        result = self.redis.get(self.cache_key)
        if result:
            return pickle.loads(result)
        return None

    def store_in_cache(self, data: Any):
        result = self.redis.set(self.cache_key, data, ex=self.CACHE_TIME)
        if result:
            return pickle.loads(result)
        return None

    def __enter__(self):
        # If value is already in cache, return it
        if result := self.get_from_cache():
            return result
        try:
            # Make sure computation is only done once. Sleep 2 seconds between every new check if locked
            with self.redis.lock(
                self.locking_key,
                sleep=2,
                blocking_timeout=True,
                timeout=self.lock_timeout,
            ) as lock:
                if result := self.get_from_cache():
                    return result
                # Perform real calculation
                result = None
        except LockError:
            if result := self.get_from_cache():
                return result

    def __exit__(self, *exc_details):
        pass
