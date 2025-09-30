import time
from typing import Optional

from crawl.loader import Loader
from crawl.rate_limiter import RateLimiter


class RateLimitingLoader(Loader):
    def __init__(self, loader: Loader):
        self.loader = loader
        self.rate_limiter = RateLimiter()

    def load(self, url: str) -> tuple[Optional[str], Optional[str]]:
        while not self.rate_limiter.predict(time.time()):
            time.sleep(1)

        content, err = self.loader.load(url)
        self.rate_limiter.add_result(time.time(), err is None)
        return content, err
