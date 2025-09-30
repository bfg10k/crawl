from typing import Optional, Protocol


class Loader(Protocol):
    def load(self, url: str) -> tuple[Optional[str], Optional[str]]: ...
