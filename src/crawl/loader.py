from typing import Optional, Protocol


class Loader(Protocol):
    """
    Users should implement this interface for a specific website or a set of homogeneous urls.
    load() should return (content, error) and should not raise exceptions.
    """

    def load(self, url: str) -> tuple[Optional[str], Optional[str]]: ...
