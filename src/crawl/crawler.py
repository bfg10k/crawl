import os
import queue
import sqlite3
import threading
import traceback
from typing import Optional

from crawl.loader import Loader


class Crawler:
    def __init__(self, dbfile: str, loader: Loader, *, concurrency: int = 5):
        self.loader = loader
        self.concurrency = concurrency
        self.queue = queue.Queue(maxsize=1)

        self.db = sqlite3.connect(dbfile, check_same_thread=False, timeout=300)
        self.dblock = threading.Lock()

    def iter_urls(self):
        for (url,) in self.db.execute(
            "select url from page where content is null and err is null"
        ):
            yield url

    def update_page(self, url: str, content: Optional[str], err: Optional[str]):
        with self.dblock:
            self.db.execute(
                "update page set content = ?, err = ? where url = ?",
                (content, err, url),
            )
            self.db.commit()

    def worker(self):
        try:
            while True:
                try:
                    url = self.queue.get()
                except queue.ShutDown:
                    break

                content, err = self.loader.load(url)
                self.update_page(url, content, err)
                self.queue.task_done()
        except Exception:
            print("uncaught exception; exiting")
            print(traceback.format_exc())
            os._exit(1)

    def run(self):
        for _ in range(self.concurrency):
            threading.Thread(target=self.worker, daemon=True).start()

        for url in self.iter_urls():
            self.queue.put(url)
        self.queue.join()
        self.queue.shutdown()
        self.db.close()
