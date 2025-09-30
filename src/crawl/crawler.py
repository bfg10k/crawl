import os
import queue
import sqlite3
import threading
import traceback
from typing import Optional

from crawl.loader import Loader


def iter_urls(db):
    for (url,) in db.execute(
        "select url from page where content is null and err is null"
    ):
        yield url


def update_page(db, url: str, content: Optional[str], err: Optional[str]):
    db.execute(
        "update page set content = ?, err = ? where url = ?",
        (content, err, url),
    )
    db.commit()


class Crawler:
    def __init__(self, dbfile: str, loader: Loader, *, concurrency: int = 5):
        self.loader = loader
        self.concurrency = concurrency
        self.dbfile = dbfile
        self.queue = queue.Queue(maxsize=1)

    def worker(self):
        try:
            while True:
                try:
                    url = self.queue.get()
                except queue.ShutDown:
                    break

                content, err = self.loader.load(url)
                if err is not None:
                    print(url, "ERROR", err)
                else:
                    print(url, "OK")
                update_page(self.db, url, content, err)
                self.queue.task_done()
        except Exception:
            print("uncaught exception; exiting")
            print(traceback.format_exc())
            os._exit(1)

    def run(self):
        self.db = sqlite3.connect(self.dbfile, check_same_thread=False)
        for _ in range(self.concurrency):
            threading.Thread(target=self.worker, daemon=True).start()

        for url in iter_urls(self.db):
            self.queue.put(url)
        self.queue.join()
        self.queue.shutdown()
        self.db.close()
