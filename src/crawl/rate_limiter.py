import threading
from typing import NamedTuple

from sklearn import tree


class Result(NamedTuple):
    ts: int
    succ: bool
    stats: list[float]


class RateLimiter:
    """
    A rate limiter that captures request results (success or failure) with timestamps.

    Internally maintains statistics (total requests, successes, and success rates)
    across multiple time windows: 1 hour, 10 minutes, 1 minute, and 10 seconds.

    Uses these statistics to predict the likelihood of success for future requests.

    The mutate methods are thread-safe.
    """

    def __init__(self):
        self.results = []
        self.lock = threading.Lock()
        self.last_pred_true = None

    def add_result(self, now: int, succ: bool):
        with self.lock:
            stats = self.get_stats(now)
            self.results.append(Result(now, int(succ), stats))

    def get_stats(self, now: int):
        windows = [3600, 600, 60, 10]

        # remove old results
        # FIX: results should probably stay longer than the stats time window
        while len(self.results) > 0 and self.results[0].ts < now - windows[0]:
            self.results.pop(0)

        # 1h, 10m, 1m, 10s
        totals = [0] * 4
        succs = [0] * 4
        for ts, succ, _ in self.results:
            for i, window in enumerate(windows):
                if ts >= now - window:
                    totals[i] += 1
                    if succ:
                        succs[i] += 1
        rates = [
            succs[i] / totals[i] if totals[i] > 0 else 0 for i in range(len(totals))
        ]
        return totals + succs + rates

    def _build_tree(self):
        x = [r.stats for r in self.results]
        y = [r.succ for r in self.results]
        clf = tree.DecisionTreeClassifier()
        clf = clf.fit(x, y)
        return clf

    def predict(self, now: int):
        with self.lock:
            if len(self.results) == 0:
                result = True
            elif self.last_pred_true is not None and now - self.last_pred_true > 60:
                # wait for up to 60 seconds
                result = True
            else:
                stats = self.get_stats(now)
                # FIX: do not rebuild the tree too often
                tree = self._build_tree()
                result = tree.predict([stats])[0]

            if result:
                self.last_pred_true = now
            return result
