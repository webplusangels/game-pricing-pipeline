from pathlib import Path
from datetime import datetime, timedelta

from util.io_helper import load_json, save_json

class CacheManager:
    def __init__(self, cache_path):
        self.path = Path(cache_path)
        self.cache = load_json(self.path) or {}

    def get(self, key):
        return self.cache.get(str(key), None)

    def set(self, key, value):
        self.cache[str(key)] = value

    def is_stale(self, key, hours):
        entry = self.get(key)
        if not entry or "collected_at" not in entry:
            return True
        try:
            collected_time = datetime.fromisoformat(entry["collected_at"])
            return (datetime.now() - collected_time) > timedelta(hours=hours)
        except:
            return True

    def save(self):
        save_json(self.path, self.cache)

    def record_fail(self, key):
        self.cache[str(key)] = "failed"

    def too_many_fails(self, key, max_attempts=3):
        entry = self.get(key)
        return entry == "failed"