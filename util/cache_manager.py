from pathlib import Path
from datetime import datetime, timedelta, timezone

from util.io_helper import load_json, save_json

class CacheManager:
    def __init__(self, cache_path):
        self.path = Path(cache_path)
        self.cache = load_json(self.path) or {}

    def get(self, key):
        return self.cache.get(str(key), None)

    def set(self, key, value):
        if isinstance(value, dict):
            value.setdefault("collected_at", datetime.now(timezone.utc).isoformat())
        self.cache[str(key)] = value
        
    def items(self):
        return self.cache.items()
    
    def values(self):
        return self.cache.values()

    def keys(self):
        return self.cache.keys()

    def is_stale(self, key, hours):
        entry = self.get(key)
        if not entry or "collected_at" not in entry:
            return True
        try:
            collected_time = datetime.fromisoformat(entry["collected_at"])
            if collected_time.tzinfo is None:
                collected_time = collected_time.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            return (now - collected_time) > timedelta(hours=hours)
        except Exception as e:
            print(f"⚠️ collected_at 파싱 실패: {entry.get('collected_at')} / error: {e}")
            return True

    def save(self):
        save_json(self.path, self.cache)

    def record_fail(self, key):
        entry = self.get(key) or {}
        fail_count = entry.get("fail_count", 0) + 1
        
        self.cache[str(key)] = {
            "status": "failed",
            "fail_count": fail_count,
            "collected_at": datetime.now(timezone.utc).isoformat()
            }

    def too_many_fails(self, key, max_attempts=3):
        entry = self.get(key)
        return (
            isinstance(entry, dict) and 
            entry.get("status") == "failed" and 
            entry.get("fail_count", 0) >= max_attempts
        )