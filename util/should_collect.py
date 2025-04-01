from datetime import datetime, timedelta

def should_collect(entry, days=1):
    try:
        if not isinstance(entry, dict) or "collected_at" not in entry:
            return True  # 수집 기록 없음 → 수집 대상
        collected_time = datetime.fromisoformat(entry["collected_at"])
        return datetime.now() - collected_time > timedelta(days=days)
    except Exception:
        return True  # malformed → 수집