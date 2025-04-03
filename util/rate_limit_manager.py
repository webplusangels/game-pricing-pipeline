from datetime import datetime
import time
import logging

class RateLimitManager:
    def __init__(self, window_seconds=60, threshold=5, initial_backoff=5, max_backoff=150):
        self.window_seconds = window_seconds  # 측정 윈도우(초)
        self.threshold = threshold  # 임계값
        self.initial_backoff = initial_backoff  # 초기 백오프 시간(초)
        self.max_backoff = max_backoff  # 최대 백오프 시간(초)
        self.timestamps = []  # 요청 제한 발생 시간
        self.logger = logging.getLogger(__name__)
        
    def record_rate_limit(self):
        """요청 제한 발생 기록"""
        now = datetime.now()
        self.timestamps.append(now)
        # 윈도우 밖의 타임스탬프 제거
        self.timestamps = [ts for ts in self.timestamps 
                         if (now - ts).total_seconds() < self.window_seconds]
        return len(self.timestamps)
        
    def get_backoff_time(self):
        """현재 상황에 맞는 백오프 시간 계산"""
        count = len(self.timestamps)
        if count >= self.threshold:
            return self.max_backoff
        return min(self.initial_backoff * (2 ** (count - 1)), self.max_backoff)
        
    def should_slow_down(self):
        """요청 속도를 줄여야 하는지 확인"""
        return len(self.timestamps) > 0
        
    def get_current_delay(self, base_delay):
        """현재 상황에 맞는 요청 지연 시간 계산"""
        count = len(self.timestamps)
        if count == 0:
            return base_delay
        return min(base_delay + count * 0.3, 3.0)  # 최대 5초
        
    def handle_rate_limit(self, app_id=None):
        """요청 제한 발생 시 처리 및 대기"""
        count = self.record_rate_limit()
        backoff = self.get_backoff_time()
        
        if app_id:
            self.logger.warning(f"[{app_id}] 요청 제한 감지 ({count}번째)")
        else:
            self.logger.warning(f"요청 제한 감지 ({count}번째)")
            
        if count >= self.threshold:
            self.logger.warning(f"⚠️ 과도한 요청 제한 감지! {backoff}초 동안 대기합니다.")
        else:
            self.logger.info(f"🕒 요청 제한으로 {backoff}초 대기 중...")
            
        time.sleep(backoff)
        return backoff