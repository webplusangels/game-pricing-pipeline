import time
from functools import wraps

def retry_on_exception(max_retries=3, delay=1, exceptions=(Exception,), logger=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            instance = args[0]  # self 인스턴스를 가져옴
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        # 최종 시도에서는 원래의 예외 처리 로직을 유지
                        raise
                    
                    if logger:
                        logger.warning(f"[Retry {attempt}/{max_retries}] {func.__name__} 실패: {e}")
                    elif hasattr(instance, 'logger'):
                        instance.logger.warning(f"[Retry {attempt}/{max_retries}] {func.__name__} 실패: {e}")
                    else:
                        print(f"[Retry {attempt}/{max_retries}] {func.__name__} 실패: {e}")
                    
                    time.sleep(delay * attempt)
                    
            raise Exception(f"[최대 재시도 초과] {func.__name__} 실패")
        return wrapper
    return decorator

# from utils.retry import retry_on_exception  # 상단 import

# # ...
# @retry_on_exception(max_retries=3, delay=2, exceptions=(requests.exceptions.RequestException,), logger=logging)
# def fetch_review_data(self, app_id):