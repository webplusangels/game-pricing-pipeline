from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

class BaseFetcher(ABC):
    def __init__(self, cache, logger, max_retries=3, thread_workers=2):
        self.cache = cache
        self.logger = logger
        self.MAX_RETRIES = max_retries
        self.THREAD_WORKERS = thread_workers
        self.failed_list = []
        self.errored_list = []

    @abstractmethod
    def fetch_single(self, id_):
        pass

    def fetch_batch(self, ids, batch_size=40):
        for i in range(0, len(ids), batch_size):
            batch = ids[i:i + batch_size]
            self.logger.info(f"🔄 배치 처리 중: {i+1}-{i+len(batch)}/{len(ids)}")

            with ThreadPoolExecutor(max_workers=self.THREAD_WORKERS) as executor:
                futures = []
                for id_ in batch:
                    if self.cache.get(id_) == "success":
                        continue
                    futures.append(executor.submit(self._safe_fetch, id_))
                    time.sleep(0.8)

                for _ in tqdm(as_completed(futures), total=len(futures), desc="📦 Fetching"):
                    pass

    def _safe_fetch(self, id_):
        try:
            self.fetch_single(id_)
            self.cache.set(id_, {"status": "success"})
        except Exception as e:
            self.logger.error(f"[{id_}] 에러 발생: {e}")
            self.errored_list.append(id_)
            self.cache.set(id_, {"status": "error"})

    def retry_loop(self, target_list, label, max_retries=3):
        for i in range(max_retries):
            if not target_list:
                break
            self.logger.info(f"🔁 {label} 재시도 {i+1}회")
            retry_targets = list(set(target_list))
            target_list.clear()
            self.fetch_batch(retry_targets, batch_size=20)
            time.sleep(2)