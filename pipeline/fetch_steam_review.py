import requests
import time
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path
from requests.exceptions import HTTPError, Timeout, ConnectionError

from util.io_helper import load_json, save_json, save_csv  
from util.logger import setup_logger
from util.retry import retry_on_exception

class SteamReviewFetcher:
    def __init__(self, 
                 output_dir="./data/raw", 
                 cache_dir="./data/cache",
                 error_dir="./data/error",
                 log_dir='./log/fetcher',
                 max_retries=3, 
                 thread_workers=2):
        # ------------------------- 설정 -------------------------
        self.MAX_RETRIES = max_retries
        self.THREAD_WORKERS = thread_workers
        self.OUTPUT_DIR = Path(output_dir)
        self.OUTPUT_DIR.mkdir(exist_ok=True)
        self.CACHE_DIR = Path(cache_dir)
        self.CACHE_DIR.mkdir(exist_ok=True)
        self.ERROR_DIR = Path(error_dir)
        self.ERROR_DIR.mkdir(exist_ok=True)
        self.LOG_DIR = Path(log_dir)
        self.LOG_DIR.mkdir(exist_ok=True)
        
        # 로깅 설정
        self.logger = setup_logger(
            name="steam_review_fetcher", 
            log_dir=self.LOG_DIR,
        )
        
        # 파일 경로 설정
        self.CACHE_FILE = self.CACHE_DIR / "review_status_cache.json"
        self.FAILED_IDS_FILE = self.ERROR_DIR / "failed_review_ids.csv"
        self.reviews_df_path = self.OUTPUT_DIR / "steam_game_reviews.csv"
        
        # 헤더 설정
        self.HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        
        # ------------------------- 데이터 저장소 -------------------------
        self.reviews_data = []
        self.failed_list = []
        self.errored_list = []
        self.status_cache = self._load_cache()
        
    def _load_cache(self):
        """캐시 파일에서 상태 정보 로드"""
        try:
            cache = load_json(self.CACHE_FILE)
            if not isinstance(cache, dict):
                self.logger.warning("⚠️ 캐시 파일이 비정상적입니다. 빈 캐시로 초기화합니다.")
                return {}
            # 중복 키 제거 (마지막 값 유지)
            deduplicated_cache = {}
            for key, value in cache.items():
                deduplicated_cache[key] = value
            
            # 로깅 추가: 중복 제거된 키의 수 확인
            original_count = len(cache)
            deduplicated_count = len(deduplicated_cache)
            
            if original_count != deduplicated_count:
                self.logger.info(f"🔍 캐시에서 {original_count - deduplicated_count}개의 중복 키가 제거되었습니다.")
            
            return deduplicated_cache
        
        except Exception as e:
            self.logger.warning(f"⚠️ 캐시 파일 로드 실패: {e}. 빈 캐시로 초기화합니다.")
            return {}
        
    @retry_on_exception(max_retries=2, 
                    exceptions=(Timeout, ConnectionError, HTTPError), 
                    logger=None)
    def fetch_review_data(self, app_id):
        """Steam API에서 리뷰 데이터 가져오기"""
        if app_id in self.status_cache and self.status_cache[app_id] == "success":
            return
        
        url = f"https://store.steampowered.com/appreviews/{app_id}?json=1&filter=all&language=all&day_range=all&review_type=all&purchase_type=all"
        
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            response_json = response.json()

            if response_json.get("success") != 1:
                self.failed_list.append(app_id)
                self.status_cache[app_id] = "failed"
                return
            
            query_summary = response_json.get("query_summary", {})
            if query_summary:
                review_data = {
                    "appid": app_id,
                    "num_reviews": query_summary.get("num_reviews", 0),
                    "review_score": query_summary.get("review_score", 0),
                    "total_reviews": query_summary.get("total_reviews", 0)
                }
                self.reviews_data.append(review_data)
                self.status_cache[app_id] = {
                    "status": "success",
                    "collected_at": datetime.now().isoformat()
                }
            else:
                self.failed_list.append(app_id)
                self.status_cache[app_id] = "no_data"
                
        except Timeout:
            self.errored_list.append(app_id)
            self.status_cache[app_id] = "timeout"
            self.logger.warning(f"[{app_id}] 요청 타임아웃")
            raise
            
        except ConnectionError as e:
            self.errored_list.append(app_id)
            self.status_cache[app_id] = "connection_error"
            self.logger.error(f"[{app_id}] 연결 오류: {e}")
            raise
            
        except HTTPError as e:
            self.errored_list.append(app_id)
            status_code = getattr(e.response, 'status_code', None)
            self.status_cache[app_id] = f"http_error_{status_code}"
            
            if status_code == 429:  # Rate limit
                self.logger.warning(f"[{app_id}] 요청 제한 감지")
            raise
                
        except Exception as e:
            self.errored_list.append(app_id)
            self.status_cache[app_id] = "error"
            self.logger.error(f"[{app_id}] 에러 발생: {e}")
            raise
        
    def save_checkpoint(self):
        """현재 수집 상태 저장"""
        try:
            # 리뷰 데이터 저장
            new_reviews_df = pd.DataFrame(self.reviews_data)
            
            # 기존 CSV 로드 및 병합
            if self.reviews_df_path.exists():
                old_reviews_df = pd.read_csv(self.reviews_df_path)
                merged_reviews_df = pd.concat([old_reviews_df, new_reviews_df], ignore_index=True)
                merged_reviews_df.drop_duplicates(subset="appid", inplace=True)
            else:
                merged_reviews_df = new_reviews_df
                
            # 상태 캐시의 중복 제거
            clean_status_cache = {}
            for key, value in self.status_cache.items():
                clean_status_cache[key] = value
            
            # 저장 (io_helper의 save_csv, save_json 활용)
            save_csv(merged_reviews_df, self.reviews_df_path)
            save_json(self.CACHE_FILE, clean_status_cache)
            
            # 진행 상황 출력
            total = len(self.reviews_data) + len(self.failed_list) + len(self.errored_list)
            success_rate = len(self.reviews_data) / total * 100 if total > 0 else 0
            self.logger.info(f"💾 중간 저장 완료 - 누적 수집: {len(merged_reviews_df)}개")
            self.logger.info(f"진행 상황: {len(self.reviews_data)}개 성공 / {len(self.failed_list)}개 실패 / {len(self.errored_list)}개 오류 (성공률: {success_rate:.1f}%)")
            
            # 저장 후 데이터 초기화 (메모리 관리)
            self.reviews_data = []
            
        except Exception as e:
            self.logger.error(f"체크포인트 저장 중 오류: {e}")
    
    def fetch_in_parallel(self, app_ids, batch_size=40):
        """배치 단위로 병렬 처리"""
        for i in range(0, len(app_ids), batch_size):
            batch = app_ids[i:i+batch_size]
            self.logger.info(f"배치 처리 중: {i+1}-{i+len(batch)}/{len(app_ids)}")
            
            with ThreadPoolExecutor(max_workers=self.THREAD_WORKERS) as executor:
                # 요청 제출 시 약간의 지연 추가
                futures = []
                for app_id in batch:
                    futures.append(executor.submit(self.fetch_review_data, app_id))
                    time.sleep(0.8)  # 요청 간 800ms 지연
                
                for future in tqdm(as_completed(futures), total=len(batch), desc="📦 Fetching"):
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"스레드 실행 중 오류: {e}")
            
            # 각 배치 후 저장
            self.save_checkpoint()
    
    def retry_loop(self, target_list, label, max_retries=3):
        """실패한 요청 재시도"""
        for i in range(max_retries):
            if not target_list:
                break
                
            self.logger.info(f"🔁 {label} 재시도 {i+1}회 - 대상 {len(target_list)}개")
            
            retry_targets = list(set(target_list))
            target_list.clear()
            
            # 재시도는 더 적은 배치 크기로 처리
            self.fetch_in_parallel(retry_targets, batch_size=20)
            time.sleep(2)  # 재시도 사이 더 긴 대기 시간
    
    def get_collected_appids(self):
        """이미 수집된 ID 목록 가져오기"""
        collected_appids = set()
        if self.reviews_df_path.exists():
            try:
                existing_df = pd.read_csv(self.reviews_df_path)
                collected_appids.update(existing_df["appid"].tolist())
            except Exception as e:
                self.logger.warning(f"⚠️ 기존 CSV 로드 실패: {e}")
        return collected_appids
    
    def run(self, input_csv_path):
        """전체 데이터 수집 프로세스 실행"""
        # 이미 수집된 ID 확인
        collected_appids = self.get_collected_appids()
        
        # ID 목록 불러오기 및 필터링
        ids_list = pd.read_csv(input_csv_path)["appid"].tolist()
        ids_list = [appid for appid in ids_list if appid not in collected_appids]
        self.logger.info(f"📋 새로 수집할 appid 수: {len(ids_list)}")
        
        # 1차 수집
        self.fetch_in_parallel(ids_list)
        
        # 실패한 ID 가져오기
        failed_ids_from_cache = [
            int(app_id) for app_id, status in self.status_cache.items()
            if status == "failed"
        ]
        self.logger.info(f"캐시에서 실패한 ID 수: {len(failed_ids_from_cache)}")

        # 실패한 ID 파일에서 추가 실패 ID 가져오기
        if self.FAILED_IDS_FILE.exists():
            failed_ids_from_file = pd.read_csv(self.FAILED_IDS_FILE)["id"].tolist()
        else:
            failed_ids_from_file = []
        self.logger.info(f"파일에서 실패한 ID 수: {len(failed_ids_from_file)}")
        
        # 실패한 ID 합치기 (중복 제거)
        retry_ids = set(failed_ids_from_cache + failed_ids_from_file)
        
        # 이미 성공한 ID는 제외
        retry_ids = [appid for appid in retry_ids if self.status_cache.get(appid) != "success"]
        retry_ids = list(set(retry_ids))
        self.logger.info(f"재시도할 ID 수: {len(retry_ids)}")
        
        # 재시도 루프
        retry_stages = [
            {"label": "에러", "targets": self.errored_list, "max_retries": 3},
            {"label": "실패", "targets": self.failed_list, "max_retries": 2},
            {"label": "최종 에러", "targets": self.errored_list, "max_retries": 1},
            {"label": "최종 실패", "targets": self.failed_list, "max_retries": 1},
            {"label": "마지막", "targets": retry_ids, "max_retries": 1},
        ]
        
        for stage in retry_stages:
            target_list = stage["targets"]
            label = stage["label"]
            max_retries = stage["max_retries"]
            
            # 중복 제거
            target_list[:] = list(set(target_list))
            
            if target_list:
                self.retry_loop(target_list, label, max_retries)
        
        # 최종 결과 출력
        self.logger.info("✅ 데이터 수집 완료")
        
        # 총 수집 결과 확인
        if self.reviews_df_path.exists():
            final_df = pd.read_csv(self.reviews_df_path)
            self.logger.info(f"🎯 총 수집된 게임 리뷰 수: {len(final_df)}")
        
        self.logger.info(f"❌ 최종 실패: {len(self.failed_list)}개, 에러: {len(self.errored_list)}개")
        
        # 실패한 ID 목록 저장
        if self.failed_list or self.errored_list:
            failed_df = pd.DataFrame({
                "id": self.failed_list + self.errored_list,
                "status": ["failed"] * len(self.failed_list) + ["error"] * len(self.errored_list)
            })
            save_csv(failed_df, self.FAILED_IDS_FILE)
            self.logger.info(f"❗ 실패한 ID 목록이 {self.FAILED_IDS_FILE}에 저장되었습니다.")
