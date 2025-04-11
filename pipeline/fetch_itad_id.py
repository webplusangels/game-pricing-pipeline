import requests
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from pathlib import Path
from requests.exceptions import HTTPError, Timeout, ConnectionError
from datetime import datetime
from config import settings

from util.io_helper import save_csv, load_csv, upload_to_s3, download_from_s3
from util.cache_manager import CacheManager
from util.logger import setup_logger
from util.rate_limit_manager import RateLimitManager

class ITADIdFetcher:
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
            name="itad_id_fetcher", 
            log_dir=self.LOG_DIR,
        )

        # 요청 제어기 설정
        self.rate_limit_manager = RateLimitManager()
        
        # 파일 경로 설정
        self.CACHE_FILE = self.CACHE_DIR / "itad_id_status_cache.json"
        self.FAILED_IDS_FILE = self.ERROR_DIR / "itad_failed_ids.csv"
        self.OUTPUT_FILE = self.OUTPUT_DIR / "itad_game_ids.csv"
        
        # API 설정
        self.ID_BASE_URL = "https://api.isthereanydeal.com/games/lookup/v1"
        self.KEY = settings.ITAD_KEY
                
        # S3 업로드 설정
        self.S3_CACHE_KEY = "data/cache/itad_id_status_cache.json"
        self.S3_OUTPUT_KEY = "data/raw/itad_game_ids.csv"
        if self.CACHE_FILE.exists():
            self.logger.info("📁 로컬 캐시 사용")
        elif download_from_s3(self.S3_CACHE_KEY, self.CACHE_FILE):
            self.logger.info("✅ S3 캐시 다운로드 성공")
        else:
            self.logger.warning("❗ 캐시 파일 없음. 빈 캐시로 초기화됩니다.")

        # 헤더 설정
        self.HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        
        # ------------------------- 데이터 저장소 -------------------------
        self.fetched_data = []
        self.failed_list = []
        self.errored_list = []
        self.cache = CacheManager(self.CACHE_FILE)
        
    def fetch_game_info(self, row):
        """ITAD API에서 게임 ID 가져오기"""
        app_id = row['appid']
        name = row.get('name', 'Unknown')
        cached = self.cache.get(app_id)
        
        if cached and cached.get("status") == "success":
            return False
        if self.cache.too_many_fails(app_id):
            self.logger.info(f"🚫 앱 {app_id}은 실패가 누적되어 건너뜁니다.")
            return False
            
        url = f"{self.ID_BASE_URL}?key={self.KEY}&appid={app_id}"
        
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # 응답 데이터 확인
            if data is None:
                self.failed_list.append(app_id)
                self.cache.record_fail(app_id)
                self.logger.warning(f"[{app_id}] 빈 응답")
                return False
                
            if "found" not in data:
                self.failed_list.append(app_id)
                self.cache.record_fail(app_id)
                self.logger.warning(f"[{app_id}] 'found' 키가 없음: {data}")
                return False
                
            # 데이터 처리
            if data["found"] == True and "game" in data and "id" in data["game"]:
                itad_id = data["game"]["id"]
                processed_data = {
                    "appid": app_id,
                    "name": name,
                    "itad_id": itad_id,
                    "collected_at": datetime.now().isoformat()
                }
                
                self.fetched_data.append(processed_data)
                self.cache.set(app_id, {
                    "status": "success",
                })
                self.logger.info(f"[{app_id}] 성공적으로 ITAD ID 찾음: {itad_id}")
                return True
            else:
                # found가 False인 경우 또는 game/id 키가 없는 경우
                processed_data = {
                    "appid": app_id,
                    "name": name,
                    "itad_id": None,
                    "collected_at": datetime.now().isoformat()
                }
                
                self.fetched_data.append(processed_data)
                self.cache.record_fail(app_id)
                self.logger.info(f"[{app_id}] 게임을 ITAD에서 찾을 수 없음")
                return False
                
        except Timeout:
            self.errored_list.append(app_id)
            self.cache.record_fail(app_id)
            self.logger.warning(f"[{app_id}] 요청 타임아웃")
            raise
            
        except ConnectionError as e:
            self.errored_list.append(app_id)
            self.cache.record_fail(app_id)
            self.logger.error(f"[{app_id}] 연결 오류: {e}")
            raise
            
        except HTTPError as e:
            self.errored_list.append(app_id)
            status_code = getattr(e.response, 'status_code', None)
            self.cache.set(app_id, f"http_error_{status_code}")
            
            if status_code == 429:  # Rate limit
                self.logger.warning(f"[{app_id}] 요청 제한 감지")
                self.rate_limit_manager.handle_rate_limit(app_id)
            raise
                
        except Exception as e:
            self.errored_list.append(app_id)
            self.cache.record_fail(app_id)
            self.logger.error(f"[{app_id}] 에러 발생: {str(e)}")
            raise
    
    def save_checkpoint(self):
        """현재 수집 상태 저장"""
        try:
            # id 데이터 저장
            new_data_df = pd.DataFrame(self.fetched_data)
            
            # 기존 CSV 로드 및 병합
            if self.OUTPUT_FILE.exists():
                old_data_df = load_csv(self.OUTPUT_FILE)
                merged_df = pd.concat([old_data_df, new_data_df], ignore_index=True)
                merged_df.drop_duplicates(subset="appid", inplace=True)
            else:
                merged_df = new_data_df
            
            # 저장
            save_csv(merged_df, self.OUTPUT_FILE)
            self.cache.save()       

            # 저장 후 데이터 초기화 (메모리 관리)
            self.fetched_data = []
            
        except Exception as e:
            self.logger.error(f"체크포인트 저장 중 오류: {e}")
    
    def fetch_in_parallel(self, app_ids, batch_size=40):
        """배치 단위로 병렬 처리"""
        base_delay = 0.5
        
        for i in range(0, len(app_ids), batch_size):
            batch = app_ids[i:i+batch_size]
            self.logger.info(f"배치 처리 중: {i+1}-{i+len(batch)}/{len(app_ids)}")
            
            # 요청 제한 상황에 따라 지연 시간 조정
            request_delay = self.rate_limit_manager.get_current_delay(base_delay)
            if self.rate_limit_manager.should_slow_down():
                self.logger.info(f"⚠️ 요청 제한 감지로 지연 시간 증가: {request_delay:.2f}초")
            
            with ThreadPoolExecutor(max_workers=self.THREAD_WORKERS) as executor:
                # 요청 제출 시 약간의 지연 추가
                futures = []
                for app_id in batch:
                    futures.append((app_id, executor.submit(self.fetch_game_info, app_id)))

                for app_id, future in tqdm(futures, desc="📦 Fetching"):
                    try:
                        should_sleep = future.result()
                        if should_sleep:
                            time.sleep(base_delay)
                    except Exception as e:
                        self.logger.error(f"스레드 실행 중 오류: {e}")
            
            # 각 배치 후 저장
            self.save_checkpoint()
    
    def retry_loop(self, target_list, label, input_df, max_retries=3):
        """실패한 요청 재시도"""
        for i in range(max_retries):
            if not target_list:
                break
                
            self.logger.info(f"🔁 {label} 재시도 {i+1}회 - 대상 {len(target_list)}개")
            
            retry_targets = list(set(target_list.copy()))
            target_list.clear()
            
            # 재시도할 행 추출
            retry_rows = input_df[input_df['appid'].isin(retry_targets)].to_dict('records')
            
            # 재시도는 더 적은 배치 크기로 처리
            self.fetch_in_parallel(retry_rows, batch_size=20)
            time.sleep(2)  # 재시도 사이 더 긴 대기 시간
    
    def get_collected_appids(self):
        """이미 수집된 ID 목록 가져오기"""
        collected_appids = set()
        if self.OUTPUT_FILE.exists():
            try:
                existing_df = load_csv(self.OUTPUT_FILE)
                collected_appids.update(existing_df["appid"].tolist())
            except Exception as e:
                self.logger.warning(f"⚠️ 기존 CSV 로드 실패: {e}")
        return collected_appids
    
    def run(self, input_csv_path, id_column="appid"):
        """전체 데이터 수집 프로세스 실행"""
        try:
            # 입력 데이터에서 ID 목록 로드
            df_input = load_csv(input_csv_path)
            
            # 필수 칼럼 확인
            if id_column not in df_input.columns:
                raise ValueError(f"입력 CSV 파일에 '{id_column}' 칼럼이 없습니다.")
            
            # appid 칼럼이 없으면 id_column으로 복사
            if 'appid' not in df_input.columns and id_column != 'appid':
                df_input['appid'] = df_input[id_column]
            
            # name 칼럼이 없으면 추가
            if 'name' not in df_input.columns:
                df_input['name'] = 'Unknown'
            
            # 이미 수집된 ID 제외
            collected_ids = self.get_collected_appids()
            df_to_collect = df_input[~df_input['appid'].isin(collected_ids)]

            # 수집 대상 appid 리스트 추출
            rows_to_collect = df_to_collect.to_dict("records")
            
            # 1차 수집
            self.fetch_in_parallel(rows_to_collect)
            
            # 실패한 ID 가져오기
            failed_ids_from_cache = [
                int(app_id) for app_id, status in self.cache.items()
                if status == "failed"
            ]
            self.logger.info(f"캐시에서 실패한 ID 수: {len(failed_ids_from_cache)}")
    
            # 실패한 ID 파일에서 추가 실패 ID 가져오기
            if self.FAILED_IDS_FILE.exists():
                failed_ids_from_file = load_csv(self.FAILED_IDS_FILE)["appid"].tolist()
            else:
                failed_ids_from_file = []
            self.logger.info(f"파일에서 실패한 ID 수: {len(failed_ids_from_file)}")
            
            # 실패한 ID 합치기 (중복 제거)
            retry_ids = set(failed_ids_from_cache + failed_ids_from_file)
            
            # 이미 성공한 ID는 제외
            retry_ids = [appid for appid in retry_ids if self.cache.get(appid) != "success"]
            retry_ids = list(set(retry_ids))
            self.logger.info(f"재시도할 ID 수: {len(retry_ids)}")
                
            # 재시도 루프
            retry_stages = [
                {"label": "에러", "targets": self.errored_list, "max_retries": 3},
                {"label": "실패", "targets": self.failed_list, "max_retries": 2},
                {"label": "최종 에러", "targets": self.errored_list, "max_retries": 1},
                {"label": "최종 실패", "targets": self.failed_list, "max_retries": 1},
                {"label": "마지막", "targets": retry_ids, "max_retries": 1}
            ]
            
            for stage in retry_stages:
                target_list = stage["targets"]
                label = stage["label"]
                max_retries = stage["max_retries"]
                
                # 중복 제거
                target_list[:] = list(set(target_list))
                
                if target_list:
                    self.retry_loop(target_list, label, df_input, max_retries)
            
            # 최종 결과 출력
            self.logger.info("\n✅ 데이터 수집 완료")
            
            # 총 수집 결과 확인
            if self.OUTPUT_FILE.exists():
                final_df = load_csv(self.OUTPUT_FILE)
                found_count = final_df['itad_id'].notna().sum()
                self.logger.info(f"🎯 총 수집된 게임 수: {len(final_df)}")
                self.logger.info(f"🔍 ITAD에서 찾은 게임 수: {found_count} (전체 중 {found_count/len(final_df)*100:.1f}%)")
            
            self.logger.info(f"❌ 최종 실패: {len(self.failed_list)}개, 에러: {len(self.errored_list)}개")
            
            # 실패한 ID 목록 저장
            if self.failed_list or self.errored_list:
                failed_df = pd.DataFrame({
                    "appid": self.failed_list + self.errored_list,
                    "status": ["failed"] * len(self.failed_list) + ["error"] * len(self.errored_list)
                })
                save_csv(failed_df, self.ERROR_DIR / "itad_failed_ids.csv")
                self.logger.info(f"❗ 실패한 ID 목록이 itad_failed_ids.csv에 저장되었습니다.")
                
            # S3 업로드
            upload_to_s3(self.OUTPUT_FILE, self.S3_OUTPUT_KEY)
            upload_to_s3(self.CACHE_FILE, self.S3_CACHE_KEY)
            self.logger.info(f"✅ S3에 업로드 완료: {self.S3_OUTPUT_KEY}, {self.S3_CACHE_KEY}")
            
        except Exception as e:
            self.logger.error(f"프로세스 실행 중 오류: {e}")

# # 사용 예시
# if __name__ == "__main__":
#     fetcher = ITADIdFetcher()
#     fetcher.run(settings.PAID_LIST_DIR)
