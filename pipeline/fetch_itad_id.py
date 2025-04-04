import requests
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path
from requests.exceptions import HTTPError, Timeout, ConnectionError
from datetime import datetime
from config import settings

from util.io_helper import load_json, save_json, save_csv  
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
        self.data_df_path = self.OUTPUT_DIR / "itad_game_ids.csv"
        
        # API 설정
        self.ID_BASE_URL = "https://api.isthereanydeal.com/games/lookup/v1"
        self.KEY = settings.ITAD_KEY
        
        # 헤더 설정
        self.HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        
        # ------------------------- 데이터 저장소 -------------------------
        self.fetched_data = []
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
        
    def fetch_game_info(self, row):
        """ITAD API에서 게임 정보 가져오기"""
        app_id = row['appid']
        name = row.get('name', 'Unknown')
        
        if str(app_id) in self.status_cache and self.status_cache[str(app_id)] == "success":
            self.logger.info(f"[{app_id}] 이미 수집된 데이터, 건너뜀")
            return
            
        url = f"{self.ID_BASE_URL}?key={self.KEY}&appid={app_id}"
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # 응답 데이터 디버그 로깅
            self.logger.debug(f"[{app_id}] 응답 데이터: {data}")
            
            # 응답 데이터 확인
            if data is None:
                self.failed_list.append(app_id)
                self.status_cache[str(app_id)] = "failed_empty_response"
                self.logger.warning(f"[{app_id}] 빈 응답")
                return
                
            if "found" not in data:
                self.failed_list.append(app_id)
                self.status_cache[str(app_id)] = "failed_no_found_key"
                self.logger.warning(f"[{app_id}] 'found' 키가 없음: {data}")
                return
                
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
                self.status_cache[str(app_id)] = "success"
                self.logger.info(f"[{app_id}] 성공적으로 ITAD ID 찾음: {itad_id}")
            else:
                # found가 False인 경우 또는 game/id 키가 없는 경우
                processed_data = {
                    "appid": app_id,
                    "name": name,
                    "itad_id": None,
                    "collected_at": datetime.now().isoformat()
                }
                
                self.fetched_data.append(processed_data)
                self.status_cache[str(app_id)] = "not_found"
                self.logger.info(f"[{app_id}] 게임을 ITAD에서 찾을 수 없음")
                
        except Timeout:
            self.errored_list.append(app_id)
            self.status_cache[str(app_id)] = "timeout"
            self.logger.warning(f"[{app_id}] 요청 타임아웃")
            raise
            
        except ConnectionError as e:
            self.errored_list.append(app_id)
            self.status_cache[str(app_id)] = "connection_error"
            self.logger.error(f"[{app_id}] 연결 오류: {e}")
            raise
            
        except HTTPError as e:
            self.errored_list.append(app_id)
            status_code = getattr(e.response, 'status_code', None)
            self.status_cache[str(app_id)] = f"http_error_{status_code}"
            
            if status_code == 429:  # Rate limit
                self.logger.warning(f"[{app_id}] 요청 제한 감지")
                self.rate_limit_manager.handle_rate_limit(app_id)
            raise
                
        except Exception as e:
            self.errored_list.append(app_id)
            self.status_cache[str(app_id)] = "error"
            self.logger.error(f"[{app_id}] 에러 발생: {str(e)}")
            raise
    
    def save_checkpoint(self):
        """현재 수집 상태 저장"""
        try:
            # 새 데이터를 DataFrame으로 변환
            if self.fetched_data:
                new_data_df = pd.DataFrame(self.fetched_data)
                
                # 기존 CSV 로드 및 병합
                if self.data_df_path.exists():
                    old_data_df = pd.read_csv(self.data_df_path)
                    merged_df = pd.concat([old_data_df, new_data_df], ignore_index=True)
                    merged_df.drop_duplicates(subset="appid", inplace=True)
                    save_csv(merged_df, self.data_df_path)
                else:
                    save_csv(new_data_df, self.data_df_path)
            
            # 상태 캐시의 중복 제거
            clean_status_cache = {}
            for key, value in self.status_cache.items():
                clean_status_cache[key] = value
                
            # 캐시 저장
            save_json(self.CACHE_FILE, clean_status_cache)
            
            # 진행 상황 출력
            total = len(self.fetched_data) + len(self.failed_list) + len(self.errored_list)
            success_rate = len(self.fetched_data) / total * 100 if total > 0 else 0
            
            # 총 수집된 데이터 수 확인
            total_collected = 0
            if self.data_df_path.exists():
                total_collected = len(pd.read_csv(self.data_df_path))
                
            print(f"💾 중간 저장 완료 - 누적 수집: {total_collected}개")
            print(f"진행 상황: {len(self.fetched_data)}개 성공 / {len(self.failed_list)}개 실패 / {len(self.errored_list)}개 오류 (성공률: {success_rate:.1f}%)")
            
            # 저장 후 데이터 초기화 (메모리 관리)
            self.fetched_data = []
            
        except Exception as e:
            self.logger.error(f"체크포인트 저장 중 오류: {e}")
            print(f"❌ 체크포인트 저장 중 오류: {e}")
    
    def fetch_in_parallel(self, rows, batch_size=40):
        """배치 단위로 병렬 처리"""
        base_delay = 0.5
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            self.logger.info(f"배치 처리 중: {i+1}-{i+len(batch)}/{len(rows)}")
            print(f"배치 처리 중: {i+1}-{i+len(batch)}/{len(rows)}")
            
            # 요청 제한 상황에 따라 지연 시간 조정
            request_delay = self.rate_limit_manager.get_current_delay(base_delay)
            if self.rate_limit_manager.should_slow_down():
                self.logger.info(f"⚠️ 요청 제한 감지로 지연 시간 증가: {request_delay:.2f}초")
            
            with ThreadPoolExecutor(max_workers=self.THREAD_WORKERS) as executor:
                # 요청 제출 시 약간의 지연 추가
                futures = []
                for row in batch:
                    futures.append(executor.submit(self.fetch_game_info, row))
                    time.sleep(request_delay)  # 요청 간 지연 (API 제한 방지)
                
                for future in tqdm(as_completed(futures), total=len(batch), desc="📦 Fetching"):
                    try:
                        future.result()
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
            print(f"🔁 {label} 재시도 {i+1}회...")
            
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
        if self.data_df_path.exists():
            try:
                existing_df = pd.read_csv(self.data_df_path)
                collected_appids.update(existing_df["appid"].tolist())
            except Exception as e:
                self.logger.warning(f"⚠️ 기존 CSV 로드 실패: {e}")
                print(f"⚠️ 기존 CSV 로드 실패: {e}")
        return collected_appids
    
    def run(self, input_csv_path, id_column="appid"):
        """전체 데이터 수집 프로세스 실행"""
        try:
            # 입력 데이터에서 ID 목록 로드
            df_input = pd.read_csv(input_csv_path)
            
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
            
            # 이미 상태 캐시에 있는 성공한 ID 제외
            success_ids = [k for k, v in self.status_cache.items() if v == "success"]
            df_to_collect = df_to_collect[~df_to_collect['appid'].astype(str).isin(success_ids)]
            
            rows_to_collect = df_to_collect.to_dict('records')
            
            self.logger.info(f"📋 총 {len(df_input)}개 게임 중 {len(rows_to_collect)}개 게임 수집 예정")
            print(f"📋 총 {len(df_input)}개 게임 중 {len(rows_to_collect)}개 게임 수집 예정")
            
            if not rows_to_collect:
                self.logger.info("모든 게임이 이미 수집되었습니다.")
                print("모든 게임이 이미 수집되었습니다.")
                return
                
            # 1차 수집
            self.fetch_in_parallel(rows_to_collect)
            
            # 실패한 ID 가져오기
            failed_ids_from_cache = [
                int(app_id) for app_id, status in self.status_cache.items()
                if isinstance(status, str) and "failed" in status
            ]
            self.logger.info(f"캐시에서 실패한 ID 수: {len(failed_ids_from_cache)}")
    
            # 실패한 ID 파일에서 추가 실패 ID 가져오기
            if self.FAILED_IDS_FILE.exists():
                failed_ids_from_file = pd.read_csv(self.FAILED_IDS_FILE)["appid"].tolist()
            else:
                failed_ids_from_file = []
            self.logger.info(f"파일에서 실패한 ID 수: {len(failed_ids_from_file)}")
            
            # 실패한 ID 합치기 (중복 제거)
            retry_ids = set(failed_ids_from_cache + failed_ids_from_file)
            
            # 재시도 루프
            retry_stages = [
                {"label": "에러", "targets": self.errored_list, "max_retries": 3},
                {"label": "실패", "targets": self.failed_list, "max_retries": 2},
                {"label": "최종 에러", "targets": self.errored_list, "max_retries": 1},
                {"label": "최종 실패", "targets": self.failed_list, "max_retries": 1},
                {"label": "마지막", "targets": list(retry_ids), "max_retries": 1}
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
            print("\n✅ 데이터 수집 완료")
            
            # 총 수집 결과 확인
            if self.data_df_path.exists():
                final_df = pd.read_csv(self.data_df_path)
                found_count = final_df['itad_id'].notna().sum()
                self.logger.info(f"🎯 총 수집된 게임 수: {len(final_df)}")
                self.logger.info(f"🔍 ITAD에서 찾은 게임 수: {found_count} (전체 중 {found_count/len(final_df)*100:.1f}%)")
                print(f"🎯 총 수집된 게임 수: {len(final_df)}")
                print(f"🔍 ITAD에서 찾은 게임 수: {found_count} (전체 중 {found_count/len(final_df)*100:.1f}%)")
            
            self.logger.info(f"❌ 최종 실패: {len(self.failed_list)}개, 에러: {len(self.errored_list)}개")
            print(f"❌ 최종 실패: {len(self.failed_list)}개, 에러: {len(self.errored_list)}개")
            
            # 실패한 ID 목록 저장
            if self.failed_list or self.errored_list:
                failed_df = pd.DataFrame({
                    "appid": self.failed_list + self.errored_list,
                    "status": ["failed"] * len(self.failed_list) + ["error"] * len(self.errored_list)
                })
                save_csv(failed_df, self.ERROR_DIR / "itad_failed_ids.csv")
                self.logger.info(f"❗ 실패한 ID 목록이 itad_failed_ids.csv에 저장되었습니다.")
                print(f"❗ 실패한 ID 목록이 itad_failed_ids.csv에 저장되었습니다.")
                
        except Exception as e:
            self.logger.error(f"프로세스 실행 중 오류: {e}")
            print(f"❌ 오류 발생: {e}")

# # 사용 예시
# if __name__ == "__main__":
#     fetcher = ITADIdFetcher()
#     fetcher.run(settings.PAID_LIST_DIR)
