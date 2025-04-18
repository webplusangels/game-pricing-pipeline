import requests
import time
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from pathlib import Path
from requests.exceptions import HTTPError, Timeout, ConnectionError

from util.io_helper import save_csv, load_csv, upload_to_s3, download_from_s3
from util.cache_manager import CacheManager
from util.logger import setup_logger

class SteamActivePlayerFetcher:
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
            name="steam_active_player_fetcher", 
            log_dir=self.LOG_DIR,
        )
        
        # 파일 경로 설정
        self.CACHE_FILE = self.CACHE_DIR / "ap_status_cache.json"
        self.FAILED_IDS_FILE = self.ERROR_DIR / "failed_ap_ids.csv"
        self.players_df_path = self.OUTPUT_DIR / "steam_game_active_player.csv"
        
         # S3 업로드 설정
        self.S3_CACHE_KEY = "data/cache/ap_status_cache.json"
        self.S3_OUTPUT_KEY = "data/raw/steam_game_active_player.csv"
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
        self.players_data = []
        self.failed_list = []
        self.errored_list = []
        self.cache = CacheManager(self.CACHE_FILE)
        
    def fetch_active_player_data(self, app_id):
        """Steam API에서 활성 플레이어 데이터 가져오기"""
        cached = self.cache.get(app_id)
        if cached and cached.get("status") == "success" and not self.cache.is_stale(app_id, hours=24):
            self.logger.info(f"[{app_id}] 캐시된 데이터 사용")
            return False
        
        if self.cache.too_many_fails(app_id):
            self.logger.warning(f"🚫 앱 {app_id}은 실패가 누적되어 건너뜁니다.")
            return False
        
        url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}"
        
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            response_json = response.json()

            if not response_json or "response" not in response_json:
                self.failed_list.append(app_id)
                self.cache.record_fail(app_id)
                return False

            player_data = response_json.get("response", {})
            if player_data.get("player_count") is not None:
                active_player_data = {
                    "appid": app_id,
                    "player_count": player_data.get("player_count", 0)
                }
                self.players_data.append(active_player_data)
                self.cache.set(app_id, {
                    "status": "success",
                    "collected_at": datetime.now().isoformat()
                })
                return True
            else:
                self.failed_list.append(app_id)
                self.cache.record_fail(app_id)
                self.logger.warning(f"[{app_id}] 응답 데이터 없음")
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
            raise
                
        except Exception as e:
            self.errored_list.append(app_id)
            self.cache.record_fail(app_id)
            self.logger.error(f"[{app_id}] 에러 발생: {e}")
            raise
        
    def save_checkpoint(self):
        """현재 수집 상태 저장"""
        try:
            # 리뷰 데이터 저장
            new_player_df = pd.DataFrame(self.players_data)
            
            # 기존 CSV 로드 및 병합
            if self.players_df_path.exists():
                old_player_df = load_csv(self.players_df_path)
                merged_player_df = pd.concat([old_player_df, new_player_df], ignore_index=True)
                merged_player_df.drop_duplicates(subset="appid", keep="last", inplace=True)
            else:
                merged_player_df = new_player_df
            
            # 저장 (io_helper의 save_csv, save_json 활용)
            save_csv(merged_player_df, self.players_df_path)
            self.cache.save()
            
            # 진행 상황 출력
            total = len(self.players_data) + len(self.failed_list) + len(self.errored_list)
            success_rate = len(self.players_data) / total * 100 if total > 0 else 0
            self.logger.info(f"💾 중간 저장 완료 - 누적 수집: {len(merged_player_df)}개")
            self.logger.info(f"진행 상황: {len(self.players_data)}개 성공 / {len(self.failed_list)}개 실패 / {len(self.errored_list)}개 오류 (성공률: {success_rate:.1f}%)")
            
            # 저장 후 데이터 초기화 (메모리 관리)
            self.players_data = []
            
        except Exception as e:
            self.logger.error(f"체크포인트 저장 중 오류: {e}")
    
    def fetch_in_parallel(self, app_ids, batch_size=40):
        """배치 단위로 병렬 처리"""
        base_delay = 0.8
        
        for i in range(0, len(app_ids), batch_size):
            batch = app_ids[i:i+batch_size]
            self.logger.info(f"배치 처리 중: {i+1}-{i+len(batch)}/{len(app_ids)}")
            
            with ThreadPoolExecutor(max_workers=self.THREAD_WORKERS) as executor:
                # 요청 제출 시 약간의 지연 추가
                futures = []
                for app_id in batch:
                    futures.append((app_id, executor.submit(self.fetch_active_player_data, app_id)))

                for app_id, future in tqdm(futures, desc="📦 Fetching"):
                    try:
                        should_sleep = future.result()
                        if should_sleep:
                            time.sleep(base_delay)
                    except Exception as e:
                        self.logger.error(f"스레드 실행 중 오류: {e}")
            
            # 각 배치 후 저장
            self.save_checkpoint()
    
    def retry_loop(self, target_list, label, max_retries=2):
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
    
    # def get_collected_appids(self):
    #     """이미 수집된 ID 목록 가져오기"""
    #     collected_appids = set()
    #     if self.players_df_path.exists():
    #         try:
    #             existing_df = pd.read_csv(self.players_df_path)
    #             collected_appids.update(existing_df["appid"].tolist())
    #         except Exception as e:
    #             self.logger.warning(f"⚠️ 기존 CSV 로드 실패: {e}")
    #     return collected_appids
    
    # def init_app_list(self, ids_list):
    #     """수집한 ID 목록 초기화"""
    #     df = load_csv(self.players_df_path)
    #     if not df.empty:
    #         # ids_list에 없는 ID 제거
    #         df = df[~df["appid"].isin(ids_list)]
    #     save_csv(df, self.players_df_path)
    #     return df["appid"].tolist()
    
    def run(self, input_csv_path):
        """전체 데이터 수집 프로세스 실행"""
        
        # ID 목록 불러오기 및 필터링
        ids_list = load_csv(input_csv_path)["appid"].tolist()
        # ids_list = self.init_app_list(ids_list)
        # ids_list = [appid for appid in ids_list if appid not in collected_appids]
        self.logger.info(f"📋 수집할 appid 수: {len(ids_list)}")
        
        # 1차 수집
        self.fetch_in_parallel(ids_list)
        
        # 실패한 ID 가져오기
        failed_ids_from_cache = [
            int(app_id) for app_id, status in self.cache.items()
            if status == "failed"
        ]
        self.logger.info(f"캐시에서 실패한 ID 수: {len(failed_ids_from_cache)}")

        # 실패한 ID 파일에서 추가 실패 ID 가져오기
        if self.FAILED_IDS_FILE.exists():
            failed_ids_from_file = load_csv(self.FAILED_IDS_FILE)["id"].tolist()
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
        if self.players_df_path.exists():
            final_df = load_csv(self.players_df_path)
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
            
        # S3 업로드
        upload_to_s3(self.players_df_path, self.S3_OUTPUT_KEY)
        upload_to_s3(self.CACHE_FILE, self.S3_CACHE_KEY)
        self.logger.info(f"✅ S3에 업로드 완료: {self.S3_OUTPUT_KEY}, {self.S3_CACHE_KEY}")
