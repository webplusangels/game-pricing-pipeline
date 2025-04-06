import requests
import time
import html
from datetime import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path
from requests.exceptions import HTTPError, Timeout, ConnectionError

from util.io_helper import save_csv
from util.cache_manager import CacheManager
from util.logger import setup_logger
from util.rate_limit_manager import RateLimitManager

class SteamDetailFetcher:
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
            name="steam_detail_fetcher", 
            log_dir=self.LOG_DIR,
        )
        
        # 요청 제어기 설정
        self.rate_limit_manager = RateLimitManager()
        
        # 파일 경로 설정
        self.CACHE_FILE = self.CACHE_DIR / "detail_status_cache.json"
        self.FAILED_IDS_FILE = self.ERROR_DIR / "failed_detail_ids.csv"
        self.original_df_path = self.OUTPUT_DIR / "steam_game_detail_original.csv"
        self.parsed_df_path = self.OUTPUT_DIR / "steam_game_detail_parsed.csv"
        
        # 헤더 설정
        self.HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        
        # ------------------------- 데이터 저장소 -------------------------
        self.original_data = {}
        self.parsed_data = []
        self.failed_list = []
        self.errored_list = []
        self.cache = CacheManager(self.CACHE_FILE)
        
    def clean_html_entities(self, text):
        """
        html 요소와 zwnbsp 제거
        """
        if pd.isna(text):
            return text
        return html.unescape(text.replace('\uFEFF', ''))    
        
    def parse_game_data(self, data):
        """API 응답에서 필요한 게임 정보 추출"""
        if not isinstance(data, dict) or not data.get("steam_appid"):
            self.logger.warning(f"유효하지 않은 게임 데이터 형식")
            return None
            
        return {
            "appid": data.get("steam_appid"),
            "name": data.get("name"),
            "short_description": self.clean_html_entities(data.get("short_description")),
            "is_free": data.get("is_free", False),
            "release_date": data.get("release_date", {}).get("date", "정보 없음"),            
            "header_image": data.get("header_image"),
            "developer": data.get("developers", [None])[0],
            "publisher": data.get("publishers", [None])[0],
            "initial_price": int(data.get("price_overview", {}).get("initial", 0) / 100),
            "final_price": int(data.get("price_overview", {}).get("final", 0) / 100),
            "discount_percent": data.get("price_overview", {}).get("discount_percent", 0),
            "categories": [c["description"] for c in data.get("categories", [])],
            "genres": [g["description"] for g in data.get("genres", [])],
        }
    
    def fetch_detail_data(self, app_id):
        """Steam API에서 상세 데이터 가져오기"""
        cached = self.cache.get(app_id)
        
        if cached and cached.get("status") == "success" and \
                not self.cache.is_stale(app_id, hours=24):
            return
        
        # 블랙리스트 처리
        if self.cache.too_many_fails(app_id):
            self.logger.info(f"🚫 앱 {app_id}은 실패가 누적되어 건너뜁니다.")
            return    
        
        url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&l=korean"
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            response_json = response.json()
            app_data = response_json.get(str(app_id), {})

            if not app_data.get("success", False):
                self.failed_list.append(app_id)
                self.cache.record_fail(app_id)
                return
            
            data = app_data.get("data", {})
            if data.get("type") == "game":
                self.original_data[app_id] = data
                parsed = self.parse_game_data(data)
                if parsed:
                    self.parsed_data.append(parsed)
                self.cache.set(app_id, {
                    "status": "success",
                    "collected_at": datetime.now().isoformat()
                })
            else:
                self.failed_list.append(app_id)
                self.cache.record_fail(app_id)
                
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
            self.cache.record_fail(app_id)
            
            if status_code == 429:  # Rate limit
                self.logger.warning(f"[{app_id}] 요청 제한 감지")
                self.rate_limit_manager.handle_rate_limit(app_id)
            raise
                
        except Exception as e:
            self.errored_list.append(app_id)
            self.cache.record_fail(app_id)
            self.logger.error(f"[{app_id}] 에러 발생: {e}")
            raise
        
    def save_checkpoint(self):
        """현재 수집 상태 저장"""
        try:
            # 원본 데이터 저장
            new_original_df = pd.DataFrame.from_dict(self.original_data, orient="index")
            
            # 기존 CSV 로드 및 병합
            if self.original_df_path.exists():
                old_original_df = pd.read_csv(self.original_df_path)
                merged_original_df = pd.concat([old_original_df, new_original_df], ignore_index=True)
                merged_original_df.drop_duplicates(subset="steam_appid", inplace=True)
            else:
                merged_original_df = new_original_df
                
            # 파싱 데이터 저장
            new_parsed_df = pd.DataFrame(self.parsed_data)
            
            if self.parsed_df_path.exists():
                old_parsed_df = pd.read_csv(self.parsed_df_path)
                merged_parsed_df = pd.concat([old_parsed_df, new_parsed_df], ignore_index=True)
                merged_parsed_df.drop_duplicates(subset="appid", inplace=True)
            else:
                merged_parsed_df = new_parsed_df
            
            # # 상태 캐시의 중복 제거
            # clean_status_cache = {}
            # for key, value in self.status_cache.items():
            #     clean_status_cache[key] = value
            
            # 저장
            save_csv(merged_original_df, self.original_df_path)
            save_csv(merged_parsed_df, self.parsed_df_path)
            self.cache.save()
            
            # 진행 상황 출력
            total = len(self.parsed_data) + len(self.failed_list) + len(self.errored_list)
            success_rate = len(self.parsed_data) / total * 100 if total > 0 else 0
            print(f"💾 중간 저장 완료 - 누적 수집: {len(merged_parsed_df)}개")
            print(f"진행 상황: {len(self.parsed_data)}개 성공 / {len(self.failed_list)}개 실패 / {len(self.errored_list)}개 오류 (성공률: {success_rate:.1f}%)")
            
            # 저장 후 데이터 초기화 (메모리 관리)
            # self.original_data = {}
            # self.parsed_data = []
            
        except Exception as e:
            self.logger.error(f"체크포인트 저장 중 오류: {e}")
    
    def fetch_in_parallel(self, app_ids, batch_size=40):
        """배치 단위로 병렬 처리"""
        base_delay = 1
        
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
                time.sleep(request_delay)  # 요청 간 1s 지연
                for app_id in batch:
                    futures.append(executor.submit(self.fetch_detail_data, app_id))
                
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
        if self.parsed_df_path.exists():
            try:
                existing_df = pd.read_csv(self.parsed_df_path)
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
            int(app_id) for app_id, status in self.cache.items()
            if status == "failed"
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
        
        # 이미 성공한 ID는 제외
        retry_ids = [appid for appid in retry_ids if self.cache.get(appid)["status"] != "success"]
        retry_ids = list(set(retry_ids))
        self.logger.info(f"재시도할 ID 수: {len(retry_ids)}")
        
        # 재시도 루프
        retry_stages = [
            {"label": "에러", "targets": self.errored_list, "max_retries": 3},
            {"label": "실패", "targets": self.failed_list, "max_retries": 3},
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
        
        if self.parsed_df_path.exists():
            # 수집 완료 후 정제
            parsed_df = pd.read_csv(self.parsed_df_path)
            common_ids = pd.read_csv(input_csv_path)["appid"].tolist()
            filtered_df = parsed_df[parsed_df["appid"].isin(common_ids)]
            # 총 수집 결과 확인
            self.logger.info(f"🎯 총 수집된 게임 수: {len(filtered_df)}")
            save_csv(filtered_df, self.parsed_df_path)
        
        self.logger.info(f"❌ 최종 실패: {len(self.failed_list)}개, 에러: {len(self.errored_list)}개")
        
        # 실패한 ID 목록 저장
        if self.failed_list or self.errored_list:
            failed_df = pd.DataFrame({
                "appid": self.failed_list + self.errored_list,
                "status": ["failed"] * len(self.failed_list) + ["error"] * len(self.errored_list)
            })
            failed_df.to_csv(self.ERROR_DIR / "failed_detail_ids.csv", index=False)
            self.logger.info(f"❗ 실패한 ID 목록이 failed_detail_ids.csv에 저장되었습니다.")
