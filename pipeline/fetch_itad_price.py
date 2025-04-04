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

class ITADPriceFetcher:
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
            name="itad_price_fetcher", 
            log_dir=self.LOG_DIR,
        )
        
        # 요청 제어기 설정
        self.rate_limit_manager = RateLimitManager()
        
        # 파일 경로 설정
        self.CACHE_FILE = self.CACHE_DIR / "itad_price_status_cache.json"
        self.FAILED_IDS_FILE = self.ERROR_DIR / "itad_price_failed_ids.csv"
        self.INPUT_FILE = self.OUTPUT_DIR / "itad_game_ids.csv"
        self.OUTPUT_FILE = self.OUTPUT_DIR / "itad_game_prices.csv"
        
        # API 설정
        self.PRICE_BASE_URL = "https://api.isthereanydeal.com/games/prices/v3"
        self.KEY = settings.ITAD_KEY
        self.COUNTRY = "KR"
        
        # 헤더 설정
        self.HEADERS = {
            "Content-Type": "application/json",
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
    
    def load_game_ids(self):
        """게임 ID 파일에서 게임 ID 목록 로드"""
        try:
            if not self.INPUT_FILE.exists():
                raise FileNotFoundError(f"{self.INPUT_FILE} 파일이 존재하지 않습니다.")

            df = pd.read_csv(self.INPUT_FILE)
            if 'itad_id' not in df.columns:
                raise ValueError("입력 CSV에 'itad_id' 컬럼이 존재하지 않습니다.")

            valid_ids = df.dropna(subset=['itad_id'])

            # 이미 처리된 ID 제외
            if self.OUTPUT_FILE.exists():
                existing_df = pd.read_csv(self.OUTPUT_FILE)
                if 'itad_id' in existing_df.columns:
                    processed_ids = set(existing_df['itad_id'].tolist())
                    valid_ids = valid_ids[~valid_ids['itad_id'].isin(processed_ids)]

            self.logger.info(f"총 {len(valid_ids)}개의 유효한 게임 ID를 로드했습니다.")
            return valid_ids

        except Exception as e:
            self.logger.error(f"게임 ID 로드 중 오류 발생: {e}")
            print(f"❌ 게임 ID 로드 중 오류: {e}")
            return pd.DataFrame()
    
    def get_game_prices(self, game_ids_batch):
        """게임 가격 정보 API 요청"""
        url = f"{self.PRICE_BASE_URL}?key={self.KEY}&country={self.COUNTRY}"
        try:
            response = requests.post(
                url,
                headers=self.HEADERS,
                json=game_ids_batch,
                timeout=15
            )
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, list):
                processed_data = {}
                for entry in data:
                    if 'id' in entry:
                        processed_data[entry['id']] = entry
                return processed_data            
            return data
        except Exception as e:
            self.logger.error(f"API 요청 중 오류: {e}")
            raise e
    
    def process_price_data(self, data, game_ids_batch):
        """API 응답 데이터 처리"""
        results = []
        processed_ids = set()

        if data is None:
            self.logger.warning("API에서 빈 응답을 반환했습니다.")
            self.failed_list.extend(game_ids_batch)
            return results

        for game_id in game_ids_batch:
            if game_id in processed_ids:
                continue

            processed_ids.add(game_id)

            if game_id in self.status_cache and self.status_cache[game_id] == "success":
                self.logger.info(f"[{game_id}] 이미 수집된 데이터, 건너뜀")
                continue

            if game_id not in data:
                self.logger.warning(f"[{game_id}] API 응답에 해당 게임 ID가 없습니다.")
                self.failed_list.append(game_id)
                continue

            game_data = data[game_id]

            try:
                # 역대 최저가 정보 추출
                history_low_price = None
                history_low_currency = None
                if 'historyLow' in game_data and 'all' in game_data['historyLow']:
                    history_low_price = game_data['historyLow']['all'].get('amount')
                    history_low_currency = game_data['historyLow']['all'].get('currency')
                    self.logger.debug(f"[{game_id}] 역대 최저가: {history_low_price} {history_low_currency}")

                # # 딜 정보가 없는 경우 처리
                # if 'deals' not in game_data or not game_data['deals']:
                #     self.logger.info(f"[{game_id}] 딜 정보가 없습니다.")
                #     result = {
                #         'itad_id': game_id,
                #         'history_low_price': history_low_price,
                #         'history_low_currency': history_low_currency,
                #         'shop_id': None,
                #         'shop_name': None,
                #         'current_price': None,
                #         'regular_price': None,
                #         'discount_percent': None,
                #         'currency': None,
                #         'collected_at': datetime.now().isoformat()
                #     }
                #     results.append(result)
                #     self.status_cache[game_id] = "success"
                #     continue

                ####################################
                # 딜 정보 로깅
                if 'deals' not in game_data:
                    self.logger.warning(f"[{game_id}] 'deals' 키가 없습니다: {game_data}")
                elif not game_data['deals']:
                    self.logger.info(f"[{game_id}] 딜 정보가 비어 있습니다.")
                else:
                    self.logger.info(f"[{game_id}] 딜 수: {len(game_data['deals'])}")

                # 각 딜 정보 처리
                for deal in game_data['deals']:
                    result = {
                        'itad_id': game_id,
                        'history_low_price': history_low_price,
                        'history_low_currency': history_low_currency,
                        'shop_id': deal.get('shop', {}).get('id'),
                        'shop_name': deal.get('shop', {}).get('name'),
                        'current_price': deal.get('price', {}).get('amount'),
                        'regular_price': deal.get('regular', {}).get('amount'),
                        'url': deal.get('url', {}),
                        'discount_percent': deal.get('cut', 0),
                        'collected_at': datetime.now().isoformat()
                    }
                    results.append(result)

                self.status_cache[game_id] = "success"
                self.logger.info(f"[{game_id}] 처리 완료: {len(game_data.get('deals', []))}개의 딜 정보")

            except Exception as e:
                self.logger.error(f"[{game_id}] 데이터 처리 중 오류: {e}")
                self.errored_list.append(game_id)
                self.status_cache[game_id] = "error"

        return results
    
    def fetch_batch(self, game_ids_batch):
        """배치 단위 게임 가격 정보 수집"""
        retry_count = 0
        max_retries = self.MAX_RETRIES

        while retry_count < max_retries:
            try:
                self.logger.info(f"배치 요청 시작: {len(game_ids_batch)}개 게임")
                #####################################
                self.logger.info(f"요청 게임 ID 샘플: {game_ids_batch[:5]}...")
                
                # 요청 제한 상황에 따라 지연 시간 조정
                if self.rate_limit_manager.should_slow_down():
                    delay = self.rate_limit_manager.get_current_delay(1.0)
                    self.logger.info(f"⚠️ 요청 제한 감지로 지연 시간 증가: {delay:.2f}초")
                    time.sleep(delay)
                
                data = self.get_game_prices(game_ids_batch)
                processed_data = self.process_price_data(data, game_ids_batch)
                if processed_data:
                    self.fetched_data.extend(processed_data)
                return
                
            except Timeout:
                retry_count += 1
                self.logger.warning(f"요청 타임아웃 (재시도 {retry_count}/{max_retries})")
                time.sleep(3 * retry_count)
                
            except ConnectionError as e:
                retry_count += 1
                self.logger.error(f"연결 오류: {e} (재시도 {retry_count}/{max_retries})")
                time.sleep(5 * retry_count)
                
            except HTTPError as e:
                status_code = getattr(e.response, 'status_code', None)
                if status_code == 429:  # Rate limit
                    retry_count += 1
                    self.rate_limit_manager.handle_rate_limit()
                    wait_time = 10 * retry_count
                    self.logger.warning(f"요청 제한 감지. {wait_time}초 대기 (재시도 {retry_count}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    self.errored_list.extend(game_ids_batch)
                    self.logger.error(f"HTTP 오류 {status_code}: {e}")
                    return
                    
            except Exception as e:
                self.errored_list.extend(game_ids_batch)
                self.logger.error(f"처리 중 오류 발생: {e}")
                return

        if retry_count >= max_retries:
            self.errored_list.extend(game_ids_batch)
            self.logger.error(f"최대 재시도 횟수 초과. 배치 처리 실패.")
    
    def save_checkpoint(self):
        """현재 수집 상태를 저장"""
        try:
            if not self.fetched_data:
                self.logger.info("저장할 새 데이터가 없습니다.")
                return
            
            # 새 데이터를 DataFrame으로 변환
            new_data_df = pd.DataFrame(self.fetched_data)
    
            if self.OUTPUT_FILE.exists():
                try:
                    old_data_df = pd.read_csv(self.OUTPUT_FILE)
                    merged_df = pd.concat([old_data_df, new_data_df], ignore_index=True)
                    if 'shop_id' in merged_df.columns:
                        merged_df.drop_duplicates(subset=['itad_id', 'shop_id'], keep='last', inplace=True)
                    else:
                        merged_df.drop_duplicates(subset=['itad_id'], keep='last', inplace=True)
                    save_csv(merged_df, self.OUTPUT_FILE)
                except Exception as e:
                    self.logger.error(f"기존 CSV 병합 중 오류: {e}")
                    backup_path = self.ERROR_DIR / f"itad_game_prices_backup_{int(time.time())}.csv"
                    save_csv(new_data_df, backup_path)
                    self.logger.info(f"새 데이터를 백업 파일에 저장: {backup_path}")
            else:
                save_csv(new_data_df, self.OUTPUT_FILE)
    
            # 캐시 저장
            save_json(self.CACHE_FILE, self.status_cache)
    
            # 정보 출력
            total_processed = len(set([d['itad_id'] for d in self.fetched_data]))
            self.logger.info(f"체크포인트 저장 완료. 총 {total_processed}개 게임, {len(self.fetched_data)}개 딜 정보 저장")
    
            if self.OUTPUT_FILE.exists():
                df = pd.read_csv(self.OUTPUT_FILE)
                unique_games = df['itad_id'].nunique()
                total_deals = len(df)
                print(f"💾 중간 저장 완료 - 누적 수집: {unique_games}개 게임, {total_deals}개 딜 정보")
    
            self.fetched_data = []

        except Exception as e:
            self.logger.error(f"체크포인트 저장 중 오류: {e}")
            print(f"❌ 체크포인트 저장 중 오류: {e}")
    
    def fetch_in_parallel(self, game_ids, batch_size=40):
        """병렬 처리로 게임 가격 정보 수집"""
        batches = [game_ids[i:i+batch_size] for i in range(0, len(game_ids), batch_size)]
        
        with tqdm(total=len(batches), desc="🔍 배치 처리") as pbar:
            for i, batch in enumerate(batches):
                print(f"\n배치 {i+1}/{len(batches)} 처리 중 ({len(batch)}개 게임)")
                with ThreadPoolExecutor(max_workers=self.THREAD_WORKERS) as executor:
                    future = executor.submit(self.fetch_batch, batch)
                    future.result()
                self.save_checkpoint()
                pbar.update(1)
                if i < len(batches) - 1:
                    time.sleep(2)
    
    def retry_failed_ids(self, batch_size=20, max_retry_rounds=2):
        """실패한 요청 재시도"""
        retry_ids = list(set(self.failed_list + self.errored_list))
        if not retry_ids:
            return
        
        print(f"\n🔁 총 {len(retry_ids)}개 실패 ID에 대해 재시도 시작")
        self.logger.info(f"총 {len(retry_ids)}개 실패 ID에 대해 재시도 시작")

        for round_num in range(1, max_retry_rounds + 1):
            print(f"\n🔄 재시도 라운드 {round_num}/{max_retry_rounds}")
            self.logger.info(f"재시도 라운드 {round_num}/{max_retry_rounds}")
            self.failed_list = []
            self.errored_list = []
            batches = [retry_ids[i:i+batch_size] for i in range(0, len(retry_ids), batch_size)]
            
            with tqdm(total=len(batches), desc=f"♻️ 재시도 {round_num}") as pbar:
                for batch in batches:
                    self.fetch_batch(batch)
                    self.save_checkpoint()
                    pbar.update(1)
                    time.sleep(2)
                    
            retry_ids = list(set(self.failed_list + self.errored_list))
            if not retry_ids:
                self.logger.info("✅ 재시도 성공! 더 이상 실패한 ID가 없습니다.")
                print("✅ 재시도 성공! 더 이상 실패한 ID가 없습니다.")
                break
                
        # if retry_ids:
        #     self.logger.info(f"❌ 여전히 실패한 ID 수: {len(retry_ids)}")
        #     print(f"❌ 여전히 실패한 ID 수: {len(retry_ids)}")
        #     for game_id in retry_ids:
        #         self.status_cache[game_id] = "no_data"
        #         self.fetched_data.append({
        #             'itad_id': game_id,
        #             'history_low_price': None,
        #             'history_low_currency': None,
        #             'shop_id': None,
        #             'shop_name': None,
        #             'current_price': None,
        #             'regular_price': None,
        #             'discount_percent': None,
        #             'currency': None,
        #             'collected_at': datetime.now().isoformat()
        #         })    
        #     self.save_checkpoint()
    
    def run(self, batch_size=40):
        """전체 데이터 수집 프로세스 실행"""
        try:
            game_ids_df = self.load_game_ids()
            if game_ids_df.empty:
                self.logger.warning("❌ 처리할 게임 ID가 없습니다.")
                print("❌ 처리할 게임 ID가 없습니다.")
                return

            game_ids = game_ids_df['itad_id'].dropna().tolist()
            if not game_ids:
                self.logger.warning("❌ 유효한 itad_id가 없습니다.")
                print("❌ 유효한 itad_id가 없습니다.")
                return

            total_games = len(game_ids)
            self.logger.info(f"📋 총 {total_games}개 게임 가격 정보 수집 시작")
            print(f"📋 총 {total_games}개 게임 가격 정보 수집 시작")

            # 1차 수집
            self.fetch_in_parallel(game_ids, batch_size=batch_size)
            
            # 실패한 ID 재시도
            self.retry_failed_ids(batch_size=batch_size, max_retry_rounds=2)

            # 최종 통계
            total_success = sum(1 for status in self.status_cache.values() if status == "success")
            self.logger.info("\n✅ 데이터 수집 완료")
            self.logger.info(f"🎯 성공: {total_success}개 게임")
            self.logger.info(f"❌ 실패: {len(self.failed_list)}개, 에러: {len(self.errored_list)}개")
            print("\n✅ 데이터 수집 완료")
            print(f"🎯 성공: {total_success}개 게임")
            print(f"❌ 실패: {len(self.failed_list)}개, 에러: {len(self.errored_list)}개")

            # 실패한 ID 목록 저장
            if self.failed_list or self.errored_list:
                failed_df = pd.DataFrame({
                    "itad_id": self.failed_list + self.errored_list,
                    "status": ["failed"] * len(self.failed_list) + ["error"] * len(self.errored_list)
                })
                save_csv(failed_df, self.FAILED_IDS_FILE)
                self.logger.info(f"❗ 실패한 ID 목록이 {self.FAILED_IDS_FILE}에 저장되었습니다.")
                print(f"❗ 실패한 ID 목록이 {self.FAILED_IDS_FILE}에 저장되었습니다.")

            # 최종 결과 출력
            if self.OUTPUT_FILE.exists():
                final_df = pd.read_csv(self.OUTPUT_FILE)
                unique_games = final_df['itad_id'].nunique()
                total_deals = len(final_df)
                shops_count = final_df['shop_name'].value_counts().to_dict()
                self.logger.info(f"\n📊 최종 결과:")
                self.logger.info(f"총 {unique_games}개 게임, {total_deals}개 딜 정보 수집")
                self.logger.info(f"상점별 분포: {shops_count}")
                print(f"\n📊 최종 결과:")
                print(f"총 {unique_games}개 게임, {total_deals}개 딜 정보 수집")
                print(f"상점별 분포: {shops_count}")

        except Exception as e:
            self.logger.error(f"프로세스 실행 중 오류: {e}")
            print(f"❌ 오류 발생: {e}")

# # 사용 예시
# if __name__ == "__main__":
#     fetcher = ITADPriceFetcher()
#     fetcher.run(batch_size=40)