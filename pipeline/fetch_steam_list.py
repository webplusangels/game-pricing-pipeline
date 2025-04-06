import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path
from requests.exceptions import HTTPError, Timeout, ConnectionError

from util.io_helper import save_csv
from util.cache_manager import CacheManager
from util.logger import setup_logger
from config import settings

class SteamListFetcher:
    def __init__(self, 
                 output_dir="./data/raw", 
                 cache_dir="./data/cache",
                 error_dir="./data/error",
                 log_dir='./log/fetcher',
                 max_retries=3, 
                 thread_workers=2,
                 steamcharts_games=1000,
                 webapi_key=settings.STEAM_KEY):
        # ------------------------- 설정 -------------------------
        self.MAX_RETRIES = max_retries
        self.THREAD_WORKERS = thread_workers
        self.STEAMCHARTS_GAMES = steamcharts_games
        self.WEBAPI_KEY = webapi_key
        
        # 디렉토리 설정
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
            name="steam_list_fetcher", 
            log_dir=self.LOG_DIR,
        )
        
        # 파일 경로 설정
        self.CACHE_FILE_STEAMCHART = self.CACHE_DIR / "steamcharts_status_cache.json"
        self.CACHE_FILE_ALL_APPS = self.CACHE_DIR / "all_apps_cache.json"
        self.FAILED_PAGES_FILE = self.ERROR_DIR / "failed_steamcharts_pages.csv"
        self.steamcharts_path = self.OUTPUT_DIR / "steamcharts_top_games.csv"
        self.all_apps_path = self.OUTPUT_DIR / "all_app_list.csv"
        self.common_ids_path = self.OUTPUT_DIR / "common_ids.csv"
        
        # 헤더 설정
        self.HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
        
        # ------------------------- 데이터 저장소 -------------------------
        self.game_data = []
        self.failed_pages = []
        self.cache_steamchart = CacheManager(self.CACHE_FILE_STEAMCHART)
        self.cache_all_apps = CacheManager(self.CACHE_FILE_ALL_APPS)
    
    def scrape_steamcharts_page(self, page_number):
        """단일 SteamCharts 페이지 스크래핑"""
        if self.cache_steamchart.get(page_number) and self.cache_steamchart.get(page_number).get("status") == "success" and not self.cache.is_stale(page_number, hours=24):
            return
        
        base_url = "https://steamcharts.com/top/p.{}"
        url = base_url.format(page_number)
        
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", id="top-games")
            
            if not table:
                self.failed_pages.append(page_number)
                self.cache_steamchart.set(page_number, "no_table")
                self.logger.warning(f"페이지 {page_number}에 테이블이 없습니다.")
                return
            
            tbody = table.find("tbody")
            rows = tbody.find_all("tr")
            
            for row in rows:
                rank = row.find("td").text.strip()
                game_td = row.find("td", class_="game-name")
                if game_td:
                    game_link = game_td.find("a")
                    if game_link:
                        appid = game_link["href"].split("/")[-1]
                        game_name = game_link.text.strip()
                        self.game_data.append((rank, appid, game_name))
            
            self.cache_steamchart.set(page_number, { "status": "success" })
            
        except Timeout:
            self.failed_pages.append(page_number)
            self.cache_steamchart.set(page_number, "timeout")
            self.logger.warning(f"[페이지 {page_number}] 요청 타임아웃")
            raise
            
        except ConnectionError as e:
            self.failed_pages.append(page_number)
            self.cache_steamchart.set(page_number, "connection_error")
            self.logger.error(f"[페이지 {page_number}] 연결 오류: {e}")
            raise
            
        except HTTPError as e:
            self.failed_pages.append(page_number)
            status_code = getattr(e.response, 'status_code', None)
            self.cache_steamchart.set(page_number, f"http_error_{status_code}")
            
            if status_code == 429:  # Rate limit
                self.logger.warning(f"[페이지 {page_number}] 요청 제한 감지")
            raise
                
        except Exception as e:
            self.failed_pages.append(page_number)
            self.cache_steamchart.set(page_number, "error")
            self.logger.error(f"[페이지 {page_number}] 에러 발생: {e}")
            raise
    
    def scrape_steamcharts_parallel(self):
        """페이지를 병렬로 스크래핑"""
        pages_to_scrape = [page for page in range(1, int(self.STEAMCHARTS_GAMES/25) + 1) 
                           if not self.cache_steamchart.get(page) or 
                           self.cache_steamchart.get(page).get("status") != "success" or 
                           self.cache_steamchart.is_stale(page, hours=24)]
        
        with ThreadPoolExecutor(max_workers=self.THREAD_WORKERS) as executor:
            futures = []
            for page in pages_to_scrape:
                futures.append(executor.submit(self.scrape_steamcharts_page, page))
                time.sleep(0.5)  # 요청 간 지연
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="📦 Scraping"):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"스레드 실행 중 오류: {e}")
        
        # 결과 저장
        self.save_steamcharts_results()
    
    def save_steamcharts_results(self):
        """SteamCharts 결과 저장"""
        try:
            games_list_df = pd.DataFrame(self.game_data, columns=["rank", "appid", "game_name"])
            save_csv(games_list_df, self.steamcharts_path)
            
            # 캐시와 상태 저장
            self.cache_steamchart.save()
                        
            # 실패한 페이지 저장
            if self.failed_pages:
                failed_pages_df = pd.DataFrame({"page": self.failed_pages})
                save_csv(failed_pages_df, self.FAILED_PAGES_FILE)
            
            self.logger.info(f"✅ SteamCharts 데이터 저장 완료: {self.steamcharts_path}")
            self.logger.info(f"총 수집된 게임: {len(games_list_df)}")
            self.logger.info(f"실패한 페이지: {len(self.failed_pages)}")
        except Exception as e:
            self.logger.error(f"결과 저장 중 오류: {e}")
    
    def fetch_all_apps(self):
        """Steam API를 사용하여 전체 앱 리스트 가져오기"""
        if self.cache_all_apps.get("all_apps") and not self.cache_all_apps.is_stale("all_apps", hours=24):
            self.logger.info("ℹ️ 최근 24시간 내 수집된 앱 리스트가 있어 건너뜁니다.")
            return      
        all_apps = []
        last_appid = 0

        while True:
            url = (
                f"https://api.steampowered.com/IStoreService/GetAppList/v1/"
                f"?key={self.WEBAPI_KEY}&include_games=1&include_dlc=0&include_software=0"
                f"&include_videos=0&include_hardware=0&max_results=50000&last_appid={last_appid}"
            )
            try:
                response = requests.get(url)
                response.raise_for_status()
                response_json = response.json()

                if "response" not in response_json:
                    self.logger.error("Error: 'response' key not found in the response JSON")
                    break

                response_data = response_json["response"]
                apps = response_data.get("apps", [])
                all_apps.extend(apps)

                if not response_data.get("have_more_results", False):
                    break

                last_appid = response_data.get("last_appid", 0)

            except Exception as e:
                self.logger.error(f"앱 리스트 가져오기 중 오류: {e}")
                break

        # 필요한 데이터 추출
        parsed_data = [{"appid": app.get("appid"), "name": app.get("name")} for app in all_apps]

        # 저장
        df = pd.DataFrame(parsed_data)
        save_csv(df, self.all_apps_path)
        self.logger.info(f"✅ 전체 앱 리스트 저장 완료: {self.all_apps_path}")

        self.cache_all_apps.set(
            "all_apps", {
                "status": "success", 
                "collected_at": datetime.now().isoformat(), 
                "number_of_apps": len(parsed_data) 
            })
        self.cache_all_apps.save()
        
    def filter_common_ids(self):
        """SteamCharts와 전체 앱 리스트의 공통 ID 필터링"""
        try:
            # 파일 로드
            steamcharts_df = pd.read_csv(self.steamcharts_path)
            all_apps_df = pd.read_csv(self.all_apps_path)
            if self.common_ids_path.exists():
                old_ids_df = pd.read_csv(self.common_ids_path)
            else:
                old_ids_df = pd.DataFrame(columns=["appid", "name"])
                
            # 공통 ID 필터링
            common_ids_df = all_apps_df[all_apps_df["appid"].isin(steamcharts_df["appid"])]
            new_only_df = common_ids_df[~common_ids_df["appid"].isin(old_ids_df["appid"])]
            updated_df = pd.concat([old_ids_df, new_only_df], ignore_index=True).drop_duplicates(subset="appid")
            save_csv(updated_df, self.common_ids_path)
            
            self.logger.info(f"✅ 공통 ID 저장 완료: {self.common_ids_path}")
            self.logger.info(f"공통 ID 수: {len(common_ids_df)}")
        except Exception as e:
            self.logger.error(f"공통 ID 필터링 중 오류: {e}")

    def run(self):
        """전체 프로세스 실행"""
        try:
            self.logger.info("🚀 SteamCharts 데이터 수집 시작")
            self.scrape_steamcharts_parallel()

            self.logger.info("🚀 전체 앱 리스트 수집 시작")
            self.fetch_all_apps()

            self.logger.info("🚀 공통 ID 필터링 시작")
            self.filter_common_ids()

            self.logger.info("✅ 모든 작업 완료")
        except Exception as e:
            self.logger.error(f"전체 프로세스 실행 중 오류: {e}")
