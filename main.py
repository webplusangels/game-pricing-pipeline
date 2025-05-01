from pipeline.fetch_steam_list import SteamListFetcher
from pipeline.fetch_steam_detail import SteamDetailFetcher
from pipeline.fetch_steam_review import SteamReviewFetcher
from pipeline.fetch_steam_active_player import SteamActivePlayerFetcher
from pipeline.fetch_itad_id import ITADIdFetcher
from pipeline.fetch_itad_price import ITADPriceFetcher
from pipeline.backup_tables import backup_tables_to_csv
from pipeline.process_data import DataProcessor
from pipeline.save_to_db import DBUploader
from concurrent.futures import ThreadPoolExecutor
from config import settings
from time import time
from contextlib import contextmanager
import os
from sqlalchemy import create_engine

from util.logger import setup_logger
from util.db_schema_helper import fetch_not_null_columns
from pipeline.filter_list import filter_games

def main():
    # 디렉토리 생성
    os.makedirs("data", exist_ok=True)
    os.makedirs("log", exist_ok=True)
    
    # 환경변수 및 설정값 가져오기
    steam_key = settings.STEAM_KEY
    list_dir = settings.LIST_DIR
    paid_game_dir = settings.PAID_LIST_DIR
    free_game_dir = settings.FREE_LIST_DIR
    number_of_games = settings.NUMBER_OF_GAMES
    
    # 로그 설정
    logger = setup_logger(
        name="main_pipeline", 
        log_dir="log/main",
    )
    is_production = os.getenv("ENV") == "prod"
    is_development = os.getenv("ENV") == "dev"
    
    @contextmanager
    def log_step(name, logger):
        start = time()
        logger.info(f"[시작] {name}")
        yield
        end = time()
        logger.info(f"[완료] {name} - {end - start:.2f}초")
        
    # DB 연결 설정
    db_url = settings.DB_URL
    engine = create_engine(db_url)
    not_null_map = fetch_not_null_columns(engine)
    
    # 데이터 수집 파이프라인 시작
    logger.info("데이터 수집 파이프라인 시작")
    if is_production:
        logger.info("운영 환경에서 실행 중입니다. 데이터 수집을 건너뜁니다.")
    if is_development:
        logger.info("개발 환경에서 실행 중입니다. 데이터 수집을 진행합니다.")
    
    try: 
        # production 환경에서는 데이터 수집을 건너뜀
        if not is_production and not is_development:
            # 1. Steam 게임 리스트 가져오기
            with log_step("Steam 게임 리스트 가져오기", logger):
                steam_list_fetcher = SteamListFetcher(
                    webapi_key=steam_key,
                    steamcharts_games=number_of_games,
                )
                steam_list_fetcher.run()
                logger.info("Steam 게임 리스트 가져오기 완료")
            
            # 2. 모듈 별 데이터 가져오기
            with log_step("Steam 게임 상세 정보 가져오기", logger):
                steam_detail_fetcher = SteamDetailFetcher()
                steam_detail_fetcher.run(list_dir)
                logger.info("Steam 게임 상세 정보 가져오기 완료")
                
            # 3. 필터링 하기, 무료/유료 게임 리스트 분리
            filter_games('./data/raw/steam_game_detail_parsed.csv')
            logger.info("Steam 게임 리스트 필터링 완료")
        
            # 4. Steam 게임 리뷰 및 활성 플레이어 정보 가져오기 (병렬 처리)
            with log_step("Steam 게임 리뷰 및 활성 플레이어 정보 병렬 가져오기", logger):
                review_fetcher = SteamReviewFetcher()
                active_player_fetcher = SteamActivePlayerFetcher()
            
                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_review = executor.submit(review_fetcher.run, list_dir)
                    future_active_player = executor.submit(active_player_fetcher.run, list_dir)

                    try:
                        future_review.result()
                        logger.info("Steam 게임 리뷰 가져오기 완료")
                    except Exception as e:
                        logger.error(f"리뷰 수집 중 오류 발생: {e}")

                    try:
                        future_active_player.result()
                        logger.info("Steam 게임 활성 플레이어 정보 가져오기 완료")
                    except Exception as e:
                        logger.error(f"활성 플레이어 수집 중 오류 발생: {e}")
            
            # 5. ITAD 게임 ID 가져오기
            with log_step("ITAD ID 데이터 가져오기", logger):
                itad_id_fetcher = ITADIdFetcher()
                itad_id_fetcher.run(paid_game_dir)
                logger.info("ITAD ID 데이터 가져오기 완료")
            
            # 6. ITAD 게임 가격 정보 가져오기
            with log_step("ITAD 가격 데이터 가져오기", logger):
                itad_price_fetcher = ITADPriceFetcher()
                itad_price_fetcher.run()
                logger.info("ITAD 가격 데이터 가져오기 완료")
            
        backup_tables_to_csv([
            "category",
            "platform",
            "game_static",
            "game_dynamic",
            "game_category",
            "current_price_by_platform",
        ])
        
        # 7. 포맷팅 및 데이터 처리
        with log_step("데이터 처리", logger):
            data_processor = DataProcessor(not_null_map=not_null_map)
            data_processor.run()
            logger.info("데이터 처리 완료") 

        # 4. 데이터베이스 업로드
        with log_step("데이터베이스 업로드", logger):
            db_uploader = DBUploader()
            db_uploader.run()
            logger.info("데이터베이스 업로드 완료")
    except Exception as e:
        logger.error(f"데이터 수집 중 오류 발생: {e}")
    finally:
        logger.info("데이터 수집 파이프라인 종료")

if __name__ == "__main__":
    main()
    