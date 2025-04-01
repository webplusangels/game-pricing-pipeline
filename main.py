from pipeline.fetch_steam_list import SteamListFetcher
from pipeline.fetch_steam_detail import SteamDetailFetcher
from pipeline.fetch_steam_review import SteamReviewFetcher
from pipeline.fetch_steam_active_player import SteamActivePlayerFetcher
# from pipeline.fetch_itad_id import ITADIdFetcher
# from pipeline.fetch_itad_price import ITADPriceFetcher
# from pipeline.process_data import DataProcessor
# from pipeline.save_to_rds import RDSUploader
from concurrent.futures import ThreadPoolExecutor
from config import settings
from time import time
from contextlib import contextmanager

from util.logger import setup_logger
from pipeline.filter_list import filter_games

def main():
    steam_key = settings.STEAM_KEY
    list_dir = settings.LIST_DIR
    logger = setup_logger(
        name="main_pipeline", 
        log_dir="log/main",
    )
    
    @contextmanager
    def log_step(name, logger):
        start = time()
        logger.info(f"[시작] {name}")
        yield
        end = time()
        logger.info(f"[완료] {name} - {end - start:.2f}초")
    
    logger.info("데이터 수집 파이프라인 시작")
    
    # 0. RDS에서 데이터 가져오기 - 가져와서 비교?
    
    # 1. Steam 게임 리스트 가져오기
    with log_step("Steam 게임 리스트 가져오기", logger):
        steam_list_fetcher = SteamListFetcher(
            webapi_key=steam_key,
            steamcharts_games=500,
            )
        steam_list_fetcher.run()
        logger.info("Steam 게임 리스트 가져오기 완료")
    
    # 2. 모듈 별 데이터 가져오기
    with log_step("Steam 게임 상세 정보 가져오기", logger):
        steam_detail_fetcher = SteamDetailFetcher()
        steam_detail_fetcher.run(list_dir)
        logger.info("Steam 게임 상세 정보 가져오기 완료")
        
    # 필터링 하기, 무료/유료 게임 리스트 분리
    filter_games('./data/raw/steam_game_detail_parsed.csv')
 
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
            
    # step_start_time = time()
    # itad_id_fetcher = ITADIdFetcher(settings.ITAD_KEY)
    # itad_data = itad_id_fetcher.fetch_all(steam_list)
    # step_end_time = time()
    # logger.info(f"ITAD ID 데이터 가져오기 완료: {step_end_time - step_start_time:.2f}초 소요")
    
    # step_start_time = time()
    # itad_price_fetcher = ITADPriceFetcher(settings.ITAD_KEY)
    # itad_data = itad_price_fetcher.fetch_all(steam_list)
    # step_end_time = time()
    # logger.info(f"ITAD 가격 데이터 가져오기 완료: {step_end_time - step_start_time:.2f}초 소요")

    # # 3. 데이터 처리
    # with log_step("데이터 처리", logger):
    #     processor = DataProcessor()
    #     processed_data = processor.merge(
    #         steam_detail, 
    #         steam_review, 
    #         steam_active_player, 
    #         # itad_data
    #     )
    #     logger.info("데이터 처리 완료") 

    # # 4. 데이터베이스 업로드
    # with log_step("데이터베이스 업로드", logger):
    #     step_start_time = time()
    #     uploader = RDSUploader(settings.DB_URL)
    #     uploader.save(processed_data)
    #     logger.info("데이터베이스 업로드 완료")

if __name__ == "__main__":
    main()
    
# 웹훅 연결하기
# 에러 핸들링
