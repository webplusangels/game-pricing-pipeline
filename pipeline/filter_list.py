import pandas as pd
from util.io_helper import load_csv, save_csv
from config import settings

def filter_common_ids_by_games(games_df, common_ids_path):
    """
    공통 ID 기준으로 게임 필터링
    """
    common_id_df = load_csv(common_ids_path)
    merged_df = pd.merge(games_df, common_id_df, on="appid", how="left")
    return merged_df

def filter_required_fields(detail_df, required_fields):
    """
    NOT NULL 제약이 걸린 필드들이 결측값이 아닌지 확인
    """
    before = len(detail_df)
    filtered_df = detail_df.dropna(subset=required_fields)
    after = len(filtered_df)
    print(f"🔎 NOT NULL 필드 기준으로 {before - after}개 게임 제거됨")
    return filtered_df

def split_free_and_paid(games_df):
    """
    무료 게임과 유료 게임 분리
    """
    free_games_df = games_df[games_df['is_free'] == True]
    paid_games_df = games_df[games_df['is_free'] == False]
    return free_games_df, paid_games_df

def filter_games(games_list_path):
    """
    게임 리스트 필터링 및 저장
    """
    # 게임 리스트 로드
    games_df = load_csv(games_list_path)
    detail_df = load_csv('./data/raw/steam_game_detail_parsed.csv')
    
    # 공통 ID 기준으로 필터링
    common_id_df = filter_common_ids_by_games(games_df, settings.LIST_DIR)
    common_id_df.rename(columns={"name_x": "name"}, inplace=True)
    
    # 🔽 game_static 테이블 제약 조건 기반 필터링
    required_fields = ["short_description", "final_price"]
    detail_df = filter_required_fields(detail_df, required_fields)
    save_csv(detail_df, './data/raw/steam_game_detail_parsed.csv')
    
    # detail_df에 남아있는 ID만 남기기
    valid_ids = set(detail_df["appid"])
    common_id_df = common_id_df[common_id_df["appid"].isin(valid_ids)]
    
    # 필터링된 게임 리스트 저장
    save_csv(common_id_df[["appid", "name"]], settings.LIST_DIR)
    
    # 무료 및 유료 게임 분리
    free_games_df, paid_games_df = split_free_and_paid(common_id_df)
    
    # 무료 및 유료 게임 저장
    save_csv(paid_games_df, settings.PAID_LIST_DIR)
    save_csv(free_games_df, settings.FREE_LIST_DIR)