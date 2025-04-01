from util.io_helper import load_csv, save_csv
from config import settings

def filter_by_common_ids(games_df, common_ids_path):
    """
    공통 ID 기준으로 게임 필터링
    """
    common_id_df = load_csv(common_ids_path)
    filtered_games_df = games_df[games_df['appid'].isin(common_id_df['appid'])]
    return filtered_games_df

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
    
    # 유효한 appid 필터링
    games_df = games_df[games_df['appid'].notna()]
    
    # 공통 ID 기준으로 필터링
    games_df = filter_by_common_ids(games_df, settings.LIST_DIR)
    
    # 필터링된 게임 리스트 저장
    save_csv(games_df, games_list_path)
    
    # 무료 및 유료 게임 분리
    free_games_df, paid_games_df = split_free_and_paid(games_df)
    
    # 무료 및 유료 게임 저장
    save_csv(free_games_df, "./data/raw/free_steam_list.csv")
    save_csv(paid_games_df, "./data/raw/paid_steam_list.csv")