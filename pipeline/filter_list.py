from util.io_helper import load_csv, save_csv
from config import settings

def filter_common_ids_by_games(games_df, common_ids_path):
    """
    공통 ID 기준으로 게임 필터링
    """
    common_id_df = load_csv(common_ids_path)
    filtered_common_id_df = common_id_df[common_id_df['appid'].isin(games_df['appid'])]
    return filtered_common_id_df

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
    common_id_df = filter_common_ids_by_games(games_df, settings.LIST_DIR)
    
    # 필터링된 게임 리스트 저장
    save_csv(common_id_df, settings.LIST_DIR)
    
    # 무료 및 유료 게임 분리
    free_games_df, paid_games_df = split_free_and_paid(games_df)
    
    # 무료 및 유료 게임 저장
    save_csv(paid_games_df, settings.PAID_LIST_DIR)
    save_csv(free_games_df, settings.FREE_LIST_DIR)