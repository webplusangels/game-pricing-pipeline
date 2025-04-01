from util.io_helper import load_csv, save_csv

def filter_valid_games(games_list):
    """
    유효한 정보가 있는 게임만 필터링
    """
    valid_games_df = load_csv(games_list)
    valid_games_df = valid_games_df[valid_games_df['appid'].notna()]
    
    return valid_games_df

def split_free_and_paid(games_list):
    """
    무료 게임과 유료 게임 분리
    """
    games_df = load_csv(games_list)
    is_free_ids_df = games_df[games_df['is_free'] == True]
    is_paid_ids_df = games_df[games_df['is_free'] == False]

    return is_free_ids_df, is_paid_ids_df

def filter_games(games_list):
    """
    게임 리스트 필터링
    """
    valid_games_df = filter_valid_games(games_list)
    save_csv(valid_games_df, games_list)
    
    free_steam_list, paid_steam_list = split_free_and_paid(games_list)
    save_csv(free_steam_list, "./data/raw/free_steam_list.csv")
    save_csv(paid_steam_list, "./data/raw/paid_steam_list.csv")
    
    return