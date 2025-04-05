from pathlib import Path
import pandas as pd
import ast
import os

from util.io_helper import load_csv, save_csv
from util.logger import setup_logger

class DataProcessor:
    def __init__(self):
        # 로그 설정
        LOG_DIR = Path('log/process_data')
        LOG_DIR.mkdir(exist_ok=True)
        self.logger = setup_logger(name="process_data", log_dir=LOG_DIR)

        # 데이터 디렉토리 설정
        self.data_processed_dir = Path('data/processed')
        self.data_processed_dir.mkdir(parents=True, exist_ok=True)
        self.data_raw_dir = Path('data/raw')
        self.data_raw_dir.mkdir(parents=True, exist_ok=True)
        
        # 데이터 로드
        self.detail_parsed_df = load_csv(self.data_raw_dir / 'steam_game_detail_parsed.csv')
        self.itad_deal_df = load_csv(self.data_raw_dir / 'itad_game_prices.csv')
        self.itad_id_df = load_csv(self.data_raw_dir / 'itad_game_ids.csv')
        self.review_df = load_csv(self.data_raw_dir / 'steam_game_reviews.csv')
        self.active_player_df = load_csv(self.data_raw_dir / 'steam_game_active_player.csv')
        self.original_title_df = load_csv(self.data_raw_dir / 'all_app_list.csv')

        self.table_parsers = {
            "category": self.parse_category,
            "platform": self.parse_platform,
            "current_price_by_platform": self.parse_current_price_by_platform,
            "game_category": self.parse_game_category,
            "game_dynamic": self.parse_game_dynamic,
            "game_static": self.parse_game_static,
        }

    def difference(self, new_df, old_df):
        diff_df = pd.merge(new_df, old_df, how="outer", indicator=True)

        only_new = diff_df[diff_df["_merge"] == "left_only"]  # 새로 생긴 행
        only_old = diff_df[diff_df["_merge"] == "right_only"]  # 삭제된 행
        return only_new, only_old
    
    def load_previous_if_exists(self, filename):
        prev_path = self.data_processed_dir / filename
        if prev_path.exists():
            return load_csv(prev_path)
        return None

    def save_if_changed(self, df, filename):
        prev_df = self.load_previous_if_exists(filename)
        processed_path = self.data_processed_dir / filename
        updated_path = self.data_processed_dir / filename.replace(".csv", "_updated.csv")
        removed_path = self.data_processed_dir / filename.replace(".csv", "_removed.csv")

        if prev_df is not None:
            only_new, only_old = self.difference(df, prev_df)
            if only_new.empty and only_old.empty:
                self.logger.info(f"{filename} 변경 없음 → 저장 생략")
                if processed_path.exists():
                    os.remove(processed_path)
                return False
            else:
                self.logger.info(f"🆕 {filename} 변경됨: 추가 {len(only_new)}, 제거 {len(only_old)}")
                if not only_new.empty:
                    save_csv(only_new.drop(columns=['_merge'], errors='ignore'), updated_path)
                if not only_old.empty:
                    save_csv(only_old.drop(columns=['_merge'], errors='ignore'), removed_path)
                if processed_path.exists():
                    os.remove(processed_path)                
                return True
        else:
            self.logger.info(f"{filename} 이전 데이터 없음 → 전체를 updated로 저장")
            save_csv(df, updated_path)
            if removed_path.exists():
                os.remove(removed_path)
            if processed_path.exists():
                os.remove(processed_path)
            return True

    def parse_game_static(self):
        """정적 게임 정보 테이블"""
        self.detail_parsed_df['categories'] = self.detail_parsed_df['categories'].apply(
            lambda x: ast.literal_eval(x) if pd.notna(x) else []
        )

        self.detail_parsed_df['is_singleplay'] = self.detail_parsed_df['categories'].apply(
            lambda cats: '싱글 플레이어' in cats
        )
        self.detail_parsed_df['is_multiplay'] = self.detail_parsed_df['categories'].apply(
            lambda cats: any(c in cats for c in ['멀티플레이어', 'PvP 온라인', '협동', '스크린 공유 및 분할 PvP', 'PvP', '온라인 협동'])
        )

        static_df = self.detail_parsed_df.rename(columns={
            'appid': 'id',
            'name': 'title',
            'short_description': 'description',
            'release_date': 'release_date',
            'publisher': 'publisher',
            'developer': 'developer',
            'header_image': 'thumbnail',
            'initial_price': 'price'
        })

        original_title_df = self.original_title_df.rename(columns={'appid': 'id', 'name': 'original_title'})
        static_df = pd.merge(static_df, original_title_df[['id', 'original_title']], on='id', how='left')

        static_df = static_df[[
            'id', 'title', 'original_title', 'description', 'release_date',
            'publisher', 'developer', 'thumbnail', 'price',
            'is_singleplay', 'is_multiplay'
        ]]

        static_df = static_df.astype({
            'id': int,
            'title': str,
            'original_title': str,
            'description': str,
            'release_date': str,
            'publisher': str,
            'developer': str,
            'thumbnail': str,
            'price': int,
            'is_singleplay': bool,
            'is_multiplay': bool
        })

        self.save_if_changed(static_df, "game_static.csv")

    def parse_game_dynamic(self):
        """동적 게임 정보 테이블"""
        price_df = load_csv(self.data_processed_dir / 'current_price_by_platform.csv')
        os.remove(self.data_processed_dir / 'current_price_by_platform.csv')
        
        deal_with_appid = pd.merge(self.itad_deal_df, self.itad_id_df, on='itad_id', how='left')
        history_low_df = (
            deal_with_appid.groupby('appid')['history_low_price']
            .min()
            .reset_index()
            .rename(columns={'appid': 'game_id', 'history_low_price': 'history_lowest_price'})
        )

        lowest_price_df = (
            price_df.loc[price_df.groupby('game_id')['discount_price'].idxmin()]
            .rename(columns={
                'platform_id': 'lowest_platform',
                'discount_price': 'lowest_price'
            })
        )

        on_sale_df = price_df[['game_id', 'discount_rate']].copy()
        on_sale_df['on_sale'] = on_sale_df['discount_rate'] > 0
        on_sale_df = on_sale_df[['game_id', 'on_sale']].drop_duplicates('game_id')

        dynamic_df = pd.merge(self.review_df, self.active_player_df, on='appid', how='outer').rename(columns={'appid': 'game_id'})

        dynamic_df = pd.merge(dynamic_df, lowest_price_df[['game_id', 'lowest_platform', 'lowest_price']], on='game_id', how='left')
        dynamic_df = pd.merge(dynamic_df, history_low_df, on='game_id', how='left')
        dynamic_df = pd.merge(dynamic_df, on_sale_df, on='game_id', how='left')

        dynamic_df['lowest_platform'] = dynamic_df['lowest_platform'].fillna(61).astype(int)
        dynamic_df['on_sale'] = dynamic_df['on_sale'].fillna(False).astype(bool)

        game_price_df = self.detail_parsed_df[['appid', 'initial_price']]
        game_price_df = game_price_df.rename(columns={'appid': 'game_id'})

        dynamic_df = pd.merge(dynamic_df, game_price_df, on='game_id', how='left')
        dynamic_df['lowest_price'] = dynamic_df['lowest_price'].fillna(dynamic_df['initial_price'])
        dynamic_df['history_lowest_price'] = dynamic_df['history_lowest_price'].fillna(dynamic_df['lowest_price'])
        dynamic_df = dynamic_df.drop(columns=['initial_price'])

        dynamic_df = dynamic_df.rename(columns={
            'review_score': 'rating',
            'player_count': 'active_players'
        })

        dynamic_df = dynamic_df[[
            'game_id', 'rating', 'active_players',
            'lowest_platform', 'lowest_price',
            'history_lowest_price', 'on_sale', 'total_reviews'
        ]]

        dynamic_df = dynamic_df.astype({
            'game_id': int,
            'rating': int,
            'active_players': int,
            'lowest_platform': int,
            'lowest_price': int,
            'history_lowest_price': int,
            'on_sale': bool,
            'total_reviews': int
        })
        
        self.save_if_changed(dynamic_df, "game_dynamic.csv")

    def parse_game_category(self):
        """게임 카테고리 테이블"""
        self.detail_parsed_df['genres'] = self.detail_parsed_df['genres'].apply(
            lambda x: ast.literal_eval(x) if isinstance(x, str) else x
        )
        
        exploded_df = self.detail_parsed_df[['genres']].explode('genres')
        unique_category = exploded_df['genres'].dropna().drop_duplicates().reset_index(drop=True)
        unique_category_df = pd.DataFrame({
            'id': pd.RangeIndex(start=1, stop=len(unique_category) + 1),
            'category_name': unique_category
        })

        category_map = dict(zip(unique_category_df['category_name'], unique_category_df['id']))
        mapping_rows = []

        for _, row in self.detail_parsed_df.iterrows():
            game_id = row['appid']
            categories = row.get('genres')
            
            if not isinstance(categories, list) or len(categories) == 0:
                continue

            for category in categories:
                category = category.strip()
                category_id = category_map.get(category)
                
                if category_id is not None:
                    mapping_rows.append((category_id, game_id))

        game_category_df = pd.DataFrame(mapping_rows, columns=['category_id', 'game_id'])
        game_category_df.insert(0, 'id', range(1, len(game_category_df) + 1))  
        
        game_category_df = game_category_df.astype({
            'id': int,
            'category_id': int,
            'game_id': int,
        })

        self.save_if_changed(game_category_df, "game_category.csv")
                   
    def parse_current_price_by_platform(self):
        """플랫폼별 현재 가격 테이블"""
        merged_df = pd.merge(self.itad_deal_df, self.itad_id_df, on='itad_id', how='left')
        deal_merged_df = pd.merge(merged_df, self.detail_parsed_df, on='appid', how='left')
        
        deal_merged_df = deal_merged_df[deal_merged_df['initial_price'] != 0]
        
        # 실제 할인율 계산 (shop_id가 61인 경우를 제외)
        deal_merged_df['discount_percent_x'] = deal_merged_df.apply(
            lambda row: int(((row['initial_price'] - row['current_price']) / row['initial_price']) * 100)
            if row['shop_id'] != 61 else None,
            axis=1
        )
        
        current_price_df = deal_merged_df[[
            'appid', 'shop_id', 'discount_percent_x', 'current_price', 'url'
        ]].rename(columns={
            'appid': 'game_id',
            'shop_id': 'platform_id',
            'discount_percent_x': 'discount_rate',
            'current_price': 'discount_price'
        })
        
        # 할인율이 0 아래인 경우는 제외
        current_price_df = current_price_df[
            (deal_merged_df['discount_percent_x'] > 0) & 
            (~deal_merged_df['discount_percent_x'].isna())
        ]
        
        # 모든 게임이 shop_id가 61인 경우 추가
        steam_games = self.detail_parsed_df[['appid', 'initial_price']].copy()
        steam_games['platform_id'] = 61  # Steam의 shop_id
        steam_games['discount_rate'] = 0  # 할인율 0%
        steam_games['url'] = steam_games['appid'].apply(lambda x: f'https://store.steampowered.com/app/{x}/')
        steam_games = steam_games.rename(columns={'appid': 'game_id', 'initial_price': 'discount_price'})
        current_price_df = pd.concat([current_price_df, steam_games], ignore_index=True)   
        
        current_price_df = current_price_df.astype({
            'game_id': int,
            'platform_id': int,
            'discount_rate': int,
            'discount_price': int,
            'url': str
        })
        
        self.save_if_changed(current_price_df, "current_price_by_platform.csv")
        save_csv(current_price_df, self.data_processed_dir / 'current_price_by_platform.csv')

        
    def parse_category(self):
        """카테고리 테이블"""
        self.detail_parsed_df['genres'] = self.detail_parsed_df['genres'].apply(ast.literal_eval)
        exploded_df = self.detail_parsed_df[['genres']].explode('genres')
        unique_category = exploded_df['genres'].dropna().drop_duplicates().reset_index(drop=True)
        unique_category_df = pd.DataFrame({
            'id': pd.RangeIndex(start=1, stop=len(unique_category) + 1),
            'category_name': unique_category
        })

        unique_category_df = unique_category_df.astype({
            'id': int,
            'category_name': str,
        })

        self.save_if_changed(unique_category_df, "category.csv")
        
    def parse_platform(self):
        """플랫폼 테이블"""
        unique_platform_df = (
            self.itad_deal_df[["shop_id", "shop_name"]]
            .dropna(subset=["shop_id", "shop_name"])  # 필요한 열에 대해 결측값 제거
            .drop_duplicates()  # 중복 제거
            .sort_values("shop_id")  # shop_id 기준 정렬
            .reset_index(drop=True)  # 인덱스 재설정
            .rename(columns={"shop_id": "id", "shop_name": "name"})  # 열 이름 변경
        )
        
        unique_platform_df = unique_platform_df.astype({
            'id': int,
            'name': str,
        })

        self.save_if_changed(unique_platform_df, "platform.csv")

    def run(self):
        keep_filenames = set(f"{name}.csv" for name in self.table_parsers)
        for file in self.data_processed_dir.glob("*.csv"):
            if file.name not in keep_filenames:
                os.remove(file)
        
        for table_name, parse_fn in self.table_parsers.items():
            parse_fn()