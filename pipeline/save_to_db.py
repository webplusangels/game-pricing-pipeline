import pandas as pd
from config import settings
from sqlalchemy import create_engine
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert

class DBUploader:
    def __init__(self):
        self.engine = create_engine(
            f"postgresql://{settings.DB_USER}:{settings.DB_PASS}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        )
        self.data_dir = Path("data/processed")
        self.full_replace_tables = [
            'category',
            'platform',
            'current_price_by_platform',
            'game_static',
        ]
        self.upsert_tables = [
            'game_dynamic',
        ]
        self.tables = {
            'category': 'category.csv',
            'platform': 'platform.csv',
            'game_static': 'game_static.csv',
            'game_dynamic': 'game_dynamic.csv',
            'game_category': 'game_category.csv',
            'current_price_by_platform': 'current_price_by_platform.csv',
        }

    def run(self):
        for table, file in self.tables.items():
            file_path = self.data_dir / file
            if not file_path.exists():
                print(f"⚠️ {file} 파일 없음 → {table} 건너뜀")
                continue

            df = pd.read_csv(file_path)

            if table in self.full_replace_tables:
                df.to_sql(table, con=self.engine, if_exists='replace', index=False)
                print(f"🔁 {table} 전체 교체 완료")
            elif table in self.upsert_tables:
                self.upsert_game_dynamic(df)
                print(f"🔄 {table} upsert 완료")
            else:
                df.to_sql(table, con=self.engine, if_exists='append', index=False)
                print(f"➕ {table} append 완료")

    def upsert_game_dynamic(self, df):
        from sqlalchemy import Table, MetaData

        metadata = MetaData(bind=self.engine)
        table = Table('game_dynamic', metadata, autoload_with=self.engine)

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                stmt = insert(table).values(**row.to_dict())
                update_dict = row.to_dict()
                stmt = stmt.on_conflict_do_update(
                    index_elements=['game_id'],
                    set_={
                        'rating': update_dict['rating'],
                        'active_players': update_dict['active_players'],
                        'lowest_platform': update_dict['lowest_platform'],
                        'lowest_price': update_dict['lowest_price'],
                        'history_lowest_price': update_dict['history_lowest_price'],
                        'on_sale': update_dict['on_sale'],
                        'total_reviews': update_dict['total_reviews'],
                    }
                )
                conn.execute(stmt)