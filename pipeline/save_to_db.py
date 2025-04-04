from config import settings
from sqlalchemy import create_engine
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
import os

from util.io_helper import load_csv

class DBUploader:
    def __init__(self):
        self.engine = create_engine(
            f"postgresql://{settings.DB_USER}:{settings.DB_PASS}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
        )
        self.data_dir = Path("data/processed")

    def run(self):
        for file_path in self.data_dir.glob("*_removed.csv"):
            table = file_path.stem.replace("_removed", "")
            df = load_csv(file_path)
            self.delete_rows(table, df)
            print(f"🗑️ {table} 삭제 완료")
            
        for file_path in self.data_dir.glob("*_updated.csv"):
            table = file_path.stem.replace("_updated", "")
            df = load_csv(file_path)

            index_columns_map = {
                "category": ["id"],
                "platform": ["id"],
                "game_static": ["id"],
                "game_dynamic": ["game_id"],
                "game_category": ["id"],
                "current_price_by_platform": ["id"]
            }

            self.insert_or_update_data(table, df, file_path, index_columns_map[table])
            print(f"🔄 {table} upsert 완료")
    
    def delete_rows(self, table_name, df):
        from sqlalchemy import Table, MetaData, and_

        index_columns_map = {
            "category": ["id"],
            "platform": ["id"],
            "game_static": ["id"],
            "game_dynamic": ["game_id"],
            "game_category": ["id"],
            "current_price_by_platform": ["id"]
        }

        index_columns = index_columns_map.get(table_name)
        if not index_columns:
            print(f"❌ {table_name}에 대한 삭제 키가 정의되지 않았습니다.")
            return

        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine)

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                condition = and_(*[
                    getattr(table.c, col) == row[col]
                    for col in index_columns
                ])
                conn.execute(table.delete().where(condition))
  
    def insert_or_update_data(self, table_name, df, file_path, index_columns):
        from sqlalchemy import Table, MetaData

        # Remove DB-managed timestamp columns to avoid type mismatch (e.g. NaN in timestamp column)
        df = df.drop(columns=[c for c in ["created_at", "updated_at", "deleted_at"] if c in df.columns], errors="ignore")
        df = df.astype(object)
        df = df.where(df.notnull(), None)

        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine)

        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                stmt = insert(table).values(**row.to_dict())
                stmt = stmt.on_conflict_do_update(
                    index_elements=index_columns,
                    set_={col: row[col] for col in row.index if col not in index_columns}
                )
                conn.execute(stmt)

        if file_path.exists():
            os.remove(file_path)