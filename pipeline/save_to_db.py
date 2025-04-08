from config import settings
from sqlalchemy import create_engine
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert
import os

from util.io_helper import load_csv
from util.logger import setup_logger

class DBUploader:
    def __init__(self):
        self.engine = create_engine(settings.DB_URL)
        self.data_dir = Path("data/processed")
        
        self.logger = setup_logger(
            name="DBUploader",
            log_dir="log/db_uploader",
        )

    def run(self, dry_run=False):
        for file_path in self.data_dir.glob("*_removed.csv"):
            table = file_path.stem.replace("_removed", "")
            df = load_csv(file_path)
            if dry_run:
                self.logger.info(f"[Dry Run] {table}에서 {len(df)}개 행이 삭제될 예정입니다.")
                continue
            
            self.delete_rows(table, df)
            self.logger.info(f"🗑️ {table}에서 {len(df)}개 행 삭제 완료")
            if file_path.exists():
                os.remove(file_path)
            
        for file_path in self.data_dir.glob("*_updated.csv"):
            table = file_path.stem.replace("_updated", "")
            df = load_csv(file_path)

            if dry_run:
                self.logger.info(f"[Dry Run] {table}에 {len(df)}개 행이 업로드(또는 수정)될 예정입니다.")
                continue
            
            index_columns_map = {
                "category": ["id"],
                "platform": ["id"],
                "game_static": ["id"],
                "game_dynamic": ["game_id"],
                "game_category": ["id"],
                "current_price_by_platform": ["game_id", "platform_id"]
            }

            self.insert_or_update_data(table, df, file_path, index_columns_map[table])
            self.logger.info(f"✅ {table}에 {len(df)}개 행 업로드(또는 수정) 완료")
    
    def delete_rows(self, table_name, df):
        from sqlalchemy import Table, MetaData, tuple_

        index_columns_map = {
            "category": ["id"],
            "platform": ["id"],
            "game_static": ["id"],
            "game_dynamic": ["game_id"],
            "game_category": ["id"],
            "current_price_by_platform": ["game_id", "platform_id"]
        }

        index_columns = index_columns_map.get(table_name)
        if not index_columns:
            self.logger.error(f"❌ {table_name}에 대한 삭제 키가 정의되지 않았습니다.")
            return

        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine)

        CHUNK_SIZE = 1000
        
        with self.engine.begin() as conn:
            keys = [tuple(row[col] for col in index_columns) for _, row in df.iterrows()]
            for i in range(0, len(keys), CHUNK_SIZE):
                chunk = keys[i:i + CHUNK_SIZE]
                self.logger.info(f"🗑️ {table_name}에서 {len(chunk)}개 행 삭제 중...")
                conn.execute(
                    table.delete().where(
                        tuple_(*[table.c[col] for col in index_columns]).in_(chunk)
                    )
                )
  
    def insert_or_update_data(self, table_name, df, file_path, index_columns):
        from sqlalchemy import Table, MetaData
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        df = df.drop(columns=[c for c in ["created_at", "updated_at", "deleted_at"] if c in df.columns], errors="ignore")
        df = df.astype(object).where(df.notnull(), None)
        
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine)

        CHUNK_SIZE = 1000

        with self.engine.begin() as conn:
            for i in range(0, len(df), CHUNK_SIZE):
                chunk_df = df.iloc[i:i + CHUNK_SIZE]
                self.logger.info(f"✅ {table_name}에 {len(chunk_df)}개 행 삽입(또는 수정) 중...")
                values = chunk_df.to_dict(orient="records")
                
                insert_stmt = pg_insert(table).values(values)
                update_cols = {c: insert_stmt.excluded[c] for c in chunk_df.columns if c not in index_columns}
                stmt = insert_stmt.on_conflict_do_update(
                    index_elements=index_columns,
                    set_=update_cols,
                )
                self.logger.info(f"{table_name}에 {len(values)}개 행 삽입(또는 수정) 중...")
                conn.execute(stmt)

        if file_path.exists():
            os.remove(file_path)
            