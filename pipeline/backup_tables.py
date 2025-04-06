import pandas as pd
from sqlalchemy import create_engine
from config import settings
from pathlib import Path

def backup_tables_to_csv(tables, output_dir = "data/processed", backup_dir = "data/backup"):
    engine = create_engine(settings.DB_URL)
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(backup_dir).mkdir(parents=True, exist_ok=True)
    
    for table in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", con=engine)
            output_path = Path(output_dir) / f"{table}.csv"
            backup_path = Path(backup_dir) / f"{table}.csv"
            df.to_csv(backup_path, index=False)
            df.to_csv(output_path, index=False)
            print(f"✅ {table} 데이터 저장 완료 → {output_path}")
            print(f"✅ {table} 백업 완료 → {backup_path}")
        except Exception as e:
            print(f"❌ {table} 백업 실패: {e}")
            
if __name__ == "__main__":
    tables_to_backup = [
        "category",
        "platform",
        "game_static",
        "game_dynamic",
        "game_category",
        "current_price_by_platform",
    ]
    
    backup_tables_to_csv(tables_to_backup)