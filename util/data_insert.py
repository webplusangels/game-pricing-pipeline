# 데이터 넣어야 할 때 (데이터 삭제 후 재삽입)

import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

from config import settings

DB_URL = settings.DB_URL

engine = create_engine(DB_URL)
data_dir = Path("data/backup")

# 테이블별 CSV 파일명 매핑
tables = {
    'category': 'category.csv',
    'platform': 'platform.csv',
    'game_static': 'game_static.csv',
    'game_dynamic': 'game_dynamic.csv',
    'game_category': 'game_category.csv',
    'current_price_by_platform': 'current_price_by_platform.csv',
}

# ddl 스크립트 실행
def execute_sql_script(engine, script_path):
    with engine.connect() as connection:
        with open(script_path, 'r', encoding='utf-8') as file:
            sql_script = file.read()
            connection.execute(sql_script)

execute_sql_script(engine, 'db/ddl_initialize.sql')

# 데이터 삽입
for table, file in tables.items():
    file_path = data_dir / file
    if file_path.exists():
        df = pd.read_csv(file_path)
        df.to_sql(table, con=engine, if_exists='append', index=False)
        print(f"✅ {table} 삽입 완료")
    else:
        print(f"⚠️ {file} 파일 없음 → {table} 건너뜀")