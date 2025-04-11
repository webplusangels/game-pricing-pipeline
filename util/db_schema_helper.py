from sqlalchemy import create_engine, text
import pandas as pd

def fetch_not_null_columns(engine):
    """
    DB의 모든 public 테이블에 대해 NOT NULL 제약이 걸린 컬럼들을 딕셔너리 형태로 반환
    """
    query = """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND is_nullable = 'NO'
          AND table_name NOT LIKE 'pg_%'
          AND table_name NOT LIKE 'sql_%'
    """
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchall()
        
    not_null_map = {}
    for table_name, column_name in result:
        not_null_map.setdefault(table_name, []).append(column_name)
        
    return not_null_map

def drop_null_required_fields(table_name, df, not_null_map):
    """
    특정 테이블에 대해 NOT NULL 컬럼 기준으로 결측값이 있는 행을 제거
    """
    required_cols = not_null_map.get(table_name, [])
    existing_required_cols = [col for col in required_cols if col in df.columns]
    if not existing_required_cols:
        print(f"[경고] {table_name} 테이블의 NOT NULL 컬럼 정보를 찾을 수 없습니다.")
        return df
    return df.dropna(subset=existing_required_cols)