import json
import pandas as pd
from pathlib import Path

def save_csv(df: pd.DataFrame, path: str, index=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_csv(path, index=index)
        return True
    except Exception as e:
        print(f"❌ {path} 저장 실패: {e}")
        return False

def load_csv(path: str) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        print(f"⚠️ 파일을 찾을 수 없습니다: {path}")
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"❌ {path} 로드 실패: {e}")
        return pd.DataFrame()
    
def load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️ 파일을 찾을 수 없습니다: {path}")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ JSON 디코딩 오류 ({path}): {e}")
        return {}
    except Exception as e:
        print(f"❌ {path} 로드 실패: {e}")
        return {}

def save_json(path, data):
    # 중복 키 제거 (가장 마지막 값 유지)
    if isinstance(data, dict):
        data = {k: v for k, v in data.items()}
        
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# from utils.io_helper import save_csv, load_csv

# save_csv(df, "data/raw/steam_games.csv")
# df = load_csv("data/processed/game_static.csv")

# # 캐시 불러오기
# self.status_cache = load_json(self.CACHE_FILE)

# # 체크포인트 저장
# save_json(self.CACHE_FILE, self.status_cache)