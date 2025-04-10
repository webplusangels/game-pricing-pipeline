import json
import pandas as pd
from pathlib import Path
import boto3
import os
import requests
from botocore.exceptions import BotoCoreError, ClientError

from config import settings

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
        
def is_running_on_ec2():
    try:
        response = requests.get(
            "http://169.254.169.254/latest/meta-data/",
            timeout=0.1,
        )
        return response.status_code == 200
    except requests.RequestException:
        return False
    
def get_s3_client():
    """
    S3 클라이언트 생성
    """
    if is_running_on_ec2():
        s3 = boto3.client('s3')
        return s3
    else:
        return boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )

def upload_to_s3(local_path, s3_key, remove_after=False):
    """
    S3에 파일 업로드

    :param local_path: 업로드할 로컬 파일 경로
    :param s3_key: S3 버킷 내의 경로
    :param remove_after: 업로드 후 로컬 파일 삭제 여부
    """
    s3 = get_s3_client()
    bucket = 'warab-pipeline'

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File not found: {local_path}")

    try:
        s3.upload_file(local_path, bucket, s3_key)
        print(f"{local_path} → s3://{bucket}/{s3_key} 업로드 완료")

        if remove_after:
            os.remove(local_path)
            print(f"🗑️ 로컬 파일 삭제: {local_path}")
        
        return True

    except (BotoCoreError, ClientError) as e:
        print(f"[S3 ERROR] ❌ {local_path}에서 s3://{bucket}/{s3_key}로 업로드하는데 실패했습니다: {e}")
        return False
    
def download_from_s3(s3_key, local_path):
    """
    S3에서 파일 다운로드

    :param s3_key: S3 버킷 내의 경로
    :param local_path: 다운로드할 로컬 파일 경로
    """
    s3 = get_s3_client() 
    bucket = 'warab-pipeline'

    try:
        s3.download_file(bucket, s3_key, local_path)
        print(f"s3://{bucket}/{s3_key} → {local_path} 다운로드 완료")
        return True

    except (BotoCoreError, ClientError) as e:
        print(f"[S3 ERROR] ❌ s3://{bucket}/{s3_key}에서 {local_path}로 다운로드하는데 실패했습니다: {e}")
        return False