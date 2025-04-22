# 프로젝트 구조

```plain
game_data_pipeline/
├── README.md
├── config
│   ├── settings.py
│   └── settings.py.example
├── data
│   ├── backup
│   │   ├── category.csv
│   │   ├── current_price_by_platform.csv
│   │   ├── game_category.csv
│   │   ├── game_dynamic.csv
│   │   ├── game_static.csv
│   │   └── platform.csv
│   ├── cache
│   │   ├── all_apps_cache.json
│   │   ├── ap_status_cache.json
│   │   ├── detail_status_cache.json
│   │   ├── itad_id_status_cache.json
│   │   ├── itad_price_status_cache.json
│   │   ├── review_status_cache.json
│   │   └── steamcharts_status_cache.json
│   ├── error
│   │   ├── failed_detail_ids.csv
│   │   └── itad_price_failed_ids.csv
│   ├── processed
│   └── raw
│       ├── all_app_list.csv
│       ├── common_ids.csv
│       ├── free_steam_list.csv
│       ├── itad_game_ids.csv
│       ├── itad_game_prices.csv
│       ├── paid_steam_list.csv
│       ├── steam_game_active_player.csv
│       ├── steam_game_detail_original.csv
│       ├── steam_game_detail_parsed.csv
│       ├── steam_game_reviews.csv
│       └── steamcharts_top_games.csv
├── db
│   ├── ddl.sql
│   ├── ddl_full.sql
│   └── ddl_initialize.sql
├── log
│   ├── db_uploader
│   │   └── DBUploader.log
│   ├── fetcher
│   │   ├── itad_id_fetcher.log
│   │   ├── itad_price_fetcher.log
│   │   ├── steam_active_player_fetcher.log
│   │   ├── steam_detail_fetcher.log
│   │   ├── steam_list_fetcher.log
│   │   └── steam_review_fetcher.log
│   ├── main
│   │   └── main_pipeline.log
│   └── process_data
│       └── process_data.log
├── main.py
├── pipeline
│   ├── __init__.py
│   ├── backup_tables.py
│   ├── base_fetcher.py
│   ├── fetch_itad_id.py
│   ├── fetch_itad_price.py
│   ├── fetch_steam_active_player.py
│   ├── fetch_steam_detail.py
│   ├── fetch_steam_list.py
│   ├── fetch_steam_review.py
│   ├── filter_list.py
│   ├── process_data.py
│   └── save_to_db.py
├── pytest.ini
├── requirements.txt
├── test
│   ├── test_itad_id_fetcher.py
│   ├── test_itad_price_fetcher.py
│   ├── test_steam_active_player_fetcher.py
│   ├── test_steam_detail_fetcher.py
│   ├── test_steam_list_fetcher.py
│   └── test_steam_review_fetcher.py
├── test_data
│   ├── backup
│   ├── cache
│   │   ├── ap_status_cache.json
│   │   ├── detail_status_cache.json
│   │   ├── review_status_cache.json
│   │   └── steamcharts_status_cache.json
│   ├── error
│   │   ├── failed_ap_ids.csv
│   │   └── failed_detail_ids.csv
│   ├── log
│   │   └── itad_data_fetcher.log
│   ├── processed
│   └── raw
│       ├── all_app_list.csv
│       ├── common_ids.csv
│       ├── steam_app_ids.csv
│       ├── steam_game_active_player.csv
│       ├── steam_game_detail_original.csv
│       ├── steam_game_detail_parsed.csv
│       ├── steam_game_reviews.csv
│       └── steamcharts_top_games.csv
└── util
    ├── cache_manager.py
    ├── data_insert.py
    ├── db_schema_helper.py
    ├── io_helper.py
    ├── logger.py
    └── rate_limit_manager.py

```

# 주요 디렉터리 설명

- `config/`
  
  파이프라인 환경 설정 파일을 관리 (settings.py, 예시 파일 제공)

- `data/`
  
  수집된 원본 데이터 (`raw`), 가공된 데이터 (`processed`), 오류 데이터 (`error`), 캐시 파일 데이터 (`cache`), 백업 파일 데이터 (`backup`)를 저장

- `db/`
  
  데이터베이스 테이블 생성을 위한 DDL 스크립트 관리

- `log/`
  
  수집. 가공, 업로드 과정별 로그 파일을 기록

- `pipeline/`

  데이터 수집, 처리, 저장을 담당하는 주요 파이프라인 코드 포함

- `test/`

  각 fetcher 모듈에 대한 유닛 테스트 코드 (미완)

- `test_data/`

  테스트 데이터 셋 저장

- `util/`

  공통적으로 사용되는 헬퍼 모듈들을 모아놓은 디렉터리 (캐시 관리, DB 스키마, 로깅 등)

- `main.py`

  전체 파이프라인을 실행하는 진입점 파일

# 빠른 시작

```bash
# 패키지 설치
pip install -r requirements.txt

# 환경 설정 (자세한건 아래 참조)
cp config/settings.py.example config/settings.py
# settings.py DB 정보에 맞춰 수정

# 파이프라인 실행
python main.py
```

# 주요 기능 및 모듈 설명

- Steam 앱 리스트 수집 및 필터링
- Steam 게임 상세 정보 및 리뷰 수집
- 활성 플레이어 수 데이터 수집
- Is There Any Deal API를 통한 가격 정보 수집
- 데이터 정제 및 PostgreSQL 데이터베이스 저장
- 실패한 ID 재시도 및 캐시 관리 기능
- 로그 기록 및 백업 기능

### 파이프라인 모듈
- `fetch_steam_list.py` : Steam 앱 리스트 수집
- `fetch_steam_detail.py` : 게임 상세 정보 수집
- `fetch_steam_review.py` : 게임 리뷰 데이터 수집
- `fetch_steam_active_player.py` : 활성 플레이어 수 데이터 수집
- `fetch_itad_id.py` : ITAD에 등록된 게임 ID 수집
- `fetch_itad_price.py` : ITAD를 통한 가격 데이터 수집
- `process_data.py` : 수집된 데이터 가공 및 정리
- `save_to_db.py` : 최종 데이터베이스 저장

# 데이터 처리 흐름

```plain
[Steam API / SteamCharts Scraping]
        ↓
[Is There Any Deal API]
        ↓
[Raw Data 저장 (data/raw)]
        ↓
[필터링 및 가공 (pipeline/filter_list.py, process_data.py)]
        ↓
[가공 데이터 저장 (data/processed)]
        ↓
[최종 정제 및 PostgreSQL DB 저장]
```

# 환경 설정 가이드

### 1. 개요

이 프로젝트는 `.env` 파일을 사용하여 API 키, 데이터베이스 접속 정보, 파일 경로 등 주요 설정값을 관리합니다.
`config/settings.py`는 `.env` 파일을 읽어와 환경 변수로 설정을 적용하고, 누락된 값은 기본값(default)을 사용합니다.

### 2. 설정 방법

1. `config/settings.py.example` 참고해 `.env` 생성

```bash
cp config/settings.py.example config/settings.py
cp .env.example .env
```

2. `.env` 파일을 열어 필요한 정보를 입력

3. 실행 시에 ENV 변수를 설정해 어떤 데이터베이스 설정을 사용할지 지정

  - 기본값 `local`이 아닌 `dev`와 `prod`는 데이터 수집 파이프라인 미실행
  - local 기본값 실행 후에 성공적으로 업데이트 시 dev나 prod DB를 업데이트하는 방향으로 설계
 
  - 개발/운영 환경 예시
  
  ```bash
  ENV=dev python main.py
  ```
  
  ```bash
  ENV=prod python main.py
  ```

### 3. 환경변수 설명

steam_api_key: Steam API 요청용 키
webapi_token: Steam Web API 사용 토큰
itad_key: ITAD API 요청용 키
s3_bucket	(선택): S3에 파일 업로드 시 사용할 버킷 이름
aws_region: AWS 리전 정보 (예: ap-northeast-2)
aws_access_key_id: AWS Access Key
aws_secret_access_key: AWS Secret Access Key
local_db_host: 로컬 환경 DB 서버 주소
dev_db_host: 개발 환경 DB 서버 주소
prod_db_host: 운영 환경 DB 서버 주소
list_dir: 전체 게임 리스트 파일 경로
paid_list_dir: 유료 게임 리스트 파일 경로
free_list_dir: 무료 게임 리스트 파일 경로
env: 기본 환경 설정 (local, dev, prod)

