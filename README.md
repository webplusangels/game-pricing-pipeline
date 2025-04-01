# 프로젝트 구조

```plain
game_data_pipeline/
├── main.py                    # 전체 파이프라인 실행 스크립트
├── config/
│   ├── settings.py            # 환경 변수 로드, 상수 정의
│   └── .env                   # API 키, DB 접속 정보 등
├── data/
│   ├── raw/                   # 원본 API / 크롤링 결과
│   ├── processed/             # 정제된 결과
│   ├── error/                 # 에러 결과
│   └── cache/                 # 캐시 저장용
├── log/
│   ├── fetcher/               # 수집 로그
│   └── main/                  # 전체 로그
├── pipeline/
│   ├── __init__.py
│   ├── fetch_itad_id.py
│   ├── fetch_itad_price.py
│   ├── fetch_steam_active_player.py
│   ├── fetch_steam_detail.py
│   ├── fetch_steam_list.py
│   ├── fetch_steam_review.py
│   ├── filter_list.py
│   ├── process_data.py
│   └── save_to_rds.py
├── db/
│   ├── ddl_full.sql           # 데이터베이스 모든 테이블 정의
│   └── ddl.sql                # 테이블 정의
├── util/
│   ├── logger.py              # 공통 로거
│   ├── io_helper.py           # 파일 입출력, 캐시
│   └── retry.py               # 재시도 로직 등
│   └── should_collect.py      # 캐싱 정책
├── requirements.txt           # 의존성
├── pytest.ini                 # 테스트 설정
└── test/                      # 유닛 테스트

```
