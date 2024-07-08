# The-First-Step
LLM 기반의 투자유형 추천 시스템

## Data Pipelines

### twelveData
- 사용법 
  - TBD 

### Yahoo Finance
-  사용법
    - _configs/yahoo_config.ini_ 에 다운로드 받고자 하는 주식 symbol(ticker) 및 거래소 작성
    - _python run_yahoo.py_  를 통해 원하는 경로에 파일 다운로드 가능 
    - _python load_yahoo.py_ 를 수행하여 다운로드 된 일일 가격 데이터를 메모리애 로드 가능


## LLM 
- TBD

## Strategy
- TBD 

### insert_yahoo_data_to_db.py
- 사용법
  - pip install pymysql 먼저하기
  - python insert_yahoo_data_to_db.py 를 통해 DB로 저장
