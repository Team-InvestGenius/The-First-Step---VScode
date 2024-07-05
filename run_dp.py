from modules.data.twelve_data import TwelveData
from modules.data.data_pipeline import DataPipeline
from data.twelve_config import TWELVE_STOCKS

api_key = "931ba6aa27e940008a4ac7e406991a32"

if not api_key:
    raise ValueError("환경 변수 'TWELVE_DATA_API_KEY'가 설정되지 않았습니다.")

for _, config in TWELVE_STOCKS.items():
    td = TwelveData(api_key)
    print(config)
    pipeline = DataPipeline(td, **config)

    # 초기 30일 데이터 가져오기
    pipeline.fetch_start(days=30)

    # 최신 데이터 출력
    pipeline.print_latest_data()

    # 연속적인 데이터 수집 (5분마다)
    pipeline.continuous_fetch(interval_seconds=2)
