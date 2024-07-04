import requests
import pandas as pd
import time
import os

def get_realtime_price(symbol, apikey):
    url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={apikey}"
    response = requests.get(url)
    data = response.json()
    if 'price' in data:
        return data['price']
    else:
        print(f"Error fetching price for {symbol}: {data.get('message', 'Unknown error')}")
        return None

def append_to_csv(data, filename):
    df = pd.DataFrame(data, columns=["Symbol", "Price", "Time"])
    if os.path.isfile(filename):
        df.to_csv(filename, mode='a', header=False, index=False)
    else:
        df.to_csv(filename, index=False)
    print(df)

# 사용자 설정
apikey = "4bdb7d969e8f4525be72a5572a9ab1d7"  # 발급받은 API 키 입력
symbols = ["000100:KRX"]  # 예시: 삼성전자
filename = "realtime_stock_prices.csv"

while True:
    data = []
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    for symbol in symbols:
        price = get_realtime_price(symbol, apikey)
        if price is not None:
            data.append([symbol, price, current_time])
    
    if data:
        append_to_csv(data, filename)
    time.sleep(10)  # 1분 간격으로 데이터 업데이트
