import os
import requests
import csv
import time
from datetime import datetime

# real_time 으로 비트코인, 이더리움, 리플 가져오는 함수 
def get_crypto_prices_in_krw(crypto_ids, api_key):
    url = 'https://api.coingecko.com/api/v3/simple/price'
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": api_key
    }
    params = {
        'ids': ','.join(crypto_ids),
        'vs_currencies': 'krw'
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json()

# csv 파일에 10분에 한번씩 업데이트
def write_to_csv(data, filename='./data/crypto_data.csv'):
    fieldnames = ['timestamp', 'crypto', 'price_krw']
    file_exists = os.path.isfile(filename)

    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
        
        for crypto, price_info in data.items():
            writer.writerow({
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'crypto': crypto,
                'price_krw': price_info['krw']
            })

api_key = 'CG-xc9KizrBdzQEjwCE776DeTC9'
crypto_ids = ['bitcoin', 'ethereum', 'ripple']

while True:
    prices = get_crypto_prices_in_krw(crypto_ids, api_key)
    write_to_csv(prices)
    time.sleep(30)
