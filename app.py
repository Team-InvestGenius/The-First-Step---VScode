from flask import Flask, jsonify
import requests
import websocket
import json
import pandas as pd
from datetime import datetime, timedelta
import os
import mysql.connector
import schedule
import time

app = Flask(__name__)

API_KEY = '4bdb7d969e8f4525be72a5572a9ab1d7'
SYMBOLS = "AAPL,QQQ,ABML,IXIC,VFIAX,005930:KRX"
DB_CONFIG = {
    'user': 'root',
    'password': '12345',
    'host': '127.0.0.1',
    'database': 'stock_data_db'
}

# 현재 날짜를 기반으로 파일명을 설정합니다.
def get_csv_file():
    today = datetime.now().strftime('%Y-%m-%d')
    return f'data/stock_data_{today}.csv'

# CSV 파일이 없으면 생성합니다.
CSV_FILE = get_csv_file()
if not os.path.exists(CSV_FILE):
    os.makedirs('data', exist_ok=True)
    pd.DataFrame(columns=['symbol', 'price', 'timestamp']).to_csv(CSV_FILE, index=False)

def on_message(ws, message):
    msg = json.loads(message)
    if 'price' in msg:
        print(msg)
        record = {
            'symbol': msg['symbol'],
            'price': msg['price'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        df = pd.DataFrame([record])
        df.to_csv(CSV_FILE, mode='a', header=False, index=False)

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

def on_open(ws):
    subscribe_message = {
        "action": "subscribe",
        "params": {
            "symbols": SYMBOLS
        }
    }
    ws.send(json.dumps(subscribe_message))

def start_websocket():
    ws = websocket.WebSocketApp(f"wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

def aggregate_daily_data():
    global CSV_FILE
    
    if not os.path.exists(CSV_FILE):
        return
    
    df = pd.read_csv(CSV_FILE)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date
    
    daily_summary = df.groupby(['symbol', 'date']).agg({
        'price': 'mean',
        'timestamp': 'count'
    }).rename(columns={'price': 'average_price', 'timestamp': 'total_volume'}).reset_index()

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for _, row in daily_summary.iterrows():
        cursor.execute("""
            INSERT INTO daily_stock_summary (symbol, date, average_price, total_volume)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            average_price = VALUES(average_price),
            total_volume = VALUES(total_volume)
        """, (row['symbol'], row['date'], row['average_price'], row['total_volume']))
    
    conn.commit()
    cursor.close()
    conn.close()

    # 새로운 파일명으로 업데이트
    new_csv_file = get_csv_file()
    CSV_FILE = new_csv_file
    pd.DataFrame(columns=['symbol', 'price', 'timestamp']).to_csv(CSV_FILE, index=False)

def schedule_aggregation():
    schedule.every().day.at("23:59:59").do(aggregate_daily_data)
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.route('/')
def index():
    return jsonify({"status": "running", "data_file": CSV_FILE})

@app.route('/aggregate')
def aggregate():
    aggregate_daily_data()
    return jsonify({"status": "aggregated"})

if __name__ == "__main__":
    from threading import Thread
    websocket_thread = Thread(target=start_websocket)
    websocket_thread.start()
    
    # 스케줄링을 위한 스레드 시작
    schedule_thread = Thread(target=schedule_aggregation)
    schedule_thread.start()
    
    app.run(debug=True, use_reloader=False)
