from flask import Flask, jsonify
import requests
import websocket
import json
import pandas as pd
from datetime import datetime
import os
import mysql.connector
from threading import Thread

app = Flask(__name__)

API_KEY = '4bdb7d969e8f4525be72a5572a9ab1d7'
SYMBOLS = "EUR/USD,USD/JPY,BTC/USD"
CSV_FILE = 'data/stock_data_test.csv'
DB_CONFIG = {
    'user': 'root',
    'password': '12345',
    'host': '127.0.0.1',
    'database': 'stock_data_db'
}

# CSV 파일이 없으면 생성합니다.
if not os.path.exists(CSV_FILE):
    os.makedirs('data', exist_ok=True)
    pd.DataFrame(columns=['symbol', 'price', 'timestamp']).to_csv(CSV_FILE, index=False)

# 콘솔에 로드되는 데이터 출력
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

# 에러 발생시 콘솔에 출력
def on_error(ws, error):
    print(error)

# 웹소켓이 닫힐 경우 콘솔에 출력
def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")

# 웹소켓이 다시 시작될 경우 콘솔에 출력
def on_open(ws):
    subscribe_message = {
        "action": "subscribe",
        "params": {
            "symbols": SYMBOLS
        }
    }
    ws.send(json.dumps(subscribe_message))

# 웹소켓 Reconnection 시도를 위한 함수  
def start_websocket():
    ws = websocket.WebSocketApp(f"wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

# 일일 집계 함수
def aggregate_daily_data():
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

@app.route('/')
def index():
    return jsonify({"status": "running", "data_file": CSV_FILE})

@app.route('/aggregate')
def aggregate():
    aggregate_daily_data()
    return jsonify({"status": "aggregated"})

if __name__ == "__main__":
    websocket_thread = Thread(target=start_websocket)
    websocket_thread.start()
    app.run(debug=True, use_reloader=False)
