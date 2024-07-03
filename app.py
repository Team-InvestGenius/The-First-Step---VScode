from flask import Flask, jsonify
from flask_socketio import SocketIO
import websocket
import json
import pandas as pd
from datetime import datetime
import os
import mysql.connector
import schedule
import time
from threading import Thread

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")  # 모든 출처를 허용

API_KEY = '4bdb7d969e8f4525be72a5572a9ab1d7'
SYMBOLS = "AAPL"
DB_CONFIG = {
    'user': 'root',
    'password': '12345',
    'host': '127.0.0.1',
    'database': 'stock_data_db'
}

RECONNECT_ATTEMPTS = 2

def get_csv_file():
    today = datetime.now().strftime('%Y-%m-%d')
    return f'data/stock_data_{today}.csv'

def create_csv_file(file_path):
    if not os.path.exists(file_path):
        os.makedirs('data', exist_ok=True)
        pd.DataFrame(columns=['symbol', 'price', 'timestamp']).to_csv(file_path, index=False)

CSV_FILE = get_csv_file()
create_csv_file(CSV_FILE)

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
    global reconnect_attempts
    print("WebSocket closed")
    if reconnect_attempts < RECONNECT_ATTEMPTS:
        reconnect_attempts += 1
        print(f"Reconnecting... Attempt {reconnect_attempts}")
        time.sleep(5)
        start_websocket()
    else:
        print("Max reconnect attempts reached. Aggregating data.")
        aggregate_daily_data()

def on_open(ws):
    global reconnect_attempts
    reconnect_attempts = 0
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

    new_csv_file = get_csv_file()
    CSV_FILE = new_csv_file
    create_csv_file(CSV_FILE)

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
    reconnect_attempts = 0
    websocket_thread = Thread(target=start_websocket)
    websocket_thread.start()
    
    schedule_thread = Thread(target=schedule_aggregation)
    schedule_thread.start()
    
    socketio.run(app, debug=True, use_reloader=False)
