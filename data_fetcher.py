# data_fetcher.py

import requests
import pandas as pd
from datetime import datetime
import time
import pytz

# API的URL
url = 'https://www.okx.com/api/v5/market/history-candles'

def run_fetcher(instId, bar, start_time_str, end_time_str):
    # 将字符串时间转换为Unix时间戳（毫秒级）
    start_time_unix = int(datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S').timestamp()) * 1000
    end_time_unix = int(datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S').timestamp()) * 1000

    # 初始化一个空列表来存储各个candle的数据
    data = []

    # 分页的时间戳参数
    after = str(end_time_unix)

    # 限速相关，每2秒钟最多请求20次
    rate_limit = 20
    time_interval = 2
    requests_in_interval = 0
    time_of_first_request = time.time()

    with open('1.txt', 'w') as f:
        while True:
            params = {
                'instId': instId,
                'after': after,
                'bar': bar,
                'limit': '100'
            }

            response = requests.get(url, params=params)
            f.write(f"Request URL: {response.url}\n")
            f.write(f"Request Parameters: {params}\n")

            if response.status_code == 200:
                f.write(f"Raw Response Data: {response.text}\n")
                response_data = response.json()
                if response_data['code'] == '0':
                    candles_data = response_data['data']
                    if not candles_data or int(candles_data[-1][0]) < start_time_unix:
                        break
                    for candle in candles_data:
                        if int(candle[0]) >= start_time_unix:
                            data.append(candle)
                    after = str(int(candles_data[-1][0]) - 5 * 60 * 1000)
                    current_data_time = datetime.fromtimestamp(int(after)/1000).strftime('%Y-%m-%d %H:%M:%S')
                    print(f"Fetched data up to {current_data_time}")
                else:
                    print(f"Error fetching data: {response_data['msg']}")
                    break
            else:
                print(f"Error making request: {response.status_code}")
                break
            requests_in_interval += 1
            if requests_in_interval == rate_limit:
                time_elapsed = time.time() - time_of_first_request
                if time_elapsed < time_interval:
                    time.sleep(time_interval - time_elapsed)
                requests_in_interval = 0
                time_of_first_request = time.time()

    columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'volume_ccy', 'volume_quote_ccy']
    df = pd.DataFrame([[int(candle[0]), *candle[1:8]] for candle in data], columns=columns)
    beijing_tz = pytz.timezone('Asia/Shanghai')
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(beijing_tz)
    csv_filename = f"{instId}_{bar}_data.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Data saved to {csv_filename}")
    print(f"Data collection complete. Fetched data from {start_time_str} to {end_time_str}.")

if __name__ == "__main__":
    print("This script is intended to be imported, not run directly.")
