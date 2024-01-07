# config.py

# 需要获取数据的产品ID
instId = 'FET-USDT-SWAP'

# 设置时间粒度
bar = '5m'

# 设置开始和结束时间的字符串
start_time_str = '2023-12-01 00:00:00'
end_time_str = '2024-01-07 16:10:00'

# 运行逻辑
if __name__ == "__main__":
    from data_fetcher import run_fetcher
    run_fetcher(instId, bar, start_time_str, end_time_str)
