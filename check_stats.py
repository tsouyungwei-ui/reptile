import sqlite3
import pandas as pd
from datetime import datetime, timedelta

db_path = "data/processed/download_progress.sqlite"
conn = sqlite3.connect(db_path)

# 查詢最近一小時內的紀錄
query = """
    SELECT status, COUNT(*) as count 
    FROM progress 
    WHERE updated_at >= strftime('%Y-%m-%d %H:%M:%f', datetime('now', '-1 hour', 'localtime'))
    GROUP BY status
"""
df = pd.read_sql_query(query, conn)
print("--- 最近一小時內的爬取狀態 ---")
print(df.to_string(index=False))

# 查詢最新的 5 筆成功紀錄
query_done = """
    SELECT stock_id, year, season, updated_at 
    FROM progress 
    WHERE status = 'DONE'
    ORDER BY updated_at DESC 
    LIMIT 5
"""
df_done = pd.read_sql_query(query_done, conn)
print("\n--- 最新 5 筆成功下載紀錄 ---")
print(df_done.to_string(index=False))

# 查詢最新的 5 筆失敗紀錄
query_failed = """
    SELECT stock_id, year, season, updated_at 
    FROM progress 
    WHERE status = 'FAILED'
    ORDER BY updated_at DESC 
    LIMIT 5
"""
df_failed = pd.read_sql_query(query_failed, conn)
print("\n--- 最新 5 筆失敗紀錄 ---")
if len(df_failed) > 0:
    print(df_failed.to_string(index=False))
else:
    print("目前沒有失敗紀錄！")

conn.close()
