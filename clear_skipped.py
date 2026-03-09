import sqlite3
from src import config

def clear_skipped():
    conn = sqlite3.connect(config.DB_PATH)
    c = conn.cursor()
    c.execute("SELECT count(*) FROM progress WHERE status='SKIPPED'")
    count = c.fetchone()[0]
    if count > 0:
        print(f"找到 {count} 筆被錯誤標記為查無資料 (SKIPPED) 的紀錄。")
        c.execute("DELETE FROM progress WHERE status='SKIPPED'")
        conn.commit()
        print("已成功清除這些紀錄。下次執行主程式時將會重新掃描這些缺失的年份與季度。")
    else:
        print("目前沒有任何 SKIPPED 的紀錄。")
    conn.close()

if __name__ == '__main__':
    clear_skipped()
