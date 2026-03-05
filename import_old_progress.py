#!/usr/bin/env python3
"""
import_old_progress.py
─────────────────────────────────────────────────────────────────
將舊爬蟲的 financial_progress.json 中已完成（status=done）的記錄，
匯入新爬蟲的 SQLite 進度資料庫（download_progress.sqlite），
讓新爬蟲自動跳過已下載的季度。

使用方式：
    python import_old_progress.py [--dry-run]

    --dry-run: 只顯示會匯入的數量，不實際寫入資料庫

舊資料位置：
    ./台灣上市櫃公司財報/data/financial_progress.json

新資料庫位置：
    ./data/processed/download_progress.sqlite
"""

import os
import re
import json
import sqlite3
import argparse
from datetime import datetime

# ── 路徑設定 ──────────────────────────────────────────────────────
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
OLD_PROGRESS_JSON = os.path.join(
    BASE_DIR, "台灣上市櫃公司財報", "data", "financial_progress.json"
)
NEW_DB_PATH       = os.path.join(
    BASE_DIR, "data", "processed", "download_progress.sqlite"
)

REPORT_TYPE = "full_pdf"  # 與 main.py 一致
NOW_ISO     = datetime.now().isoformat(timespec="seconds")


# ── SQLite DDL（確保表存在）───────────────────────────────────────
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS progress (
    stock_id    TEXT    NOT NULL,
    year        INTEGER NOT NULL,
    season      INTEGER NOT NULL,
    report_type TEXT    NOT NULL,
    status      TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL,
    PRIMARY KEY (stock_id, year, season, report_type)
);
"""


def parse_stock_id(key: str) -> str:
    """
    從 '2702_華園' 格式的 key 中取出股票代號 '2702'。
    支援帶有特殊字元的公司名稱（如 '2327_國巨*'）。
    """
    # key 格式固定為 "<stock_id>_<anything>"，stock_id 為純數字
    m = re.match(r'^(\d+)_', key)
    if m:
        return m.group(1)
    # fallback
    return key.split('_')[0]


def parse_season(season_str: str) -> int:
    """
    將 'Q1' / 'Q2' ... 轉換為數字 1 / 2 ...
    """
    m = re.match(r'Q(\d+)', season_str, re.IGNORECASE)
    if m:
        return int(m.group(1))
    raise ValueError(f"無法解析季度字串：{season_str!r}")


def load_done_records(json_path: str) -> list[tuple]:
    """
    讀取 financial_progress.json，回傳所有 status=done 的
    (stock_id, year, season) tuples。
    """
    print(f"讀取舊進度檔：{json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = []
    for company_key, years in data.items():
        stock_id = parse_stock_id(company_key)
        if not isinstance(years, dict):
            continue
        for year_str, seasons in years.items():
            if not isinstance(seasons, dict):
                continue
            try:
                year = int(year_str)
            except ValueError:
                continue
            for season_str, info in seasons.items():
                if not isinstance(info, dict):
                    continue
                if info.get("status") == "done":
                    try:
                        season = parse_season(season_str)
                    except ValueError as e:
                        print(f"  警告：{e}，跳過 {company_key} {year_str} {season_str}")
                        continue
                    records.append((stock_id, year, season))

    return records


def import_to_sqlite(records: list[tuple], db_path: str, dry_run: bool = False):
    """
    將 records 匯入 SQLite 資料庫。
    已存在且為 DONE 的記錄不覆蓋（使用 INSERT OR IGNORE）。
    """
    print(f"\n目標資料庫：{db_path}")
    print(f"準備匯入：{len(records)} 筆已完成記錄")

    if dry_run:
        print("[DRY RUN] 不實際寫入，試算完成。")
        return

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(_CREATE_TABLE_SQL)
    conn.commit()

    # 先查現有 DONE 記錄，計算本次新增數
    existing_done = set(
        row for row in conn.execute(
            "SELECT stock_id, year, season FROM progress WHERE status='DONE'"
        ).fetchall()
    )

    to_insert = [
        (stock_id, year, season, REPORT_TYPE, "DONE", NOW_ISO)
        for stock_id, year, season in records
        if (stock_id, year, season) not in existing_done
    ]

    print(f"已在資料庫中：{len(existing_done)} 筆 DONE 記錄")
    print(f"本次新增：{len(to_insert)} 筆")

    if to_insert:
        conn.executemany(
            "INSERT OR IGNORE INTO progress "
            "(stock_id, year, season, report_type, status, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            to_insert,
        )
        conn.commit()

    # 匯入後統計
    total     = conn.execute("SELECT COUNT(*) FROM progress").fetchone()[0]
    done      = conn.execute("SELECT COUNT(*) FROM progress WHERE status='DONE'").fetchone()[0]
    failed    = conn.execute("SELECT COUNT(*) FROM progress WHERE status='FAILED'").fetchone()[0]
    skipped   = conn.execute("SELECT COUNT(*) FROM progress WHERE status='SKIPPED'").fetchone()[0]

    conn.close()

    print(f"\n匯入完成！資料庫目前狀態：")
    print(f"  總計={total} | DONE={done} | FAILED={failed} | SKIPPED={skipped}")


def main():
    parser = argparse.ArgumentParser(
        description="將舊財報進度 JSON 匯入新爬蟲的 SQLite 資料庫"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="只計算數量，不實際寫入資料庫"
    )
    parser.add_argument(
        "--json", default=OLD_PROGRESS_JSON,
        help=f"舊進度 JSON 路徑（預設：{OLD_PROGRESS_JSON}）"
    )
    parser.add_argument(
        "--db", default=NEW_DB_PATH,
        help=f"新爬蟲 SQLite 路徑（預設：{NEW_DB_PATH}）"
    )
    args = parser.parse_args()

    if not os.path.exists(args.json):
        print(f"[錯誤] 找不到舊進度檔：{args.json}")
        return

    records = load_done_records(args.json)
    import_to_sqlite(records, args.db, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
