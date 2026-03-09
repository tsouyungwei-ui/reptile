#!/usr/bin/env python3
"""
split_workload.py
─────────────────────────────────────────────────────────────────
分析目前進度，找出剩餘未完成的公司，將它們分成 N 份（預設 5 份），
並將每份的股票代號寫入 data/processed/node_1.txt ~ node_N.txt。

把整個資料夾複製給 N 台電腦前，先在主電腦執行此腳本一次即可。
每台電腦執行時只需指定自己的 --node 編號。

使用方式：
    python split_workload.py            # 分成 5 份（預設）
    python split_workload.py --parts 3  # 分成 3 份
    python split_workload.py --dry-run  # 只顯示分配，不寫入檔案

產生檔案：
    data/processed/node_1.txt ... data/processed/node_N.txt
    每個 .txt 一行一個股票代號，直接供 main.py --node 讀取。
"""

import os
import sqlite3
import argparse
import pandas as pd
from src import config

# ── 路徑設定 ──────────────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR  = config.PROCESSED_DIR
DB_PATH        = config.DB_PATH
STOCK_LIST_CSV = config.STOCK_LIST_CSV
REPORT_TYPE    = "full_pdf"


# ── 輔助函式 ─────────────────────────────────────────────────────

def load_done_stock_ids(db_path: str) -> set[str]:
    """
    從 SQLite 中找出「所有季度都已 DONE」的股票代號集合。
    用於排除已完成公司，定義出剩餘工作。
    """
    if not os.path.exists(db_path):
        print(f"[提示] 找不到資料庫 {db_path}，將視所有公司為未完成。")
        return set()

    conn = sqlite3.connect(db_path)

    # 取得所有有記錄的公司，以及其中有多少非 DONE 的紀錄
    rows = conn.execute("""
        SELECT stock_id,
               COUNT(*) AS total,
               SUM(CASE WHEN status = 'DONE' THEN 1 ELSE 0 END) AS done_cnt,
               SUM(CASE WHEN status != 'DONE' AND status != 'SKIPPED' THEN 1 ELSE 0 END) AS pending_cnt
        FROM progress
        WHERE report_type = ?
        GROUP BY stock_id
    """, (REPORT_TYPE,)).fetchall()
    conn.close()

    # 一間公司「完成」的條件：所有記錄都是 DONE 或 SKIPPED，且至少有一筆 DONE
    done_ids = set()
    for stock_id, total, done_cnt, pending_cnt in rows:
        if pending_cnt == 0 and done_cnt > 0:
            done_ids.add(str(stock_id))

    return done_ids


def load_remaining_stocks(stock_csv: str, done_ids: set[str]) -> pd.DataFrame:
    """
    從股票清單 CSV 中，過濾出「尚未完成」的公司。
    """
    if not os.path.exists(stock_csv):
        raise FileNotFoundError(
            f"找不到股票清單 CSV：{stock_csv}\n"
            "請先執行：python -m src.main --fetch-list"
        )

    df = pd.read_csv(stock_csv, dtype={"stock_id": str})
    df["stock_id"] = df["stock_id"].str.strip()

    remaining = df[~df["stock_id"].isin(done_ids)].reset_index(drop=True)
    return remaining


def split_round_robin(df: pd.DataFrame, n_parts: int) -> list[pd.DataFrame]:
    """
    以 round-robin 方式將 DataFrame 分成 n_parts 份，
    確保每份數量差距最多 1 間。
    """
    parts = [[] for _ in range(n_parts)]
    for i, row in enumerate(df.itertuples(index=False)):
        parts[i % n_parts].append(row)

    return [
        pd.DataFrame(p, columns=df.columns) if p else pd.DataFrame(columns=df.columns)
        for p in parts
    ]


def write_node_files(parts: list[pd.DataFrame], output_dir: str):
    """將每份公司清單寫入 node_1.txt ... node_N.txt"""
    os.makedirs(output_dir, exist_ok=True)
    for i, part_df in enumerate(parts, start=1):
        path = os.path.join(output_dir, f"node_{i}.txt")
        with open(path, "w", encoding="utf-8") as f:
            for sid in part_df["stock_id"]:
                f.write(sid + "\n")
        print(f"  已寫入：{path}（{len(part_df)} 間公司）")


# ── 主程式 ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="將剩餘爬取工作分割成 N 份，供多台電腦並行執行"
    )
    parser.add_argument(
        "--parts", type=int, default=5,
        help="要分成幾份（預設：5）"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="只顯示分配結果，不寫入檔案"
    )
    parser.add_argument(
        "--db", default=DB_PATH,
        help=f"進度資料庫路徑（預設：{DB_PATH}）"
    )
    parser.add_argument(
        "--csv", default=STOCK_LIST_CSV,
        help=f"股票清單 CSV 路徑（預設：{STOCK_LIST_CSV}）"
    )
    args = parser.parse_args()

    print("=" * 60)
    print(f"  工作量分割工具（分成 {args.parts} 份）")
    print("=" * 60)

    # Step 1：找出已完成公司
    print("\n[1/3] 分析進度資料庫...")
    done_ids = load_done_stock_ids(args.db)
    print(f"      已完成公司：{len(done_ids)} 間")

    # Step 2：找出剩餘公司
    print("\n[2/3] 載入股票清單，篩選剩餘公司...")
    try:
        remaining_df = load_remaining_stocks(args.csv, done_ids)
    except FileNotFoundError as e:
        print(f"\n[錯誤] {e}")
        return

    total_remaining = len(remaining_df)
    print(f"      剩餘待爬公司：{total_remaining} 間")

    if total_remaining == 0:
        print("\n所有公司均已完成，無需分割！")
        return

    # Step 3：分割並輸出
    print(f"\n[3/3] 以 round-robin 分成 {args.parts} 份...")
    parts = split_round_robin(remaining_df, args.parts)

    print()
    print("─" * 60)
    print("  分配結果與建議執行指令")
    print("─" * 60)

    for i, part_df in enumerate(parts, start=1):
        ids_preview = ", ".join(part_df["stock_id"].tolist()[:5])
        if len(part_df) > 5:
            ids_preview += f" ... 等 {len(part_df)} 間"
        print(f"\n  Node {i}/{args.parts}：共 {len(part_df)} 間公司")
        print(f"    範圍預覽：{ids_preview}")
        print(f"    ▶ 執行指令：python -m src.main --all --node {i}")

    print()
    print("─" * 60)

    if args.dry_run:
        print("\n[DRY RUN] 未寫入檔案。")
    else:
        print("\n正在寫入 node 分配檔...")
        write_node_files(parts, PROCESSED_DIR)
        print(f"\n完成！node_1.txt ～ node_{args.parts}.txt 已儲存至：")
        print(f"  {PROCESSED_DIR}")
        print("\n將資料夾複製給各台電腦後，各台電腦執行對應指令即可：")
        for i in range(1, args.parts + 1):
            print(f"  電腦 {i}：python -m src.main --all --node {i}")

    print()


if __name__ == "__main__":
    main()
