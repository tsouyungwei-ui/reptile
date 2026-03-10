"""
main.py
─────────────────────────────────────────────────────────────────
台灣上市櫃公司財務報告 PDF 下載器 — 主程式入口

目標：完整下載所有公司歷年四份財報 PDF（不解析，保留原始檔案）
輸出：data/pdfs/{stock_id}/{ce_year}/Q{season}.pdf

使用方式：
  # 先抓（或更新）全台股票清單
  python -m src.main --fetch-list

  # 下載單一公司（自動從其上市年份開始）
  python -m src.main --stock 2330

  # 批量下載所有公司
  python -m src.main --all

  # 批量下載，只跑前 N 間（測試用）
  python -m src.main --all --limit 5

  # 重試所有失敗的項目
  python -m src.main --retry-failed

  # 手動指定年份
  python -m src.main --stock 2330 --start-year 2010 --end-year 2024
"""

import os
import time
import random
import argparse
import logging
import requests
import pandas as pd
from datetime import datetime

from src.stock_list import StockListFetcher
from src.pdf_downloader import PdfDownloader
from src.progress_tracker import ProgressTracker
from src import config

# ── Logging 設定 ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)

# PDF 進度追蹤使用的 report_type 固定字串
REPORT_TYPE = 'full_pdf'


# ── 計算可查詢的最新季度 ──────────────────────────────────────────

def build_quarter_list(start_year: int, end_year: int) -> list[tuple[int, int]]:
    """
    建立所有需要下載的 (ce_year, season) 清單，
    自動排除尚未到達的未來季度。
    """
    now = datetime.now()

    quarters = []
    for y in range(start_year, end_year + 1):
        for s in range(1, 5):
            # 推算該財報公告的「合理最早公開月份」
            # Q1 約 5 月公布、Q2 約 8 月、Q3 約 11 月、Q4 約隔年 3 月或 4 月
            if s == 1:
                pub_year, pub_month = y, 5
            elif s == 2:
                pub_year, pub_month = y, 8
            elif s == 3:
                pub_year, pub_month = y, 11
            else: # s == 4
                pub_year, pub_month = y + 1, 3
            
            # 如果目前的時間早於該財報的合理發表月份，就不產生該季度的檢查
            if (now.year, now.month) < (pub_year, pub_month):
                continue
                
            quarters.append((y, s))
            
    return quarters


# ── 下載單一季度 ──────────────────────────────────────────────────

def download_quarter(stock_id: str, ce_year: int, season: int,
                     market_type: str, tracker: ProgressTracker,
                     retry_failed: bool = False,
                     session: requests.Session = None) -> str:
    """
    下載單一季度的完整財報 PDF。
    回傳狀態字串：
      'noop'       — 已是 DONE 或已有記錄，完全跳過（不發請求）
      'skipped'    — 發了請求但查無資料
      'downloaded' — 有實際下載動作（成功或失敗）
    """
    # ── 實體檔案優先判斷 (0秒閃過) ─────────────────────────
    # 只要檔案確實存在且大小正常，就完全跳過，不再受制於 DB 紀錄
    save_path = PdfDownloader.get_save_path(stock_id, ce_year, season)
    if PdfDownloader.is_valid_file(save_path):
        # 幫忙補上 DB 紀錄，以防之後需要
        tracker.mark(stock_id, ce_year, season, REPORT_TYPE, 'DONE')
        logger.debug(f"  SKIP(EXIST): {stock_id} {ce_year}Q{season}")
        return 'noop'

    # ── 實際下載 ──────────────────────────────────────────────
    result = PdfDownloader.download(stock_id, ce_year, season, market_type, session=session)

    if result:            # str 路徑 → 成功下載
        tracker.mark(stock_id, ce_year, season, REPORT_TYPE, 'DONE')
        return 'downloaded'
    elif result is None:  # None → TWSE 查無資料（公司尚未上市或無存檔）
        tracker.mark(stock_id, ce_year, season, REPORT_TYPE, 'SKIPPED')
        
        # 判斷是否這是「零請求路過」(例如剛好命中 cache)
        # 如果是命中 cache，PdfDownloader.download 會幾乎瞬間返回且不再發網路請求
        # 這裡我們回傳 'noop'，讓上層迴圈不要浪費 time.sleep 去等待
        logger.debug(f"  SKIP(NO_DATA): {stock_id} {ce_year}Q{season} (查無資料)")
        return 'noop'
    else:                 # False → 下載失敗（網路錯誤、所有 retry 用盡）
        tracker.mark(stock_id, ce_year, season, REPORT_TYPE, 'FAILED')
        logger.warning(
            f"  FAILED: {stock_id} {ce_year}Q{season} 下載失敗，"
            "已標記為 FAILED，可用 --retry-failed 重試"
        )
        return 'downloaded'  # 發了請求，計入 requests_made（以觸發正常延遲）


# ── 下載整間公司的所有季度 ────────────────────────────────────────

def download_company(stock_id: str, start_year: int, end_year: int,
                     market_type: str, tracker: ProgressTracker,
                     retry_failed: bool = False, session: requests.Session = None) -> int:
    """
    下載單一公司在指定年份區間的所有季度財報 PDF。

    回傳:
      files_downloaded (int): 實際嘗試下載檔案的數量。
    """
    quarters = build_quarter_list(start_year, end_year)
    total    = len(quarters)

    logger.info(
        f"  → {stock_id} [{market_type}] {start_year}～{end_year}，"
        f"共 {total} 個季度"
    )

    company_session = session
    files_downloaded = 0

    for i, (ce_year, season) in enumerate(quarters):
        # 實體檔案優先判斷，避免提早初始化 session 導致誤算網路請求
        save_path = PdfDownloader.get_save_path(stock_id, ce_year, season)
        if not retry_failed and PdfDownloader.is_valid_file(save_path):
            tracker.mark(stock_id, ce_year, season, REPORT_TYPE, 'DONE')
            # 交給原來的 log 機制，或在這裡 debug 也可以
            continue

        # 遇到真的需要下載或查詢的季度，才初始化連線
        if company_session is None:
            company_session = PdfDownloader.get_initialized_session()

        result = download_quarter(stock_id, ce_year, season, market_type, tracker, retry_failed, session=company_session)
        if result == 'downloaded':
            files_downloaded += 1

    # 清空這間公司的快取，避免佔用過多記憶體
    PdfDownloader.clear_cache()

    logger.info(f"  [{stock_id}] 完成，共 {total} 季。")
    return files_downloaded


# ── 批量下載所有公司 ──────────────────────────────────────────────

def run_all(stock_df: pd.DataFrame, end_year: int,
            tracker: ProgressTracker, retry_failed: bool,
            limit: int = None, node_file: str = None):
    """批量下載股票清單中所有公司的財報 PDF"""

    # ── Node 篩選：只執行此台電腦負責的公司 ─────────────────────
    if node_file:
        if not os.path.exists(node_file):
            logger.error(
                f"找不到 Node 分配檔：{node_file}\n"
                "請先在主電腦執行：python split_workload.py"
            )
            return
        with open(node_file, "r", encoding="utf-8") as f:
            assigned_ids = {line.strip() for line in f if line.strip()}
        stock_df = stock_df[stock_df["stock_id"].isin(assigned_ids)].reset_index(drop=True)
        logger.info(f"[Node 模式] 本機負責 {len(stock_df)} 間公司（來源：{node_file}）")

    if limit:
        stock_df = stock_df.head(limit)
        logger.info(f"測試模式：只下載前 {limit} 間公司")

    global_session = PdfDownloader.get_initialized_session()

    total = len(stock_df)
    for idx, row in enumerate(stock_df.itertuples(), start=1):
        stock_id    = str(row.stock_id).strip()
        stock_name  = getattr(row, 'stock_name', '')
        market_type = getattr(row, 'market_type', '上市')
        start_year  = int(getattr(row, 'listing_year', config.DEFAULT_EARLIEST_YEAR))

        # 不早於 IFRS 強制採用年（2013）—（早於此年份的資料幾乎全部查無）
        effective_start = max(start_year, config.IFRS_START_YEAR)
        if effective_start > start_year:
            logger.debug(
                f"  {stock_id} 上市年 {start_year}，"
                f"調整為 IFRS 起始年 {effective_start}"
            )
        start_year = effective_start

        logger.info(
            f"\n{'='*60}\n"
            f"[{idx}/{total}] {stock_id} {stock_name} "
            f"（{market_type}，{start_year}～{end_year}）\n"
            f"{'='*60}"
        )

        initial_requests = PdfDownloader.network_requests_this_session
        files_downloaded = download_company(
            stock_id, start_year, end_year,
            market_type, tracker, retry_failed, session=global_session
        )
        final_requests = PdfDownloader.network_requests_this_session
        requests_diff = final_requests - initial_requests

        # 若該公司有「實質發送網路請求」，且尚未到達最後一間公司，則進行跨公司長假等待
        if requests_diff > 0 and idx < total:
            if files_downloaded > 0:
                delay = random.uniform(config.INTER_COMPANY_MIN_DELAY, config.INTER_COMPANY_MAX_DELAY)
                logger.info(f"  [防封鎖] 處理此公司送出了 {requests_diff} 次網路請求並下載了 {files_downloaded} 份檔案，長間隔等待 {delay:.1f} 秒...")
            else:
                delay = random.uniform(1.0, 2.5)
                logger.info(f"  [防封鎖] 處理此公司送出了 {requests_diff} 次網路請求 (僅查詢無新檔)，短間隔等待 {delay:.1f} 秒...")
            time.sleep(delay)
        elif idx < total:
            logger.debug(f"  [極速路過] {stock_id} 已完整下載 (0 網路請求)，0 秒跳過！")

    tracker.print_summary()
    logger.info("全部公司下載作業完成！")


# ── 重試失敗項目 ──────────────────────────────────────────────────

def run_retry_failed(tracker: ProgressTracker, stock_df: pd.DataFrame,
                     end_year: int):
    """重試所有標記為 FAILED 的項目"""
    failed_list = tracker.get_failed()
    if not failed_list:
        logger.info("沒有失敗的項目需要重試。")
        return

    logger.info(f"共有 {len(failed_list)} 筆失敗項目，開始逐一重試...")

    # 建立 stock_id → market_type 的快速查找
    market_map = {}
    if stock_df is not None:
        market_map = dict(zip(
            stock_df['stock_id'].astype(str),
            stock_df.get('market_type', pd.Series(['上市'] * len(stock_df)))
        ))

    for stock_id, year, season, report_type in failed_list:
        market_type = market_map.get(str(stock_id), '上市')
        time.sleep(random.uniform(config.INTER_COMPANY_MIN_DELAY, config.INTER_COMPANY_MAX_DELAY))
        download_quarter(stock_id, year, season, market_type, tracker, retry_failed=True)

    tracker.print_summary()


# ── 主程式 ────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="台灣上市櫃公司歷年財報 PDF 下載器",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--fetch-list', action='store_true',
        help='（重新）抓取全台股票清單並儲存 CSV'
    )
    parser.add_argument(
        '--stock', type=str,
        help='指定單一股票代號（如：2330）'
    )
    parser.add_argument(
        '--all', action='store_true',
        help='批量下載清單中所有公司'
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help='配合 --all 使用，只爬前 N 間公司（測試用）'
    )
    parser.add_argument(
        '--start-year', type=int, default=None,
        help='手動指定起始西元年（預設：各公司上市年份）'
    )
    parser.add_argument(
        '--end-year', type=int, default=config.CURRENT_YEAR,
        help=f'手動指定結束西元年（預設：{config.CURRENT_YEAR}）'
    )
    parser.add_argument(
        '--retry-failed', action='store_true',
        help='重試所有先前標記為 FAILED 的項目'
    )
    parser.add_argument(
        '--node', type=int, default=None,
        help=(
            '指定本機為第幾號（1-indexed）。\n'
            '需先執行 split_workload.py 產生 node_N.txt 分配檔。\n'
            '例如：--node 1（代表此電腦執行第 1 份工作）'
        )
    )

    args = parser.parse_args()

    # 確保目錄存在
    os.makedirs(config.PDF_DIR, exist_ok=True)
    os.makedirs(config.PROCESSED_DIR, exist_ok=True)

    # 初始化斷點追蹤器
    tracker = ProgressTracker(db_path=config.DB_PATH)

    # ── Step 1：更新股票清單 ──────────────────────────────────────
    if args.fetch_list:
        logger.info("開始抓取全台股票清單（含上市日期）...")
        StockListFetcher.fetch_all_stocks()

    # 嘗試載入股票清單（retry-failed 和 --all 都需要用到）
    stock_df = None
    if args.retry_failed or args.all or args.stock:
        stock_df = StockListFetcher.load_stock_list()

    # ── Step 2：重試失敗項目 ──────────────────────────────────────
    if args.retry_failed:
        run_retry_failed(tracker, stock_df, end_year=args.end_year)
        return

    # ── Step 3：單一公司模式 ──────────────────────────────────────
    if args.stock:
        start_year  = args.start_year
        market_type = '上市'  # 預設

        # 從清單查上市年份與市場類型
        if stock_df is not None:
            row = stock_df[stock_df['stock_id'] == args.stock]
            if not row.empty:
                if start_year is None:
                    start_year = int(row.iloc[0]['listing_year'])
                    logger.info(f"{args.stock} 上市年份：{start_year}")
                if 'market_type' in row.columns:
                    market_type = str(row.iloc[0]['market_type'])
            else:
                logger.warning(f"清單中找不到 {args.stock}，使用預設值。")

        if start_year is None:
            start_year = config.DEFAULT_EARLIEST_YEAR

        download_company(
            args.stock, start_year, args.end_year,
            market_type, tracker, retry_failed=args.retry_failed
        )
        tracker.print_summary()
        return

    # ── Step 4：批量模式 ──────────────────────────────────────────
    if args.all:
        if stock_df is None or stock_df.empty:
            logger.error("無法載入股票清單，請先執行 --fetch-list。")
            return

        # 解析 node 分配檔路徑
        node_file = None
        if args.node is not None:
            node_file = os.path.join(config.PROCESSED_DIR, f"node_{args.node}.txt")
            logger.info(f"Node 模式啟動：node={args.node}，分配檔={node_file}")

        run_all(
            stock_df     = stock_df,
            end_year     = args.end_year,
            tracker      = tracker,
            retry_failed = args.retry_failed,
            limit        = args.limit,
            node_file    = node_file,
        )
        return

    # 若沒有任何有效參數
    parser.print_help()


if __name__ == "__main__":
    main()
