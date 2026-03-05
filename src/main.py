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

def get_latest_available_season(current_month: int) -> int:
    """
    根據當前月份，推算 MOPS 上最新可查的季度。
    Q1 約 5 月公告、Q2 約 8 月、Q3 約 11 月、Q4 約隔年 3 月。
    """
    if 1 <= current_month <= 4:
        return 3  # 去年 Q3（年報 Q4 可能也可查）
    elif 5 <= current_month <= 7:
        return 1
    elif 8 <= current_month <= 10:
        return 2
    else:
        return 3


def build_quarter_list(start_year: int, end_year: int) -> list[tuple[int, int]]:
    """
    建立所有需要下載的 (ce_year, season) 清單，
    自動排除尚未到達的未來季度。
    """
    now = datetime.now()
    current_year   = now.year
    latest_season  = get_latest_available_season(now.month)

    quarters = [
        (y, s)
        for y in range(start_year, end_year + 1)
        for s in range(1, 5)
        if not (y == current_year and s > latest_season)
        and not (y > current_year)
    ]
    return quarters


# ── 下載單一季度 ──────────────────────────────────────────────────

def download_quarter(stock_id: str, ce_year: int, season: int,
                     market_type: str, tracker: ProgressTracker,
                     retry_failed: bool = False) -> str:
    """
    下載單一季度的完整財報 PDF。
    回傳狀態字串：
      'noop'       — 已是 DONE 或已有記錄，完全跳過（不發請求）
      'skipped'    — 發了請求但查無資料
      'downloaded' — 有實際下載動作（成功或失敗）
    """
    # ── 斷點續爬判斷 ──────────────────────────────────────────
    if tracker.is_done(stock_id, ce_year, season, REPORT_TYPE):
        logger.debug(f"  SKIP(DONE): {stock_id} {ce_year}Q{season}")
        return 'noop'

    if not retry_failed and tracker.is_recorded(stock_id, ce_year, season, REPORT_TYPE):
        logger.debug(f"  SKIP(recorded): {stock_id} {ce_year}Q{season}")
        return 'noop'

    # ── 實際下載 ──────────────────────────────────────────────
    result = PdfDownloader.download(stock_id, ce_year, season, market_type)

    if result:
        tracker.mark(stock_id, ce_year, season, REPORT_TYPE, 'DONE')
        return 'downloaded'
    else:
        # 查無資料（公司尚未上市、或該季度 MOPS 無存檔）→ SKIPPED
        tracker.mark(stock_id, ce_year, season, REPORT_TYPE, 'SKIPPED')
        return 'skipped'


# ── 下載整間公司的所有季度 ────────────────────────────────────────

def download_company(stock_id: str, start_year: int, end_year: int,
                     market_type: str, tracker: ProgressTracker,
                     retry_failed: bool = False):
    """
    下載單一公司在指定年份區間的所有季度財報 PDF。

    延遲策略：
      - 實際下載 PDF 後：5~12 秒（跨季請求是 MOPS 主要封鎖風險點）
      - 查無資料（SKIPPED）：0.5~1.5 秒（短暫緩衝即可，不需完整間隔）
      - 完全跳過（noop）：不等待
    """
    quarters = build_quarter_list(start_year, end_year)
    total    = len(quarters)

    logger.info(
        f"  → {stock_id} [{market_type}] {start_year}～{end_year}，"
        f"共 {total} 個季度"
    )

    for i, (ce_year, season) in enumerate(quarters):
        result = download_quarter(stock_id, ce_year, season, market_type, tracker, retry_failed)

        if result == 'downloaded':
            # 實際條發請求 → 完整跨季延遲
            delay = random.uniform(config.MIN_DELAY_SECONDS, config.MAX_DELAY_SECONDS)
            logger.debug(f"  下載完成，跨季等待 {delay:.1f} 秒...")
            time.sleep(delay)
        elif result == 'skipped':
            # 發了請求但查無資料 → 短暫緩衝
            delay = random.uniform(config.SKIP_MIN_DELAY, config.SKIP_MAX_DELAY)
            logger.debug(f"  查無資料，短暫等待 {delay:.1f} 秒...")
            time.sleep(delay)
        # 'noop' — 完全跳過，不需延遲

    logger.info(f"  [{stock_id}] 完成，共 {total} 季。")


# ── 批量下載所有公司 ──────────────────────────────────────────────

def run_all(stock_df: pd.DataFrame, end_year: int,
            tracker: ProgressTracker, retry_failed: bool,
            limit: int = None):
    """批量下載股票清單中所有公司的財報 PDF"""
    if limit:
        stock_df = stock_df.head(limit)
        logger.info(f"測試模式：只下載前 {limit} 間公司")

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

        download_company(
            stock_id, start_year, end_year,
            market_type, tracker, retry_failed
        )

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
        time.sleep(random.uniform(config.MIN_DELAY_SECONDS, config.MAX_DELAY_SECONDS))
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

        run_all(
            stock_df  = stock_df,
            end_year  = args.end_year,
            tracker   = tracker,
            retry_failed = args.retry_failed,
            limit     = args.limit,
        )
        return

    # 若沒有任何有效參數
    parser.print_help()


if __name__ == "__main__":
    main()
