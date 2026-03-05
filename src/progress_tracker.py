"""
progress_tracker.py
─────────────────────────────────────────────────────────────────
斷點續爬模組：使用 SQLite 記錄每筆 (stock_id, year, season, report_type)
的爬取狀態，讓爬蟲中斷後可以從斷點繼續，不重複做白工。

狀態說明：
  DONE    - 成功抓取並寫入資料庫
  FAILED  - 抓取失敗（可用 --retry-failed 重試）
  SKIPPED - 該期沒有資料（例如公司尚未上市的季度）
"""
import sqlite3
import logging
from datetime import datetime
from . import config

logger = logging.getLogger(__name__)

# progress 資料表的 DDL
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS progress (
    stock_id    TEXT    NOT NULL,
    year        INTEGER NOT NULL,
    season      INTEGER NOT NULL,
    report_type TEXT    NOT NULL,
    status      TEXT    NOT NULL,        -- DONE / FAILED / SKIPPED
    updated_at  TEXT    NOT NULL,
    PRIMARY KEY (stock_id, year, season, report_type)
);
"""


class ProgressTracker:
    """管理爬蟲進度的 SQLite 紀錄器"""

    def __init__(self, db_path: str = config.DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """初始化資料庫，確保 progress 表存在"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.commit()

    def is_done(self, stock_id: str, year: int, season: int, report_type: str) -> bool:
        """
        查詢某筆工作是否已完成（狀態為 DONE）。
        已完成的項目不需要重爬。
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT status FROM progress "
                "WHERE stock_id=? AND year=? AND season=? AND report_type=?",
                (stock_id, year, season, report_type)
            ).fetchone()
        return row is not None and row[0] == 'DONE'

    def is_recorded(self, stock_id: str, year: int, season: int, report_type: str) -> bool:
        """
        查詢某筆工作是否有任何紀錄（不管成功還是失敗）。
        用來判斷是否應跳過（非 --retry-failed 模式）。
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT 1 FROM progress "
                "WHERE stock_id=? AND year=? AND season=? AND report_type=?",
                (stock_id, year, season, report_type)
            ).fetchone()
        return row is not None

    def mark(self, stock_id: str, year: int, season: int,
             report_type: str, status: str):
        """
        記錄爬取結果。
        status 應為 'DONE'、'FAILED' 或 'SKIPPED'。
        使用 INSERT OR REPLACE 以支援斷點重試更新。
        """
        now = datetime.now().isoformat(timespec='seconds')
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO progress "
                "(stock_id, year, season, report_type, status, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (stock_id, year, season, report_type, status, now)
            )
            conn.commit()

    def count_by_status(self, status: str = None) -> int:
        """統計各狀態的數量，用於顯示爬取進度摘要"""
        with sqlite3.connect(self.db_path) as conn:
            if status:
                row = conn.execute(
                    "SELECT COUNT(*) FROM progress WHERE status=?", (status,)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) FROM progress"
                ).fetchone()
        return row[0] if row else 0

    def get_failed(self):
        """
        回傳所有失敗紀錄的清單，格式為：
        list of (stock_id, year, season, report_type)
        供 --retry-failed 模式使用。
        """
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT stock_id, year, season, report_type FROM progress "
                "WHERE status='FAILED'"
            ).fetchall()
        return rows

    def print_summary(self):
        """印出目前爬取進度摘要"""
        done    = self.count_by_status('DONE')
        failed  = self.count_by_status('FAILED')
        skipped = self.count_by_status('SKIPPED')
        total   = self.count_by_status()
        logger.info(
            f"[進度摘要] 總計={total} | "
            f"完成={done} | 失敗={failed} | 跳過={skipped}"
        )
