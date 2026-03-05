"""
parser.py
─────────────────────────────────────────────────────────────────
負責向公開資訊觀測站 (MOPS) 請求財報並解析為 Pandas DataFrame。

三大財報端點皆透過 _fetch_mops_table() 統一處理，
解析出來的 DataFrame 欄位格式：
  stock_id | year | season | report_type | account_name
  | current_period | previous_period | [earlier_period, ...]
"""
import pandas as pd
import logging
from io import StringIO
from . import config
from .fetcher import fetcher

logger = logging.getLogger(__name__)


class MopsParser:
    """向 MOPS 請求財報並解析為結構化 DataFrame"""

    @staticmethod
    def _fetch_mops_table(url: str, stock_id: str, year: int,
                          season: int, report_type: str):
        """
        共用的 MOPS 查詢與表格解析模組。
        保留財報原始全部欄位（通常包含當期、前期、前前期等多期金額），
        以提供 AI 訓練所需的時間序列資訊。

        回傳 DataFrame，欄位：
          stock_id, year, season, report_type,
          account_name, current_period, previous_period, [period_n, ...]
        失敗時回傳 None。
        """
        # MOPS 查詢 payload
        data = {
            'encodeURIComponent': '1',
            'step':        '1',
            'firstin':     '1',
            'off':         '1',
            'queryName':   'co_id',
            'inpuType':    'co_id',
            'TYPEK':       'all',
            'isnew':       'false',
            'co_id':       str(stock_id),
            'year':        str(year),
            'season':      str(season).zfill(2),  # MOPS 要求補零，如 01、02
        }

        logger.info(f"正在抓取 [{report_type}] {stock_id} {year}年Q{season}...")

        response = fetcher.robust_post(url, data=data)
        if not response:
            return None

        # 確保回傳的是 HTML
        content_type = response.headers.get('Content-Type', '').lower()
        if 'text/html' not in content_type and 'text/plain' not in content_type:
            logger.warning(
                f"{stock_id} ({year}Q{season}) [{report_type}] "
                f"回傳格式非 HTML ({content_type})，可能是 PDF 或 Word，已略過。"
            )
            return None

        if ("THE PAGE CANNOT BE ACCESSED" in response.text or
                "頁面無法執行" in response.text):
            logger.error(
                f"{stock_id} ({year}Q{season}) [{report_type}] "
                "遭到 MOPS 阻擋。"
            )
            return None

        try:
            html_io = StringIO(response.text)
            dfs = pd.read_html(html_io)
            if not dfs:
                logger.warning(
                    f"{stock_id} ({year}Q{season}) [{report_type}] "
                    "找不到任何表格資料（該期可能尚未公告）。"
                )
                return None

            # 取資料列數最多的表格（通常就是財報主表）
            df = max(dfs, key=len).copy()

            if len(df.columns) < 2:
                logger.warning(
                    f"{stock_id} ({year}Q{season}) [{report_type}] "
                    "表格欄位數不足，已略過。"
                )
                return None

            # ── 動態命名欄位 ──────────────────────────────────────
            # 財報通常欄位順序：科目名稱 | 當期金額 | 前期金額 | 前前期金額 ...
            n_cols = len(df.columns)
            col_names = ['account_name', 'current_period']
            if n_cols >= 3:
                col_names.append('previous_period')
            for i in range(3, n_cols):
                col_names.append(f'period_{i - 1}')
            df.columns = col_names

            # ── 清理資料 ──────────────────────────────────────────
            # 移除科目名稱或當期金額均為空的列
            df = df.dropna(subset=['account_name', 'current_period'])

            # 數值欄位：去除逗號並轉為浮點數（無效值設為 NaN）
            numeric_cols = [c for c in df.columns if c != 'account_name']
            for col in numeric_cols:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(',', '', regex=False),
                    errors='coerce'
                )

            # 加入識別欄位
            df.insert(0, 'report_type', report_type)
            df.insert(0, 'season',      season)
            df.insert(0, 'year',        year)
            df.insert(0, 'stock_id',    str(stock_id))

            return df

        except ValueError:
            # pd.read_html 在找不到表格時拋出 ValueError
            logger.warning(
                f"{stock_id} ({year}Q{season}) [{report_type}] "
                "該季尚無資料。"
            )
            return None
        except Exception as e:
            logger.error(
                f"{stock_id} ({year}Q{season}) [{report_type}] "
                f"解析時發生錯誤：{e}"
            )
            return None

    # ── 公開介面 ──────────────────────────────────────────────────

    @classmethod
    def get_income_statement(cls, stock_id, year, season):
        """獲取綜合損益表"""
        return cls._fetch_mops_table(
            config.MOPS_INCOME_STATEMENT_URL,
            stock_id, year, season,
            report_type='income_statement'
        )

    @classmethod
    def get_balance_sheet(cls, stock_id, year, season):
        """獲取資產負債表"""
        return cls._fetch_mops_table(
            config.MOPS_BALANCE_SHEET_URL,
            stock_id, year, season,
            report_type='balance_sheet'
        )

    @classmethod
    def get_cash_flow(cls, stock_id, year, season):
        """獲取現金流量表"""
        return cls._fetch_mops_table(
            config.MOPS_CASH_FLOW_URL,
            stock_id, year, season,
            report_type='cash_flow'
        )

    @classmethod
    def get_all_reports(cls, stock_id, year, season):
        """
        一次取得三大財報，回傳 dict：
        {
          'income_statement': DataFrame or None,
          'balance_sheet':    DataFrame or None,
          'cash_flow':        DataFrame or None,
        }
        """
        return {
            'income_statement': cls.get_income_statement(stock_id, year, season),
            'balance_sheet':    cls.get_balance_sheet(stock_id, year, season),
            'cash_flow':        cls.get_cash_flow(stock_id, year, season),
        }


if __name__ == "__main__":
    # 快速測試：台積電 (2330) 113年第4季
    result = MopsParser.get_all_reports("2330", 113, 4)
    for name, df in result.items():
        if df is not None:
            print(f"\n=== {name} ===")
            print(df.head(5))
        else:
            print(f"\n=== {name} === (無資料)")
