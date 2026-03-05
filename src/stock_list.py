"""
stock_list.py
─────────────────────────────────────────────────────────────────
抓取台股上市、上櫃、興櫃公司清單（含上市日期）。

ISIN 網頁的 HTML 用了非標準格式，pandas.read_html 解析失敗，
改用 BeautifulSoup 直接讀取 <table> 的 <tr>/<td>。

欄位：stock_id, stock_name, listing_year, industry, market_type
"""

import re
import os
import logging
import pandas as pd
from bs4 import BeautifulSoup
from . import config
from .fetcher import fetcher

logger = logging.getLogger(__name__)


class StockListFetcher:
    """負責抓取台股各類別（上市、上櫃、興櫃）公司名單，並解析上市日期"""

    # strMode 對應 ISIN 網站的市場類別
    MARKET_MODES = {
        '上市': '2',
        '上櫃': '4',
        '興櫃': '5',
    }

    # ISIN 表格中各市場的欄位名稱（Row 0）
    # 實測：['有價證券代號及名稱', 'ISIN Code', '上市日', '市場別', '產業別', 'CFICode', '備註']
    COL_NAME   = '有價證券代號及名稱'
    COL_DATE   = '上市日'          # 上市 / 上櫃 / 興櫃均使用此欄位名稱
    COL_INDUS  = '產業別'
    COL_CFI    = 'CFICode'

    # ── 日期解析 ──────────────────────────────────────────────────

    @staticmethod
    def _parse_listing_year(date_str: str) -> int:
        """
        解析上市日期字串為西元年。
        ISIN 頁面格式為西元年 'YYYY/MM/DD'（如 '1962/02/09'）。
        若解析失敗，回傳 DEFAULT_EARLIEST_YEAR。
        """
        if not isinstance(date_str, str):
            return config.DEFAULT_EARLIEST_YEAR

        date_str = date_str.strip()

        # 西元年：YYYY/MM/DD
        m = re.match(r'^(\d{4})/\d{2}/\d{2}$', date_str)
        if m:
            return int(m.group(1))

        # 民國年：YYY/MM/DD（防禦性處理）
        m = re.match(r'^(\d{2,3})/\d{2}/\d{2}$', date_str)
        if m:
            return int(m.group(1)) + 1911

        return config.DEFAULT_EARLIEST_YEAR

    # ── HTML 解析（改用 BeautifulSoup，pd.read_html 無法解析此頁）────

    @classmethod
    def _parse_isin_html(cls, html: str, market_name: str) -> pd.DataFrame:
        """
        解析 ISIN 頁面 HTML，回傳整理好的 DataFrame。
        直接透過 BeautifulSoup 讀取最大的 <table>。
        """
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table')
        if not tables:
            logger.error(f"{market_name} 找不到任何 <table>")
            return pd.DataFrame()

        # 找列數最多的 table（通常是資料主表）
        big_table = max(tables, key=lambda t: len(t.find_all('tr')))
        rows = big_table.find_all('tr')
        if len(rows) < 2:
            return pd.DataFrame()

        # Row 0 = 欄位標題
        headers = [td.get_text(strip=True) for td in rows[0].find_all(['td', 'th'])]
        logger.debug(f"{market_name} 表格欄位：{headers}")

        # 確認必要欄位存在
        if cls.COL_NAME not in headers:
            logger.error(f"{market_name} 找不到欄位「{cls.COL_NAME}」，實際：{headers}")
            return pd.DataFrame()

        col_idx = {h: i for i, h in enumerate(headers)}

        records = []
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if not cells or len(cells) < 2:
                continue

            # 跳過分類標題列（如「股票」「ETF」「受益憑證」等）
            cfi_val = cells[col_idx[cls.COL_CFI]].strip() if cls.COL_CFI in col_idx and len(cells) > col_idx[cls.COL_CFI] else ''
            if cfi_val != 'ESVUFR':
                continue  # 只保留普通股

            name_cell = cells[col_idx[cls.COL_NAME]] if len(cells) > col_idx[cls.COL_NAME] else ''
            # 分割代號與名稱（全形空白 \u3000 或半形空白）
            parts = re.split(r'\u3000| ', name_cell, maxsplit=1)
            if len(parts) < 2:
                continue
            stock_id, stock_name = parts[0].strip(), parts[1].strip()
            if not stock_id:
                continue

            # 上市日期
            date_str = ''
            if cls.COL_DATE in col_idx and len(cells) > col_idx[cls.COL_DATE]:
                date_str = cells[col_idx[cls.COL_DATE]]
            listing_year = cls._parse_listing_year(date_str)

            # 產業別
            industry = ''
            if cls.COL_INDUS in col_idx and len(cells) > col_idx[cls.COL_INDUS]:
                industry = cells[col_idx[cls.COL_INDUS]]

            records.append({
                'stock_id':    stock_id,
                'stock_name':  stock_name,
                'listing_year': listing_year,
                'industry':    industry,
                'market_type': market_name,
            })

        return pd.DataFrame(records)

    # ── 抓取介面 ──────────────────────────────────────────────────

    @classmethod
    def fetch_market_list(cls, market_name: str, mode_id: str) -> pd.DataFrame:
        """抓取單一市場類別股票清單（含上市日期）"""
        url = config.ISIN_BASE_URL.format(mode_id)
        logger.info(f"開始抓取 {market_name} 公司清單：{url}")

        response = fetcher.robust_get(url)
        if not response:
            logger.error(f"無法取得 {market_name} 公司清單")
            return pd.DataFrame()

        try:
            df = cls._parse_isin_html(response.text, market_name)
            if not df.empty:
                logger.info(f"{market_name} 共解析 {len(df)} 筆公司（含上市日期）")
            return df
        except Exception as e:
            logger.error(f"解析 {market_name} 清單發生錯誤：{e}")
            return pd.DataFrame()

    @classmethod
    def fetch_all_stocks(cls, save_csv: bool = True) -> pd.DataFrame | None:
        """抓取所有目標市場清單並合併，結果含 listing_year 欄位"""
        all_dfs = []
        for market_name, mode_id in cls.MARKET_MODES.items():
            df = cls.fetch_market_list(market_name, mode_id)
            if not df.empty:
                all_dfs.append(df)

        if not all_dfs:
            logger.error("所有市場清單抓取失敗。")
            return None

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['stock_id'])

        if save_csv:
            os.makedirs(config.PROCESSED_DIR, exist_ok=True)
            final_df.to_csv(config.STOCK_LIST_CSV, index=False, encoding='utf-8-sig')
            logger.info(
                f"已儲存股票清單至 {config.STOCK_LIST_CSV}，"
                f"共 {len(final_df)} 間公司。"
            )

        return final_df

    @classmethod
    def load_stock_list(cls) -> pd.DataFrame | None:
        """
        讀取本地 CSV 股票清單；若不存在則自動從網路抓取。
        回傳 DataFrame。
        """
        if os.path.exists(config.STOCK_LIST_CSV):
            logger.info(f"讀取本地股票清單：{config.STOCK_LIST_CSV}")
            return pd.read_csv(config.STOCK_LIST_CSV, dtype={'stock_id': str})
        else:
            logger.info("本地股票清單不存在，開始從網路抓取...")
            return cls.fetch_all_stocks(save_csv=True)


if __name__ == '__main__':
    df = StockListFetcher.fetch_all_stocks()
    if df is not None:
        print(df.head(10).to_string())
