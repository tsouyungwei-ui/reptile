"""
pdf_downloader.py
─────────────────────────────────────────────────────────────────
從 TWSE 電子資料查詢作業 (doc.twse.com.tw) 下載台灣上市公司
完整財務報告 PDF。

【實際流程】（由 devtools 實測確認）

Step 1: POST https://doc.twse.com.tw/server-java/t57sb01
        params: step=1, colorchg=1, co_id=..., year=..., seamon=..., mtype=A
        → 回傳 HTML 列出所有可下載的財報 PDF
        → PDF 連結隱藏在 onclick="readfile2('A','2330','202304_2330_AI1.pdf')"

Step 2: POST https://doc.twse.com.tw/server-java/t57sb01
        params: step=9, colorchg=1, kind=A, co_id=..., filename=202304_2330_AI1.pdf
        → 回傳 PDF 二進位串流

優先下載「IFRSs合併財報」（AI1），若不存在則下載「IFRSs個體財報」（AI3）。

下載後存放路徑：
  data/pdfs/{stock_id}/{ce_year}/Q{season}.pdf
"""

import os
import re
import random
import logging
import requests
import urllib3
from bs4 import BeautifulSoup
from . import config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

TWSE_DOC_URL = "https://doc.twse.com.tw/server-java/t57sb01"


def ce_to_roc(ce_year: int) -> int:
    """西元年轉民國年（e.g. 2024 → 113）"""
    return ce_year - 1911


class PdfDownloader:
    """從 TWSE 文件中心下載財務報告 PDF（合併財報優先）"""

    # ── 路徑工具 ──────────────────────────────────────────────────

    @staticmethod
    def get_save_path(stock_id: str, ce_year: int, season: int) -> str:
        """回傳 PDF 的完整儲存路徑，並確保目錄存在"""
        directory = os.path.join(config.PDF_DIR, str(stock_id), str(ce_year))
        os.makedirs(directory, exist_ok=True)
        return os.path.join(directory, f"Q{season}.pdf")

    @staticmethod
    def is_valid_pdf(path: str, min_size_kb: int = 50) -> bool:
        """
        檢查 PDF 是否存在且大小合理。
        財報 PDF 通常 > 1 MB，50 KB 以下通常是錯誤頁面。
        """
        if not os.path.exists(path):
            return False
        return os.path.getsize(path) >= min_size_kb * 1024

    @staticmethod
    def _headers(referer: str = TWSE_DOC_URL) -> dict:
        return {
            "User-Agent": random.choice(config.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.5",
            "Referer": referer,
            "Connection": "keep-alive",
        }

    # ── Step 1：查詢可下載的 PDF 清單 ────────────────────────────

    @classmethod
    def _query_file_list(cls, sess: requests.Session,
                         stock_id: str, roc_year: int, season: int) -> list[dict]:
        """
        POST step=1 到 TWSE 文件中心，解析可下載的 PDF 清單。

        連結隱藏在 JavaScript onclick 裡：
          readfile2("A","2330","202304_2330_AI1.pdf")
        必須用 regex 解析，不能用 BeautifulSoup 的 find_all('a', href=...)。

        回傳:
          [{'kind': 'A', 'co_id': '2330', 'filename': '202304_2330_AI1.pdf',
            'description': 'IFRSs合併財報'}, ...]
        """
        data = {
            'step':     '1',
            'colorchg': '1',
            'co_id':    str(stock_id),
            'year':     str(roc_year),
            'seamon':   str(season),
            'mtype':    'A',   # 財務報告（mtype=A 即可，無須指定dtype）
        }
        try:
            resp = sess.post(
                TWSE_DOC_URL,
                data=data,
                headers=cls._headers(),
                timeout=20,
                verify=False,
            )
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"  查詢失敗（{stock_id} {roc_year}年Q{season}）：{e}")
            return []

        # 解析 HTML（Big5 編碼）
        html = resp.content.decode('big5', errors='replace')

        # 找「查無資料」
        if '查無所需資料' in html:
            logger.debug(f"  {stock_id} 民國{roc_year}年Q{season}：TWSE 查無資料")
            return []

        # 以 regex 解析所有 readfile2("kind","coid","filename") 呼叫
        # 同時用 BeautifulSoup 抓每個連結的描述文字
        soup = BeautifulSoup(html, 'lxml')
        rows = soup.find_all('tr')

        results = []
        # readfile2 regex：匹配三個雙引號引數
        pattern = re.compile(r'readfile2?\("([^"]+)","([^"]+)","([^"]+)"\)')

        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '')
            m = pattern.search(href)
            if not m:
                continue
            kind, co_id, filename = m.group(1), m.group(2), m.group(3)

            # 取同一行的「資料細節說明」欄位文字（如 IFRSs合併財報）
            desc = ''
            tr = a_tag.find_parent('tr')
            if tr:
                tds = tr.find_all('td')
                if len(tds) >= 6:
                    desc = tds[5].get_text(strip=True)

            results.append({
                'kind':     kind,
                'co_id':    co_id,
                'filename': filename,
                'desc':     desc,
            })

        return results

    # ── Step 2：串流下載 PDF ──────────────────────────────────────

    @classmethod
    def _download_file(cls, sess: requests.Session,
                       kind: str, co_id: str, filename: str,
                       save_path: str) -> bool:
        """
        TWSE 三步下載流程：
          Step 2: POST step=9  → 回傳 HTML（非 PDF），內含時間戳記 /pdf/... 連結
          Step 3: GET  /pdf/.. → 實際的 PDF 串流

        若任一步驟失敗，自動清除不完整檔案。
        """
        # ── Step 2：觸發下載，取得臨時 PDF 連結 ─────────────────
        data = {
            'step':     '9',
            'colorchg': '1',
            'kind':     kind,
            'co_id':    co_id,
            'filename': filename,
        }
        try:
            r2 = sess.post(
                TWSE_DOC_URL,
                data=data,
                headers=cls._headers(),
                timeout=30,
                verify=False,
            )
            r2.raise_for_status()
        except Exception as e:
            logger.error(f"  ✗ step=9 請求失敗（{filename}）：{e}")
            return False

        # 解析回傳 HTML（Big5 編碼）中的臨時 PDF 路徑
        # 格式：<a href='/pdf/202304_2330_AI1_20260304_094212.pdf'>
        html = r2.content.decode('big5', errors='replace')
        m = re.search(r"href='(/pdf/[^']+\.pdf)'", html)
        if not m:
            logger.warning(f"  step=9 回應中找不到 /pdf/... 連結，filename={filename}")
            logger.debug(f"  回應內容：{html[:200]}")
            return False

        pdf_path = m.group(1)
        pdf_url  = f"https://doc.twse.com.tw{pdf_path}"
        logger.info(f"  → GET {pdf_url}")

        # ── Step 3：GET 下載實際 PDF 串流 ────────────────────────
        try:
            with sess.get(
                pdf_url,
                headers=cls._headers(referer=TWSE_DOC_URL),
                stream=True,
                timeout=config.PDF_DOWNLOAD_TIMEOUT,
                verify=False,
            ) as r3:
                r3.raise_for_status()

                # 確認是 PDF / 二進位內容（非 HTML 錯誤頁）
                ctype = r3.headers.get('Content-Type', '')
                if 'html' in ctype.lower():
                    logger.warning(f"  step=3 仍收到 HTML，可能臨時連結失效：{pdf_url}")
                    return False

                with open(save_path, 'wb') as f:
                    for chunk in r3.iter_content(chunk_size=65536):
                        f.write(chunk)

            size_kb = os.path.getsize(save_path) / 1024
            if size_kb < 50:
                logger.warning(f"  檔案過小（{size_kb:.0f} KB），可能不是有效 PDF")
                os.remove(save_path)
                return False

            logger.info(f"  ✓ 下載完成（{size_kb:.0f} KB）：{save_path}")
            return True

        except Exception as e:
            logger.error(f"  ✗ PDF GET 失敗（{pdf_url}）：{e}")
            if os.path.exists(save_path):
                os.remove(save_path)
            return False

    # ── 公開主介面 ────────────────────────────────────────────────

    @classmethod
    def download(cls, stock_id: str, ce_year: int, season: int,
                 market_type: str = '上市') -> str | None:
        """
        下載指定公司、年份、季度的完整財務報告 PDF。

        優先下載中文合併財報（AI1），其次個體財報（AI3），
        最後接受任何其他財報格式。

        Args:
            stock_id:    股票代號（如 '2330'）
            ce_year:     西元年（如 2023）
            season:      季度 1~4
            market_type: 目前未使用（TWSE 不分市場類型），保留供擴充

        Returns:
            成功時回傳 PDF 儲存路徑（str）；失敗 / 查無資料時回傳 None。
        """
        roc_year  = ce_to_roc(ce_year)
        save_path = cls.get_save_path(stock_id, ce_year, season)

        # ── 快取檢查：有效 PDF 直接跳過 ──────────────────────────
        if cls.is_valid_pdf(save_path):
            logger.debug(f"  已存在（跳過）：{save_path}")
            return save_path

        logger.info(
            f"  [{stock_id}] {ce_year}年Q{season}（民國{roc_year}年第{season}季）"
        )

        # 建立獨立 Session，先訪問首頁取得合法 Cookie
        sess = requests.Session()
        try:
            sess.get(TWSE_DOC_URL, headers=cls._headers(), timeout=10, verify=False)
        except Exception as e:
            logger.error(f"  初始化 Session 失敗：{e}")
            return None

        # ── 查詢可下載的 PDF 清單 ─────────────────────────────────
        files = cls._query_file_list(sess, stock_id, roc_year, season)

        if not files:
            return None

        logger.debug(f"  找到 {len(files)} 個檔案：{[f['filename'] for f in files]}")

        # ── 選擇優先下載的檔案 ────────────────────────────────────
        # 優先順序：AI1（合併中文）> AI3（個體中文）> 其他
        def priority(f):
            n = f['filename'].upper()
            if 'AI1' in n:  return 0   # IFRSs合併財報（最完整）
            if 'AI3' in n:  return 1   # IFRSs個體財報
            if 'AIA' in n:  return 2   # 英文合併版（跳過優先）
            return 3

        files_sorted = sorted(files, key=priority)

        # ── 下載（依優先順序，成功一個就結束）────────────────────
        for file_info in files_sorted:
            fn = file_info['filename']
            desc = file_info.get('desc', '')
            logger.info(f"  → 下載：{fn}（{desc}）")

            if cls._download_file(
                sess,
                file_info['kind'],
                file_info['co_id'],
                fn,
                save_path
            ):
                return save_path

        logger.error(f"  [{stock_id}] {ce_year}Q{season} 全部下載連結失敗")
        return None
