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
    
    # 用於暫存某間公司歷年所有財報清單的快取，避免每季發送查詢請求
    _company_cache = {
        'stock_id': None,
        'files': []
    }

    @classmethod
    def clear_cache(cls):
        """清空當前公司的檔案清單快取"""
        cls._company_cache = {
            'stock_id': None,
            'files': []
        }

    # ── 路徑工具 ──────────────────────────────────────────────────

    @staticmethod
    def get_save_path(stock_id: str, ce_year: int, season: int, ext: str = "pdf") -> str:
        """回傳檔案的完整儲存路徑，並確保目錄存在"""
        directory = os.path.join(config.PDF_DIR, str(stock_id), str(ce_year))
        os.makedirs(directory, exist_ok=True)
        ext = ext.lstrip('.')
        return os.path.join(directory, f"Q{season}.{ext}")

    @staticmethod
    def is_valid_file(path: str, min_size_kb: int = 50) -> bool:
        """
        檢查檔案是否存在且大小合理。
        財報通常 > 1 MB，50 KB 以下通常是錯誤頁面。
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
            "Connection": "close",  # 避免 TWSE 伺服器主動中斷 keep-alive 導致 BadStatusLine 錯誤
        }

    # ── Step 1：查詢可下載的 PDF 清單 ────────────────────────────

    @classmethod
    def _query_file_list(cls, sess: requests.Session,
                         stock_id: str, roc_year: int, season: int, max_retries: int = 5) -> list[dict] | None:
        """
        從 TWSE 取得指定股票、年份、季度的 PDF 清單。
        內部實作：首次查詢某股票時，送出不帶「年份」與「季度」的查詢，
        取得該公司歷年所有財報檔案清單並快取，後續直接從快取過濾。

        連結隱藏在 JavaScript onclick 裡：
          readfile2("A","2330","202304_2330_AI1.pdf")
        必須用 regex 解析，不能用 BeautifulSoup 的 find_all('a', href=...)。

        回傳:
          - [] 如果確實查無資料
          - None 如果發生錯誤或被 WAF 阻擋
          - [{...}, ...] 正常結果
        """
        stock_id_str = str(stock_id)

        # 1) 如果快取已經是這間公司，且不是空陣列（或者已經確認查無任何資料），直接從快取過濾
        if cls._company_cache['stock_id'] == stock_id_str:
            cached_files = cls._company_cache['files']
            if not cached_files:
                return []
                
            # 過濾出符合指定年份與季度的檔案
            # 檔名格式通常如：202304_2330_AI1.pdf 或是 10803_2330_AI1.pdf 
            # 前面代表 西元年+月份 或 民國年+月份。但保險起見，也可以不管完整名稱，只要確保包含年份與對應月份。
            # 第一季：01~03（通常財報檔名是 01, 03, 05, etc 但 TWSE 慣例是用 01 代表 Q1, 02 代表 Q2, 03 代表 Q3, 04 代表 Q4）
            # 新版格式如 202304, 舊版可能是 10204 等。
            # 我們直接使用字串比對： {西元年}{對應季碼} 或 {民國年}{對應季碼}
            ce_year = roc_year + 1911
            # 季碼通常會補零： '01', '02', '03', '04'
            target_postfix_ce  = f"{ce_year}{season:02d}_{stock_id_str}"
            target_postfix_roc = f"{roc_year}{season:02d}_{stock_id_str}"
            
            results = []
            for f in cached_files:
                fn = f['filename']
                if target_postfix_ce in fn or target_postfix_roc in fn:
                    results.append(f)
            return results

        # 2) 如果快取不是這間公司，則發送一次「全查」請求
        logger.info(f"  [{stock_id}] 首次查詢，拉取歷年所有財報檔案清單...")
        data = {
            'step':     '1',
            'colorchg': '1',
            'co_id':    stock_id_str,
            'year':     '',       # 留空以取得全部年份
            'seamon':   '',       # 留空以取得全部季度
            'mtype':    'A',
        }
        
        current_sess = sess
        for attempt in range(max_retries):
            try:
                resp = current_sess.post(
                    TWSE_DOC_URL,
                    data=data,
                    headers=cls._headers(),
                    timeout=20,
                    verify=False,
                )
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"  ✗ 查詢失敗（{stock_id_str} 歷年資料）嘗試 {attempt+1}/{max_retries}：{e}")
                if attempt < max_retries - 1:
                    # 指數退避：10, 20, 40, ... 加上擾動
                    delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                    logger.info(f"  等待 {delay:.1f} 秒後重試...")
                    import time
                    time.sleep(delay)
                    new_sess = cls.get_initialized_session()
                    if new_sess:
                        current_sess = new_sess
                continue

            # 解析 HTML（Big5 編碼）
            html = resp.content.decode('big5', errors='replace')

            # 找「查無資料」
            if '查無所需資料' in html:
                logger.debug(f"  [{stock_id_str}] TWSE 查無任何歷年資料")
                # 快取空結果
                cls._company_cache['stock_id'] = stock_id_str
                cls._company_cache['files'] = []
                return []

            soup = BeautifulSoup(html, 'lxml')
            all_files = []
            pattern = re.compile(r'readfile2?\("([^"]+)","([^"]+)","([^"]+)"\)')

            for a_tag in soup.find_all('a', href=True):
                href = a_tag.get('href', '')
                m = pattern.search(href)
                if not m:
                    continue
                kind, co_id, filename = m.group(1), m.group(2), m.group(3)

                desc = ''
                tr = a_tag.find_parent('tr')
                if tr:
                    tds = tr.find_all('td')
                    if len(tds) >= 6:
                        desc = tds[5].get_text(strip=True)

                all_files.append({
                    'kind':     kind,
                    'co_id':    co_id,
                    'filename': filename,
                    'desc':     desc,
                })

            if all_files:
                logger.debug(f"  [{stock_id_str}] 成功載入 {len(all_files)} 筆歷史財報清單並寫入快取")
                cls._company_cache['stock_id'] = stock_id_str
                cls._company_cache['files'] = all_files
                
                # 遞迴過濾並回傳該年份季度的檔案
                return cls._query_file_list(current_sess, stock_id, roc_year, season, max_retries)
                
            # If no results and no '查無所需資料' found: WAF blocked
            logger.warning(f"  ✗ step=1 收到非預期回應（可能遭阻擋），嘗試 {attempt+1}/{max_retries}")
            if attempt < max_retries - 1:
                delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                logger.info(f"  等待 {delay:.1f} 秒後重試...")
                import time
                time.sleep(delay)
                new_sess = cls.get_initialized_session()
                if new_sess:
                    current_sess = new_sess
        
        # Exceeded retries
        return None

    # ── Step 2：串流下載 PDF ──────────────────────────────────────

    @classmethod
    def _download_file(cls, sess: requests.Session,
                       kind: str, co_id: str, filename: str,
                       save_path: str, max_retries: int = 5) -> bool:
        """
        TWSE 三步下載流程：
          Step 2: POST step=9  → 回傳 HTML（非 PDF），內含時間戳記 /pdf/... 連結
          Step 3: GET  /pdf/.. → 實際的 PDF 串流

        若任一步驟失敗，加入自動重試與強制重新建立 Session，避免因 WAF 阻擋（BadStatusLine）而立即失敗。
        """
        current_sess = sess
        
        for attempt in range(max_retries):
            # ── Step 2：觸發下載，取得臨時 PDF 連結 ─────────────────
            data = {
                'step':     '9',
                'colorchg': '1',
                'kind':     kind,
                'co_id':    co_id,
                'filename': filename,
            }
            try:
                r2 = current_sess.post(
                    TWSE_DOC_URL,
                    data=data,
                    headers=cls._headers(),
                    timeout=30,
                    verify=False,
                )
                r2.raise_for_status()
            except Exception as e:
                logger.error(f"  ✗ step=9 請求失敗（{filename}）嘗試 {attempt+1}/{max_retries}：{e}")
                if attempt < max_retries - 1:
                    delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                    logger.info(f"  等待 {delay:.1f} 秒後重試...")
                    import time
                    time.sleep(delay)
                    new_sess = cls.get_initialized_session()
                    if new_sess:
                        current_sess = new_sess
                continue

            # 解析回傳 HTML（Big5 編碼）中的臨時檔案路徑
            # 強制要求路徑包含 /pdf/ ，避免抓到 <link href='/ppp.css'>
            html = r2.content.decode('big5', errors='replace')
            m = re.search(r"href='(/pdf/[^']+\.[a-zA-Z0-9]+)'", html)
            if not m:
                logger.warning(f"  step=9 回應中找不到下載連結，filename={filename}")
                logger.debug(f"  回應內容：{html[:200]}")
                if attempt < max_retries - 1:
                    delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                    logger.info(f"  等待 {delay:.1f} 秒後重試...")
                    import time
                    time.sleep(delay)
                    new_sess = cls.get_initialized_session()
                    if new_sess:
                        current_sess = new_sess
                continue

            file_path = m.group(1)
            file_url  = f"https://doc.twse.com.tw{file_path}"
            logger.info(f"  → GET {file_url}")

            # ── Step 3：GET 下載實際檔案串流 ────────────────────────
            try:
                with current_sess.get(
                    file_url,
                    headers=cls._headers(referer=TWSE_DOC_URL),
                    stream=True,
                    timeout=config.PDF_DOWNLOAD_TIMEOUT,
                    verify=False,
                ) as r3:
                    r3.raise_for_status()

                    # 確認非 HTML 錯誤頁（正常會是 application/pdf 或 application/msword 等）
                    ctype = r3.headers.get('Content-Type', '')
                    if 'text/html' in ctype.lower():
                        logger.warning(f"  step=3 仍收到 HTML，可能臨時連結失效或被擋：{file_url}")
                        if attempt < max_retries - 1:
                            delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                            logger.info(f"  等待 {delay:.1f} 秒後重試...")
                            import time
                            time.sleep(delay)
                            new_sess = cls.get_initialized_session()
                            if new_sess:
                                current_sess = new_sess
                        continue

                    with open(save_path, 'wb') as f:
                        for chunk in r3.iter_content(chunk_size=65536):
                            f.write(chunk)

                size_kb = os.path.getsize(save_path) / 1024
                if size_kb < 50:
                    logger.warning(f"  檔案過小（{size_kb:.0f} KB），可能不是有效檔案")
                    os.remove(save_path)
                    if attempt < max_retries - 1:
                        delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                        logger.info(f"  等待 {delay:.1f} 秒後重試...")
                        import time
                        time.sleep(delay)
                        new_sess = cls.get_initialized_session()
                        if new_sess:
                            current_sess = new_sess
                    continue

                logger.info(f"  ✓ 下載完成（{size_kb:.0f} KB）：{save_path}")
                
                # 如果在這個過程中成功更換了 session，我們需要把修改過的 session 寫回去 (如果外層有關心)
                # 這裡不特別依賴外部存取，因為通常下一季會重新呼叫 download() 但共用 session
                # 為了更好的封裝性，我們其實只在此 function 內用 current_sess
                return True

            except Exception as e:
                logger.error(f"  ✗ 檔案 GET 失敗（{file_url}）嘗試 {attempt+1}/{max_retries}：{e}")
                if os.path.exists(save_path):
                    os.remove(save_path)
                if attempt < max_retries - 1:
                    delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                    logger.info(f"  等待 {delay:.1f} 秒後重試...")
                    import time
                    time.sleep(delay)
                    new_sess = cls.get_initialized_session()
                    if new_sess:
                        current_sess = new_sess
                continue
            
        return False

    # ── Session 初始化 ────────────────────────────────────────────

    @classmethod
    def get_initialized_session(cls) -> requests.Session | None:
        """建立並初始化一個 Session（取得合法的初始 Cookie）"""
        sess = requests.Session()
        try:
            sess.get(TWSE_DOC_URL, headers=cls._headers(), timeout=10, verify=False)
            return sess
        except Exception as e:
            logger.error(f"  初始化 Session 失敗：{e}")
            return None

    # ── 公開主介面 ────────────────────────────────────────────────

    @classmethod
    def download(cls, stock_id: str, ce_year: int, season: int,
                 market_type: str = '上市',
                 session: requests.Session = None) -> str | None | bool:
        """
        下載指定公司、年份、季度的完整財務報告檔案（不限於 PDF）。

        優先下載中文合併財報（AI1），其次個體財報（AI3），
        最後接受任何其他財報格式。

        Args:
            stock_id:    股票代號（如 '2330'）
            ce_year:     西元年（如 2023）
            season:      季度 1~4
            market_type: 目前未使用（TWSE 不分市場類型），保留供擴充

        Returns:
            - str  : 成功，回傳檔案儲存路徑
            - None : TWSE 查無此季度資料（公司尚未上市或無存檔）
            - False: 下載過程失敗（網路錯誤、Session 初始化失敗、所有 retry 用盡）
        """
        roc_year  = ce_to_roc(ce_year)

        # ── 快取檢查：有效檔案直接跳過 ──────────────────────────
        directory = os.path.join(config.PDF_DIR, str(stock_id), str(ce_year))
        if os.path.exists(directory):
            for f in os.listdir(directory):
                if f.startswith(f"Q{season}.") and cls.is_valid_file(os.path.join(directory, f)):
                    save_path = os.path.join(directory, f)
                    logger.debug(f"  已存在（跳過）：{save_path}")
                    return save_path

        logger.info(
            f"  [{stock_id}] {ce_year}年Q{season}（民國{roc_year}年第{season}季）"
        )

        # 使用傳入的 Session，或建立獨立 Session（先訪問首頁取得合法 Cookie）
        if session is not None:
            sess = session
        else:
            sess = cls.get_initialized_session()
            if sess is None:
                # Session 初始化失敗屬於「下載失敗」，非「查無資料」
                return False

        # ── 查詢可下載的 PDF 清單 ─────────────────────────────────
        files = cls._query_file_list(sess, stock_id, roc_year, season)

        if files is None:
            # 查詢過程失敗 (例如被 WAF 阻擋或發生 Exception)，回傳 False 讓上層標記為 FAILED
            logger.error(f"  [{stock_id}] {ce_year}Q{season} 查詢檔案清單失敗")
            return False

        if not files:
            # 正常查詢但確實無資料 (回傳 [])
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
            
            # 從檔名決定副檔名 (若無則預設 pdf)
            ext = fn.split('.')[-1].lower() if '.' in fn else 'pdf'
            save_path = cls.get_save_path(stock_id, ce_year, season, ext)
            
            logger.info(f"  → 下載：{fn}（{desc}）")

            if cls._download_file(
                sess,
                file_info['kind'],
                file_info['co_id'],
                fn,
                save_path
            ):
                return save_path

        # 有找到檔案清單，但所有連結都下載失敗 → 屬於「下載失敗」
        logger.error(f"  [{stock_id}] {ce_year}Q{season} 全部下載連結失敗")
        return False
