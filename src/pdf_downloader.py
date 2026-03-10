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
import time
import concurrent.futures
from bs4 import BeautifulSoup
from . import config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

TWSE_DOC_URL = "https://doc.twse.com.tw/server-java/t57sb01"

# 全域連鎖失敗計數器，用於觸發長期休眠
_consecutive_failures = 0
_MAX_CONSECUTIVE_FAILURES = 15

# 遞增休眠（指數退避）設定
_BASE_COOLDOWN_SECONDS = 900  # 基礎休眠：15 分鐘
_MAX_COOLDOWN_SECONDS = 7200  # 最大休眠：2 小時
_current_cooldown = _BASE_COOLDOWN_SECONDS


def ce_to_roc(ce_year: int) -> int:
    """西元年轉民國年（e.g. 2024 → 113）"""
    return ce_year - 1911


class PdfDownloader:
    """從 TWSE 文件中心下載財務報告 PDF（合併財報優先）"""
    
    # 用於暫存某間公司歷年所有財報清單的快取，避免每季發送查詢請求
    _company_cache = {
        'stock_id': None,
        'fetched': False,
        'files': []
    }
    
    # 精準記錄真實的網路封包請求數量，以決定是否需要長時間的防封鎖等待
    network_requests_this_session = 0

    @classmethod
    def clear_cache(cls):
        """清空當前公司的檔案清單快取"""
        cls._company_cache = {
            'stock_id': None,
            'fetched': False,
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
        
    # ── 防卡死 (Hard Timeout) 網路請求 ─────────────────────────────────

    @classmethod
    def _safe_request(cls, method, url, max_time=30, **kwargs):
        """
        確保 requests 呼叫在 max_time 秒內必然返回或拋出 TimeoutError，
        用於對付 WAF Tarpit (慢速滴漏攻擊) 所導致的無窮卡死問題。
        """
        def do_req():
            cls.network_requests_this_session += 1
            if method.lower() == 'get':
                return requests.get(url, **kwargs)
            else:
                return requests.post(url, **kwargs)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(do_req)
            try:
                # 設定硬時限
                return future.result(timeout=max_time)
            except concurrent.futures.TimeoutError:
                # 超時後發出例外，主執行緒不再被綁架
                raise TimeoutError(f"HTTP 請求強制逾時 (>{max_time}s) 疑似遭遇 Tarpit 限制")
                
    @classmethod
    def _safe_download_stream(cls, url, save_path, max_time=60, **kwargs):
        """
        以串流方式安全下載檔案，具備整體最大耗時保證 (Hard Timeout)。
        """
        def do_download():
            cls.network_requests_this_session += 1
            with requests.get(url, stream=True, **kwargs) as r:
                r.raise_for_status()
                ctype = r.headers.get('Content-Type', '')
                if 'text/html' in ctype.lower():
                    return "HTML" # 被擋，返回特殊標記
                    
                with open(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)
                return "SUCCESS"

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(do_download)
            try:
                return future.result(timeout=max_time)
            except concurrent.futures.TimeoutError:
                raise TimeoutError(f"下載串流強制逾時 (>{max_time}s)")


    @classmethod
    def _check_cooldown(cls):
        """檢查是否達到連續失敗次數，觸發長時冷卻（具備遞增懲罰機制）"""
        global _consecutive_failures, _current_cooldown
        if _consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
            logger.critical(f"偵測到連續 {_consecutive_failures} 次嚴重失敗，可能已遭 WAF 封鎖 IP！")
            
            minutes = _current_cooldown // 60
            logger.critical(f"強制進入 {minutes} 分鐘 ({_current_cooldown} 秒) 的深層休眠冷卻...")
            time.sleep(_current_cooldown)
            
            # 醒來後，將下次的冷卻時間翻倍（上限為 _MAX_COOLDOWN_SECONDS）
            _current_cooldown = min(_current_cooldown * 2, _MAX_COOLDOWN_SECONDS)
            
            _consecutive_failures = 0  # 醒來後歸零重試
            logger.info("深層休眠結束，恢復爬蟲作業。")

    # ── Step 1：查詢可下載的 PDF 清單 ────────────────────────────

    @classmethod
    def _query_file_list(cls, sess: requests.Session,
                         stock_id: str, roc_year: int, season: int, max_retries: int = 5) -> list[dict] | None:
        """
        從 TWSE 取得指定股票、年份、季度的 PDF 清單。
        """
        global _consecutive_failures, _current_cooldown
        stock_id_str = str(stock_id)

        # 1) 如果快取已經是這間公司且已查詢過，直接從快取過濾
        if cls._company_cache['stock_id'] == stock_id_str and cls._company_cache['fetched']:
            cached_files = cls._company_cache['files']
                
            ce_year = roc_year + 1911
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
        
        current_cookies = sess.cookies.get_dict() if sess else {}
        
        for attempt in range(max_retries):
            cls._check_cooldown()
            try:
                resp = cls._safe_request(
                    'post',
                    TWSE_DOC_URL,
                    data=data,
                    headers=cls._headers(),
                    cookies=current_cookies,
                    timeout=20,     # requests 的 recv timeout
                    max_time=35,    # Hard timeout，35秒強切
                    verify=False,
                )
                resp.raise_for_status()
                _consecutive_failures = 0 # 成功則歸零
                _current_cooldown = _BASE_COOLDOWN_SECONDS # 成功連線，重置冷卻時間
                
            except Exception as e:
                logger.error(f"  ✗ 查詢失敗（{stock_id_str} 歷年資料）嘗試 {attempt+1}/{max_retries}：{e}")
                _consecutive_failures += 1
                if attempt < max_retries - 1:
                    delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                    logger.info(f"  等待 {delay:.1f} 秒後重試...")
                    time.sleep(delay)
                    new_sess = cls.get_initialized_session()
                    if new_sess:
                        current_cookies = new_sess.cookies.get_dict()
                continue

            html = resp.content.decode('big5', errors='replace')

            if '查無所需資料' in html:
                logger.debug(f"  [{stock_id_str}] TWSE 查無任何歷年資料")
                cls._company_cache['stock_id'] = stock_id_str
                cls._company_cache['fetched'] = True
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
                cls._company_cache['fetched'] = True
                cls._company_cache['files'] = all_files
                
                return cls._query_file_list(sess, stock_id, roc_year, season, max_retries)
                
            logger.warning(f"  ✗ step=1 收到非預期回應（可能遭阻擋），嘗試 {attempt+1}/{max_retries}")
            _consecutive_failures += 1
            if attempt < max_retries - 1:
                delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                logger.info(f"  等待 {delay:.1f} 秒後重試...")
                time.sleep(delay)
                new_sess = cls.get_initialized_session()
                if new_sess:
                    current_cookies = new_sess.cookies.get_dict()
        
        return None

    # ── Step 2：串流下載 PDF ──────────────────────────────────────

    @classmethod
    def _download_file(cls, sess: requests.Session,
                       kind: str, co_id: str, filename: str,
                       save_path: str, max_retries: int = 5) -> bool:
        """
        TWSE 三步下載流程：
          Step 2: POST step=9  → 回傳 HTML，內含臨時 /pdf/... 連結
          Step 3: GET  /pdf/.. → PDF 串流
        加入 Timeout 防止爬蟲卡死整晚。
        """
        global _consecutive_failures, _current_cooldown
        current_cookies = sess.cookies.get_dict() if sess else {}
        
        for attempt in range(max_retries):
            cls._check_cooldown()
            # ── Step 2：觸發下載，取得臨時 PDF 連結 ─────────────────
            data = {
                'step':     '9',
                'colorchg': '1',
                'kind':     kind,
                'co_id':    co_id,
                'filename': filename,
            }
            try:
                r2 = cls._safe_request(
                    'post',
                    TWSE_DOC_URL,
                    data=data,
                    headers=cls._headers(),
                    cookies=current_cookies,
                    timeout=20,
                    max_time=35, # Hard Timeout 35s
                    verify=False,
                )
                r2.raise_for_status()
                _consecutive_failures = 0
                _current_cooldown = _BASE_COOLDOWN_SECONDS
            except Exception as e:
                logger.error(f"  ✗ step=9 請求失敗（{filename}）嘗試 {attempt+1}/{max_retries}：{e}")
                _consecutive_failures += 1
                if attempt < max_retries - 1:
                    delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                    logger.info(f"  等待 {delay:.1f} 秒後重試...")
                    time.sleep(delay)
                    new_sess = cls.get_initialized_session()
                    if new_sess:
                        current_cookies = new_sess.cookies.get_dict()
                continue

            html = r2.content.decode('big5', errors='replace')
            m = re.search(r"href='(/pdf/[^']+\.[a-zA-Z0-9]+)'", html)
            if not m:
                logger.warning(f"  step=9 回應中找不到下載連結，filename={filename}")
                _consecutive_failures += 1
                if attempt < max_retries - 1:
                    delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                    logger.info(f"  等待 {delay:.1f} 秒後重試...")
                    time.sleep(delay)
                    new_sess = cls.get_initialized_session()
                    if new_sess:
                        current_cookies = new_sess.cookies.get_dict()
                continue

            file_path = m.group(1)
            file_url  = f"https://doc.twse.com.tw{file_path}"
            logger.info(f"  → GET {file_url}")

            # ── Step 3：GET 下載實際檔案串流 ────────────────────────
            try:
                # 設定最長下載容忍時間：原本通常只需5秒以內，我們設 75 秒 Hard Timeout 防卡死
                result = cls._safe_download_stream(
                    file_url,
                    save_path,
                    max_time=75,
                    headers=cls._headers(referer=TWSE_DOC_URL),
                    cookies=current_cookies,
                    timeout=config.PDF_DOWNLOAD_TIMEOUT,
                    verify=False
                )
                
                if result == "HTML":
                    logger.warning(f"  step=3 仍收到 HTML，可能臨時連結失效或被擋：{file_url}")
                    _consecutive_failures += 1
                    if attempt < max_retries - 1:
                        delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                        logger.info(f"  等待 {delay:.1f} 秒後重試...")
                        time.sleep(delay)
                        new_sess = cls.get_initialized_session()
                        if new_sess:
                            current_cookies = new_sess.cookies.get_dict()
                    continue

                size_kb = os.path.getsize(save_path) / 1024
                if size_kb < 50:
                    logger.warning(f"  檔案過小（{size_kb:.0f} KB），可能不是有效檔案")
                    os.remove(save_path)
                    if attempt < max_retries - 1:
                        delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                        logger.info(f"  等待 {delay:.1f} 秒後重試...")
                        time.sleep(delay)
                        new_sess = cls.get_initialized_session()
                        if new_sess:
                            current_cookies = new_sess.cookies.get_dict()
                    continue

                logger.info(f"  ✓ 下載完成（{size_kb:.0f} KB）：{save_path}")
                _consecutive_failures = 0
                _current_cooldown = _BASE_COOLDOWN_SECONDS
                return True

            except Exception as e:
                logger.error(f"  ✗ 檔案 GET 失敗（{file_url}）嘗試 {attempt+1}/{max_retries}：{e}")
                _consecutive_failures += 1
                if os.path.exists(save_path):
                    os.remove(save_path)
                if attempt < max_retries - 1:
                    delay = min(10 * (2 ** attempt) + random.uniform(1, 5), 120)
                    logger.info(f"  等待 {delay:.1f} 秒後重試...")
                    time.sleep(delay)
                    new_sess = cls.get_initialized_session()
                    if new_sess:
                        current_cookies = new_sess.cookies.get_dict()
                continue
            
        return False

    # ── Session 初始化 ────────────────────────────────────────────

    @classmethod
    def get_initialized_session(cls) -> requests.Session | None:
        """建立並初始化一個 Session（取得合法的初始 Cookie），受冷卻與逾時保護"""
        sess = requests.Session()
        try:
            # 同樣包上一層安全網防卡死
            cls._safe_request(
                'get',
                TWSE_DOC_URL,
                max_time=25,
                timeout=10,
                headers=cls._headers(),
                verify=False
            )
            # 將 Cookie 手動綁入 sess 中 (雖然 safe_request 每次起新請求，但 session 可被延續)
            response_cookies = cls._safe_request(
                 'get',
                 TWSE_DOC_URL,
                 max_time=25,
                 timeout=10,
                 headers=cls._headers(),
                 verify=False
            )
            sess.cookies.update(response_cookies.cookies)
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

        if session is not None:
            sess = session
        else:
            sess = cls.get_initialized_session()
            if sess is None:
                return False

        # ── 查詢可下載的 PDF 清單 ─────────────────────────────────
        files = cls._query_file_list(sess, stock_id, roc_year, season)

        if files is None:
            logger.error(f"  [{stock_id}] {ce_year}Q{season} 查詢檔案清單失敗")
            return False

        if not files:
            return None

        logger.debug(f"  找到 {len(files)} 個檔案：{[f['filename'] for f in files]}")

        # ── 選擇優先下載的檔案 ────────────────────────────────────
        def priority(f):
            n = f['filename'].upper()
            if 'AI1' in n:  return 0   # IFRSs合併財報
            if 'AI3' in n:  return 1   # IFRSs個體財報
            if 'AIA' in n:  return 2   # 英文合併版
            return 3

        files_sorted = sorted(files, key=priority)

        # ── 下載（依優先順序，成功一個就結束）────────────────────
        for file_info in files_sorted:
            fn = file_info['filename']
            desc = file_info.get('desc', '')
            
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

        logger.error(f"  [{stock_id}] {ce_year}Q{season} 全部下載連結失敗")
        return False
