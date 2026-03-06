import os
import datetime

# ── 目錄設定 ──────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ⚠️  資料目錄刻意放在專案資料夾「外部」，
#     避免 VS Code / AI 助理索引數 GB 的 PDF 而大量消耗 token。
#     若需要改路徑，設定環境變數 TAIWAN_REPORT_DATA_DIR 即可覆蓋，
#     例如：export TAIWAN_REPORT_DATA_DIR=/Volumes/ExternalDisk/台灣財報資料
DATA_DIR      = os.environ.get(
    "TAIWAN_REPORT_DATA_DIR",
    os.path.join(os.path.expanduser("~"), "Documents", "台灣財報資料")
)
PDF_DIR       = os.path.join(DATA_DIR, "pdfs")       # 財報 PDF 存放根目錄
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")  # CSV / SQLite

# 確保資料夾存在
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# SQLite 進度追蹤資料庫路徑（不儲存財報數字，只記錄下載進度）
DB_PATH = os.path.join(PROCESSED_DIR, "download_progress.sqlite")

# 股票清單 CSV 路徑
STOCK_LIST_CSV = os.path.join(PROCESSED_DIR, "taiwan_stock_list.csv")

# ── 年份設定 ──────────────────────────────────────────────
CURRENT_YEAR = datetime.datetime.now().year
DEFAULT_EARLIEST_YEAR = 2013  # 上市日期查不到時的 fallback

# IFRS 強制採用年：台灣上市公司自 2013 年起按 IFRS 編製財報
# MOPS/TWSE 早於此年份的資料幾乎查無，設為爬蟲起始年下限
IFRS_START_YEAR = 2013

# ── 抓取設定 ──────────────────────────────────────────────
RETRY_COUNT = 5

# 同一公司的跨季度之間（因為重用 Session，風險較低）
INTRA_COMPANY_MIN_DELAY = 1.0
INTRA_COMPANY_MAX_DELAY = 2.5

# 跨公司之間（需要重置連線，主要封鎖風險點）
INTER_COMPANY_MIN_DELAY = 5.0
INTER_COMPANY_MAX_DELAY = 10.0

# 查無資料（SKIPPED）時的短暫緩衝延遲（避免過快請求，但不需要完整間隔）
SKIP_MIN_DELAY = 0.5
SKIP_MAX_DELAY = 1.5

# PDF 下載逾時（秒）：財報 PDF 可能較大，給足夠時間
PDF_DOWNLOAD_TIMEOUT = 120

# ── 文件下載端點 ──────────────────────────────────────────
# TWSE 文件中心：上市公司財務報告
TWSE_DOC_QUERY_URL = "https://doc.twse.com.tw/server-java/t57sb01"
TWSE_DOC_BASE_URL  = "https://doc.twse.com.tw"

# MOPS 文件查詢：作為備援（主要用於上櫃/興櫃）
MOPS_DOC_QUERY_URL = "https://mops.twse.com.tw/mops/web/ajax_t164sb03"
MOPS_BASE_URL      = "https://mops.twse.com.tw"

# ISIN 頁面（抓股票清單用）
ISIN_BASE_URL = "https://isin.twse.com.tw/isin/C_public.jsp?strMode={}"

# ── User-Agent 清單（隨機挑選以偽裝成不同瀏覽器）─────────────
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]
