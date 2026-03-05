# 台灣上市櫃公司財報爬蟲

自動下載全台上市、上櫃、興櫃公司歷年財報 PDF，支援斷點續爬與多台電腦分散執行。

---

## 安裝

```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

## 單機執行

### 1. 取得最新股票清單
```bash
python -m src.main --fetch-list
```

### 2. 開始下載所有公司財報
```bash
python -m src.main --all
```

### 其他指令

| 指令 | 說明 |
|------|------|
| `python -m src.main --stock 2330` | 下載單一公司 |
| `python -m src.main --all --limit 5` | 只下載前 5 間（測試用）|
| `python -m src.main --retry-failed` | 重試失敗項目 |
| `python -m src.main --all --start-year 2020` | 指定起始年份 |

---

## 多台電腦分散執行

將未完成的公司清單平均分配給多台電腦，各自獨立爬取，互不干擾。

### 步驟一：在主電腦產生分配檔（只需做一次）

```bash
python split_workload.py          # 預設分成 5 份
python split_workload.py --parts 3  # 分成 3 份
python split_workload.py --dry-run  # 只預覽，不寫入
```

執行後會自動在 `data/processed/` 產生 `node_1.txt` ～ `node_N.txt`，
每個檔案記錄該台電腦負責爬取的股票代號。

### 步驟二：複製整個資料夾給各台電腦

> 確保 `data/processed/node_*.txt` 和 `data/processed/taiwan_stock_list.csv` 都一起複製過去。

### 步驟三：各台電腦在背景啟動爬蟲

```bash
bash run_node.sh 1    # 電腦 1
bash run_node.sh 2    # 電腦 2
bash run_node.sh 3    # 電腦 3
bash run_node.sh 4    # 電腦 4
bash run_node.sh 5    # 電腦 5
```

### 管理背景爬蟲

```bash
bash run_node.sh 1 status   # 查看執行狀態與最新 log
bash run_node.sh 1 stop     # 停止爬蟲
```

Log 存放於 `logs/node_1.log`、`logs/node_2.log` ...

---

## 目錄結構

```
.
├── src/
│   ├── main.py            # 主程式
│   ├── config.py          # 設定（延遲、路徑、URL 等）
│   ├── stock_list.py      # 股票清單抓取
│   ├── pdf_downloader.py  # PDF 下載核心
│   └── progress_tracker.py# SQLite 斷點追蹤
├── data/
│   ├── pdfs/              # 下載的財報 PDF（依 stock_id 分資料夾）
│   └── processed/
│       ├── download_progress.sqlite  # 下載進度資料庫
│       ├── taiwan_stock_list.csv     # 股票清單
│       └── node_*.txt                # 各電腦分配清單
├── logs/                  # 背景執行 log 與 PID 檔
├── split_workload.py      # 工作量分割工具
├── run_node.sh            # 背景啟動/停止腳本
└── import_old_progress.py # 舊進度匯入工具
```

---

## 輸出格式

財報 PDF 存放路徑：`data/pdfs/{stock_id}/{ce_year}/Q{season}.pdf`

範例：`data/pdfs/2330/2023/Q3.pdf`

