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

為了加速下載，目前專案已經預先將 2000 多家上市櫃公司，均分為 5 等份（存放在 `data/processed/node_1.txt` ~ `node_5.txt`）。

你只需要將這個已經分配好資料夾的專案（含 `data/processed/`），整包複製到其他不同的電腦上，並指定每一台跑不同的 Node 編號即可。

### 步驟說明

1. 將整個專案（包含 `data/processed/node_*.txt` 等資料）複製到 5 台不同的電腦。
2. 在各自的電腦上安裝環境：
   ```bash
   python -m venv venv
   source venv/bin/activate      # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. 在各台電腦的終端機執行對應的腳本，請依序將 `[你的編號]` 換成 1~5：

#### 方式 A：背景執行（推薦，關閉終端機也會繼續跑）
```bash
bash run_node.sh [你的編號]
# 範例：第一台電腦執行 bash run_node.sh 1
# 範例：第二台電腦執行 bash run_node.sh 2
```

**管理背景爬蟲**
- 查看狀態：`bash run_node.sh 1 status`
- 查看即時日誌：`tail -f logs/node_1.log`
- 停止程式：`bash run_node.sh 1 stop`

#### 方式 B：前景執行（方便直接看畫面）
```bash
python -m src.main --all --node [你的編號]
# 範例：第一台電腦執行 python -m src.main --all --node 1
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

