#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# run_node.sh  — 背景啟動爬蟲
#
# 使用方式：
#   bash run_node.sh 1    ← 第 1 台電腦執行 node 1
#   bash run_node.sh 2    ← 第 2 台電腦執行 node 2
#   ...以此類推
#
#   bash run_node.sh 1 stop   ← 停止 node 1 的背景爬蟲
#   bash run_node.sh 1 status ← 查看 node 1 是否在執行
# ─────────────────────────────────────────────────────────────────

NODE="${1}"
ACTION="${2:-start}"   # 預設 start

# ── 基本驗證 ──────────────────────────────────────────────────────
if [[ -z "$NODE" ]]; then
    echo "❌ 請指定 node 編號，例如：bash run_node.sh 1"
    exit 1
fi

# ── 路徑設定 ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_FILE="${LOG_DIR}/node_${NODE}.log"
PID_FILE="${LOG_DIR}/node_${NODE}.pid"

mkdir -p "${LOG_DIR}"

# ── stop ──────────────────────────────────────────────────────────
if [[ "$ACTION" == "stop" ]]; then
    if [[ -f "$PID_FILE" ]]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID"
            rm -f "$PID_FILE"
            echo "✅ Node ${NODE} 已停止（PID ${PID}）"
        else
            echo "⚠️  Node ${NODE} 的 PID ${PID} 已不存在，清除 PID 檔"
            rm -f "$PID_FILE"
        fi
    else
        echo "⚠️  找不到 node_${NODE}.pid，可能未在執行"
    fi
    exit 0
fi

# ── status ────────────────────────────────────────────────────────
if [[ "$ACTION" == "status" ]]; then
    if [[ -f "$PID_FILE" ]]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "✅ Node ${NODE} 執行中（PID ${PID}）"
            echo "   日誌：${LOG_FILE}"
            echo "   最後 5 行："
            tail -5 "${LOG_FILE}" 2>/dev/null | sed 's/^/     /'
        else
            echo "❌ Node ${NODE} 已停止（PID ${PID} 已結束）"
            rm -f "$PID_FILE"
        fi
    else
        echo "❌ Node ${NODE} 未在執行"
    fi
    exit 0
fi

# ── start ─────────────────────────────────────────────────────────

# 確認 node 分配檔存在
NODE_FILE="${SCRIPT_DIR}/data/processed/node_${NODE}.txt"
if [[ ! -f "$NODE_FILE" ]]; then
    echo "❌ 找不到分配檔：${NODE_FILE}"
    echo "   請先在主電腦執行：python split_workload.py"
    exit 1
fi

# 若已有 PID 且仍在執行，拒絕重複啟動
if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "⚠️  Node ${NODE} 已在執行中（PID ${PID}），請先執行 stop"
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

# 啟動（使用 venv 內的 python 若存在，否則用系統 python3）
if [[ -f "${SCRIPT_DIR}/venv/bin/python" ]]; then
    PYTHON="${SCRIPT_DIR}/venv/bin/python"
else
    PYTHON="python3"
fi

echo "🚀 Node ${NODE} 啟動中..."
echo "   Python  ：${PYTHON}"
echo "   分配檔  ：${NODE_FILE}"
echo "   日誌    ：${LOG_FILE}"

nohup "${PYTHON}" -m src.main --all --node "${NODE}" \
    >> "${LOG_FILE}" 2>&1 &

echo $! > "${PID_FILE}"
echo "✅ 已在背景啟動，PID = $(cat "$PID_FILE")"
echo "   查看進度：bash run_node.sh ${NODE} status"
echo "   停止爬蟲：bash run_node.sh ${NODE} stop"
