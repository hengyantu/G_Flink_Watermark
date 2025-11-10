#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${PROJECT_DIR}/target"
JAR_NAME="flink-watermark-latency-1.0.0.jar"
JAR_PATH="${TARGET_DIR}/${JAR_NAME}"

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "未找到 ${JAR_PATH}，请先执行：mvn -q -f \"${PROJECT_DIR}\" clean package -DskipTests"
  exit 1
fi

TS=$(date +%Y%m%d_%H%M%S)
OUT_BASE="${PROJECT_DIR}/outputs/${TS}"
mkdir -p "${OUT_BASE}"

WATERMARK_DELAYS=(2000 5000 8000 12000)

echo "开始实验，输出目录：${OUT_BASE}"
for DELAY in "${WATERMARK_DELAYS[@]}"; do
  RUN_DIR="${OUT_BASE}/delay_${DELAY}"
  mkdir -p "${RUN_DIR}"
  echo "[RUN] watermarkDelayMs=${DELAY}"
  "${FLINK_HOME}/bin/flink" run \
    -c com.example.watermark.WatermarkLatencyAnalysisJob \
    "${JAR_PATH}" \
    --watermarkDelayMs "${DELAY}" \
    --windowType tumbling \
    --windowSizeMs 10000 \
    --eventsPerSecond 200 \
    --runDurationMs 60000 \
    --outputDir "file://${RUN_DIR}/window_results" \
    --lateOutputDir "file://${RUN_DIR}/late_events"
done

echo "实验完成，开始分析与绘图..."

# 依赖：python3 + matplotlib
if ! command -v python3 >/dev/null 2>&1 ; then
  echo "未检测到 python3，请先安装 python3。"
  exit 1
fi

python3 "${PROJECT_DIR}/scripts/analyze_and_plot.py" \
  --base_dir "${OUT_BASE}" \
  --out_dir "${OUT_BASE}/analysis"

echo "完成。请查看："
echo "  ${OUT_BASE}/analysis/summary.csv"
echo "  ${OUT_BASE}/analysis/avg_trigger_lag.png"
echo "  ${OUT_BASE}/analysis/late_events.png"


