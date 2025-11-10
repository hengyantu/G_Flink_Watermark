#!/usr/bin/env bash

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${PROJECT_DIR}/target"
JAR_NAME="flink-watermark-latency-1.0.0-shaded.jar"
JAR_PATH="${TARGET_DIR}/${JAR_NAME}"

if [[ ! -f "${JAR_PATH}" ]]; then
  echo "Jar 文件不存在，请先运行 'mvn clean package -DskipTests' 生成 ${JAR_NAME}"
  exit 1
fi

OUTPUT_BASE="${PROJECT_DIR}/outputs/$(date +%Y%m%d_%H%M%S)"
mkdir -p "${OUTPUT_BASE}"

WATERMARK_DELAYS=(2000 5000 8000 12000)

echo "实验输出目录：${OUTPUT_BASE}"

for DELAY in "${WATERMARK_DELAYS[@]}"; do
  RUN_DIR="${OUTPUT_BASE}/delay_${DELAY}"
  mkdir -p "${RUN_DIR}"

  echo "执行水位线延迟 ${DELAY} ms 的实验..."
  flink run -c com.example.watermark.WatermarkLatencyAnalysisJob \
    "${JAR_PATH}" \
    --watermarkDelayMs "${DELAY}" \
    --windowType tumbling \
    --windowSizeMs 10000 \
    --eventsPerSecond 200 \
    --runDurationMs 120000 \
    --outputDir "file://${RUN_DIR}/window_results" \
    --lateOutputDir "file://${RUN_DIR}/late_events" \
    --parallelism 2
done

echo "实验全部完成。结果位于 ${OUTPUT_BASE}"


