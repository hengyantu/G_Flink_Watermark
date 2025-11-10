# Flink 水位线延迟实验项目

## 1. 研究背景与目的
在事件时间语义下，Flink 通过水位线（Watermark）机制解决数据乱序导致的窗口计算不准确问题。水位线延迟时间决定窗口触发的时机，也决定迟到数据是否被接受。本项目通过可控的乱序数据源和自定义实验脚本，系统分析水位线延迟时间对窗口触发延迟、迟到数据处理和结果准确度的影响，并探索在实时性与准确性之间的权衡策略。

## 2. 研究内容概述
- **水位线机制**：回顾单调递增水位线、延迟设定、允许迟到与侧输出等概念。
- **乱序数据建模**：构建可配置的乱序事件源，模拟日志上报场景。
- **窗口计算实验**：在事件时间滚动 / 滑动窗口上进行统计聚合，观察不同水位线延迟下的窗口触发延迟。
- **迟到数据分析**：记录被丢弃的迟到事件数量及迟到程度，分析延迟配置的影响。
- **权衡策略总结**：给出如何在延迟与准确性之间取得平衡的建议。

## 3. 代码结构
```
code
├── pom.xml
├── README.md
├── scripts
│   └── run_experiments.sh          # 多组参数实验脚本（可根据需要扩展）
├── src
│   ├── main
│   │   ├── java
│   │   │   └── com/example/watermark
│   │   │       ├── EventAccumulator.java
│   │   │       ├── EventAggregateFunction.java
│   │   │       ├── LateEventMetric.java
│   │   │       ├── LateEventProcessFunction.java
│   │   │       ├── OutOfOrderEventSource.java
│   │   │       ├── UserEvent.java
│   │   │       ├── WatermarkLatencyAnalysisJob.java
│   │   │       └── WindowResultProcessFunction.java
│   │   └── resources
│   └── test
│       └── java
│           └── com/example/watermark
│               └── EventAggregateFunctionTest.java
```

## 4. 核心模块说明
- `OutOfOrderEventSource`：自定义乱序事件源，可配置吞吐量、乱序程度、严重迟到比例、运行时长等。
- `WatermarkLatencyAnalysisJob`：主作业，支持命令行参数调整水位线延迟、窗口类型（滚动/滑动）、窗口大小、允许迟到、结果输出位置等。
- `WindowResultProcessFunction`：输出窗口触发时的水位线、触发延迟、聚合指标等。
- `LateEventProcessFunction`：统计被丢弃的迟到事件信息（迟到时长、发生时间等）。

## 5. 构建与运行
### 5.1 环境准备
- JDK 11+
- Maven 3.8+
- Apache Flink 1.17.x（本项目使用 Maven Shade 打包，可直接在本地或集群上提交 Jar）

### 5.2 编译
```bash
cd /root/bigdata/G_Flink_Watermark/code
mvn clean package -DskipTests
```
编译完成后，`target/` 目录下会生成可执行的 `flink-watermark-latency-1.0.0-shaded.jar`。

### 5.3 本地运行（Print Sink）
```bash
flink run -c com.example.watermark.WatermarkLatencyAnalysisJob \
  target/flink-watermark-latency-1.0.0-shaded.jar \
  --watermarkDelayMs 5000 \
  --windowType tumbling \
  --windowSizeMs 10000 \
  --eventsPerSecond 200 \
  --runDurationMs 60000
```

### 5.4 输出到文件
```bash
flink run -c com.example.watermark.WatermarkLatencyAnalysisJob \
  target/flink-watermark-latency-1.0.0-shaded.jar \
  --watermarkDelayMs 8000 \
  --windowType sliding \
  --windowSizeMs 20000 \
  --windowSlideMs 10000 \
  --allowedLatenessMs 2000 \
  --outputDir file:///tmp/flink/watermark/windows \
  --lateOutputDir file:///tmp/flink/watermark/late
```

### 5.5 参数说明
| 参数名 | 默认值 | 说明 |
| --- | --- | --- |
| `watermarkDelayMs` | 5000 | 水位线延迟时间 |
| `windowType` | `tumbling` | `tumbling` 或 `sliding` |
| `windowSizeMs` | 10000 | 窗口长度 |
| `windowSlideMs` | 同 `windowSizeMs` | 滑动窗口步长 |
| `allowedLatenessMs` | 0 | 允许迟到的时间 |
| `eventsPerSecond` | 200 | 每秒事件数（每个并行子任务） |
| `runDurationMs` | 60000 | 事件源运行时长 |
| `maxOutOfOrdernessMs` | 同 `watermarkDelayMs` | 常规乱序范围 |
| `lateEventFraction` | 0.05 | 严重迟到事件概率 |
| `severeLatenessUpperBoundMs` | 同 `watermarkDelayMs` | 严重迟到最大值 |
| `parallelism` | 2 | Flink 作业并行度 |
| `autoWatermarkIntervalMs` | 200 | 自动生成水位线的间隔 |
| `enableCheckpointing` | false | 是否开启 checkpoint |
| `outputDir` | 空 | 窗口结果输出路径，为空则打印 |
| `lateOutputDir` | 空 | 迟到事件输出路径，为空则打印 |

## 6. 实验设计建议
1. **变量控制**：一次实验仅改变 `watermarkDelayMs`，保持其他参数不变，比较窗口触发延迟和迟到事件情况。
2. **窗口类型对比**：分别在滚动与滑动窗口下运行，观察不同窗口策略对延迟的放大或缓解作用。
3. **允许迟到设置**：调整 `allowedLatenessMs` 观察迟到事件“被补救”的比例变化。
4. **乱序强度调整**：通过 `maxOutOfOrdernessMs`、`lateEventFraction` 和 `severeLatenessUpperBoundMs` 模拟不同乱序场景。
5. **多组实验脚本**：可使用 `scripts/run_experiments.sh` 批量运行，自动记录结果到时间标记目录中。

## 7. 结果记录与分析
- `WindowAggregateResult` 输出字段包含窗口起止时间、当前水位线、触发延迟、窗口内事件统计等，可用于绘制折线图展示水位线延迟与触发延迟关系。
- `LateEventMetric` 输出字段包含迟到事件的用户、事件时间、检测时间、迟到毫秒数，可统计迟到事件数量和平均迟到程度。
- 在分析报告中建议至少包含：
  - 不同水位线延迟下窗口触发延迟分布；
  - 迟到事件数量、占比随水位线延迟变化的曲线；
  - 同时展示允许迟到与否对结果准确性的影响；
  - 结合实时性需求，给出推荐的水位线延迟区间。

## 8. 扩展思路
- 引入真实日志数据，验证模拟数据结论的可迁移性。
- 结合状态大小、Checkpoint 成本，进一步评估开启允许迟到和延长水位线延迟的代价。
- 扩展不同触发策略（自定义触发器）或多流 join 场景，探索复杂算子的水位线影响。

## 9. 参考文档
- [Flink 官方文档：Watermarks and Event Time](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/event-time/generating_watermarks/)
- [Flink Training：Event Time & Watermarks](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/learn-flink/datastream_api/#event-time-and-watermarks)
- [Flink CEP 与 Watermark 联系](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/libs/cep/)

> 提示：请在撰写实验报告时附带 Flink Web UI 截图、窗口结果样例、迟到事件统计表，并对现象给出解释。


