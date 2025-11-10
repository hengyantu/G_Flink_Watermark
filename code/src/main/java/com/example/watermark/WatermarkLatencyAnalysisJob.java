package com.example.watermark;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * Flink 水位线延迟实验主作业。
 */
public final class WatermarkLatencyAnalysisJob {

    private WatermarkLatencyAnalysisJob() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        long watermarkDelayMs = params.getLong("watermarkDelayMs", 5000L);
        long windowSizeMs = params.getLong("windowSizeMs", 10000L);
        long windowSlideMs = params.getLong("windowSlideMs", windowSizeMs);
        long allowedLatenessMs = params.getLong("allowedLatenessMs", 0L);
        String windowType = params.get("windowType", "tumbling");
        int eventsPerSecond = params.getInt("eventsPerSecond", 200);
        long runDurationMs = params.getLong("runDurationMs", 60000L);
        long maxOutOfOrdernessMs = params.getLong("maxOutOfOrdernessMs", watermarkDelayMs);
        double lateEventFraction = params.getDouble("lateEventFraction", 0.05);
        long severeLatenessUpperBoundMs = params.getLong("severeLatenessUpperBoundMs", watermarkDelayMs);
        int parallelism = params.getInt("parallelism", 2);
        long autoWatermarkIntervalMs = params.getLong("autoWatermarkIntervalMs", 200L);
        boolean enableCheckpointing = params.getBoolean("enableCheckpointing", false);
        long checkpointIntervalMs = params.getLong("checkpointIntervalMs", 60000L);
        String outputDir = params.get("outputDir", "");
        String lateOutputDir = params.get("lateOutputDir", "");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().setAutoWatermarkInterval(autoWatermarkIntervalMs);
        env.getConfig().setGlobalJobParameters(params);

        if (enableCheckpointing) {
            env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointIntervalMs / 2);
            env.getCheckpointConfig().setCheckpointTimeout(Math.max(checkpointIntervalMs * 2, 120_000L));
            env.setStateBackend(new MemoryStateBackend());
        }

        WatermarkStrategy<UserEvent> watermarkStrategy = WatermarkStrategy
                .<UserEvent>forBoundedOutOfOrderness(Duration.ofMillis(watermarkDelayMs))
                .withTimestampAssigner((event, recordTimestamp) -> event.getEventTime());

        DataStream<UserEvent> rawEvents = env.addSource(
                new OutOfOrderEventSource(
                        eventsPerSecond,
                        runDurationMs,
                        maxOutOfOrdernessMs,
                        lateEventFraction,
                        severeLatenessUpperBoundMs))
                .name("synthetic-out-of-order-source");

        DataStream<UserEvent> eventsWithTimestamps = rawEvents
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("assign-watermarks");

        OutputTag<UserEvent> lateOutputTag = new OutputTag<UserEvent>("late-events") {
        };

        WindowedStream<UserEvent, String, TimeWindow> windowedStream = createWindowedStream(
                eventsWithTimestamps,
                windowType,
                windowSizeMs,
                windowSlideMs);

        if (allowedLatenessMs > 0) {
            windowedStream = windowedStream.allowedLateness(Time.milliseconds(allowedLatenessMs));
        }

        windowedStream = windowedStream.sideOutputLateData(lateOutputTag);

        SingleOutputStreamOperator<WindowAggregateResult> windowResults = windowedStream
                .aggregate(new EventAggregateFunction(), new WindowResultProcessFunction(watermarkDelayMs))
                .name("window-aggregation");

        DataStream<UserEvent> lateEvents = windowResults.getSideOutput(lateOutputTag);
        DataStream<LateEventMetric> lateMetrics = lateEvents
                .process(new LateEventProcessFunction())
                .name("late-event-metrics");

        sinkResults(windowResults, outputDir, "window-results");
        sinkLateMetrics(lateMetrics, lateOutputDir, "late-metrics");

        env.execute("Flink Watermark Latency Analysis");
    }

    private static WindowedStream<UserEvent, String, TimeWindow> createWindowedStream(
            DataStream<UserEvent> events,
            String windowType,
            long windowSizeMs,
            long windowSlideMs) {
        Time windowSize = Time.milliseconds(windowSizeMs);
        Time windowSlide = Time.milliseconds(windowSlideMs);

        if ("sliding".equalsIgnoreCase(windowType)) {
            return events
                    .keyBy(UserEvent::getUserId)
                    .window(SlidingEventTimeWindows.of(windowSize, windowSlide));
        }

        return events
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(windowSize));
    }

    private static void sinkResults(DataStream<?> stream, String outputDir, String sinkName) {
        if (outputDir == null || outputDir.isEmpty()) {
            stream.addSink(new PrintSinkFunction<>()).name(sinkName + "-print");
            return;
        }

        stream.map(Object::toString)
                .name(sinkName + "-map-to-string")
                .writeAsText(outputDir, WriteMode.OVERWRITE)
                .name(sinkName + "-file");
    }

    private static void sinkLateMetrics(DataStream<LateEventMetric> lateMetrics, String outputDir, String sinkName) {
        if (lateMetrics == null) {
            return;
        }

        if (outputDir == null || outputDir.isEmpty()) {
            lateMetrics.addSink(new PrintSinkFunction<>()).name(sinkName + "-print");
        } else {
            lateMetrics.map(LateEventMetric::toString)
                    .name(sinkName + "-map-to-string")
                    .writeAsText(outputDir, WriteMode.OVERWRITE)
                    .name(sinkName + "-file");
        }
    }
}

