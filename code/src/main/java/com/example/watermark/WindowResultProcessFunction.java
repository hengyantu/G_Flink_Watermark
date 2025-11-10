package com.example.watermark;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 将窗口内的累加器转换为最终的聚合结果，并记录触发时的水位线信息。
 */
public class WindowResultProcessFunction extends ProcessWindowFunction<EventAccumulator, WindowAggregateResult, String, TimeWindow> {

    private final long configuredWatermarkDelayMs;

    public WindowResultProcessFunction(long configuredWatermarkDelayMs) {
        this.configuredWatermarkDelayMs = configuredWatermarkDelayMs;
    }

    @Override
    public void process(String key, Context context, Iterable<EventAccumulator> elements, Collector<WindowAggregateResult> out) {
        EventAccumulator accumulator = elements.iterator().next();

        long currentWatermark = context.currentWatermark();
        long windowEnd = context.window().getEnd();
        long triggerLagMs = currentWatermark >= 0 ? Math.max(0L, currentWatermark - windowEnd) : -1L;
        long triggerSystemTime = System.currentTimeMillis();

        WindowAggregateResult result = new WindowAggregateResult(
                key,
                context.window().getStart(),
                windowEnd,
                configuredWatermarkDelayMs,
                currentWatermark,
                triggerLagMs,
                triggerSystemTime,
                accumulator.getCount(),
                accumulator.getSumAmount(),
                accumulator.getAverageAmount(),
                accumulator.getMinAmount(),
                accumulator.getMaxAmount(),
                accumulator.getMinEventTime(),
                accumulator.getMaxEventTime()
        );

        out.collect(result);
    }
}


