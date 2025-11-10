package com.example.watermark;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 将迟到事件转换为对应的指标对象。
 */
public class LateEventProcessFunction extends ProcessFunction<UserEvent, LateEventMetric> {

    @Override
    public void processElement(UserEvent value, Context ctx, Collector<LateEventMetric> out) {
        long currentWatermark = ctx.timerService().currentWatermark();
        long lateness = currentWatermark >= 0 ? currentWatermark - value.getEventTime() : -1L;
        out.collect(new LateEventMetric(
                value.getUserId(),
                value.getEventTime(),
                System.currentTimeMillis(),
                lateness));
    }
}


