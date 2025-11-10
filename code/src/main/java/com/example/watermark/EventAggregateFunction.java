package com.example.watermark;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 将窗口内的 {@link UserEvent} 汇总为 {@link EventAccumulator}。
 */
public class EventAggregateFunction implements AggregateFunction<UserEvent, EventAccumulator, EventAccumulator> {

    @Override
    public EventAccumulator createAccumulator() {
        return new EventAccumulator();
    }

    @Override
    public EventAccumulator add(UserEvent value, EventAccumulator accumulator) {
        accumulator.add(value);
        return accumulator;
    }

    @Override
    public EventAccumulator getResult(EventAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public EventAccumulator merge(EventAccumulator a, EventAccumulator b) {
        a.merge(b);
        return a;
    }
}


