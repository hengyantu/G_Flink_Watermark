package com.example.watermark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * 简单验证 {@link EventAggregateFunction} 聚合逻辑正确性。
 */
class EventAggregateFunctionTest {

    @Test
    void testAggregate() {
        EventAggregateFunction function = new EventAggregateFunction();
        EventAccumulator accumulator = function.createAccumulator();

        UserEvent e1 = new UserEvent("user-a", "click", 1000L, 10.0, 1010L);
        UserEvent e2 = new UserEvent("user-a", "purchase", 1500L, 25.5, 1510L);
        UserEvent e3 = new UserEvent("user-a", "view", 1200L, 5.5, 1210L);

        accumulator = function.add(e1, accumulator);
        accumulator = function.add(e2, accumulator);
        accumulator = function.add(e3, accumulator);

        EventAccumulator result = function.getResult(accumulator);
        assertEquals(3L, result.getCount());
        assertEquals(41.0, result.getSumAmount(), 1e-6);
        assertEquals(1000L, result.getMinEventTime());
        assertEquals(1500L, result.getMaxEventTime());
        assertEquals(5.5, result.getMinAmount(), 1e-6);
        assertEquals(25.5, result.getMaxAmount(), 1e-6);
        assertEquals(41.0 / 3, result.getAverageAmount(), 1e-6);

        EventAccumulator accumulator2 = function.createAccumulator();
        accumulator2 = function.add(new UserEvent("user-b", "click", 900L, 7.0, 905L), accumulator2);
        result.merge(accumulator2);
        assertEquals(4L, result.getCount());
        assertTrue(result.getMaxEventTime() >= result.getMinEventTime());
    }
}

