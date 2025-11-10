package com.example.watermark;

/**
 * 聚合函数的累加器，用于在窗口内累积指标。
 */
public class EventAccumulator {

    private long count;
    private double sumAmount;
    private long minEventTime = Long.MAX_VALUE;
    private long maxEventTime = Long.MIN_VALUE;
    private double minAmount = Double.MAX_VALUE;
    private double maxAmount = -Double.MAX_VALUE;

    public void add(UserEvent event) {
        count++;
        sumAmount += event.getAmount();
        minEventTime = Math.min(minEventTime, event.getEventTime());
        maxEventTime = Math.max(maxEventTime, event.getEventTime());
        minAmount = Math.min(minAmount, event.getAmount());
        maxAmount = Math.max(maxAmount, event.getAmount());
    }

    public void merge(EventAccumulator other) {
        count += other.count;
        sumAmount += other.sumAmount;
        minEventTime = Math.min(minEventTime, other.minEventTime);
        maxEventTime = Math.max(maxEventTime, other.maxEventTime);
        minAmount = Math.min(minAmount, other.minAmount);
        maxAmount = Math.max(maxAmount, other.maxAmount);
    }

    public long getCount() {
        return count;
    }

    public double getSumAmount() {
        return sumAmount;
    }

    public long getMinEventTime() {
        return minEventTime;
    }

    public long getMaxEventTime() {
        return maxEventTime;
    }

    public double getMinAmount() {
        return minAmount;
    }

    public double getMaxAmount() {
        return maxAmount;
    }

    public double getAverageAmount() {
        return count == 0 ? 0.0 : sumAmount / count;
    }
}


