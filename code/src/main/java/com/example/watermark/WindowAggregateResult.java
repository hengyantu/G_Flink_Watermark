package com.example.watermark;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;

/**
 * 窗口聚合结果以及触发时刻的相关指标。
 */
public class WindowAggregateResult {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withLocale(Locale.CHINA)
            .withZone(ZoneId.systemDefault());

    private String userId;
    private long windowStart;
    private long windowEnd;
    private long configuredWatermarkDelayMs;
    private long currentWatermark;
    private long triggerLagMs;
    private long triggerSystemTime;
    private long eventCount;
    private double sumAmount;
    private double averageAmount;
    private double minAmount;
    private double maxAmount;
    private long minEventTime;
    private long maxEventTime;

    public WindowAggregateResult() {
    }

    public WindowAggregateResult(
            String userId,
            long windowStart,
            long windowEnd,
            long configuredWatermarkDelayMs,
            long currentWatermark,
            long triggerLagMs,
            long triggerSystemTime,
            long eventCount,
            double sumAmount,
            double averageAmount,
            double minAmount,
            double maxAmount,
            long minEventTime,
            long maxEventTime) {
        this.userId = userId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.configuredWatermarkDelayMs = configuredWatermarkDelayMs;
        this.currentWatermark = currentWatermark;
        this.triggerLagMs = triggerLagMs;
        this.triggerSystemTime = triggerSystemTime;
        this.eventCount = eventCount;
        this.sumAmount = sumAmount;
        this.averageAmount = averageAmount;
        this.minAmount = minAmount;
        this.maxAmount = maxAmount;
        this.minEventTime = minEventTime;
        this.maxEventTime = maxEventTime;
    }

    public String getUserId() {
        return userId;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public long getConfiguredWatermarkDelayMs() {
        return configuredWatermarkDelayMs;
    }

    public long getCurrentWatermark() {
        return currentWatermark;
    }

    public long getTriggerLagMs() {
        return triggerLagMs;
    }

    public long getTriggerSystemTime() {
        return triggerSystemTime;
    }

    public long getEventCount() {
        return eventCount;
    }

    public double getSumAmount() {
        return sumAmount;
    }

    public double getAverageAmount() {
        return averageAmount;
    }

    public double getMinAmount() {
        return minAmount;
    }

    public double getMaxAmount() {
        return maxAmount;
    }

    public long getMinEventTime() {
        return minEventTime;
    }

    public long getMaxEventTime() {
        return maxEventTime;
    }

    public String toCsvLine() {
        return String.join(",",
                userId,
                String.valueOf(windowStart),
                String.valueOf(windowEnd),
                String.valueOf(configuredWatermarkDelayMs),
                String.valueOf(currentWatermark),
                String.valueOf(triggerLagMs),
                String.valueOf(triggerSystemTime),
                String.valueOf(eventCount),
                String.format(Locale.US, "%.2f", sumAmount),
                String.format(Locale.US, "%.2f", averageAmount),
                String.format(Locale.US, "%.2f", minAmount),
                String.format(Locale.US, "%.2f", maxAmount),
                String.valueOf(minEventTime),
                String.valueOf(maxEventTime));
    }

    @Override
    public String toString() {
        return "WindowAggregateResult{"
                + "userId='" + userId + '\''
                + ", windowStart=" + format(windowStart)
                + ", windowEnd=" + format(windowEnd)
                + ", configuredWatermarkDelayMs=" + configuredWatermarkDelayMs
                + ", currentWatermark=" + format(currentWatermark)
                + ", triggerLagMs=" + triggerLagMs
                + ", triggerSystemTime=" + format(triggerSystemTime)
                + ", eventCount=" + eventCount
                + ", sumAmount=" + sumAmount
                + ", averageAmount=" + averageAmount
                + ", minAmount=" + minAmount
                + ", maxAmount=" + maxAmount
                + ", minEventTime=" + format(minEventTime)
                + ", maxEventTime=" + format(maxEventTime)
                + '}';
    }

    private String format(long epochMillis) {
        if (epochMillis <= 0L) {
            return "NA";
        }
        return FORMATTER.format(Instant.ofEpochMilli(epochMillis));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowAggregateResult that = (WindowAggregateResult) o;
        return windowStart == that.windowStart
                && windowEnd == that.windowEnd
                && configuredWatermarkDelayMs == that.configuredWatermarkDelayMs
                && currentWatermark == that.currentWatermark
                && triggerLagMs == that.triggerLagMs
                && triggerSystemTime == that.triggerSystemTime
                && eventCount == that.eventCount
                && Double.compare(that.sumAmount, sumAmount) == 0
                && Double.compare(that.averageAmount, averageAmount) == 0
                && Double.compare(that.minAmount, minAmount) == 0
                && Double.compare(that.maxAmount, maxAmount) == 0
                && minEventTime == that.minEventTime
                && maxEventTime == that.maxEventTime
                && Objects.equals(userId, that.userId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, windowStart, windowEnd, configuredWatermarkDelayMs, currentWatermark, triggerLagMs,
                triggerSystemTime, eventCount, sumAmount, averageAmount, minAmount, maxAmount, minEventTime, maxEventTime);
    }
}


