package com.example.watermark;

import java.time.Instant;
import java.util.Objects;

/**
 * 模拟数据流中的用户事件。
 */
public class UserEvent {

    private String userId;
    private String eventType;
    private long eventTime;
    private double amount;
    private long ingestionTime;

    public UserEvent() {
    }

    public UserEvent(String userId, String eventType, long eventTime, double amount, long ingestionTime) {
        this.userId = userId;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.amount = amount;
        this.ingestionTime = ingestionTime;
    }

    public String getUserId() {
        return userId;
    }

    public String getEventType() {
        return eventType;
    }

    public long getEventTime() {
        return eventTime;
    }

    public double getAmount() {
        return amount;
    }

    public long getIngestionTime() {
        return ingestionTime;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public void setIngestionTime(long ingestionTime) {
        this.ingestionTime = ingestionTime;
    }

    public Instant eventTimeAsInstant() {
        return Instant.ofEpochMilli(eventTime);
    }

    public Instant ingestionTimeAsInstant() {
        return Instant.ofEpochMilli(ingestionTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserEvent userEvent = (UserEvent) o;
        return eventTime == userEvent.eventTime
                && Double.compare(userEvent.amount, amount) == 0
                && ingestionTime == userEvent.ingestionTime
                && Objects.equals(userId, userEvent.userId)
                && Objects.equals(eventType, userEvent.eventType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, eventType, eventTime, amount, ingestionTime);
    }

    @Override
    public String toString() {
        return "UserEvent{"
                + "userId='" + userId + '\''
                + ", eventType='" + eventType + '\''
                + ", eventTime=" + eventTime
                + ", amount=" + amount
                + ", ingestionTime=" + ingestionTime
                + '}';
    }
}


