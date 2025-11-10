package com.example.watermark;

/**
 * 记录迟到事件的统计信息。
 */
public class LateEventMetric {
    private String userId;
    private long eventTime;
    private long detectionTime;
    private long latenessMs;

    public LateEventMetric() {
    }

    public LateEventMetric(String userId, long eventTime, long detectionTime, long latenessMs) {
        this.userId = userId;
        this.eventTime = eventTime;
        this.detectionTime = detectionTime;
        this.latenessMs = latenessMs;
    }

    public String getUserId() {
        return userId;
    }

    public long getEventTime() {
        return eventTime;
    }

    public long getDetectionTime() {
        return detectionTime;
    }

    public long getLatenessMs() {
        return latenessMs;
    }

    public String toCsvLine() {
        return userId + "," + eventTime + "," + detectionTime + "," + latenessMs;
    }

    @Override
    public String toString() {
        return "LateEventMetric{"
                + "userId='" + userId + '\''
                + ", eventTime=" + eventTime
                + ", detectionTime=" + detectionTime
                + ", latenessMs=" + latenessMs
                + '}';
    }
}


