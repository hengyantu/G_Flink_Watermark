package com.example.watermark;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 可配置的乱序事件源，用于模拟事件时间与处理时间的不一致。
 */
public class OutOfOrderEventSource extends RichParallelSourceFunction<UserEvent> {

    private final int eventsPerSecond;
    private final long runDurationMs;
    private final long maxOutOfOrdernessMs;
    private final double lateEventFraction;
    private final long severeLatenessUpperBoundMs;

    private volatile boolean running = true;

    private transient Random random;
    private transient String[] eventTypes;
    private transient String[] userPool;

    /**
     * @param eventsPerSecond             每个并行子任务生成的事件数
     * @param runDurationMs               源运行时长，毫秒；&lt;=0 表示持续运行
     * @param maxOutOfOrdernessMs         正常乱序范围
     * @param lateEventFraction           生成严重乱序（超过水位线延迟）事件的比例
     * @param severeLatenessUpperBoundMs  严重乱序的最大延迟（超过正常乱序范围）
     */
    public OutOfOrderEventSource(
            int eventsPerSecond,
            long runDurationMs,
            long maxOutOfOrdernessMs,
            double lateEventFraction,
            long severeLatenessUpperBoundMs) {
        this.eventsPerSecond = eventsPerSecond;
        this.runDurationMs = runDurationMs;
        this.maxOutOfOrdernessMs = maxOutOfOrdernessMs;
        this.lateEventFraction = lateEventFraction;
        this.severeLatenessUpperBoundMs = severeLatenessUpperBoundMs;
    }

    @Override
    public void open(Configuration parameters) {
        this.random = new Random();
        this.eventTypes = new String[]{"click", "view", "purchase", "logout"};
        // 创建固定的用户池，而不是每次生成新的UUID
        this.userPool = new String[100];
        for (int i = 0; i < userPool.length; i++) {
            this.userPool[i] = "user-" + i;
        }
    }

    @Override
    public void run(SourceContext<UserEvent> ctx) throws Exception {
        final ThreadLocalRandom threadRandom = ThreadLocalRandom.current();
        final long startMillis = System.currentTimeMillis();
        long emitted = 0L;

        // 主要数据生成阶段
        while (running && shouldContinue(startMillis)) {
            final long emitStart = System.currentTimeMillis();

            for (int i = 0; i < eventsPerSecond; i++) {
                long baseTimestamp = System.currentTimeMillis();
                long eventTimestamp = baseTimestamp - threadRandom.nextLong(maxOutOfOrdernessMs + 1);

                if (threadRandom.nextDouble() < lateEventFraction) {
                    long extraDelay = maxOutOfOrdernessMs + threadRandom.nextLong(severeLatenessUpperBoundMs + 1);
                    eventTimestamp = baseTimestamp - extraDelay;
                }

                UserEvent event = new UserEvent(
                        randomUserId(),
                        randomEventType(),
                        eventTimestamp,
                        threadRandom.nextDouble(1.0, 500.0),
                        baseTimestamp);

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(event);
                }

                emitted++;
            }

            final long emitDuration = System.currentTimeMillis() - emitStart;
            if (emitDuration < 1000L) {
                Thread.sleep(1000L - emitDuration);
            }
        }

        // Grace period: 继续发送少量事件来推进 watermark
        // 确保所有窗口都能被触发
        if (runDurationMs > 0L && running) {
            long gracePeriodMs = maxOutOfOrdernessMs + 5000L;
            long gracePeriodStart = System.currentTimeMillis();

            while (running && (System.currentTimeMillis() - gracePeriodStart) < gracePeriodMs) {
                // 发送少量事件，时间戳为当前时间（不乱序）
                long currentTimestamp = System.currentTimeMillis();
                UserEvent event = new UserEvent(
                        randomUserId(),
                        randomEventType(),
                        currentTimestamp,
                        threadRandom.nextDouble(1.0, 500.0),
                        currentTimestamp);

                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(event);
                }

                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    private boolean shouldContinue(long startMillis) {
        if (runDurationMs <= 0L) {
            return true;
        }
        return System.currentTimeMillis() - startMillis < runDurationMs;
    }

    private String randomUserId() {
        return userPool[random.nextInt(userPool.length)];
    }

    private String randomEventType() {
        return eventTypes[random.nextInt(eventTypes.length)];
    }
}


