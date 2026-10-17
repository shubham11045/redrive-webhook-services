package dev.prita.redrive.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * All tunables in one place so every limit is explicit and testable.
 * Nothing in the delivery path is unbounded: attempts, backoff, concurrency
 * and timeouts all have configured ceilings.
 */
@ConfigurationProperties(prefix = "redrive")
public record RedriveProperties(
        String topic,
        int topicPartitions,
        Outbox outbox,
        Delivery delivery,
        Breaker breaker
) {
    public record Outbox(long pollIntervalMs, int batchSize, int maxPublishAttempts) {}

    public record Delivery(
            long requestTimeoutMs,
            long connectTimeoutMs,
            int maxAttempts,
            long baseBackoffMs,
            long maxBackoffMs,
            double jitterRatio,
            int globalConcurrency,
            int perEndpointConcurrency,
            long dispatchPollIntervalMs,
            int dispatchBatchSize
    ) {}

    public record Breaker(int failureThreshold, long openCooldownMs) {}
}
