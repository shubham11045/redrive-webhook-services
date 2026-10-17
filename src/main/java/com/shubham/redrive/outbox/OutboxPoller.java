package com.shubham.redrive.outbox;

import com.shubham.redrive.config.RedriveProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Polls the outbox and publishes pending rows to Kafka.
 *
 * Semantics: at-least-once from DB to Kafka.
 *  - We publish synchronously (get with timeout), then mark the row published
 *    in the same DB transaction that claimed it.
 *  - Crash window: if the process dies AFTER Kafka acks but BEFORE the commit,
 *    the row stays unpublished and will be re-published on restart -- duplicate
 *    in Kafka. That is accepted and documented; the delivery layer dedupes via
 *    the (event_id, subscription_id) unique constraint.
 *  - Exactly-once would require Kafka transactions + storing offsets in the DB;
 *    deliberately out of scope (see ADR-0002).
 */
@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final RedriveProperties props;
    private final Counter publishedCounter;
    private final Counter failedCounter;

    public OutboxPoller(OutboxRepository outbox, KafkaTemplate<String, String> kafka,
                       RedriveProperties props, MeterRegistry metrics) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.props = props;
        this.publishedCounter = Counter.builder("redrive_outbox_published_total").register(metrics);
        this.failedCounter = Counter.builder("redrive_outbox_publish_failures_total").register(metrics);
    }

    @Scheduled(fixedDelayString = "${redrive.outbox.poll-interval-ms}")
    @Transactional
    public void publishPending() {
        var batch = outbox.claimUnpublished(
                props.outbox().maxPublishAttempts(),
                PageRequest.of(0, props.outbox().batchSize()));
        if (batch.isEmpty()) {
            return;
        }
        for (var record : batch) {
            try {
                kafka.send(record.getTopic(), record.getMessageKey(), record.getPayload())
                        .get(10, TimeUnit.SECONDS);
                record.markPublished();
                publishedCounter.increment();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Row stays unpublished; retried next poll. Bounded by
                // max_publish_attempts so a poison row cannot spin forever.
                record.recordFailedAttempt();
                failedCounter.increment();
                log.warn("Outbox publish failed (attempt {}) for outbox id {}: {}",
                        record.getPublishAttempts(), record.getId(), e.getMessage());
            }
        }
    }
}
