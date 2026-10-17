package dev.prita.redrive.delivery;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.prita.redrive.subscription.SubscriptionRepository;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Kafka consumer: fans an event out into delivery jobs.
 *
 * At-least-once handling: offsets are committed manually AFTER the delivery
 * rows are committed. If we crash in between, Kafka redelivers the record -
 * and the UNIQUE (event_id, subscription_id) constraint turns the duplicate
 * into a no-op. This is idempotent consumption via the database, not via
 * Kafka "exactly-once" machinery (see ADR-0002 for why).
 */
@Component
public class DeliveryConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeliveryConsumer.class);

    private final SubscriptionRepository subscriptions;
    private final DeliveryRepository deliveries;
    private final ObjectMapper mapper;

    public DeliveryConsumer(SubscriptionRepository subscriptions, DeliveryRepository deliveries, ObjectMapper mapper) {
        this.subscriptions = subscriptions;
        this.deliveries = deliveries;
        this.mapper = mapper;
    }

    @KafkaListener(topics = "${redrive.topic}")
    @Transactional
    public void onEvent(String envelopeJson, Acknowledgment ack) {
        try {
            var envelope = mapper.readTree(envelopeJson);
            var eventId = UUID.fromString(envelope.get("eventId").asText());
            var eventType = envelope.get("eventType").asText();

            for (var sub : subscriptions.findByActiveTrue()) {
                if (!sub.matches(eventType)) continue;
                try {
                    if (!deliveries.existsByEventIdAndSubscriptionId(eventId, sub.getId())) {
                        deliveries.save(new Delivery(eventId, sub.getId()));
                    }
                } catch (DataIntegrityViolationException dup) {
                    // Redelivered Kafka record raced us - constraint wins, fine.
                    log.debug("Duplicate delivery row suppressed for event={} sub={}", eventId, sub.getId());
                }
            }
            ack.acknowledge();
        } catch (Exception e) {
            // Do not ack: record will be redelivered. Poison-message guard is
            // the maxPublishAttempts on the producing side + alerting on lag;
            // a proper parking-lot topic is listed as future work.
            log.error("Failed to process event envelope, will be redelivered: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
