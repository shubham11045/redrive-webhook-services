package dev.prita.redrive.delivery;

import dev.prita.redrive.config.RedriveProperties;
import dev.prita.redrive.ingest.EventRepository;
import dev.prita.redrive.subscription.SubscriptionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The delivery engine.
 *
 * Loop: claim due PENDING deliveries (SKIP LOCKED) -> hand each to a virtual
 * thread -> attempt HTTP -> record outcome in a short write transaction.
 *
 * Concurrency is bounded at two levels:
 *  - globalSlots: hard cap on in-flight deliveries process-wide (memory/socket
 *    protection). Virtual threads make blocking cheap, not capacity infinite.
 *  - perEndpointSlots: cap per endpoint host, so one slow-but-alive subscriber
 *    cannot absorb the whole global budget (slow != failing: the breaker only
 *    catches failures, the semaphore catches slowness).
 *
 * Deliberate design: attempts happen OUTSIDE any DB transaction. We claim the
 * row, release the claim, do I/O, then write the outcome. Crash between
 * attempt and outcome-write = the row stays PENDING and is retried later =>
 * possible duplicate delivery to the subscriber. That is the documented
 * at-least-once contract (X-Redrive-Delivery-Id enables subscriber dedup).
 */
@Component
public class DeliveryDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DeliveryDispatcher.class);

    private final DeliveryRepository deliveries;
    private final SubscriptionRepository subscriptions;
    private final EventRepository events;
    private final HttpDeliverer http;
    private final BackoffCalculator backoff;
    private final EndpointHealth endpointHealth;
    private final RedriveProperties props;
    private final TransactionTemplate tx;

    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore globalSlots;
    private final Map<String, Semaphore> perEndpointSlots = new ConcurrentHashMap<>();

    private final Counter deliveredCounter;
    private final Counter failedAttemptCounter;
    private final Counter deadCounter;
    private final Counter breakerSkipCounter;
    private final Timer attemptTimer;

    public DeliveryDispatcher(DeliveryRepository deliveries, SubscriptionRepository subscriptions,
                              EventRepository events, HttpDeliverer http, BackoffCalculator backoff,
                              EndpointHealth endpointHealth, RedriveProperties props,
                              PlatformTransactionManager txManager, MeterRegistry metrics) {
        this.deliveries = deliveries;
        this.subscriptions = subscriptions;
        this.events = events;
        this.http = http;
        this.backoff = backoff;
        this.endpointHealth = endpointHealth;
        this.props = props;
        this.tx = new TransactionTemplate(txManager);
        this.globalSlots = new Semaphore(props.delivery().globalConcurrency());

        this.deliveredCounter = Counter.builder("redrive_deliveries_success_total").register(metrics);
        this.failedAttemptCounter = Counter.builder("redrive_delivery_attempts_failed_total").register(metrics);
        this.deadCounter = Counter.builder("redrive_deliveries_dead_total").register(metrics);
        this.breakerSkipCounter = Counter.builder("redrive_breaker_skips_total").register(metrics);
        this.attemptTimer = Timer.builder("redrive_delivery_attempt_duration")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(metrics);
        metrics.gauge("redrive_delivery_global_slots_available", globalSlots, Semaphore::availablePermits);
    }

    @Scheduled(fixedDelayString = "${redrive.delivery.dispatch-poll-interval-ms}")
    public void dispatchDue() {
        // Claim inside a short transaction; the claim ends when the tx commits.
        var due = tx.execute(status ->
                deliveries.claimDue(Instant.now(), PageRequest.of(0, props.delivery().dispatchBatchSize())));
        if (due == null || due.isEmpty()) return;

        for (var delivery : due) {
            workers.submit(() -> attempt(delivery.getId()));
        }
    }

    /** One attempt for one delivery. Runs on a virtual thread. */
    void attempt(UUID deliveryId) {
        boolean acquiredGlobal = false;
        Semaphore endpointSem = null;
        boolean acquiredEndpoint = false;
        try {
            var delivery = deliveries.findById(deliveryId).orElse(null);
            if (delivery == null || delivery.getStatus() != Delivery.Status.PENDING) return;

            var sub = subscriptions.findById(delivery.getSubscriptionId()).orElse(null);
            var event = events.findById(delivery.getEventId()).orElse(null);
            if (sub == null || event == null) return;
            if (!sub.isActive()) return; // paused: row stays PENDING, picked up on resume

            String endpointKey = endpointKey(sub.getEndpointUrl());

            if (!endpointHealth.allowAttempt(endpointKey)) {
                // Breaker open: reschedule quietly WITHOUT consuming an attempt.
                breakerSkipCounter.increment();
                reschedule(delivery, props.breaker().openCooldownMs());
                return;
            }

            globalSlots.acquire();
            acquiredGlobal = true;
            endpointSem = perEndpointSlots.computeIfAbsent(endpointKey,
                    k -> new Semaphore(props.delivery().perEndpointConcurrency()));
            if (!endpointSem.tryAcquire()) {
                // Endpoint saturated by its own slow responses: back off briefly.
                reschedule(delivery, 2000);
                return;
            }
            acquiredEndpoint = true;

            var envelope = envelopeJson(event.getId(), event.getEventType(), event.getPublisherId(),
                    event.getCreatedAt().toString(), event.getPayload());

            long t0 = System.nanoTime();
            var outcome = http.deliver(sub, delivery.getId(), event.getId(), event.getEventType(), envelope);
            attemptTimer.record(java.time.Duration.ofNanos(System.nanoTime() - t0));

            if (outcome instanceof HttpDeliverer.Success success) {
                endpointHealth.recordSuccess(endpointKey);
                deliveredCounter.increment();
                tx.executeWithoutResult(s -> deliveries.findById(deliveryId).ifPresent(d -> {
                    d.markDelivered(success.statusCode());
                    deliveries.save(d);
                }));
            } else if (outcome instanceof HttpDeliverer.Failure failure) {
                endpointHealth.recordFailure(endpointKey);
                failedAttemptCounter.increment();
                tx.executeWithoutResult(s -> deliveries.findById(deliveryId).ifPresent(d -> {
                    if (isPermanentFailure(failure.statusCode())) {
                        d.markPermanentFailure(failure.statusCode());
                        deliveries.save(d);
                        log.info("Delivery {} permanently failed with HTTP {} (sub={})",
                                d.getId(), failure.statusCode(), d.getSubscriptionId());
                        return;
                    }
                    var next = backoff.nextAttemptTime(d.getAttemptCount() + 1, failure.retryAfterSeconds());
                    d.recordFailure(failure.statusCode(), failure.error(), next, props.delivery().maxAttempts());
                    deliveries.save(d);
                    if (d.getStatus() == Delivery.Status.DEAD) {
                        deadCounter.increment();
                        log.warn("Delivery {} dead-lettered after {} attempts (sub={})",
                                d.getId(), d.getAttemptCount(), d.getSubscriptionId());
                    }
                }));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Unexpected error in delivery attempt {}: {}", deliveryId, e.getMessage(), e);
        } finally {
            if (acquiredEndpoint) endpointSem.release();
            if (acquiredGlobal) globalSlots.release();
        }
    }

    private void reschedule(Delivery delivery, long delayMs) {
        tx.executeWithoutResult(s -> deliveries.findById(delivery.getId()).ifPresent(d -> {
            if (d.getStatus() == Delivery.Status.PENDING) {
                d.deferTo(Instant.now().plusMillis(delayMs));
                deliveries.save(d);
            }
        }));
    }

    private static final java.util.Set<Integer> PERMANENT_STATUS_CODES =
            java.util.Set.of(400, 401, 403, 404, 410);

    static boolean isPermanentFailure(Integer statusCode) {
        return statusCode != null && PERMANENT_STATUS_CODES.contains(statusCode);
    }

    static String endpointKey(String url) {
        try {
            var uri = URI.create(url);
            return uri.getHost() + ":" + (uri.getPort() == -1 ? defaultPort(uri) : uri.getPort());
        } catch (Exception e) {
            return url;
        }
    }

    private static int defaultPort(URI uri) {
        return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
    }

    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private static String envelopeJson(UUID eventId, String type, String publisher, String createdAt, String payload) {
        try {
            var node = MAPPER.createObjectNode()
                    .put("eventId", eventId.toString())
                    .put("eventType", type)
                    .put("publisherId", publisher)
                    .put("createdAt", createdAt);
            node.set("payload", MAPPER.readTree(payload));
            return MAPPER.writeValueAsString(node);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("failed to build envelope", e);
        }
    }
}
