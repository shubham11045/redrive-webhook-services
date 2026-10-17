package dev.prita.redrive.delivery;

import dev.prita.redrive.config.RedriveProperties;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * Minimal hand-rolled circuit breaker, per endpoint host.
 *
 * CLOSED: normal operation. N consecutive failures -> OPEN.
 * OPEN:   deliveries to this endpoint are skipped (rescheduled, not failed)
 *         until the cooldown elapses.
 * HALF-OPEN (implicit): after cooldown, the next attempt is allowed through;
 *         success closes the breaker, failure re-opens it.
 *
 * Hand-rolled instead of Resilience4j ON PURPOSE: ~50 lines, no magic, and
 * every state transition is visible in the code. Documented
 * limitation: state is in-memory per instance - replicas each keep their own
 * view, which is acceptable (each replica independently discovers the outage).
 *
 * NOTE: the breaker is only one layer. Timeouts, per-endpoint concurrency
 * caps and bounded retries do the rest (see DeliveryDispatcher/HttpDeliverer).
 */
@Component
public class EndpointHealth {

    private final RedriveProperties props;

    private static final class State {
        final AtomicInteger consecutiveFailures = new AtomicInteger();
        final AtomicLong openUntilEpochMs = new AtomicLong(0);
    }

    private final Map<String, State> states = new ConcurrentHashMap<>();

    public EndpointHealth(RedriveProperties props) {
        this.props = props;
    }

    /** May we attempt a delivery to this endpoint right now? */
    public boolean allowAttempt(String endpointKey) {
        var s = states.get(endpointKey);
        if (s == null) return true;
        long openUntil = s.openUntilEpochMs.get();
        if (openUntil == 0) return true;
        if (Instant.now().toEpochMilli() >= openUntil) {
            // Half-open probe: allow one attempt through; leave openUntil set -
            // recordSuccess clears it, another failure pushes it out again.
            return true;
        }
        return false;
    }

    public void recordSuccess(String endpointKey) {
        var s = states.get(endpointKey);
        if (s != null) {
            s.consecutiveFailures.set(0);
            s.openUntilEpochMs.set(0);
        }
    }

    public void recordFailure(String endpointKey) {
        var s = states.computeIfAbsent(endpointKey, k -> new State());
        int failures = s.consecutiveFailures.incrementAndGet();
        if (failures >= props.breaker().failureThreshold()) {
            s.openUntilEpochMs.set(Instant.now().toEpochMilli() + props.breaker().openCooldownMs());
        }
    }

    public boolean isOpen(String endpointKey) {
        var s = states.get(endpointKey);
        return s != null && s.openUntilEpochMs.get() > Instant.now().toEpochMilli();
    }
}
