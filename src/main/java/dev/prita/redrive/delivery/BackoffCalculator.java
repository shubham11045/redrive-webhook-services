package dev.prita.redrive.delivery;

import dev.prita.redrive.config.RedriveProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.stereotype.Component;

/**
 * Exponential backoff with full-jitter-ish randomization and a hard cap.
 *
 * delay(n) = min(base * 2^(n-1), cap) +/- jitterRatio
 *
 * Why jitter: without it, a burst of deliveries that failed together retries
 * together forever (thundering herd / retry storm against a recovering
 * endpoint). Randomizing spreads the load.
 *
 * A 429 with Retry-After overrides the computed delay: the subscriber told
 * us its capacity, ignoring it would be hostile.
 */
@Component
public class BackoffCalculator {

    private final RedriveProperties props;

    public BackoffCalculator(RedriveProperties props) {
        this.props = props;
    }

    public Instant nextAttemptTime(int attemptsSoFar, Long retryAfterSeconds) {
        if (retryAfterSeconds != null && retryAfterSeconds > 0) {
            long capped = Math.min(retryAfterSeconds * 1000, props.delivery().maxBackoffMs());
            return Instant.now().plus(Duration.ofMillis(capped));
        }
        return Instant.now().plus(Duration.ofMillis(delayMs(attemptsSoFar)));
    }

    long delayMs(int attemptsSoFar) {
        var d = props.delivery();
        int exponent = Math.max(0, attemptsSoFar - 1);
        // Guard against overflow before capping.
        double raw = d.baseBackoffMs() * Math.pow(2, Math.min(exponent, 20));
        long capped = (long) Math.min(raw, d.maxBackoffMs());
        double jitter = 1.0 + ((ThreadLocalRandom.current().nextDouble() * 2) - 1) * d.jitterRatio();
        return Math.max(0, (long) (capped * jitter));
    }
}
