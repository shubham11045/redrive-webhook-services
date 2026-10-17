package com.shubham.redrive.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import com.shubham.redrive.config.RedriveProperties;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class BackoffCalculatorTest {

    private final RedriveProperties props = new RedriveProperties(
            "redrive.events", 6,
            new RedriveProperties.Outbox(500, 100, 10),
            new RedriveProperties.Delivery(10000, 3000, 8, 2000, 600000, 0.3, 200, 4, 1000, 200),
            new RedriveProperties.Breaker(5, 30000));

    private final BackoffCalculator backoff = new BackoffCalculator(props);

    @Test
    void delayGrowsExponentially() {
        // With jitter +/-30%, attempt n's minimum possible delay must still
        // exceed attempt n-2's maximum possible delay for growth to be real.
        long d1Max = (long) (2000 * 1.3);
        long d3Min = (long) (2000 * 4 * 0.7);
        assertThat(d3Min).isGreaterThan(d1Max);

        for (int i = 0; i < 100; i++) {
            assertThat(backoff.delayMs(1)).isBetween((long) (2000 * 0.7), (long) (2000 * 1.3));
            assertThat(backoff.delayMs(3)).isBetween((long) (8000 * 0.7), (long) (8000 * 1.3));
        }
    }

    @Test
    void delayIsCappedAtMaxBackoff() {
        for (int i = 0; i < 100; i++) {
            assertThat(backoff.delayMs(30)).isLessThanOrEqualTo((long) (600000 * 1.3));
        }
    }

    @Test
    void largeAttemptNumbersDoNotOverflow() {
        assertThat(backoff.delayMs(Integer.MAX_VALUE)).isPositive();
    }

    @Test
    void retryAfterHeaderOverridesComputedBackoff() {
        Instant before = Instant.now();
        Instant next = backoff.nextAttemptTime(1, 42L);
        assertThat(next).isAfterOrEqualTo(before.plusSeconds(41));
        assertThat(next).isBeforeOrEqualTo(before.plusSeconds(43));
    }

    @Test
    void retryAfterIsCappedAtMaxBackoff() {
        Instant before = Instant.now();
        Instant next = backoff.nextAttemptTime(1, 86_400L); // subscriber asks for 24h
        assertThat(next).isBeforeOrEqualTo(before.plusMillis(600000 + 2000));
    }
}
