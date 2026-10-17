package dev.prita.redrive.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import dev.prita.redrive.config.RedriveProperties;
import org.junit.jupiter.api.Test;

class EndpointHealthTest {

    private RedriveProperties props(int threshold, long cooldownMs) {
        return new RedriveProperties(
                "redrive.events", 6,
                new RedriveProperties.Outbox(500, 100, 10),
                new RedriveProperties.Delivery(10000, 3000, 8, 2000, 600000, 0.3, 200, 4, 1000, 200),
                new RedriveProperties.Breaker(threshold, cooldownMs));
    }

    @Test
    void closedByDefault() {
        var health = new EndpointHealth(props(3, 60000));
        assertThat(health.allowAttempt("host:80")).isTrue();
        assertThat(health.isOpen("host:80")).isFalse();
    }

    @Test
    void opensAfterThresholdConsecutiveFailures() {
        var health = new EndpointHealth(props(3, 60000));
        health.recordFailure("host:80");
        health.recordFailure("host:80");
        assertThat(health.isOpen("host:80")).isFalse();
        health.recordFailure("host:80");
        assertThat(health.isOpen("host:80")).isTrue();
        assertThat(health.allowAttempt("host:80")).isFalse();
    }

    @Test
    void successResetsFailureCount() {
        var health = new EndpointHealth(props(3, 60000));
        health.recordFailure("host:80");
        health.recordFailure("host:80");
        health.recordSuccess("host:80");
        health.recordFailure("host:80");
        health.recordFailure("host:80");
        assertThat(health.isOpen("host:80")).isFalse();
    }

    @Test
    void halfOpenAllowsProbeAfterCooldown() throws InterruptedException {
        var health = new EndpointHealth(props(1, 50));
        health.recordFailure("host:80");
        assertThat(health.allowAttempt("host:80")).isFalse();
        Thread.sleep(80);
        assertThat(health.allowAttempt("host:80")).isTrue(); // probe allowed
        health.recordSuccess("host:80");
        assertThat(health.isOpen("host:80")).isFalse();
    }

    @Test
    void breakersAreIndependentPerEndpoint() {
        var health = new EndpointHealth(props(1, 60000));
        health.recordFailure("bad-host:80");
        assertThat(health.allowAttempt("bad-host:80")).isFalse();
        assertThat(health.allowAttempt("good-host:80")).isTrue();
    }
}
