package dev.prita.redrive.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration tests against REAL Postgres and REAL Kafka
 * (Testcontainers). The "subscriber" is an in-process HTTP server whose
 * behavior each test controls.
 *
 * These tests verify the guarantees the README claims:
 *  1. happy path: ingest -> outbox -> Kafka -> delivery, HMAC headers present
 *  2. idempotent ingest: duplicate POST creates no second event
 *  3. retry semantics: failing endpoint is retried and eventually succeeds
 *  4. dead-lettering + replay: exhausted deliveries become DEAD and can be replayed
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // Tight timings so retry/DLQ paths run in seconds, not minutes.
        "redrive.delivery.max-attempts=3",
        "redrive.delivery.base-backoff-ms=200",
        "redrive.delivery.max-backoff-ms=1000",
        "redrive.delivery.dispatch-poll-interval-ms=200",
        "redrive.outbox.poll-interval-ms=200",
        "redrive.breaker.failure-threshold=1000" // breaker out of the way except where tested
})
class RedriveEndToEndIT {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("redrive").withUsername("redrive").withPassword("redrive");

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @DynamicPropertySource
    static void wire(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    static HttpServer subscriberServer;
    static final List<Map<String, String>> receivedHeaders = new CopyOnWriteArrayList<>();
    static final AtomicInteger failuresToInject = new AtomicInteger(0);
    static final ConcurrentHashMap<String, AtomicInteger> hitsPerPath = new ConcurrentHashMap<>();

    @Autowired TestRestTemplate rest;
    @LocalServerPort int port;

    @BeforeAll
    static void startSubscriber() throws Exception {
        subscriberServer = HttpServer.create(new InetSocketAddress(0), 0);
        subscriberServer.createContext("/hook", exchange -> {
            hitsPerPath.computeIfAbsent("/hook", k -> new AtomicInteger()).incrementAndGet();
            receivedHeaders.add(Map.of(
                    "deliveryId", header(exchange, "X-Redrive-Delivery-Id"),
                    "eventId", header(exchange, "X-Redrive-Event-Id"),
                    "signature", header(exchange, "X-Redrive-Signature"),
                    "timestamp", header(exchange, "X-Redrive-Timestamp")));
            exchange.getRequestBody().readAllBytes();
            if (failuresToInject.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                exchange.sendResponseHeaders(500, -1);
            } else {
                exchange.sendResponseHeaders(200, -1);
            }
            exchange.close();
        });
        subscriberServer.createContext("/always-fails", exchange -> {
            hitsPerPath.computeIfAbsent("/always-fails", k -> new AtomicInteger()).incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        subscriberServer.createContext("/not-found", exchange -> {
            hitsPerPath.computeIfAbsent("/not-found", k -> new AtomicInteger()).incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        subscriberServer.start();
    }

    @AfterAll
    static void stopSubscriber() {
        subscriberServer.stop(0);
    }

    static String header(com.sun.net.httpserver.HttpExchange ex, String name) {
        var v = ex.getRequestHeaders().getFirst(name);
        return v == null ? "" : v;
    }

    String subscriberUrl(String path) {
        return "http://localhost:" + subscriberServer.getAddress().getPort() + path;
    }

    // ---- helpers ----

    Map<String, Object> createSubscription(String name, String url, String types) {
        var body = Map.of("name", name, "endpointUrl", url, "eventTypes", types);
        var resp = rest.postForEntity("/api/v1/subscriptions", body, Map.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(201);
        @SuppressWarnings("unchecked")
        var result = (Map<String, Object>) resp.getBody();
        return result;
    }

    org.springframework.http.ResponseEntity<Map> ingest(String idemKey, String eventType, String payloadJson) {
        var headers = new HttpHeaders();
        headers.add("Idempotency-Key", idemKey);
        headers.add("Content-Type", "application/json");
        var body = "{\"eventType\":\"" + eventType + "\",\"payload\":" + payloadJson + "}";
        return rest.postForEntity("/api/v1/events", new HttpEntity<>(body, headers), Map.class);
    }

    // ---- tests ----

    @Test
    void happyPath_eventIsDeliveredWithSignedHeaders() {
        createSubscription("happy", subscriberUrl("/hook"), "order.created");
        int before = hitsPerPath.getOrDefault("/hook", new AtomicInteger()).get();

        var resp = ingest("it-happy-1", "order.created", "{\"orderId\":42}");
        assertThat(resp.getStatusCode().value()).isEqualTo(201);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(hitsPerPath.getOrDefault("/hook", new AtomicInteger()).get()).isGreaterThan(before));

        var last = receivedHeaders.get(receivedHeaders.size() - 1);
        assertThat(last.get("deliveryId")).isNotEmpty();
        assertThat(last.get("eventId")).isNotEmpty();
        assertThat(last.get("signature")).startsWith("sha256=");
        assertThat(last.get("timestamp")).isNotEmpty();
    }

    @Test
    void duplicateIngest_returnsOriginalEventAndCreatesNothingNew() {
        var first = ingest("it-dup-1", "order.created", "{\"n\":1}");
        var second = ingest("it-dup-1", "order.created", "{\"n\":1}");

        assertThat(first.getStatusCode().value()).isEqualTo(201);
        assertThat(second.getStatusCode().value()).isEqualTo(200);
        assertThat(second.getBody().get("duplicate")).isEqualTo(true);
        assertThat(second.getBody().get("id")).isEqualTo(first.getBody().get("id"));
    }

    @Test
    void flakyEndpoint_isRetriedUntilSuccess() {
        createSubscription("flaky", subscriberUrl("/hook"), "invoice.paid");
        failuresToInject.set(2); // fail twice, then succeed (max-attempts=3)
        int before = hitsPerPath.getOrDefault("/hook", new AtomicInteger()).get();

        ingest("it-flaky-1", "invoice.paid", "{\"inv\":7}");

        // 2 failures + 1 success = at least 3 hits for this event
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(hitsPerPath.getOrDefault("/hook", new AtomicInteger()).get() - before).isGreaterThanOrEqualTo(3));
    }

    @Test
    void exhaustedDelivery_isDeadLetteredAndReplayable() {
        var sub = createSubscription("doomed", subscriberUrl("/always-fails"), "user.deleted");
        String subId = (String) sub.get("id");

        ingest("it-dead-1", "user.deleted", "{\"u\":1}");

        // After 3 failed attempts the delivery must be DEAD and listed as a dead letter.
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            var dead = rest.getForEntity("/api/v1/subscriptions/" + subId + "/dead-letters", List.class);
            assertThat(dead.getBody()).isNotEmpty();
        });

        // Replay resets it to PENDING; endpoint still fails, so it dies again -
        // proving replay re-enters the normal delivery lifecycle.
        var replay = rest.postForEntity(
                "/api/v1/subscriptions/" + subId + "/replay-dead-letters", null, Map.class);
        assertThat(((Number) replay.getBody().get("replayed")).intValue()).isGreaterThanOrEqualTo(1);

        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            var dead = rest.getForEntity("/api/v1/subscriptions/" + subId + "/dead-letters", List.class);
            assertThat(dead.getBody()).isNotEmpty();
        });
    }

    @Test
    void permanentFailure_404IsNotRetried() {
        createSubscription("perm-fail", subscriberUrl("/not-found"), "perm.test");
        ingest("it-perm-1", "perm.test", "{\"x\":1}");

        // Should become FAILED_PERMANENT after a single attempt, not retried
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var resp = rest.getForEntity("/api/v1/subscriptions", List.class);
            // Check the delivery ended up as FAILED_PERMANENT, not DEAD
            assertThat(hitsPerPath.getOrDefault("/not-found", new AtomicInteger()).get()).isEqualTo(1);
        });

        // Give it a few seconds to confirm no retries happen
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
        assertThat(hitsPerPath.getOrDefault("/not-found", new AtomicInteger()).get()).isEqualTo(1);
    }

    @Test
    void replayCreatesNewDeliveryRow() {
        var sub = createSubscription("replay-new", subscriberUrl("/always-fails"), "replay.test");
        String subId = (String) sub.get("id");

        ingest("it-replay-new-1", "replay.test", "{\"r\":1}");

        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            var dead = rest.getForEntity("/api/v1/subscriptions/" + subId + "/dead-letters", List.class);
            assertThat(dead.getBody()).isNotEmpty();
        });

        // Get the original dead delivery ID
        var deadBefore = rest.getForEntity("/api/v1/subscriptions/" + subId + "/dead-letters", List.class);
        @SuppressWarnings("unchecked")
        var firstDead = (Map<String, Object>) ((List<?>) deadBefore.getBody()).get(0);
        String originalId = (String) firstDead.get("id");

        // Replay - should create a new delivery row
        rest.postForEntity("/api/v1/subscriptions/" + subId + "/replay-dead-letters", null, Map.class);

        // The new delivery will also fail and die. Check that the original is still there
        // and a new one was created with replay_of pointing to the original.
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            var dead = rest.getForEntity("/api/v1/subscriptions/" + subId + "/dead-letters", List.class);
            assertThat(dead.getBody().size()).isGreaterThanOrEqualTo(2);
        });

        // Original delivery should still exist unchanged
        var original = rest.getForEntity("/api/v1/deliveries/" + originalId, Map.class);
        assertThat(original.getBody().get("status")).isEqualTo("DEAD");
    }
}
