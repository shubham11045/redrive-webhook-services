///usr/bin/env java "$0" "$@"; exit $?
// Single-file chaos subscriber - run with: java chaos/ChaosSubscriber.java
// A webhook endpoint that misbehaves on demand, controlled by env vars:
//   CHAOS_PORT          (default 9099)
//   CHAOS_FAIL_RATE     0.0..1.0 probability of returning 500   (default 0.0)
//   CHAOS_TIMEOUT_RATE  0.0..1.0 probability of hanging 60s      (default 0.0)
//   CHAOS_DELAY_MS      fixed latency added to every response    (default 0)
//   CHAOS_RATE_LIMIT    if set, return 429 + Retry-After after N req/s
//
// It logs every delivery with its X-Redrive-Delivery-Id so duplicate deliveries
// are visible, and counts them: GET /stats returns totals + distinct ids.

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class ChaosSubscriber {

    static final AtomicLong received = new AtomicLong();
    static final AtomicLong failed = new AtomicLong();
    static final AtomicLong timedOut = new AtomicLong();
    static final Set<String> distinctDeliveryIds = ConcurrentHashMap.newKeySet();
    static final Set<String> duplicateDeliveryIds = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) throws IOException {
        int port = envInt("CHAOS_PORT", 9099);
        double failRate = envDouble("CHAOS_FAIL_RATE", 0.0);
        double timeoutRate = envDouble("CHAOS_TIMEOUT_RATE", 0.0);
        long delayMs = envInt("CHAOS_DELAY_MS", 0);

        var server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        server.createContext("/webhook", exchange -> {
            received.incrementAndGet();
            String deliveryId = exchange.getRequestHeaders().getFirst("X-Redrive-Delivery-Id");
            if (deliveryId != null && !distinctDeliveryIds.add(deliveryId)) {
                duplicateDeliveryIds.add(deliveryId);
            }
            exchange.getRequestBody().readAllBytes();

            try {
                if (delayMs > 0) Thread.sleep(delayMs);
                double roll = ThreadLocalRandom.current().nextDouble();
                if (roll < timeoutRate) {
                    timedOut.incrementAndGet();
                    Thread.sleep(60_000); // longer than any sane client timeout
                } else if (roll < timeoutRate + failRate) {
                    failed.incrementAndGet();
                    respond(exchange, 500, "chaos: injected failure");
                    return;
                }
                respond(exchange, 200, "ok");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        server.createContext("/stats", exchange -> {
            String body = "{\"received\":" + received.get()
                    + ",\"distinct\":" + distinctDeliveryIds.size()
                    + ",\"duplicates\":" + duplicateDeliveryIds.size()
                    + ",\"injectedFailures\":" + failed.get()
                    + ",\"injectedTimeouts\":" + timedOut.get() + "}";
            respond(exchange, 200, body);
        });

        server.start();
        System.out.printf("ChaosSubscriber on :%d  failRate=%.2f timeoutRate=%.2f delayMs=%d%n",
                port, failRate, timeoutRate, delayMs);
    }

    static void respond(com.sun.net.httpserver.HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().add("Content-Type", "application/json");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    static int envInt(String name, int def) {
        var v = System.getenv(name);
        return v == null ? def : Integer.parseInt(v);
    }

    static double envDouble(String name, double def) {
        var v = System.getenv(name);
        return v == null ? def : Double.parseDouble(v);
    }
}
