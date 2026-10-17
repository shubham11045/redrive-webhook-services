package dev.prita.invoice;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.util.HexFormat;

// Run: java InvoiceSubscriber.java <whsec_... secret> [port]
public class InvoiceSubscriber {

    private static final String HMAC_ALGO = "HmacSHA256";

    private final String secret;
    private final Set<String> processed = ConcurrentHashMap.newKeySet();

    public InvoiceSubscriber(String secret) {
        this.secret = secret;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java InvoiceSubscriber.java <secret> [port]");
            System.exit(1);
        }
        String secret = args[0];
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9095;

        var subscriber = new InvoiceSubscriber(secret);
        var server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/webhook", subscriber::handle);
        server.start();
        System.out.println("Invoice subscriber listening on port " + port);
    }

    private void handle(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        byte[] bodyBytes = exchange.getRequestBody().readAllBytes();
        String body = new String(bodyBytes, StandardCharsets.UTF_8);

        String deliveryId = header(exchange, "X-Redrive-Delivery-Id");
        String eventId = header(exchange, "X-Redrive-Event-Id");
        String eventType = header(exchange, "X-Redrive-Event-Type");
        String timestamp = header(exchange, "X-Redrive-Timestamp");
        String signature = header(exchange, "X-Redrive-Signature");

        // Step 1: verify HMAC signature
        if (!verifySignature(timestamp, body, signature)) {
            System.out.println("[REJECTED] Invalid signature for delivery " + deliveryId);
            exchange.sendResponseHeaders(401, -1);
            exchange.close();
            return;
        }

        // Step 2: deduplicate on delivery ID
        if (!processed.add(deliveryId)) {
            System.out.println("[SKIPPED] Already processed delivery " + deliveryId);
            // Return 200 so Redrive marks it delivered (don't cause a retry for a dup)
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
            return;
        }

        // Step 3: process the event
        System.out.printf("[OK] event=%s type=%s delivery=%s payload=%s%n",
                eventId, eventType, deliveryId, body);

        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private boolean verifySignature(String timestamp, String body, String receivedSignature) {
        if (timestamp == null || timestamp.isBlank() || receivedSignature == null) {
            return false;
        }
        String expected = computeSignature(timestamp, body);
        // Constant-time comparison to prevent timing attacks
        return constantTimeEquals(expected, receivedSignature);
    }

    private String computeSignature(String timestamp, String body) {
        try {
            var mac = Mac.getInstance(HMAC_ALGO);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_ALGO));
            byte[] signed = mac.doFinal((timestamp + "." + body).getBytes(StandardCharsets.UTF_8));
            return "sha256=" + HexFormat.of().formatHex(signed);
        } catch (Exception e) {
            throw new RuntimeException("HMAC computation failed", e);
        }
    }

    private static boolean constantTimeEquals(String a, String b) {
        if (a.length() != b.length()) return false;
        int result = 0;
        for (int i = 0; i < a.length(); i++) {
            result |= a.charAt(i) ^ b.charAt(i);
        }
        return result == 0;
    }

    private static String header(HttpExchange ex, String name) {
        var v = ex.getRequestHeaders().getFirst(name);
        return v == null ? "" : v;
    }
}
