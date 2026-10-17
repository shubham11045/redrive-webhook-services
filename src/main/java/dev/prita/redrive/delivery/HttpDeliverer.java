package dev.prita.redrive.delivery;

import dev.prita.redrive.config.RedriveProperties;
import dev.prita.redrive.subscription.Subscription;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Performs one HTTP delivery attempt. Pure I/O - no state decisions here;
 * the outcome is returned and DeliveryDispatcher owns the state machine.
 *
 * Every request has a connect timeout and a response timeout. There is no
 * such thing as an unbounded outbound call in this system.
 */
@Component
public class HttpDeliverer {

    private final HttpClient client;
    private final HmacSigner signer;
    private final RedriveProperties props;

    public HttpDeliverer(HmacSigner signer, RedriveProperties props) {
        this.signer = signer;
        this.props = props;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(props.delivery().connectTimeoutMs()))
                .followRedirects(HttpClient.Redirect.NEVER) // webhooks should not silently follow redirects
                .build();
    }

    public sealed interface Outcome permits Success, Failure {}
    public record Success(int statusCode) implements Outcome {}
    public record Failure(Integer statusCode, String error, Long retryAfterSeconds) implements Outcome {}

    public Outcome deliver(Subscription sub, UUID deliveryId, UUID eventId, String eventType, String envelopeJson) {
        long ts = Instant.now().getEpochSecond();
        var request = HttpRequest.newBuilder()
                .uri(URI.create(sub.getEndpointUrl()))
                .timeout(Duration.ofMillis(props.delivery().requestTimeoutMs()))
                .header("Content-Type", "application/json")
                .header("User-Agent", "redrive/0.1")
                .header("X-Redrive-Delivery-Id", deliveryId.toString()) // subscriber-side dedup key
                .header("X-Redrive-Event-Id", eventId.toString())
                .header("X-Redrive-Event-Type", eventType)
                .header("X-Redrive-Timestamp", String.valueOf(ts))
                .header("X-Redrive-Signature", signer.sign(sub.getSecret(), ts, envelopeJson))
                .POST(HttpRequest.BodyPublishers.ofString(envelopeJson))
                .build();
        try {
            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
            int code = response.statusCode();
            if (code >= 200 && code < 300) {
                return new Success(code);
            }
            Long retryAfter = null;
            if (code == 429) {
                retryAfter = response.headers().firstValue("Retry-After")
                        .map(v -> {
                            try { return Long.parseLong(v.trim()); } catch (NumberFormatException e) { return null; }
                        }).orElse(null);
            }
            return new Failure(code, "HTTP " + code, retryAfter);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Failure(null, "interrupted", null);
        } catch (Exception e) {
            return new Failure(null, e.getClass().getSimpleName() + ": " + e.getMessage(), null);
        }
    }
}
