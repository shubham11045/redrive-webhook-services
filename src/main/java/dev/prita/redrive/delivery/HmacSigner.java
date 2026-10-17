package dev.prita.redrive.delivery;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.stereotype.Component;

/**
 * HMAC-SHA256 payload signing.
 *
 * What this gives the subscriber: AUTHENTICITY and INTEGRITY - proof the
 * delivery came from Redrive (holder of the shared secret) and wasn't modified.
 *
 * What it does NOT give: deduplication. Duplicate protection comes from the
 * stable X-Redrive-Delivery-Id / X-Redrive-Event-Id headers, which the SUBSCRIBER
 * must track - Redrive cannot make an external endpoint idempotent.
 *
 * Signed string = "<timestamp>.<body>" so a captured request cannot be
 * replayed much later without the signature failing freshness checks
 * (subscribers should reject stale timestamps).
 */
@Component
public class HmacSigner {

    private static final String ALGO = "HmacSHA256";

    public String sign(String secret, long timestampEpochSeconds, String body) {
        try {
            var mac = Mac.getInstance(ALGO);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGO));
            var signed = mac.doFinal((timestampEpochSeconds + "." + body).getBytes(StandardCharsets.UTF_8));
            return "sha256=" + HexFormat.of().formatHex(signed);
        } catch (Exception e) {
            throw new IllegalStateException("HMAC signing failed", e);
        }
    }
}
