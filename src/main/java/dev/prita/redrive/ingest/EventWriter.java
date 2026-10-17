package dev.prita.redrive.ingest;

import dev.prita.redrive.config.RedriveProperties;
import dev.prita.redrive.outbox.OutboxRecord;
import dev.prita.redrive.outbox.OutboxRepository;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional write path, separated from EventService on purpose.
 *
 * If the unique-constraint violation were caught INSIDE the same
 * transaction, Spring would have already marked it rollback-only and the
 * commit would fail with UnexpectedRollbackException. The catch must happen
 * OUTSIDE the transaction boundary - and because Spring proxies don't
 * intercept self-invocation, "outside" means a separate bean.
 */
@Component
public class EventWriter {

    private final EventRepository events;
    private final OutboxRepository outbox;
    private final RedriveProperties props;

    public EventWriter(EventRepository events, OutboxRepository outbox, RedriveProperties props) {
        this.events = events;
        this.outbox = outbox;
        this.props = props;
    }

    /**
     * Inserts event + outbox row in ONE transaction (transactional outbox).
     * Throws DataIntegrityViolationException on idempotency-key conflict -
     * caller handles it after this transaction has cleanly rolled back.
     */
    @Transactional
    public EventRecord insert(String publisherId, String eventType, String payloadJson, String idempotencyKey) {
        var event = events.save(
                new EventRecord(UUID.randomUUID(), publisherId, eventType, payloadJson, idempotencyKey, hashPayload(payloadJson)));
        outbox.save(OutboxRecord.forEvent(event, props.topic()));
        return event;
    }

    public static String hashPayload(String payloadJson) {
        try {
            var digest = MessageDigest.getInstance("MD5");
            byte[] hash = digest.digest(payloadJson.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 not available", e);
        }
    }
}
