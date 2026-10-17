package dev.prita.redrive.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/**
 * Ingest orchestration.
 *
 * Idempotency: (publisher_id, idempotency_key) is UNIQUE in the schema.
 * We rely on the constraint - not a read-then-write check - because two
 * concurrent duplicates would both pass a pre-read; the constraint makes the
 * race safe and the loser returns the winner's row.
 *
 * The fast-path pre-read below is purely an optimization for the common
 * retry case (avoids a doomed INSERT); correctness never depends on it.
 *
 * NOTE this class is deliberately NOT @Transactional - see EventWriter for
 * why the constraint violation must be caught outside the transaction.
 */
@Service
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);

    private final EventRepository events;
    private final EventWriter writer;

    public EventService(EventRepository events, EventWriter writer) {
        this.events = events;
        this.writer = writer;
    }

    public record IngestResult(EventRecord event, boolean duplicate) {}

    public IngestResult ingest(String publisherId, String eventType, String payloadJson, String idempotencyKey) {
        var existing = events.findByPublisherIdAndIdempotencyKey(publisherId, idempotencyKey);
        if (existing.isPresent()) {
            checkPayloadConflict(existing.get(), payloadJson, idempotencyKey);
            return new IngestResult(existing.get(), true);
        }
        try {
            return new IngestResult(writer.insert(publisherId, eventType, payloadJson, idempotencyKey), false);
        } catch (DataIntegrityViolationException e) {
            log.debug("Concurrent duplicate ingest for publisher={} key={}", publisherId, idempotencyKey);
            var winner = events.findByPublisherIdAndIdempotencyKey(publisherId, idempotencyKey)
                    .orElseThrow(() -> e);
            checkPayloadConflict(winner, payloadJson, idempotencyKey);
            return new IngestResult(winner, true);
        }
    }

    private void checkPayloadConflict(EventRecord existing, String newPayloadJson, String idempotencyKey) {
        String newHash = EventWriter.hashPayload(newPayloadJson);
        if (!newHash.equals(existing.getPayloadHash())) {
            throw new IdempotencyKeyConflictException(idempotencyKey);
        }
    }

    public static class IdempotencyKeyConflictException extends RuntimeException {
        public IdempotencyKeyConflictException(String key) {
            super("Idempotency key '" + key + "' already used with a different payload");
        }
    }
}
