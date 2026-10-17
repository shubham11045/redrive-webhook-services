package dev.prita.redrive.ingest;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "events")
public class EventRecord {

    @Id
    private UUID id;

    @Column(name = "publisher_id", nullable = false)
    private String publisherId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "idempotency_key", nullable = false)
    private String idempotencyKey;

    @Column(name = "payload_hash", nullable = false)
    private String payloadHash;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected EventRecord() {}

    public EventRecord(UUID id, String publisherId, String eventType, String payload, String idempotencyKey, String payloadHash) {
        this.id = id;
        this.publisherId = publisherId;
        this.eventType = eventType;
        this.payload = payload;
        this.idempotencyKey = idempotencyKey;
        this.payloadHash = payloadHash;
        this.createdAt = Instant.now();
    }

    public UUID getId() { return id; }
    public String getPublisherId() { return publisherId; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getPayloadHash() { return payloadHash; }
    public Instant getCreatedAt() { return createdAt; }
}
