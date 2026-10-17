package com.shubham.redrive.delivery;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Durable delivery state machine.
 *
 * PENDING --success--> DELIVERED
 *   |  ^
 *   |  '--retry with backoff (attempt_count++, next_attempt_at pushed out)
 *   '--max attempts exhausted--> DEAD --replay--> new PENDING row (replay_of = original)
 */
@Entity
@Table(name = "deliveries")
public class Delivery {

    public enum Status { PENDING, DELIVERED, DEAD, FAILED_PERMANENT }

    @Id
    private UUID id;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "subscription_id", nullable = false)
    private UUID subscriptionId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "last_status_code")
    private Integer lastStatusCode;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "delivered_at")
    private Instant deliveredAt;

    @Column(name = "replay_of")
    private UUID replayOf;

    protected Delivery() {}

    public Delivery(UUID eventId, UUID subscriptionId) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.subscriptionId = subscriptionId;
        this.status = Status.PENDING;
        this.attemptCount = 0;
        var now = Instant.now();
        this.nextAttemptAt = now;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void markDelivered(int statusCode) {
        this.status = Status.DELIVERED;
        this.lastStatusCode = statusCode;
        this.deliveredAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public void recordFailure(Integer statusCode, String error, Instant nextAttempt, int maxAttempts) {
        this.attemptCount++;
        this.lastStatusCode = statusCode;
        this.lastError = truncate(error);
        this.updatedAt = Instant.now();
        if (this.attemptCount >= maxAttempts) {
            this.status = Status.DEAD;
        } else {
            this.nextAttemptAt = nextAttempt;
        }
    }

    public void markPermanentFailure(int statusCode) {
        this.status = Status.FAILED_PERMANENT;
        this.lastStatusCode = statusCode;
        this.lastError = "HTTP " + statusCode;
        this.updatedAt = Instant.now();
    }

    /** Push the next attempt out WITHOUT consuming an attempt (breaker open / endpoint saturated). */
    public void deferTo(Instant nextAttempt) {
        this.nextAttemptAt = nextAttempt;
        this.updatedAt = Instant.now();
    }

    public Delivery replayAs() {
        var d = new Delivery(this.eventId, this.subscriptionId);
        d.replayOf = this.id;
        return d;
    }

    private static String truncate(String s) {
        if (s == null) return null;
        return s.length() > 500 ? s.substring(0, 500) : s;
    }

    public UUID getId() { return id; }
    public UUID getEventId() { return eventId; }
    public UUID getSubscriptionId() { return subscriptionId; }
    public Status getStatus() { return status; }
    public int getAttemptCount() { return attemptCount; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public Integer getLastStatusCode() { return lastStatusCode; }
    public String getLastError() { return lastError; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getDeliveredAt() { return deliveredAt; }
    public UUID getReplayOf() { return replayOf; }
}
