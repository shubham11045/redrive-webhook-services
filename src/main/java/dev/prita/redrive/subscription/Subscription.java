package dev.prita.redrive.subscription;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;

@Entity
@Table(name = "subscriptions")
public class Subscription {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String name;

    @Column(name = "endpoint_url", nullable = false)
    private String endpointUrl;

    /** Shared secret used to HMAC-sign deliveries. Authenticity, NOT dedup. */
    @Column(nullable = false)
    private String secret;

    /** Comma-separated event types, '*' matches all. */
    @Column(name = "event_types", nullable = false)
    private String eventTypes;

    @Column(nullable = false)
    private boolean active;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Subscription() {}

    public Subscription(UUID id, String name, String endpointUrl, String secret, String eventTypes) {
        this.id = id;
        this.name = name;
        this.endpointUrl = endpointUrl;
        this.secret = secret;
        this.eventTypes = eventTypes == null || eventTypes.isBlank() ? "*" : eventTypes;
        this.active = true;
        this.createdAt = Instant.now();
    }

    public boolean matches(String eventType) {
        if (!active) return false;
        if ("*".equals(eventTypes)) return true;
        return Arrays.stream(eventTypes.split(","))
                .map(String::trim)
                .anyMatch(t -> t.equals(eventType));
    }

    public UUID getId() { return id; }
    public String getName() { return name; }
    public String getEndpointUrl() { return endpointUrl; }
    public String getSecret() { return secret; }
    public String getEventTypes() { return eventTypes; }
    public boolean isActive() { return active; }
    public Instant getCreatedAt() { return createdAt; }

    public void setActive(boolean active) { this.active = active; }
    public void setEndpointUrl(String url) { this.endpointUrl = url; }
    public void setEventTypes(String eventTypes) { this.eventTypes = eventTypes; }
}
