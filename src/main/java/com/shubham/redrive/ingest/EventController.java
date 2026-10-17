package com.shubham.redrive.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final EventService service;
    private final EventRepository events;

    public EventController(EventService service, EventRepository events) {
        this.service = service;
        this.events = events;
    }

    public record IngestRequest(
            @NotBlank String eventType,
            @NotNull JsonNode payload
    ) {}

    /**
     * Idempotency contract: the Idempotency-Key header is REQUIRED.
     * Retrying the same request returns the original event with 200
     * instead of 201, and no new event is created.
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> ingest(
            @RequestHeader("Idempotency-Key") @NotBlank String idempotencyKey,
            @RequestHeader(value = "X-Publisher-Id", defaultValue = "default") String publisherId,
            @Valid @RequestBody IngestRequest request) {

        var result = service.ingest(publisherId, request.eventType(), request.payload().toString(), idempotencyKey);
        var body = toBody(result.event(), result.duplicate());
        if (result.duplicate()) {
            return ResponseEntity.ok(body);
        }
        return ResponseEntity.created(URI.create("/api/v1/events/" + result.event().getId())).body(body);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable UUID id) {
        return events.findById(id)
                .map(e -> ResponseEntity.ok(toBody(e, false)))
                .orElse(ResponseEntity.notFound().build());
    }

    private Map<String, Object> toBody(EventRecord e, boolean duplicate) {
        return Map.of(
                "id", e.getId().toString(),
                "eventType", e.getEventType(),
                "publisherId", e.getPublisherId(),
                "createdAt", e.getCreatedAt().toString(),
                "duplicate", duplicate
        );
    }
}
