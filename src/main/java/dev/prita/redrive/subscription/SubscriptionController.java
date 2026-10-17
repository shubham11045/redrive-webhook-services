package dev.prita.redrive.subscription;

import dev.prita.redrive.common.ApiExceptions.NotFoundException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.net.URI;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hibernate.validator.constraints.URL;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/subscriptions")
public class SubscriptionController {

    private static final SecureRandom RANDOM = new SecureRandom();

    private final SubscriptionRepository subscriptions;

    public SubscriptionController(SubscriptionRepository subscriptions) {
        this.subscriptions = subscriptions;
    }

    public record CreateRequest(
            @NotBlank String name,
            @NotBlank @URL String endpointUrl,
            String eventTypes // optional, defaults to '*'
    ) {}

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@Valid @RequestBody CreateRequest req) {
        // Secret is generated server-side and returned ONCE, like real webhook
        // providers do. We store it plaintext in v1 (documented limitation -
        // production systems store a hash or encrypt at rest).
        var secretBytes = new byte[32];
        RANDOM.nextBytes(secretBytes);
        var secret = "whsec_" + Base64.getUrlEncoder().withoutPadding().encodeToString(secretBytes);

        var sub = new Subscription(UUID.randomUUID(), req.name(), req.endpointUrl(), secret, req.eventTypes());
        subscriptions.save(sub);
        return ResponseEntity.created(URI.create("/api/v1/subscriptions/" + sub.getId()))
                .body(Map.of("id", sub.getId().toString(), "secret", secret));
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        return subscriptions.findAll().stream().map(this::toBody).toList();
    }

    @GetMapping("/{id}")
    public Map<String, Object> get(@PathVariable UUID id) {
        return subscriptions.findById(id).map(this::toBody)
                .orElseThrow(() -> new NotFoundException("subscription not found"));
    }

    @PostMapping("/{id}/pause")
    public Map<String, Object> pause(@PathVariable UUID id) {
        return setActive(id, false);
    }

    @PostMapping("/{id}/resume")
    public Map<String, Object> resume(@PathVariable UUID id) {
        return setActive(id, true);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) {
        subscriptions.deleteById(id);
        return ResponseEntity.noContent().build();
    }

    private Map<String, Object> setActive(UUID id, boolean active) {
        var sub = subscriptions.findById(id)
                .orElseThrow(() -> new NotFoundException("subscription not found"));
        sub.setActive(active);
        subscriptions.save(sub);
        return toBody(sub);
    }

    private Map<String, Object> toBody(Subscription s) {
        return Map.of(
                "id", s.getId().toString(),
                "name", s.getName(),
                "endpointUrl", s.getEndpointUrl(),
                "eventTypes", s.getEventTypes(),
                "active", s.isActive(),
                "createdAt", s.getCreatedAt().toString()
        );
    }
}
