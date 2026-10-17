package dev.prita.redrive.delivery;

import dev.prita.redrive.common.ApiExceptions.NotFoundException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Delivery inspection + replay.
 *
 * Replay creates a NEW delivery row linked to the original (replay_of),
 * so failure history is preserved.
 */
@RestController
@RequestMapping("/api/v1")
public class ReplayController {

    private final DeliveryRepository deliveries;

    public ReplayController(DeliveryRepository deliveries) {
        this.deliveries = deliveries;
    }

    @GetMapping("/deliveries/{id}")
    public Map<String, Object> get(@PathVariable UUID id) {
        var d = deliveries.findById(id).orElseThrow(() -> new NotFoundException("delivery not found"));
        return toBody(d);
    }

    @GetMapping("/subscriptions/{subscriptionId}/dead-letters")
    public List<Map<String, Object>> deadLetters(@PathVariable UUID subscriptionId) {
        var dead = deliveries.findBySubscriptionIdAndStatus(subscriptionId, Delivery.Status.DEAD);
        var perm = deliveries.findBySubscriptionIdAndStatus(subscriptionId, Delivery.Status.FAILED_PERMANENT);
        var all = new java.util.ArrayList<>(dead);
        all.addAll(perm);
        return all.stream().map(this::toBody).toList();
    }

    @PostMapping("/deliveries/{id}/replay")
    @Transactional
    public ResponseEntity<Map<String, Object>> replayOne(@PathVariable UUID id) {
        var d = deliveries.findById(id).orElseThrow(() -> new NotFoundException("delivery not found"));
        if (d.getStatus() != Delivery.Status.DEAD) {
            throw new IllegalArgumentException("only DEAD deliveries can be replayed; status=" + d.getStatus());
        }
        var replayed = deliveries.save(d.replayAs());
        return ResponseEntity.accepted().body(toBody(replayed));
    }

    @PostMapping("/subscriptions/{subscriptionId}/replay-dead-letters")
    @Transactional
    public ResponseEntity<Map<String, Object>> replayAllDead(@PathVariable UUID subscriptionId) {
        var dead = deliveries.findBySubscriptionIdAndStatus(subscriptionId, Delivery.Status.DEAD);
        int count = 0;
        for (var d : dead) {
            deliveries.save(d.replayAs());
            count++;
        }
        return ResponseEntity.accepted().body(Map.of("replayed", count));
    }

    private Map<String, Object> toBody(Delivery d) {
        var map = new java.util.LinkedHashMap<String, Object>();
        map.put("id", d.getId().toString());
        map.put("eventId", d.getEventId().toString());
        map.put("subscriptionId", d.getSubscriptionId().toString());
        map.put("status", d.getStatus().name());
        map.put("attemptCount", d.getAttemptCount());
        map.put("nextAttemptAt", d.getNextAttemptAt() == null ? null : d.getNextAttemptAt().toString());
        map.put("lastStatusCode", d.getLastStatusCode());
        map.put("lastError", d.getLastError());
        map.put("deliveredAt", d.getDeliveredAt() == null ? null : d.getDeliveredAt().toString());
        map.put("replayOf", d.getReplayOf() == null ? null : d.getReplayOf().toString());
        return map;
    }
}
