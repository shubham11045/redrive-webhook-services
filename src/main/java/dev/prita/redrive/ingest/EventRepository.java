package dev.prita.redrive.ingest;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EventRepository extends JpaRepository<EventRecord, UUID> {
    Optional<EventRecord> findByPublisherIdAndIdempotencyKey(String publisherId, String idempotencyKey);
}
