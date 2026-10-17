package dev.prita.redrive.outbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxRecord, Long> {

    /**
     * Claim a batch of unpublished rows.
     *
     * FOR UPDATE SKIP LOCKED: if we ever run multiple app replicas, each
     * outbox poller claims disjoint rows instead of blocking on or double-
     * publishing the same batch. This is the standard Postgres work-queue
     * idiom.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2")) // SKIP LOCKED
    @Query("select o from OutboxRecord o where o.published = false and o.publishAttempts < :maxAttempts order by o.id")
    List<OutboxRecord> claimUnpublished(@Param("maxAttempts") int maxAttempts, org.springframework.data.domain.Pageable page);
}
