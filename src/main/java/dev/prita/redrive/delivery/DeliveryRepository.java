package dev.prita.redrive.delivery;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

public interface DeliveryRepository extends JpaRepository<Delivery, UUID> {

    /** Work-claim query: due PENDING rows, oldest first, SKIP LOCKED for multi-replica safety. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select d from Delivery d where d.status = 'PENDING' and d.nextAttemptAt <= :now order by d.nextAttemptAt")
    List<Delivery> claimDue(@Param("now") Instant now, Pageable page);

    List<Delivery> findBySubscriptionIdAndStatus(UUID subscriptionId, Delivery.Status status);

    List<Delivery> findByEventId(UUID eventId);

    long countByStatus(Delivery.Status status);

    boolean existsByEventIdAndSubscriptionId(UUID eventId, UUID subscriptionId);
}
