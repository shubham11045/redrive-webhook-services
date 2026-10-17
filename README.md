# Redrive

Webhook delivery service built with Java 21, Spring Boot 3, PostgreSQL, and Apache Kafka.

Accepts events via REST API and delivers them to subscriber endpoints with retries, backoff, dead-lettering, and HMAC signing.

```
publisher --POST /events--> [ingest API]
                              |  (one tx: event row + outbox row)
                              v
                          PostgreSQL <--------------+
                              |                     | delivery state machine
                    [outbox poller]                  | PENDING -> DELIVERED/DEAD
                              |                     |
                              v                     |
                            Kafka                   |
                              |                     |
                     [delivery consumer] -----------+
                              | fan-out: 1 row per (event, subscription)
                              v
                    [delivery dispatcher]
              virtual threads / bounded concurrency
              backoff+jitter / circuit breaker
                              | HMAC-signed POST
                              v
                    subscriber endpoints        (dead letters -> replay API)
```

## How it works

**Ingest.** Publishers POST events with an `Idempotency-Key` header. The event row and an outbox row are written in a single database transaction (transactional outbox pattern). Duplicate keys return the original event. Reusing a key with a different payload returns 409.

**Outbox to Kafka.** A poller claims unpublished outbox rows using `SELECT ... FOR UPDATE SKIP LOCKED` and publishes them to Kafka. If the app crashes between Kafka ack and marking the row published, the row is re-published on restart. The delivery layer deduplicates via a unique constraint.

**Delivery.** A Kafka consumer fans each event into one delivery row per active subscription. The dispatcher claims pending deliveries with `SKIP LOCKED` and dispatches them on virtual threads. Concurrency is bounded at two levels: a global semaphore (total in-flight cap) and a per-endpoint semaphore (prevents one slow subscriber from consuming the global budget).

**Retries.** Failed deliveries are retried with exponential backoff and jitter. 429 responses with `Retry-After` override the computed delay. After max attempts (default 8), the delivery is dead-lettered. HTTP 400/401/403/404/410 are treated as permanent failures and not retried.

**Circuit breaker.** Per-endpoint, in-memory. Opens after consecutive failures, skips deliveries during cooldown, allows a single probe after cooldown expires.

**Signing.** Every delivery is signed with HMAC-SHA256: `sign(secret, timestamp + "." + body)`. Subscribers verify authenticity and can reject stale replays via the timestamp.

**Replay.** Dead-lettered deliveries can be replayed via the API. Replay creates a new delivery row linked to the original (`replay_of`), preserving failure history.

## Running locally

```bash
docker compose up --build
```

Starts 7 services: Postgres, Kafka (KRaft single-node), Redrive, chaos subscriber, Kafka UI, Prometheus, Grafana.

| Service | URL |
|---|---|
| API | http://localhost:8080 |
| Kafka UI | http://localhost:8081 |
| Grafana | http://localhost:3000 (admin/redrive) |
| Prometheus | http://localhost:9090 |

## Quick demo

```bash
# register a subscription
curl -s -X POST localhost:8080/api/v1/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","endpointUrl":"http://chaos-subscriber:9099/webhook","eventTypes":"*"}'

# publish an event
curl -s -X POST localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-1' \
  -d '{"eventType":"order.created","payload":{"orderId":42}}'

# same key returns 200 with duplicate:true
# same key + different payload returns 409
# check delivery: curl -s localhost:9099/stats
```

## API

| Method | Path | Description |
|---|---|---|
| POST | /api/v1/events | Publish an event (requires `Idempotency-Key` header) |
| GET | /api/v1/events/{id} | Get event by ID |
| POST | /api/v1/subscriptions | Register a subscription (returns secret once) |
| GET | /api/v1/subscriptions | List subscriptions |
| GET | /api/v1/subscriptions/{id} | Get subscription |
| POST | /api/v1/subscriptions/{id}/pause | Pause delivery |
| POST | /api/v1/subscriptions/{id}/resume | Resume delivery |
| DELETE | /api/v1/subscriptions/{id} | Delete subscription |
| GET | /api/v1/deliveries/{id} | Get delivery status |
| GET | /api/v1/subscriptions/{id}/dead-letters | List dead-lettered deliveries |
| POST | /api/v1/deliveries/{id}/replay | Replay one dead letter |
| POST | /api/v1/subscriptions/{id}/replay-dead-letters | Replay all dead letters |

## Tests

```bash
mvn verify -DskipITs   # unit tests only
mvn verify             # unit + integration (Testcontainers with real Postgres and Kafka)
```

Unit tests cover backoff calculation, HMAC signing, and circuit breaker state transitions. Integration tests run the full pipeline against real Postgres and Kafka via Testcontainers: event ingest, idempotency, delivery, permanent failure detection, and replay.

## Load test

```bash
k6 run load-test/script.js
```

Ramps to 50 VUs, holds for 60 seconds, ramps down. Publishes events to the ingest API and checks latency and error rate. Thresholds: p95 under 500ms, error rate under 1%.

## Chaos subscriber

A configurable failing endpoint for testing retry and dead-letter behavior.

```bash
CHAOS_FAIL_RATE=0.3 docker compose up chaos-subscriber   # 30% of requests return 500
```

Environment variables: `CHAOS_FAIL_RATE`, `CHAOS_TIMEOUT_RATE`, `CHAOS_DELAY_MS`. `GET /stats` returns delivery counts and duplicate tracking.

## Configuration

All tunables are in `application.yml` under the `redrive` prefix. Key defaults:

| Property | Default | Notes |
|---|---|---|
| delivery.max-attempts | 8 | Attempts before dead-lettering |
| delivery.base-backoff-ms | 2000 | Initial retry delay |
| delivery.max-backoff-ms | 600000 | Ceiling (not reached at 8 attempts; effective max is ~256s) |
| delivery.global-concurrency | 200 | Max total in-flight deliveries |
| delivery.per-endpoint-concurrency | 4 | Max in-flight per endpoint host |
| delivery.request-timeout-ms | 10000 | HTTP response timeout |
| breaker.failure-threshold | 5 | Consecutive failures to open breaker |
| breaker.open-cooldown-ms | 30000 | Cooldown before half-open probe |

## Grafana dashboard

Auto-provisioned via Docker Compose. Four panels: delivery rate (success/fail/dead), delivery latency percentiles, dispatcher slot availability, and circuit breaker skips. Fire some events and set the time range to "Last 5 minutes" to see data.

## Design decisions

ADRs in [docs/adr](docs/adr):
- [Why Kafka](docs/adr/0001-why-kafka.md)
- [Delivery semantics](docs/adr/0002-delivery-semantics.md)
- [Ordering](docs/adr/0003-ordering.md)

## Known limitations

- No API authentication. `X-Publisher-Id` is client-supplied, so idempotency key namespaces are not isolated between callers.
- Subscription secrets stored in plaintext (would hash or encrypt in production)
- Circuit breaker state is in-memory per instance, not shared across replicas
- Single-broker Kafka in the dev compose setup
- Best-effort ordering: concurrent retries can complete out of order (see ADR-0003)
