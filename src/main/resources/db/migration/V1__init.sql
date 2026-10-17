-- Redrive schema v1.
-- Design notes:
--  * events + outbox are written in ONE transaction (transactional outbox pattern).
--  * deliveries is the durable delivery state machine: one row per (event, subscription).
--  * All timestamps are UTC (timestamptz).

CREATE TABLE events (
    id               UUID PRIMARY KEY,
    publisher_id     TEXT        NOT NULL,
    event_type       TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    idempotency_key  TEXT        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The idempotency guarantee: same publisher + same key can only ever insert once.
    CONSTRAINT uq_events_publisher_idem UNIQUE (publisher_id, idempotency_key)
);

CREATE TABLE event_outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      UUID        NOT NULL REFERENCES events (id),
    topic         TEXT        NOT NULL,
    message_key   TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    published     BOOLEAN     NOT NULL DEFAULT FALSE,
    publish_attempts INT      NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

-- Fast scan for the outbox poller: unpublished rows in insertion order.
CREATE INDEX idx_outbox_unpublished ON event_outbox (id) WHERE NOT published;

CREATE TABLE subscriptions (
    id           UUID PRIMARY KEY,
    name         TEXT        NOT NULL,
    endpoint_url TEXT        NOT NULL,
    secret       TEXT        NOT NULL,
    -- Comma-separated list of event types, '*' = all. Kept simple on purpose (v1).
    event_types  TEXT        NOT NULL DEFAULT '*',
    active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE deliveries (
    id             UUID PRIMARY KEY,
    event_id       UUID        NOT NULL REFERENCES events (id),
    subscription_id UUID       NOT NULL REFERENCES subscriptions (id),
    status         TEXT        NOT NULL, -- PENDING | DELIVERED | DEAD
    attempt_count  INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status_code INT,
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at   TIMESTAMPTZ,
    -- Consumer redelivery (Kafka at-least-once) must not create duplicate delivery jobs.
    CONSTRAINT uq_deliveries_event_sub UNIQUE (event_id, subscription_id)
);

-- The dispatcher's work-claim query: due PENDING rows.
CREATE INDEX idx_deliveries_due ON deliveries (next_attempt_at) WHERE status = 'PENDING';
CREATE INDEX idx_deliveries_sub_status ON deliveries (subscription_id, status);
