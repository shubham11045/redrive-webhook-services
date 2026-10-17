# ADR-0002: At-least-once delivery

**Status:** accepted · **Date:** 2026-07

## Context
Exactly-once delivery to an external HTTP endpoint is impossible. If the subscriber returns 200 but the response is lost (network blip, app crash before writing the outcome), the delivery looks like it failed, so it gets retried. You have to pick: maybe lose events (at-most-once) or maybe duplicate them (at-least-once).

Redrive picks at-least-once and sends stable IDs (`X-Redrive-Delivery-Id`, `X-Redrive-Event-Id`) on every attempt so subscribers can dedupe.

## Crash windows
1. Crash after DB commit, before outbox publish - event gets published late on restart. No loss.
2. Crash after Kafka ack, before outbox row marked - duplicate message in Kafka, collapsed by the deliveries table unique constraint. No extra delivery jobs.
3. Crash after subscriber returns 200, before outcome write - delivery retried, subscriber sees a duplicate.
4. Kafka down - outbox rows pile up, catch up on recovery. No loss.

## Why not Kafka exactly-once (EOS)
Kafka EOS covers Kafka-to-Kafka processing, not HTTP side effects. It wouldn't change the end-to-end story since the last hop is always an HTTP call.
