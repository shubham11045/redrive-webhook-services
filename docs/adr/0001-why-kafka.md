# ADR-0001: Why Kafka

**Status:** accepted · **Date:** 2026-07

## Context
Events need to get from Postgres to delivery workers reliably, even during traffic spikes or crashes.

Postgres alone could handle this at the current scale using `FOR UPDATE SKIP LOCKED` as a job queue. Kafka was chosen for what it adds as the system grows.

## Why Kafka
- Buffers ingest spikes so delivery workers don't slow down publishers
- Event stream is replayable and can feed additional consumers (e.g. analytics) without changing the ingest path
- Consumer groups scale horizontally with partition count

## Alternatives
| Option | Why not |
|---|---|
| Postgres job table only | Works at this scale but no replay, single consumer, delivery queries contend with ingest |
| RabbitMQ | No retention/replay, retry topology (TTL + DLX) is more complex than DB-scheduled retries |
| SQS/SNS | Would work but wanted the stack to be self-hostable and inspectable |

## Tradeoff
One more stateful service to run. KRaft single-node in dev, would need replication in production.
