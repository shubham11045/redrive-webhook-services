# ADR-0003: Best-effort ordering

**Status:** accepted · **Date:** 2026-07

## Context
Kafka partitions guarantee consumption order, but that doesn't mean delivery completion order. If event 1 fails and backs off while event 2 succeeds immediately, they complete out of order.

## Options
1. **Strict ordering:** don't attempt event N+1 until event N is DELIVERED or DEAD. Problem: one failing endpoint blocks the entire subscription for potentially hours (maxAttempts x maxBackoff). Concurrency drops to 1 per subscription.
2. **Best-effort (chosen):** deliveries are created in order and dispatched oldest-first, but retries and concurrency can complete them out of order. Subscribers that need ordering use the event timestamp or their own sequence field.

## Decision
Best-effort for now. Strict ordering could be added later as an opt-in per subscription with an explicit head-of-line blocking tradeoff.
