# Examples

## shop-publisher.sh

Simulates an e-commerce backend publishing order events. Demonstrates:
- Publishing events with idempotency keys
- Retrying a duplicate (same key returns 200 + `duplicate: true`)

```bash
chmod +x shop-publisher.sh
./shop-publisher.sh http://localhost:8080
```

## invoice-subscriber/

A minimal webhook consumer that does the two things every subscriber MUST do:
1. **Verify the HMAC signature** - reject forged or tampered deliveries
2. **Deduplicate on delivery ID** - handle Redrive's at-least-once redelivery

### Run it

```bash
# Get the secret from the subscription creation response
java examples/invoice-subscriber/src/main/java/dev/prita/invoice/InvoiceSubscriber.java <your-whsec-secret> 9095
```

Then create a subscription pointing at `http://localhost:9095/webhook` and publish events.

### What to look for

- `[OK]` lines show successfully verified + processed deliveries
- `[REJECTED]` means the signature didn't match (tampered payload or wrong secret)
- `[SKIPPED]` means the delivery ID was already seen (Redrive redelivered after a timeout)
