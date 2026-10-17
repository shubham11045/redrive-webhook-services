#!/usr/bin/env bash
#
# Simulates an e-commerce backend publishing order events to Redrive.
# Usage: ./shop-publisher.sh [REDRIVE_URL]

set -euo pipefail

REDRIVE="${1:-http://localhost:8080}"
PUBLISHER="shop-service"

publish_event() {
    local idem_key="$1"
    local event_type="$2"
    local payload="$3"

    echo "Publishing $event_type (key=$idem_key)..."
    curl -s -w "\n  HTTP %{http_code}\n" \
        -X POST "$REDRIVE/api/v1/events" \
        -H "Content-Type: application/json" \
        -H "Idempotency-Key: $idem_key" \
        -H "X-Publisher-Id: $PUBLISHER" \
        -d "{\"eventType\": \"$event_type\", \"payload\": $payload}"
    echo ""
}

echo "=== Shop Publisher ==="
echo "Target: $REDRIVE"
echo ""

ORDER_ID="order-$(date +%s)"

publish_event "${ORDER_ID}-created" "order.created" \
    "{\"orderId\": \"$ORDER_ID\", \"total\": 79.99, \"currency\": \"USD\", \"items\": [\"widget-a\", \"widget-b\"]}"

sleep 1

publish_event "${ORDER_ID}-paid" "order.paid" \
    "{\"orderId\": \"$ORDER_ID\", \"amount\": 79.99, \"method\": \"card\"}"

sleep 1

# Demonstrate idempotency: re-send the same event with the same key.
echo "--- Resending order.created (same idempotency key, should return 200 + duplicate=true) ---"
publish_event "${ORDER_ID}-created" "order.created" \
    "{\"orderId\": \"$ORDER_ID\", \"total\": 79.99, \"currency\": \"USD\", \"items\": [\"widget-a\", \"widget-b\"]}"

echo "Done. Check your subscriber logs for delivered webhooks."
