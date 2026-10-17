import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const duplicates = new Counter('duplicate_responses');

const BASE_URL = __ENV.REDRIVE_URL || 'http://localhost:8080';

export const options = {
    stages: [
        { duration: '15s', target: 50 },   // ramp up
        { duration: '60s', target: 50 },   // steady
        { duration: '10s', target: 0 },    // ramp down
    ],
    thresholds: {
        http_req_duration: ['p(95)<500'],
        http_req_failed: ['rate<0.01'],
    },
};

export default function () {
    const key = `k6-${__VU}-${__ITER}-${Date.now()}`;
    const payload = JSON.stringify({
        eventType: 'order.created',
        payload: {
            orderId: key,
            total: Math.random() * 100,
            currency: 'USD',
        },
    });

    const res = http.post(`${BASE_URL}/api/v1/events`, payload, {
        headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': key,
        },
    });

    check(res, {
        'status is 201': (r) => r.status === 201,
    });

    if (res.status === 200) {
        duplicates.add(1);
    }

    sleep(0.05);
}
