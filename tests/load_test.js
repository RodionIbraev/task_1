import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';

export const options = {
  discardResponseBodies: false,
  scenarios: {
    smoke_get: {
      executor: 'constant-vus',
      exec: 'smokeGet',
      vus: 5,
      duration: '15s',
      tags: { test_type: 'smoke', endpoint: 'get_root' },
    },
    steady_get: {
      executor: 'ramping-vus',
      exec: 'steadyGet',
      startTime: '15s',
      stages: [
        { duration: '20s', target: 20 },
        { duration: '40s', target: 50 },
        { duration: '20s', target: 50 },
        { duration: '20s', target: 0 },
      ],
      gracefulRampDown: '5s',
      tags: { test_type: 'steady', endpoint: 'get_echo' },
    },
    steady_post: {
      executor: 'ramping-vus',
      exec: 'steadyPost',
      startTime: '15s',
      stages: [
        { duration: '20s', target: 10 },
        { duration: '40s', target: 30 },
        { duration: '20s', target: 30 },
        { duration: '20s', target: 0 },
      ],
      gracefulRampDown: '5s',
      tags: { test_type: 'steady', endpoint: 'post_echo' },
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.02'],
    http_req_duration: ['p(95)<800', 'p(99)<1500'],
    checks: ['rate>0.99'],
  },
};

const defaultParams = {
  timeout: '35s',
  headers: {
    'Content-Type': 'application/x-www-form-urlencoded',
  },
};

export function smokeGet() {
  const res = http.get(`${BASE_URL}/`, {
    ...defaultParams,
    tags: { name: 'GET /' },
  });

  check(res, {
    'GET / status is 200': (r) => r.status === 200,
    'GET / has upstream greeting': (r) => r.body.includes('Hello from upstream'),
  });

  sleep(0.2);
}

export function steadyGet() {
  const path = `anything-${__VU}-${__ITER}`;
  const res = http.get(`${BASE_URL}/${path}`, {
    ...defaultParams,
    tags: { name: 'GET /{path}' },
  });

  check(res, {
    'GET /{path} status is 200': (r) => r.status === 200,
    'GET /{path} returns method GET': (r) => r.body.includes('"method":"GET"') || r.body.includes('"method": "GET"'),
  });

  sleep(0.1);
}

export function steadyPost() {
  const payload = `hello=world&vu=${__VU}&iter=${__ITER}&data=${'x'.repeat(128)}`;

  const res = http.post(`${BASE_URL}/echo`, payload, {
    ...defaultParams,
    tags: { name: 'POST /echo' },
  });

  check(res, {
    'POST /echo status is 200': (r) => r.status === 200,
    'POST /echo echoes POST': (r) => r.body.includes('"method":"POST"') || r.body.includes('"method": "POST"'),
    'POST /echo echoes body': (r) => r.body.includes('hello=world'),
  });

  sleep(0.1);
}