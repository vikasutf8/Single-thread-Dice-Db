# Load Test Log (`load_test.md`)

> APPEND-ONLY FILE — never edit or delete past entries.
> Every test run appends a new dated entry at the bottom.

## Prerequisites

- `redis-benchmark` installed (ships with Redis).
- Server running on `localhost:8080` — either:
  - `node main.js --host 127.0.0.1 --port 8080`, or
  - `node -e 'require("./async.tcp.js").RunAsyncTcpServer("127.0.0.1", 8080)'`

## Test command

```bash
redis-benchmark -n 10000 -t ping_mbulk -c 200 -h localhost -p 8080
```

- `-n 10000` → total requests
- `-t ping_mbulk` → PING test (bulk pipeline mode)
- `-c 200` → 200 parallel clients
- `-h / -p` → target host/port

## How to append a result (do this every run)

Run the benchmark and append its output with a date stamp — single command:

```bash
{
  echo ""
  echo "## Run — $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  echo ""
  echo '```'
  redis-benchmark -n 10000 -t ping_mbulk -c 200 -h localhost -p 8080
  echo '```'
} >> load_test.md
```

Verify it appended:

```bash
tail -20 load_test.md
```

## Results log (append below this line)

<!-- New runs go below. Do not edit entries above. -->

## Run — 2026-10-14 16:06:49 UTC

- Server: async (`RunAsyncTcpServer("127.0.0.1", 8081)`), 1 server × 3 concurrent scripted clients
- Client scripts (RESP over TCP):
  - client1: `PING`, `SET name vikas`, `GET name`
  - client2: `SET tmp val EX 60`, `TTL tmp`, `GET tmp`
  - client3: `GET nokey`, `TTL nokey`, `PING hello`

```text
client1: "+PONG\r\n+OK\r\n$5\r\nvikas\r\n"
client2: "+OK\r\n:60\r\n$3\r\nval\r\n"
client3: "$-1\r\n:-2\r\n$5\r\nhello\r\n"
server stats: {"active":0,"accepted":3,"rejected":0,"monitored":0}
```

- Result: PASS — all replies correct, `active` back to 0, `monitored_fds` 0 (no fd leak).
- Note: an early client2 run failed with `-ERR syntax error` because the
  test script sent `*4` for 5-element `SET tmp val EX 60`; fixed to `*5`, server behaved correctly throughout.
