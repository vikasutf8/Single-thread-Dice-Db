# PING Command — Go Study to Node.js Implementation

We are designing a Redis-like database step by step.
Today's topic is the `PING` command over RESP + TCP.

Studied in Go, implemented in Node.js with no external dependencies.

## What it does (Redis behavior)

| Client sends | Server replies | Wire reply |
|--------------|----------------|------------|
| `PING` | `PONG` (simple string) | `+PONG\r\n` |
| `PING hello` | `hello` (bulk string echo) | `$5\r\nhello\r\n` |
| `PING hello vikas` | error (too many args) | `-ERR wrong number of arguments for 'ping' command\r\n` |

Rules:

- Command name is case-insensitive: `ping`, `PING`, `Ping` all work.
- 0 args → `PONG`. 1 arg → echo that arg. >1 args → error.
- Replies are RESP-encoded bytes written with `conn.write()`.

## My work order (this repo's rule)

1. Create `.md` file named after the topic (this file).
2. Write the plan inside it (below).
3. Build it in Node.js format (`resp-encoder.js`, `redis-cmd.js`,
   `eval.js`, wiring in `main.js`, plus `test-ping.js`).

## Go functions studied → Node.js port

```text
Go: type RedisCmd struct { Cmd string; Args []string }
Go: func readCmd(conn) (*RedisCmd, error)
      └── decodeArray(buf) using Decode() → []string
          Cmd = UPPER(arr[0]), Args = arr[1:]

Go: func respond(cmd *RedisCmd, conn)
      └── evalAndRespond(cmd, conn)
            ├── on readCmd err → respondError(err, conn)
            └── switch cmd.Cmd:
                  case "PING" → evalPING(args, conn)
                  default    → evalDefault(cmd, conn)

Go: func evalPING(args []string, conn)
Go: func respondError(err error, conn) → conn.Write(EncodeError(err))

JS (same names, lowerCamel where idiomatic):
  readCmd(buf) -> { cmd, n, err }        // redis-cmd.js
  decodeArrayValue(data) -> string[]     // internal, uses Decode result
  evalAndRespond(redisCmd, conn)         // eval.js
  evalPING(args, conn)                  // eval.js
  evalDefault(redisCmd, conn)            // eval.js
  respondError(err, conn)                // eval.js
  EncodeSimpleString / EncodeBulkString / EncodeError  // resp-encoder.js
```

### Functions

| Function | Responsibility |
|----------|----------------|
| `Decode(buf)` (existing) | Raw RESP bytes → JS value + consumed `n`. |
| `decodeArrayValue(data)` | Validate decoded value is an Array of strings (reject null array, empty array, non-string elements). |
| `readCmd(buf)` | `Decode` + `decodeArrayValue` → `{ Cmd: UPPER(arr[0]), Args: arr[1:] }` + `n`. Returns `err` on bad shape/incomplete. |
| `EncodeSimpleString(s)` | `"PONG"` → `Buffer("+PONG\r\n")`. |
| `EncodeBulkString(s)` | `"hello"` → `Buffer("$5\r\nhello\r\n")`. Byte length, not char length. |
| `EncodeError(msg)` | `Error`/`string` → `Buffer("-ERR ...\r\n")`. |
| `evalPING(args, conn)` | 0 args → `+PONG`, 1 arg → bulk echo, >1 → error reply. |
| `evalDefault(redisCmd, conn)` | Unknown command → `-ERR unknown command '<cmd>'\r\n`. |
| `respondError(err, conn)` | Any `readCmd`/`eval` error → `-ERR <msg>\r\n` via `conn.write`. |
| `evalAndRespond(redisCmd, conn)` | Switch on `Cmd`: `PING` → `evalPING`, else `evalDefault`. |
| `handleClient` loop in `main.js` | Per-connection leftover `Buffer`, `while(true) readCmd → evalAndRespond`, keep remainder on incomplete. |

## Plan for Node.js build

1. New `resp-encoder.js`: pure functions returning `Buffer`. No throws for
   normal use; `EncodeBulkString(null/undefined)` → null bulk `$-1\r\n`.
2. New `redis-cmd.js`: `readCmd(buf)` accepts `Buffer|string`, calls
   `Decode`, then `decodeArrayValue`. `Cmd` uppercased, `Args` are raw
   strings. Empty array → error `empty command`. Null array → error
   `null command`.
3. New `eval.js`: takes a real socket (or fake `{ write() }` in tests).
   Never throws — all failures go out as RESP errors on the wire.
4. Rewire `main.js`: replace newline-echo with RESP loop. Keep
   `setupFlags()`, `RunSyncTcpServer()`, active-client count, logging.
   Per-connection state is a `Buffer` leftover; each `data` event appends
   and drains complete commands with `readCmd`.
5. New `test-ping.js`: covers `PING` / `PING hello` / `PING a b` (error),
   lowercase `ping`, unknown command, empty/incomplete input, plus a
   fake-socket integration test for `evalAndRespond`.

## Flow

```text
conn.on("data")
  |
  v
leftover = concat(leftover, chunk)
  |
  v
loop:
  readCmd(leftover) ── err=incomplete? → break, wait for more bytes
  │                     err=other? → respondError, slice n?, continue
  v
  evalAndRespond({ Cmd, Args }, conn)
  │── Cmd == "PING" → evalPING(Args, conn)
  │     ├── 0 args → conn.write("+PONG\r\n")
  │     ├── 1 arg  → conn.write("$<len>\r\n<arg>\r\n")
  │     └── >1     → conn.write("-ERR wrong number of arguments ...")
  └── else → evalDefault → conn.write("-ERR unknown command ...")
  |
  v
leftover = leftover.slice(n); repeat
```

## Important points

- TCP is a stream: one `PING` may arrive split across two `data` events,
  or two `PING`s may arrive in one event. The `n` from `readCmd` + leftover
  buffer handles both. Never assume one event = one command.
- `readCmd` only accepts arrays of bulk/simple strings (what `redis-cli`
  actually sends: `*1\r\n$4\r\nPING\r\n`). Integers inside the array are
  rejected — same as Go study.
- `PONG` is a Simple String (`+PONG`), but the single-arg echo is a Bulk
  String — this matches real Redis and matters to clients.
- Error replies are RESP errors on the wire, not JS throws, so the
  connection stays open after a bad `PING`.

## Run / test

```bash
node test-ping.js
node main.js --host 127.0.0.1 --port 8080
# in another terminal:
# redis-cli -p 8080 PING
# redis-cli -p 8080 PING hello
# redis-cli -p 8080 PING a b
```

## Next steps (not today)

- `ECHO`, `SET`, `GET` commands on the same `evalAndRespond` switch.
- Shared in-memory store object passed into eval functions.

```
➜  tcp-echo-server-js git:(main) ✗ node main.js --host 127.0.0.1 --port 8080
Starting Redis-like Server (PING support)...
TCP server listening on 127.0.0.1:8080
Waiting for client connections...
nc 127.0.0.1 8080New client connected: 127.0.0.1:56672 | Active clients: 1
Received cmd: PING []
Received cmd: PING ["vikas"]
Received cmd: PING []
Received cmd: PING ["hello"]
``
