# RESP Decoder — Go Study to Node.js Implementation

We are designing a Redis-like database step by step.
Today's topic is RESP (REdis Serialization Protocol) decoding.

Studied in Go, implemented in Node.js with no external dependencies.

## What it does

1. Takes raw bytes (`Buffer` in Node.js, `[]byte` in Go) from TCP.
2. `Decode` returns the first complete RESP value + how many bytes it consumed.
3. `decodeOne` inspects the first type byte (`+ - : $ *`) and dispatches.
4. Each type has its own reader: `readSimpleString`, `readError`,
   `readInt64`, `readBulkString`, `readArray`.
5. If bytes are incomplete or malformed, returns an error instead of value.
6. Later steps (not today): use this decoder in TCP server loop to parse
   client commands like `*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n`.

## My work order (this repo's rule)

1. Create `.md` file named after the topic (this file).
2. Write the plan inside it (below).
3. Build it in Node.js format (`resp-decoder.js` + test file).

## RESP2 types studied

```
Prefix  Type            Example wire bytes              Decoded value
------  --------------  ------------------------------  -------------------
`+`     Simple String   +OK\r\n                         "OK"
`-`     Error           -ERR bad\r\n                    Error("ERR bad")
`:`     Integer         :123\r\n                        123
`$`     Bulk String     $5\r\nhello\r\n                 "hello"
        Bulk Null       $-1\r\n                         null
`*`     Array           *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n   ["hello","world"]
        Array Null      *-1\r\n                         null
        Array Empty     *0\r\n                           []
```

Rules:

- Every line ends with `\r\n` (CRLF). Bare `\n` is a protocol error.
- Bulk String = `$<len>\r\n<bytes>\r\n`. `$-1` means null, no bytes follow.
- Array = `*<count>\r\n<element>...`. `*-1` means null. Elements are
  themselves full RESP values (recursive).
- Integers are signed 64-bit decimal (`:-42\r\n`, `:+12\r\n` allowed).

## Go functions studied → Node.js port

```text
Go: func Decode(data []byte) (any, int, error)
JS: function Decode(buf) -> { data, pos, err }
  |
  v
Go: func decodeOne(data []byte) (any, int, error)
JS: function decodeOne(buf, pos) -> { data, nextPos, err }
  |
  +--> '+' -> readSimpleString
  +--> '-' -> readError
  +--> ':' -> readInt64
  +--> '$' -> readBulkString
  +--> '*' -> readArray
  +--> else -> error "unknown type byte"
```

### Functions

| Function | Responsibility |
|----------|----------------|
| `Decode(buf)` | Entry point. Takes bytes, returns first value + bytes consumed. Rejects empty input. |
| `decodeOne(buf, pos)` | Reads type byte at `pos`, dispatches to correct reader. |
| `readLine(buf, pos)` (helper) | Finds `\r\n` from `pos`, returns line string + position after CRLF. Errors on missing CRLF (incomplete). |
| `readSimpleString(buf, pos)` | After `+`, read line until `\r\n`. Returns JS string. |
| `readError(buf, pos)` | After `-`, read line until `\r\n`. Returns `Error` object. |
| `readInt64(buf, pos)` | After `:`, read line, validate `/^[+-]?\d+$/`, parse to Number, range-check safe integer. |
| `readBulkString(buf, pos)` | After `$`, read length line, handle `-1` → null, else read exactly `len` bytes + trailing `\r\n`. Errors on short buffer. |
| `readArray(buf, pos)` | After `*`, read count line, handle `-1` → null, else loop `count` times calling `decodeOne` recursively. |

Return convention (to mirror Go's `(data, int, err)`):

```js
{ data: any, pos: number, err: Error | null }
// pos = next unread offset (== bytes consumed when called with pos=0)
// err != null means data is undefined, pos is unchanged/irrelevant
```

## Plan for Node.js build

1. Use `Buffer` everywhere, never coerce whole input to string first (bulk strings may be binary).
2. All readers take `(buf, pos)` where `pos` points just after the type byte. Return `{ data, pos: nextPos, err: null }` or `{ data: undefined, pos, err }`.
3. `Decode(buf)` accepts `Buffer | string | Uint8Array`, normalizes to `Buffer`, checks empty, calls `decodeOne(buf, 0)`, returns `{ data, n, err }` where `n` = consumed bytes (to mirror Go's `int` return).
4. No throws for protocol errors — always return `err`. Only programmer errors (bad args) throw TypeError.
5. Test file `test-resp-decoder.js` covers: each prefix, nulls, empty, nested arrays, integer edges, split/incomplete buffers, trailing bytes, garbage prefix.

## Flow

```text
Decode(buf)
  |
  v
decodeOne(buf, 0)
  |
  v
switch (buf[0])
  |
  +-- 0x2B '+' -> readSimpleString -> readLine
  +-- 0x2D '-' -> readError        -> readLine -> Error()
  +-- 0x3A ':' -> readInt64        -> readLine -> parseInt
  +-- 0x24 '$' -> readBulkString   -> readLine(len) -> slice(len) -> check CRLF
  +-- 0x2A '*' -> readArray        -> readLine(count) -> decodeOne x count
```

## Important points

- TCP is a byte stream: one `data` event may hold half a command or three commands. Caller uses returned `n`/`nextPos` to slice the buffer and keep the remainder.
- `readLine` is the single CRLF gatekeeper — every type uses it, so incomplete data fails fast in one place.
- Bulk strings use byte length (`Buffer.length`), not character length — UTF-8 safe.
- Arrays are recursive — an array element can be another array.
- Null (`$-1`, `*-1`) decodes to JS `null`, distinct from empty (`$0\r\n\r\n` → `""`, `*0\r\n` → `[]`).

## Run / test

```bash
node test-resp-decoder.js
```

## Next steps (not today)

- Encoder (`Encode`): JS value → RESP bytes for server replies.
- Integrate decoder into `main.js` TCP loop with a per-connection leftover buffer.
- Command dispatcher (`PING`, `GET`, `SET`) on top of decoded arrays.

```
--- Simple String (+) ---
PASS simple OK: wire="+OK\r\n" -> "OK" n=5
PASS simple hello: wire="+hello world\r\n" -> "hello world" n=14
PASS simple empty: wire="+\r\n" -> "" n=3
--- Error (-) ---
PASS error value: wire="-ERR bad command\r\n" -> Error="ERR bad command" n=18
PASS error empty msg: wire="-\r\n" -> Error="" n=3
--- Integer (:) ---
PASS int zero: wire=":0\r\n" -> 0 n=4
PASS int positive: wire=":1000\r\n" -> 1000 n=7
PASS int negative: wire=":-42\r\n" -> -42 n=6
PASS int plus sign: wire=":+12\r\n" -> 12 n=6
--- Bulk String ($) ---
PASS bulk hello: wire="$5\r\nhello\r\n" -> "hello" n=11
PASS bulk empty: wire="$0\r\n\r\n" -> "" n=6
PASS bulk null: wire="$-1\r\n" -> null n=5
PASS bulk with spaces: wire="$11\r\nhello world\r\n" -> "hello world" n=18
--- Array (*) ---
PASS array empty: wire="*0\r\n" -> [] n=4
PASS array null: wire="*-1\r\n" -> null n=5
PASS array 2 bulks: wire="*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n" -> ["hello","world"] n=26
PASS array mixed: wire="*3\r\n+OK\r\n:42\r\n$5\r\nhello\r\n" -> ["OK",42,"hello"] n=25
PASS array nested: wire="*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n+hi\r\n" -> [[1,2],["hi"]] n=25
PASS array with nulls: wire="*3\r\n$-1\r\n*-1\r\n:7\r\n" -> [null,null,7] n=18
PASS GET command shape: wire="*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n" -> ["GET","key"] n=22
--- Direct reader functions (pos after type byte) ---
PASS direct readSimpleString
PASS direct readError
PASS direct readInt64
PASS direct readBulkString
PASS direct readArray
PASS direct decodeOne
--- Trailing bytes / streaming (n) ---
PASS trailing bytes: first="OK" n=5, second=1
--- Error / incomplete cases ---
PASS unknown prefix: wire="?xx\r\n" -> err="unknown type byte: \"?\""
PASS empty input: wire="" -> err="incomplete: empty input"
PASS missing CRLF: wire="+OK" -> err="incomplete: missing CRLF"
PASS bad int: wire=":12x\r\n" -> err="invalid integer: \"12x\""
PASS short bulk: wire="$5\r\nhi\r\n" -> err="incomplete: bulk string bytes missing"
PASS bulk missing CRLF: wire="$5\r\nhelloXX" -> err="protocol error: bulk string must end with CRLF"
PASS short array: wire="*2\r\n:1\r\n" -> err="incomplete: empty input"
PASS negative bulk other than -1: wire="$-2\r\n" -> err="invalidbulk length: -2"

35 passed, 0 failed
➜  tcp-echo-server-js git:(main) ✗ 
```