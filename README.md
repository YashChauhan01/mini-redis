# Mini-Redis

A multithreaded, in-memory key-value store built from scratch in Java —
custom TCP server, lock-striped hash map, TTL + approximate-LRU eviction,
and crash-recoverable durability via a write-ahead log + snapshotting.

No external dependencies. Pure JDK (`java.net`, `java.util.concurrent`,
`java.nio`).

## Architecture

```
                       ┌──────────────────────┐
   TCP clients  ──────▶│   TcpServer          │  accepts connections,
                       │ (bounded thread pool)│  hands each to a worker
                       └──────────┬───────────┘
                                  │
                       ┌──────────▼───────────┐
                       │   ClientHandler      │  parses text protocol,
                       │  (one per connection)│  calls into KVStore
                       └──────────┬───────────┘
                                  │
              ┌───────────────────┼────────────────────┐
              ▼                                         ▼
   ┌─────────────────────┐                  ┌─────────────────────┐
   │  WriteAheadLog      │◀── logged first  │      KVStore        │
   │  (append-only file) │    then applied  │  hash(key) % stripes│
   └─────────────────────┘                  └──────────┬──────────┘
              ▲                                        │
              │ replayed on boot             ┌─────────┴─────────┐
   ┌─────────────────────┐                   ▼                   ▼
   │  SnapshotManager    │             ┌──────────┐        ┌──────────┐
   │ periodic full dump +│             │ Stripe 0 │  ...   │ Stripe N │
   │ WAL compaction      │             │ (own RW  │        │ (own RW  │
   └─────────────────────┘             │  lock)   │        │  lock)   │
                                       └──────────┘        └──────────┘
                                              ▲
                                    ┌─────────┴──────────┐
                                    │   TTLSweeper       │  background
                                    │  (daemon thread)   │  daemon thread
                                    └────────────────────┘
```

**Startup / crash recovery order:** load latest snapshot → replay WAL
entries written since that snapshot → store is now identical to its state
right before the crash.

## Build & run

```bash
# compile (no Maven needed, zero external deps)
find src -name "*.java" > sources.txt
javac -d out @sources.txt

# run the server
java -cp out com.yash.miniredis.Main --port 6380 --stripes 32 --capacity 100000

# or with Maven
mvn package
java -jar target/mini-redis.jar
```

Talk to it with the included interactive CLI (recommended for demos —
colored, redis-cli-style output):

```bash
java -cp out com.yash.miniredis.cli.MiniRedisCli localhost 6380
```

Or any raw TCP tool, e.g. `nc localhost 6380`, or the included benchmark
client.

### Protocol (newline-delimited, RESP-inspired)

```
SET key value [EX seconds]   -> +OK
GET key                      -> $value   or   $-1 (nil)
DEL key                      -> :1 or :0
EXPIRE key seconds           -> :1 or :0
STATS                        -> $keys=<n> stripes=<n>
PING                         -> +PONG
QUIT                         -> +OK, closes connection
```

### Server flags

| Flag | Default | Meaning |
|---|---|---|
| `--port` | 6380 | TCP listen port |
| `--stripes` | 32 | number of lock stripes |
| `--capacity` | 100000 | total key capacity across all stripes |
| `--core-threads` / `--max-threads` | 8 / 64 | thread pool sizing |
| `--ttl-sweep-ms` | 1000 | background expiry sweep interval |
| `--snapshot-interval-ms` | 30000 | full snapshot + WAL compaction interval |
| `--data-dir` | ./data | where the WAL + snapshot files live |

### Benchmark (proves the scaling claim, doesn't just assert it)

```bash
java -cp out com.yash.miniredis.client.BenchmarkClient --threads 32 --ops 20000
```

Run it at `--threads 1, 4, 16, 32` and watch throughput climb as stripe
contention drops. Sample run on this dev box (small VM, few cores — a real
multi-core machine will show a cleaner curve):

```
threads=1   throughput=6762 ops/sec
threads=4   throughput=16350 ops/sec
threads=16  throughput=36343 ops/sec
threads=32  throughput=51539 ops/sec
```