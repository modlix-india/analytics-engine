# analytics-engine

Lightweight analytics engine for small-scale deployments, without the operational overhead of
a full product-analytics stack.

One static Go binary. It ingests events over HTTP, acknowledges them in microseconds by
appending to a write-ahead log, folds closed segments into Parquet, and answers a fixed set of
analytics questions from pre-aggregated rollups. No Kafka, no ClickHouse, no database, no cgo.

**Status: in development.** The skeleton runs; ingest and query do not exist yet. See
[PLAN.md](PLAN.md) for the design and the milestone order.

## Running

```bash
ANALYTICS_NODE_ID=n1 go run ./cmd/engine
```

`ANALYTICS_NODE_ID` is required and has no default: it becomes part of every Parquet filename
this node writes, which is what lets several nodes share one bucket without coordinating. Two
nodes sharing an id would collide silently, so the process refuses to start instead.

| variable | default | |
|---|---|---|
| `ANALYTICS_NODE_ID` | — | **required**; stable and unique across the fleet |
| `ANALYTICS_DATA_DIR` | `/var/lib/analytics` | WAL and local Parquet |
| `ANALYTICS_LISTEN_ADDR` | `:8080` | |
| `ANALYTICS_WAL_SYNC_INTERVAL` | `100ms` | group-commit window — see Durability |
| `ANALYTICS_COMPACT_INTERVAL` | `5m` | sets query freshness |
| `ANALYTICS_WAL_SEGMENT_BYTES` | `64MiB` | |
| `ANALYTICS_WAL_SEGMENT_AGE` | `5m` | |
| `ANALYTICS_MAX_BATCH_EVENTS` | `500` | bound on one ingest request |
| `ANALYTICS_MAX_BODY_BYTES` | `1MiB` | bound on one ingest request |
| `ANALYTICS_LOG_LEVEL` | `info` | |
| `ANALYTICS_QUERY_SECRET` | — | reads are denied entirely without it |
| `ANALYTICS_S3_BUCKET` | — | set to enable object storage; everything stays local otherwise |
| `ANALYTICS_S3_ENDPOINT` | — | `http://` prefix selects plaintext; anything else uses TLS |
| `ANALYTICS_S3_ACCESS_KEY` / `_SECRET_KEY` / `_REGION` / `_PREFIX` | — | `OCI_S3_*` is accepted for the first three |
| `ANALYTICS_REPLICATE_INTERVAL` | `1m` | |
| `ANALYTICS_LOCAL_RETENTION` | `0` (keep) | how long Parquet stays on local disk after upload |
| `ANALYTICS_REMOTE_RETENTION` | `0` (keep forever) | how long it stays in the bucket |

## Endpoints

| | |
|---|---|
| `GET /healthz` | the process is alive. Checks nothing else on purpose: a liveness probe that fails on a dependency restarts a process that would have recovered |
| `GET /readyz` | this node can accept writes |
| `POST /q` | run one of the fixed widgets — see below |
| `GET /metrics` | Prometheus |

## Querying

```bash
curl -X POST http://localhost:8080/q -H 'Authorization: Bearer $ANALYTICS_QUERY_SECRET' -d '{
  "widget": "topPages",
  "site": "shop.example",
  "from": "2026-09-01T00:00:00Z",
  "to":   "2026-09-30T00:00:00Z",
  "timezone": "Asia/Kolkata",
  "limit": 10
}'
```

| widget | groups by |
|---|---|
| `pageviewsOverTime` | local calendar day |
| `eventTimeline` | local calendar day, for a named `event` |
| `topPages` | path |
| `topReferrers` | referrer host |
| `channelBreakdown` | direct / organic / social / paid / referral / email |
| `deviceBreakdown` | desktop / mobile / tablet |
| `browserBreakdown` | browser |
| `osBreakdown` | operating system |
| `geoBreakdown` | country |
| `platformBreakdown` | web / mobile_app |
| `appVersionBreakdown` | app version |
| `topEvents` | event name |
| `breakdownByProperty` | the dimension named in `property` |
| `funnel` | ordered `steps`, within `windowHours` of the first |
| `retention` | cohorts by first-seen `period` (day/week) |
| `stickiness` | how many distinct periods each visitor was active |
| `lifecycle` | new / returning / resurrecting / dormant per period |

`breakdownByProperty` accepts only an allow-listed dimension. That list is what keeps this a
fixed widget API rather than an arbitrary query surface, so `visitor`, `session` and `props`
are not among them.

There is no free-form query language, and that is the read security model: with no
client-supplied expression there is nothing to sanitise, and no request can widen its scope
beyond the site the caller was authorised for.

**`timezone` changes no stored data** — only which hourly buckets are summed. Rollups are
hourly rather than daily precisely so that any zone's day can be answered exactly. For
whole-hour offsets that is pure summation; for half-hour offsets like `Asia/Kolkata` the two
boundary hours are recomputed from raw data, which the response reports as `rawHoursScanned`.

The last four read raw data rather than rollups, because they depend on the ORDER and SPACING
of one visitor's events, which a rollup has discarded. Their counts are **exact**, and
`visitorsApproximate` is false for them. They are also the expensive ones: the scan is
hash-partitioned over visitors, so memory stays bounded at the cost of re-reading a narrow
three-column projection once per partition.

`lifecycle` returns `rangeRelative: true`. Its classifications are relative to the **queried
window**: a visitor of ten years' standing counts as `new` if their first event inside the
range falls in its first period. Reading further back would mean scanning unbounded history to
answer a bounded question.

**`visitorsApproximate` is true** wherever a visitor count comes from a rollup. Those come from
HyperLogLog sketches: ~0.8% error on headline numbers, ~2.3% on per-key breakdowns. Event
counts are exact.

## Durability

The WAL fsyncs every `ANALYTICS_WAL_SYNC_INTERVAL`. **A hard kill loses up to that much time
of events.** `analytics_wal_unsynced_events` reports the current window.

Ingest returns as soon as an event is buffered, without waiting for that fsync. This is not a
weaker guarantee than waiting would give: the sync happens on its interval regardless, so the
loss window is the same either way — waiting would only tell the caller that the write landed,
and nothing retries a pageview. The WAL also offers a waiting `Append` for callers that do act
on the answer.

Measured cost of the difference: ~10 events/sec per caller when waiting, 6.8M/sec when not.

## Security

**Ingest is public and write-only. The query endpoint must never be reachable from a browser
without passing the authorizer.**

Analytics ingest is spoofable in every product, this one included. Resolving a site from
`Origin` stops casual cross-site pollution from a browser, because browsers set `Origin`
themselves; it stops nothing from a script. Rate limiting in front is the real control. Do not
treat ingest as an authenticated path.

## Object storage

Optional. With `ANALYTICS_S3_BUCKET` set, the engine replicates to any S3-compatible endpoint —
written against OCI Object Storage, which the rest of the platform already uses.

Two jobs, deliberately separate:

- **WAL segments go up for durability**, closing the window where a node dies holding events
  that exist nowhere else. They are deleted from the bucket once compaction has turned them
  into Parquet, because at that point the copy protects nothing.
- **Parquet goes up for queryability** and stays. Any node can then read any site's history,
  which is what makes adding a node a configuration change rather than a migration: the new
  node takes its share of new traffic and serves everything prior from the same bucket, with
  no backfill.

Queries reach through to the bucket for partitions that are not on local disk, caching what
they fetch. So `ANALYTICS_LOCAL_RETENTION` trades disk for latency rather than for data.

**Nothing local is deleted before its replacement is durable elsewhere.** A remote WAL copy is
removed only once the local segment has gone, and the compactor removes a segment only after
its Parquet is written and fsynced. Local Parquet is pruned only after a confirmed upload — an
unreachable bucket costs disk, never data. Both retentions default to keeping everything.

## Tests

```bash
go test -race ./...
```

The `internal/query` package additionally checks every widget against **DuckDB** as an
independent oracle: the same Parquet files, the same question, answered once through the
rollup pipeline and once with plain SQL. Counts must match exactly; unique counts within the
sketch's error bound.

The object storage client has integration tests against a real server, skipped unless
`AE_S3_ENDPOINT` is set:

```bash
docker run -d --rm -p 19000:9000 --name ae-minio \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
AE_S3_ENDPOINT=localhost:19000 AE_S3_BUCKET=aetest \
  AE_S3_ACCESS_KEY=minioadmin AE_S3_SECRET_KEY=minioadmin \
  go test ./internal/objstore/ -run TestS3
```

DuckDB is run as a subprocess and is not a Go dependency — it never enters `go.mod` or the
binary, which is what keeps the `CGO_ENABLED=0` static build intact. Those tests skip if the
CLI is absent; install it with `brew install duckdb` to run them.

## Licence

MIT. See [LICENSE](LICENSE).
