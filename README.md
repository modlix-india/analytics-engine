# analytics-engine

Lightweight analytics engine for small-scale deployments, without the operational overhead of
a full product-analytics stack.

One static Go binary. It ingests events over HTTP, acknowledges them in microseconds by
appending to a write-ahead log, folds closed segments into Parquet, and answers a fixed set of
analytics questions from pre-aggregated rollups. No Kafka, no ClickHouse, no database, no cgo.

**Status: in development.** Ingest, storage, rollups, the fixed widget API and replication
to object storage all work end to end. Site resolution is still the standalone one (the host
is the site) and nothing is wired into Modlix yet. See [PLAN.md](PLAN.md) for the design and
the milestone order.

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
| `ANALYTICS_DEFAULT_TIMEZONE` | `UTC` | used when a query names no zone; must be an IANA id |
| `ANALYTICS_SECURITY_URL` | — | set to enable the Modlix site resolver; the host is the site otherwise |
| `ANALYTICS_RESOLVE_BUDGET` | `250ms` | longest ingest waits for a site lookup before dropping the event |
| `ANALYTICS_RESOLVE_TTL` / `_NEGATIVE_TTL` | `5m` / `1m` | how long a resolution, and a non-resolution, are kept |
| `ANALYTICS_PATH_HOSTS` | — | hosts where a path-prefixed URL may name the site; empty disables the path form |
| `ANALYTICS_REDIS_ADDR` / `_PASSWORD` / `_DB` | — | shares resolutions across nodes and receives cache evictions |
| `ANALYTICS_REDIS_PREFIX` | `cmn` | must match `redis.cache.prefix` on the Java side |
| `ANALYTICS_LOCAL_RETENTION` | `0` (keep) | how long Parquet stays on local disk after upload |
| `ANALYTICS_REMOTE_RETENTION` | `0` (keep forever) | how long it stays in the bucket |

## Endpoints

| | |
|---|---|
| `GET /healthz` | the process is alive. Checks nothing else on purpose: a liveness probe that fails on a dependency restarts a process that would have recovered |
| `GET /readyz` | this node can accept writes |
| `GET /a.js` | the browser beacon, served by the engine that receives its events |
| `POST /i` | ingest a batch — public, write-only, answers 204 either way |
| `POST /q` | run one of the fixed widgets — see below |
| `GET /metrics` | Prometheus |

## The beacon

```html
<script>window.mlx=window.mlx||function(){(window.mlx.q=window.mlx.q||[]).push(arguments)};</script>
<script async src="https://analytics.example/a.js"
        data-autocapture="true" data-pageviews="true"
        data-pageleaves="true" data-consent="required"></script>
```

The engine serves its own client, so a page-generating service needs one tag and no knowledge
of the wire format, and the two cannot drift apart across a deployment. The endpoint is the
tag's own `src`; options are data attributes. The queue stub means an event fired before the
async script arrives is held rather than lost.

```js
mlx('capture', 'checkout_started', {plan: 'pro'})
mlx('page', 'checkoutPage')       // the host application's page identity
mlx('experiment', 'pricing', 'b')
mlx('consent', true)              // false revokes and stops everything
```

With `data-heatmaps="true"` it also records **where** each click landed, as a separate
`$click` event carrying a position and nothing about what was hit. That is deliberately not
the same event as autocapture's labelled `click`: switching heatmaps on must not change what
the funnel or the event list say. It is off by default, because every click on the page
becomes an event where autocapture records only the labelled ones.

A position is stored as a PROPORTION of the viewport width (in ten-thousandths) plus an
absolute y in document pixels, and the width itself travels with it. A pixel abscissa means
the middle of a phone and the left gutter of a desktop, so a map that averages the two is a
picture of nowhere — the reader bands by width instead, and the engine keeps them apart.

It sends a session id and no visitor id: the engine derives the visitor from a daily-rotating
salt, so there is no durable identifier in the page. Page views follow SPA navigation.
Autocapture is **only** elements carrying `data-analytics-label` — deliberately narrower than
capturing every click and naming it from the DOM, because those names change with the markup
and the text of a clicked element can carry someone's own data into an event name.

## Ingesting

```bash
curl -X POST http://localhost:8080/i -H 'Origin: https://shop.example' \
  -d '{"u":"https://shop.example/pricing","r":"https://www.google.com/","b":[{"e":"pageview"}]}'
```

`u` page URL, `r` referrer, `v`/`s` visitor and session if the client tracks them, `x`/`n`
experiment and variant, `b` the batch. Per event: `e` name, `t` client timestamp, `l` label,
`g` page identity, `u` a URL that overrides the batch's for a SPA that navigated, `p` props.
The site comes from `Origin` or `Referer` — there is no site key. Keys are short because the
beacon's size is paid by the visitor on every page view, and this is a public contract: fields
may be added, none may be renamed or repurposed.

**A page view is stored as `$pageview`.** Ingest folds `pageview`, `page_view`, `page-view`
and `pageView` onto it, because every traffic widget filters on that one name and an event
stored under another spelling is not slightly wrong but invisible — the file holds it and the
dashboard answers zero with no error. Every other event name is the caller's own vocabulary
and is stored exactly as sent.

204 is returned whether an event was stored or discarded. A public endpoint that reports
which inputs it rejected is a probe for finding the ones it accepts; `analytics_events_received_total`
carries the outcome for the operator.

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
| `heatmapPages` | the pages that have clicks at all, ranked |
| `heatmap` | where clicks landed on ONE page, as a grid — takes `path`, and optionally `variant` and `viewport` |

`breakdownByProperty` accepts only an allow-listed dimension: `path`, `page`, `label`,
`referrer_host`, `channel`, `utm_source`, `utm_medium`, `utm_campaign`, `device`, `browser`,
`os`, `platform`, `app_version`, `country`, `experiment`, `variant`. That list is what keeps
this a fixed widget API rather than an arbitrary query surface, so `visitor`, `session` and
`props` are not among them.

**There is no filter on a request**, only the event name and one dimension. So an A/B readout
is `breakdownByProperty` over the conversion's own event name with `property: "variant"`, and
a caller running more than one test at a time must make its variant values globally unique —
Modlix sends `<ruleKey>:<page>` — because nothing here can narrow a variant breakdown to one
experiment. The `experiment` dimension answers questions *about* experiments; it cannot scope
another breakdown to one.

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
AE_S3_ENDPOINT=localhost:19000 AE_S3_BUCKET=aetest \
  AE_S3_ACCESS_KEY=modlix AE_S3_SECRET_KEY='Kiran@123' \
  go test ./internal/objstore/ -run TestS3
```

That server is `dbs/minio` in the Modlix monorepo (`docker compose up -d`), which publishes
19000 and creates the `aetest` bucket. Standalone:

```bash
docker run -d --rm -p 19000:9000 --name ae-minio \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z server /data
```

**quay.io, not Docker Hub.** `docker pull minio/minio` now answers "pull access denied ...
repository does not exist" for anonymous pulls; quay serves the identical image. The MinIO
server is AGPL-3.0 and is used here only as a local test double — the client this engine
links against, `minio-go`, is Apache-2.0 and a separate project.

DuckDB is run as a subprocess and is not a Go dependency — it never enters `go.mod` or the
binary, which is what keeps the `CGO_ENABLED=0` static build intact. Those tests skip if the
CLI is absent; install it with `brew install duckdb` to run them.

## Resolving an event to a site

By default the hostname is the site, which is what a standalone deployment wants.

Setting `ANALYTICS_SECURITY_URL` turns on the Modlix resolver, which answers with
`appcode_clientcode` from three signals, in this order:

1. **`ModlixApp/<version> <clientCode>/<appCode>` in the user-agent.** The mobile wrapper
   states its own site, so there is no lookup at all — and it settles platform and app
   version at the same time. Older generated apps predate the tag, so its absence falls
   through rather than discarding.
2. **A path-prefixed page URL** — `/appCode/clientCode/page/...`, which is how every app
   without a custom domain is served. Trusted only while the browser-set `Origin` agrees
   with it: the page URL travels in the payload, so without that check a script could file
   its events under any tenant it named.
3. **The hostname**, through the same security endpoint the gateway uses, so the two cannot
   disagree about which app a host belongs to.

The path form needs `ANALYTICS_PATH_HOSTS` to list the hosts it may be believed on, and the
`Origin` check alone is not enough to make it safe: a page always agrees with its own Origin,
so without that list any site could name any app by putting it in its own URL. Empty — the
default — disables the path form entirely.

An unrecognised host is discarded, and remembered as unrecognised — that is the common case
on a public endpoint, not an error. `analytics_sites_resolved_total` carries which path each
resolution took, because "no traffic" and "stopped resolving" look identical otherwise.

**A lookup never blocks ingest.** Resolution waits at most `ANALYTICS_RESOLVE_BUDGET`; past
that the event is dropped while the lookup continues in the background and fills the cache
for the next one. A slow security service costs events, never throughput. Failures are not
cached either way: security being unreachable says nothing about the host.

Caching is two layers, this process and Redis, and invalidation needs nothing new on the
Modlix side. The platform already publishes `<prefix>-gatewayClientAppCodeType:*` on
`evictionChannel` whenever a client URL or an application changes, and the engine listens for
exactly that.

## Licence

MIT. See [LICENSE](LICENSE).
