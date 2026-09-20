# analytics-engine — design and build plan

A single Go binary that ingests analytics events, stores them as Parquet, and answers a
fixed set of analytics questions. No Kafka, no ClickHouse, no Postgres, no cgo.

MIT, standalone, and deliberately useful outside Modlix — a design constraint, not a licence
footnote. Everything Modlix-specific lives behind one interface (section 4).

---

## 1. The numbers that set the budget

Two figures govern every decision here, and they are 1440x apart.

| | events/day | events/sec | Parquet/day |
|---|---|---|---|
| Today | ~100,000 | ~1.2 | ~1.3 MB |
| Design target | 144,000,000 | **1,667** | **~1.7 GB** |

Parquet/day is measured, not estimated — see section 6.

The target is 100,000 requests/minute = 1,667/sec. With ~5 events batched per request the
event rate is ~8,300/sec.

**One Go process handles this with room to spare.** A length-prefixed WAL append with group
commit sustains tens of thousands of records/sec on ordinary disk; 1,667/sec is a low
single-digit percentage of one node. The bottleneck at that rate is TLS termination and HTTP
parsing, not storage.

So the budget is: **build for one node, design so the second needs no coordination.** Not a
distributed system. Section 3 exists so growth is a config change, not a rewrite.

An earlier draft of this plan assumed ~40 bytes/event after dictionary encoding and zstd. The
measured figure is **12.6 bytes/event**, three times better, which is where the 1.7 GB above
comes from.

---

## 2. Architecture

```
   browser / mobile WebView
        │  POST /i                  (batched, gzip, public, write-only)
        ▼
   nginx   hash $http_origin consistent   ──►  one of N ingest nodes
        │                                      (locality only — see §3)
        ▼
   ┌─────────────────────────────────────────────┐
   │ resolve site: UA tag → URL → Origin  (§4)   │   no site? discard
   │ validate → enrich → append WAL → 204        │   ack in microseconds
   └───────────────┬─────────────────────────────┘
                   │  group fsync every N ms
                   ▼
            WAL segments ──────────────────────────► object storage (continuous, durability)
                   │
                   │  compactor: every N minutes
                   ▼
            Parquet  data/{site}/{date}/{node}-{seq}.parquet
                   │                                  │
                   │ hot: local disk                  │ cold: object storage (queryable)
                   ▼                                  ▼
   ┌─────────────────────────────────────────────┐
   │ query    POST /q  {widget, site, range}     │   private, never browser-facing
   │   rollups (small) ── 11 of 13 widgets       │
   │   raw Parquet scan ── funnel, retention     │
   └─────────────────────────────────────────────┘
```

Three processes' worth of work, one binary, three goroutine groups. The WAL is the queue —
that is what earns the right to defer Kafka rather than pretend we will not need it.

---

## 3. Sharding — hash on the host, and why splitting a site is safe

Shard by origin. Something in front does it; the engine knows nothing about it.

**The Cloudflare Worker is the right place**, since one already fronts every request and will
be rewritten anyway. It can hash and pick an origin per request, and it is also the natural
home for rate limiting and for caching the host-to-site lookup in Workers KV.

nginx can do the same with no code at all, which is what local and single-node deployments
should use:

```nginx
upstream analytics_ingest {
    hash $http_origin consistent;   # minimal reshuffle when a node is added
    server ingest-1:8080;
    server ingest-2:8080;
}
```

`consistent` means adding a fourth node moves about a quarter of origins, not all of them.

**Configure the hash from day one, even with a single node.** An earlier draft said
round-robin was fine until node two, on the grounds that correctness does not depend on the
hash. Correctness does not — but round-robin is not free, and saying "it works either way"
undersold the cost: under round-robin **every node writes files for every site**, so the
small-file problem multiplies by node count and every query merges partial rollups from every
node. Hashing keeps a site's data together, which means fewer and larger files and rollups
that are mostly complete on one node.

Since it is one line of configuration either way, there is no reason to start with the worse
one and no migration to do later.

### No site key in the URL — sharding does not need one

An earlier draft put the host in the path, reasoning that nginx routes before it reads the
body so the shard key had to be in the URL. That was wrong twice over.

`Origin` is a **header**, so nginx can hash on `$http_origin` directly — no path element
required. And more fundamentally, **sharding here is a locality optimisation, not a
correctness requirement**: the next subsection shows a site may be split across nodes with no
loss of correctness. nginx may therefore hash on whatever balances best, while the engine
resolves the true site itself, after reading the body, where it has far better information
than any load balancer can have.

The endpoint is plain `POST /i`.

One consequence worth naming: **all mobile-app traffic shares one `Origin`** and will hash to
a single node. Because splitting and concentrating are both harmless, this is a tuning knob
rather than a defect — if that node saturates, widen the hash key for it. Do not design
around it now.

### One site, several hostnames — and why that is fine

A site is `appcode_clientcode`, but it can have several hostnames: a custom domain, the
default host, a `www` variant. Hashing on host therefore splits one site across nodes, which
looks like it breaks the whole scheme. It does not, for two reasons:

- **Rollups are additive.** Per-node daily rollups are partial; a query sums the counts and
  **merges the HyperLogLog sketches** (section 7). Union of sketches is exact set union, so
  unique-visitor counts survive the split intact. This is the property that makes HLL the
  right choice here, beyond its size.
- **Funnel and retention read shared object storage**, not node-local disk. They scan
  `data/{site}/{date}/` across every node's files, because `{node}` is only a filename
  element, never a directory. A 30-day funnel is unaffected by which node wrote which file.

The split only touches the un-compacted hot tail, which for a 30-day window is noise.

So we get the operational simplicity of hashing on a value nginx already has, without owing
anything to the fact that it is not the true tenancy key.

### Skew

Consistent hashing with virtual nodes handles ordinary imbalance. One enormous site on one
node is the failure mode; the escape hatch is pinning that host to a dedicated upstream.
Note it, do not solve it.

---

## 4. Resolving an event to a site

The site key is `appcode_clientcode`. It is resolved **by the engine, from signals the client
does not choose**, never read from a field the page fills in — a page that could name its own
site could poison another tenant's numbers.

### Why `Origin` alone is not enough

`Origin` identifies the site only when an app has its own hostname. Modlix serves apps two
ways, and the generated mobile apps show both:

| app | start URL | site identified by |
|---|---|---|
| `app_139C…` | `https://antera.dev.modlix.com/customerDashboard` | **host** — `Origin` works |
| `app_4jpG…` | `https://apps.dev.modlix.com/monkbars/SYSTEM/page/` | **path** — one host, many apps |

The second is the default for any app without a custom domain, so it is not an edge case. The
gateway already handles both: a path-prefixed `/clientCode/appCode/` URL is a first-class
resolution path there, not a fallback.

### Resolution order

1. **`ModlixApp/<version> <clientCode>/<appCode>` in the User-Agent.** The Flutter WebView
   appends this (`AppProperties.appUserAgentTag`), so a mobile app states its site
   authoritatively on every request, with **no lookup at all**. It also settles platform:
   `ModlixApp/` present means the mobile app, not a browser that merely looks mobile.
2. **The page URL** carried in the event — host plus path, which covers the shared-host case.
3. **`Origin`, then `Referer`** as the host-only fallback.
4. Otherwise discard.

Steps 2 and 3 need a host-to-site lookup; step 1 does not. Note that older generated apps
predate the UA tag and carry no `appCode` at all, so step 1 must degrade quietly to step 2.

`Referer` is last because `Referrer-Policy: no-referrer` removes it, and it is the only
signal here a page can suppress.

Modlix already answers the lookup, and the gateway already asks it:

```
GET /api/security/clients/internal/getClientNAppCodeNType?scheme=&host=&port=
    → Tuple3<clientCode, appCode, surface>
```

cached by the gateway under `gatewayClientAppCodeType`.

**No appCode means discard the event.** Not an error to the caller — a 204 and a dropped
counter. An unknown host is far more likely to be a stale snippet or a scanner than a bug,
and a public endpoint must not be chatty about what it does not recognise.

### Two caches, not one

The invalidation problem is much smaller once these are separated, and they have different
keys and different triggers:

| cache | key | holds | invalidated when |
|---|---|---|---|
| **A** | hostname | `appcode_clientcode` | a `security_client_url` row changes — by host |
| **B** | appCode | analytics settings (enabled, sampling) | an application definition's properties change — by appCode **and every app inheriting from it** |

Your point 3 is entirely cache B, and **cache B is not keyed by hostname at all**, so it
never needs the hostname list for an appCode. Only cache A is host-keyed, and it only ever
changes when a URL changes — which is your point 2, and nothing else touches it.

That is a real simplification of what looked like one hard invalidation problem.

### Keeping the engine generic

The engine must not know what an appCode is. It declares:

```go
// Signals the client cannot freely choose. Deliberately not a "site" field.
type Signals struct {
    UserAgent string
    PageURL   string // host and path, which covers the shared-host case
    Origin    string
    Referer   string
}

type SiteResolver interface {
    Resolve(ctx context.Context, s Signals) (site string, ok bool)
}
```

The default implementation is the identity function: the host *is* the site. Modlix ships a
resolver that calls security and caches in Redis. The MIT repo stays honest, local
development needs no security service, and the Modlix behaviour is one adapter.

### Two things to verify before building this

- **Internal endpoints are protected by nginx, not by the gateway.** The
  `(.*internal.*)` gateway rule does not do what its name suggests. A Go service calling
  `/internal/` needs a deliberate network path, and that path must not be reachable from
  outside. Confirm before wiring it.
- **Resolution is on the hot ingest path.** Redis hit is fine; a Redis miss plus a security
  round-trip must never block the WAL append. Resolve asynchronously against a
  negative-cached unknown-host set, and drop rather than stall.

---

## 5. Storage: WAL, and shipping it to object storage

Append-only segments. One record:

```
┌────────┬─────────┬──────────────┐
│ len:u32│ crc32c  │ payload      │
└────────┴─────────┴──────────────┘
```

- Rotate at size or age, whichever comes first.
- **Recovery is a forward scan that stops at the first bad CRC.** A torn record at the tail
  is the normal result of a crash, not corruption — truncate and carry on. A bad record
  *before* a good one is real corruption and must be loud.
- Payload is the packed event, not JSON. Decoding happens once, at ingest.

### Why both WAL-to-S3 and Parquet-to-S3

You asked why not simply write the WAL and persist that to a bucket. We should do that — but
it does not replace Parquet, because the two solve different problems:

| | purpose | shape | answers "top pages last 30 days"? |
|---|---|---|---|
| **WAL → S3**, continuous | **durability** — survive losing the node between compactions | row-oriented, append-only | no, not without reading everything |
| **Parquet → S3**, on compaction | **queryability** — column pruning, row-group statistics, zstd | columnar | yes, reading two columns |

WAL shipping is cheap, sequential and small, and it closes the window where a node dies with
uncompacted events on local disk. This is the Litestream pattern, which is already in use
here for SQLite against the same S3-compatible endpoint — so it is a known quantity
operationally, not a new idea to prove.

Compacted WAL segments are deleted from the bucket once their Parquet is durable.

### Durability policy — measured, not assumed

**Default: group commit at 100 ms.** A hard kill loses at most that much. For analytics that
is the right trade, and it is stated on the endpoint because it is invisible until it matters.

Benchmarking the implementation corrected a mistake in the first draft of this section, which
said appends should be acknowledged after their fsync. Measured on an M-series laptop with a
250-byte payload:

| | events/sec |
|---|---|
| `Append` (waits for the fsync), 1 caller | 10 |
| `Append`, 8 callers | 80 |
| `Append`, 256 callers | 2,555 |
| **`Enqueue`** (returns once buffered) | **6,795,501** |

The `Append` figures are exactly `callers × 10`, because a caller can complete one append per
100 ms commit window. That is group commit working correctly, not a defect — but it makes the
per-connection ceiling `1/SyncInterval`, and it prices ingest latency at a full sync interval.

The correction is that **waiting buys no durability at all**. The syncer fsyncs on its
interval either way, so the loss window is identical; waiting only tells the *caller* that the
write landed. That is worth paying for when the caller would act on the answer. Nothing
retries a pageview, and the browser discards the response — so ingest uses `Enqueue`, and the
data is exactly as safe.

`Append` stays for callers that genuinely act on the result. Both are tested, including
interleaved under the race detector.

At 6.8M/sec the WAL is roughly 4,000x the 1,667/sec target and will not be the bottleneck.
HTTP parsing and JSON decoding will be — measured at milestone 3, below.

### Measured end to end (milestone 3)

Whole path: HTTP, gzip, JSON decode, UA parse, referrer classification, enrichment, packed
encode, WAL. Laptop, with the load generator competing for the same CPU.

| offered | achieved | events/sec | failed | p50 | p99 |
|---|---|---|---|---|---|
| 1,667 req/s (**the target**) | 1,665 | 4,996 | 0 | 132µs | 275µs |
| 5,000 req/s | 4,997 | 14,991 | 0 | 48µs | 147µs |
| 10,000 req/s | 9,991 | 29,974 | 0 | 47µs | 219µs |
| 20,000 req/s | 19,807 | **59,422** | 0 | 50µs | 1.4ms |

The engine met every offered rate, so these are the generator's limits rather than the
engine's — 20,000 req/s is **12x the design target** and the ceiling was not found. After 1.14M
events: 10 goroutines, 5.4 MB heap, 34 MB RSS, no growth.

Two numbers worth carrying forward:

- **fsync averages 5.1 ms** on this disk (1.29s over 253 syncs). At a 100 ms commit interval
  that is a 5% duty cycle, so there is room — but network storage is slower, and this is the
  figure to re-measure on a real volume rather than assume.
- **The WAL holds ~137 bytes/event** (156 MB for 1.14M events), uncompressed and row-oriented.
  That is below the ~250 bytes/event this plan assumed on the wire, and it is the number
  Parquet has to beat with dictionary encoding and zstd.

---

## 6. Storage: Parquet

Layout: `data/{site}/{utc-date}/{node}-{seq}.parquet`, **sorted by `ts_server` within a file**.

`{node}` is a filename element and never a directory — that is what lets nodes write into
shared storage with no coordination, and what makes section 3's split harmless.

### The date directory is a pruning hint, never a boundary

This deserves stating outright, because partitioning by a UTC date looks like it bakes a
timezone into the data. It does not, and the reason is worth being precise about:

- **No query result depends on where a partition edge falls.** A directory is chosen, then
  every row is filtered on `ts_server` against the exact requested interval. A query for an
  IST day reads two UTC date directories and returns exactly the right rows.
- Because files are **sorted by `ts_server`**, Parquet's row-group statistics prune inside a
  file as well. A daily file gives nearly the pruning of hourly files without the small-file
  cost, so there is no reason to partition finer than a date.

**Rolling up by date is the real hazard, and section 7 deals with it.** A partition keeps the
timestamps, so it can always be re-sliced. A rollup destroys them, so it cannot.

Core schema, all strings dictionary-encoded:

| column | type | note |
|---|---|---|
| `ts_server` | int64 | unix millis UTC — **aggregate on this** |
| `ts_client` | int64 | as reported; kept for diagnosis, never trusted |
| `site` | string | redundant with the path; keep it so a file is self-describing |
| `event` | string | `$pageview` or a custom name |
| `visitor` | fixed[16] | anonymous, daily-rotating salt |
| `session` | fixed[16] | |
| `path` | string | normalised; drives top pages |
| `referrer_host` | string | dictionary-encoded, low cardinality; drives the widget |
| `referrer_url` | string | full referrer when the browser supplies one — see below |
| `channel` | string | direct/organic/social/paid/referral/internal |
| `utm_source`, `utm_medium`, `utm_campaign`, `utm_term`, `utm_content` | string | parsed from **our own** page URL, never from the referrer |
| `experiment`, `variant` | string | A/B assignment as the visitor saw it; `variant` is a free label, so N-way, not two |
| `device`, `browser`, `os` | string | derived from UA at ingest |
| `platform` | string | `web` or `mobile_app` — from the `ModlixApp/` UA tag, not guessed from the OS |
| `app_version` | string | the mobile app's version, from the same tag; empty on web |
| `country` | string | 2 chars |
| `page` | string | host application's page identity |
| `label` | string | the click target's stable label |
| `props` | string | JSON tail for anything unmodelled |

**Country is free.** Traffic is behind Cloudflare, so `CF-IPCountry` gives a country per
request — no MaxMind database, no refresh job, no separate EULA. Fall back to empty rather
than shipping a geo database.

### Referrer and UTM are different things, from different places

They are easy to conflate and the distinction decides two columns:

- **UTM parameters live on our own landing URL's query string**, put there by the campaign
  link: `site.com/pricing?utm_source=google&utm_medium=cpc`. They are parsed from the page
  URL we already receive. The referrer has nothing to do with them.
- **The referrer is where the visitor came from** — `google.com`, `news.ycombinator.com`.

We keep both a dictionary-encoded `referrer_host`, which is what the widget groups by and
what stays cheap, and the full `referrer_url` for drill-down.

Two cautions on the full URL. First, browsers now default to
`strict-origin-when-cross-origin`, so a cross-origin referrer usually **arrives already
truncated to the origin** — the full path is frequently not ours to record. Second, when a
full URL does arrive it can carry search queries, tokens or internal paths, which is a
privacy surface rather than a feature. So `referrer_url` is stored when present and must be
switchable off per site.

`props` as a JSON string rather than a Parquet map keeps the writer simple and the schema
stable. Nothing in the 13 widgets filters on it.

**Small files are the enemy.** Five-minute compaction at low volume yields 288 tiny files per
site per day. Compact hourly files into daily ones on a background pass, deleting sources
only once the merged file is durable.

### Measured storage (milestone 4)

200,000 events with **realistic cardinality** — one distinct visitor per four events, an
800-path long tail, the usual spread of browsers, channels and countries:

| | |
|---|---|
| Parquet | 2.4 MB |
| **per event** | **12.6 bytes** |
| at 144M events/day | **1.7 GB/day** |

Cardinality is the whole measurement. A first pass through the load generator reported 2.8
bytes/event, which was an artefact: that generator cycles 64 visitors and 5 paths, and
`visitor` and `session` are the two high-cardinality columns that dominate a real file. Any
storage figure taken from synthetic traffic with few distinct visitors flatters itself by
several times, so the test that produces the number above generates the cardinality
deliberately and fails if the result drifts above 80 bytes/event.

The WAL holds the same events at ~137 bytes each, so compaction is a **10.9x** reduction.

---

## 7. Query layer

### Rollups are hourly, not daily — this is the timezone fix

For each `(site, utc-hour, dimension, key)` keep `events`, `sessions`, and a **HyperLogLog
sketch** for unique visitors. ~0.8% error, a couple of KB, and — the property that now matters
three times over — **sketches merge**: across hours, across days, and across nodes.

**Daily rollups would be a genuine correctness bug, not an inconvenience.** A rollup discards
the timestamps it summarises, so a bucket keyed to a UTC day can never be re-sliced into any
other timezone's day. An IST day runs 18:30 UTC to 18:30 UTC; a UTC-keyed daily total simply
does not contain the information needed to answer it. The customer sees numbers that
disagree with their own, and nothing in the system can explain why.

Hourly buckets fix it for every whole-hour offset, which is most of the world, by summing the
24 buckets that make up that timezone's day.

### Half-hour offsets, which include India

IST is +5:30, Nepal +5:45, parts of Australia +9:30. For these an hourly bucket straddles the
day boundary and cannot be split. The fix is bounded and cheap:

**Sum whole hours from the rollup for the interior of the range, and recompute only the two
boundary hours from raw Parquet.** Those two hours are one row-group range in a file already
sorted by `ts_server`, so the correction costs a fraction of a file read, and the answer is
exact for any offset — including ones that are not multiples of 15 minutes, should any appear.

Every site therefore carries a **reporting timezone** as configuration, defaulting to UTC.
Changing it changes no stored data, only which buckets a query sums — which is the whole point
of keeping rollups at a granularity finer than the reporting unit.

Eleven of the thirteen widgets read only rollups plus at most two boundary hours. Their cost
is a function of the range, not of traffic, so they stay fast permanently.

Be honest in the UI: event counts exact, uniques approximate.

### Sketch precision is the lever on this tier's size (milestone 5)

A first implementation used p=14 everywhere and produced a rollup tier **five times larger
than the raw data it summarised**, which would have made the whole tier pointless. Precision is
now chosen per row type:

| row type | precision | error | why |
|---|---|---|---|
| headline (`DimNone`) | p=14 | ~0.8% | the dashboard number somebody will compare against another tool |
| breakdown (per path, referrer, country) | p=11 | ~2.3% | an eighth of the dense size, and nobody reads the third digit of uniques for one blog post |

Sessions sketches are kept only on headline rows; a second sketch on every breakdown row
doubled the tier for a column almost nobody asks for.

### Rollups are a scale optimisation, not a universal win

Measured on one site's day, varying only event count:

| events/day | raw | rollup | rollup as % of raw |
|---|---|---|---|
| 200,000 | 2.05 MB | 3.97 MB | 193% |
| 1,000,000 | 11.11 MB | 10.52 MB | 95% |
| 4,000,000 | 45.08 MB | 17.67 MB | **39%** |

Raw grows with event count; a rollup grows with distinct keys times hours, so it plateaus.
Break-even is around a million events a day for one site.

Below that the tier costs more storage than the data it summarises, and it is kept anyway for
two reasons: the absolute cost at low volume is a few MB a day, and its real purpose is not
saving bytes but answering **unique visitor** questions, which raw data cannot do without a
distinct count over every visitor id in the range.

### Funnel and retention are the only real work

- **funnel** — read `(visitor, event, ts_server)` for the window, sort by visitor then time,
  scan each visitor's sequence for the ordered steps inside a conversion window. Memory is
  bounded by hash-partitioning on `visitor`.
- **retention** — cohort each visitor by first-seen bucket, test presence in later buckets.
  Same sort-merge shape, different accumulator.

Both touch two or three columns, so column pruning reads a small fraction of each file, and
row-group statistics give time-range pushdown on top.

### No cgo — and DuckDB earns its keep in tests

`go-duckdb` is MIT but needs `CGO_ENABLED=1`, forfeiting the static-binary-on-distroless
property `whatsapp-bridge`'s Dockerfile is explicit about wanting. With thirteen fixed query
shapes we do not need a general SQL engine, so we do not take a libc dependency to get one.

But DuckDB makes an excellent **test-time oracle**: point it at the same Parquet, express
each widget as SQL, assert our Go aggregation agrees. Strong correctness evidence from an
independent implementation, zero runtime cost, no cgo in the shipped binary.

### Built at milestone 6, and it earns its place

DuckDB is invoked as a **subprocess**, not as a Go module. It therefore never enters `go.mod`,
never enters the binary, and the tests skip where the CLI is absent — the no-cgo constraint is
kept by construction rather than by discipline.

Each widget is answered twice: once by the engine through rollups, hourly buckets and boundary
correction, and once by DuckDB with plain SQL over the raw Parquet. They must agree — counts
exactly, uniques within the sketch's error bound.

**The first run disagreed, and the cause is worth recording.** Our Parquet timestamps carry
`isAdjustedToUTC`, so DuckDB reads them as instants and renders them in the *session* timezone
— which on a machine in India is +5:30. A naive `TIMESTAMP` literal then selected a window
shifted by five and a half hours: 61% of the expected events, against 8.5/14 hours of overlap
= 0.607. The oracle was right and the SQL was wrong. Every oracle query now pins
`SET TimeZone='UTC'` rather than compensating for the shift.

**The oracle was then mutation-tested**, because a test that cannot fail is decoration:

| deliberate bug | caught? |
|---|---|
| boundary-hour correction removed | yes — 446 events missing per IST day, exactly the half-hour at each edge |
| visitor sketches never merged | yes — 100% off |

---

## 8. What we are porting from PostHog, and what we are not

### Porting — the thirteen widgets that exist today

| PostHog concept | our widgets |
|---|---|
| Trends | `pageviewsOverTime`, `eventTimeline`, `topEvents` |
| Breakdowns | `channelBreakdown`, `deviceBreakdown`, `browserBreakdown`, `osBreakdown`, `geoBreakdown`, `breakdownByProperty` |
| Top-N tables | `topPages`, `topReferrers` |
| Funnels | `funnel` |
| Retention | `retention` |

Plus the ingest-side behaviour: custom events (`TrackAnalyticsEvent`), autocapture restricted
to elements carrying a stable `analyticsLabel`, consent gating, and super properties.

### One thing we gain over PostHog

The `ModlixApp/` User-Agent tag says reliably whether a session is the Flutter WebView or an
ordinary browser, and which app version it is running. PostHog could only infer this from the
OS string, so "how many of my users are on the mobile app, and how many are stuck on an old
version?" was never properly answerable. `platform` and `app_version` are ordinary rollup
dimensions, so a `platformBreakdown` widget costs one row in the widget table and no new
machinery. Worth adding once the first three widgets prove the shape.

### Also porting: paths, stickiness, lifecycle, correlation

| analysis | what it answers | cost |
|---|---|---|
| **Paths** | the routes visitors take through pages and events, and where they leave | moderate — sequence aggregation |
| **Stickiness** | of the people who did X, on how many distinct days did they do it | **cheap** — the retention sort-merge with a different accumulator |
| **Lifecycle** | each period's actives split into new / returning / resurrecting / dormant | **cheap** — the same machinery again |
| **Correlation** | which signals separate the visitors who converted from those who dropped | see below |

Stickiness and lifecycle ship in the retention milestone; all three are one pass over
per-visitor active days. Paths is its own milestone: aggregate per-visitor ordered event
sequences into (from, to) edge counts truncated to N steps, which the UI draws as a flow. Cap
the step count and the branching factor, or the output grows faster than anyone can read it.

### Correlation, concretely

It is the one analysis here that can be confidently wrong, so the method matters.

The funnel already partitions visitors into **converted (S)** and **dropped (F)**. For each
candidate signal — an event performed, or a property value held — build a 2×2 table:

|  | has signal | lacks signal |
|---|---|---|
| converted | a | b |
| dropped | c | d |

Then:

- **Odds ratio** `(a·d)/(b·c)`, with the Haldane-Anscombe correction (add 0.5 to each cell) so
  a single empty cell does not produce infinity.
- **Significance** by chi-squared, falling back to Fisher's exact test when any expected cell
  is below 5. `gonum` is BSD-3-Clause and carries the distributions.
- **A minimum support threshold**, so a signal held by four visitors cannot top the list.
- **Benjamini-Hochberg correction across all candidates.** This is not optional and it is
  where naive implementations go wrong: testing three hundred properties at p < 0.05 produces
  roughly fifteen false positives by construction. Without the correction the feature reliably
  invents relationships, and it does so most confidently on the sites with the most properties.

Rank the survivors by odds ratio.

**The UI must say "correlated with", never "caused".** These are hypotheses to investigate.
A visitor who reached step four also visited the pricing page — that is the funnel restating
itself, not a finding. Excluding signals that are themselves funnel steps removes the most
common such artefact, and is worth doing in the engine rather than leaving to the reader.

### Not porting at all

| | why |
|---|---|
| **Session replay** | withdrawn deliberately; recording is hard-disabled in both snippet generators and the viewer components are parked |
| **HogQL / free-form SQL** | replaced by the fixed widget API. That is the point: with no client-supplied query there is nothing to sanitise, and `HogQLTenantRewriter` is deleted rather than reimplemented. More widgets can be added whenever they are wanted |
| **Surveys, data warehouse** | never used here |
| **Cohorts** | a named, reusable set of visitors ("signed up in January and used X") for filtering other analyses. Not requested; it is a saved predicate plus a materialised membership set, and it can be added later without disturbing anything here |
| **LLM observability** | tracing prompts, completions, token counts and cost for AI features. Genuinely relevant given `nocode-ai`, but it is a different data shape with a different retention profile — a sibling concern, not a widget |

### Wanted, and where each belongs

- **Error tracking — yes, and it fits this engine.** A JS exception is an event with extra
  columns (`message`, `type`, `stack`, `fingerprint`) plus grouping: hash the stack into a
  fingerprint, aggregate occurrences and distinct visitors per fingerprint. That is a rollup
  with a different key, reusing ingest, WAL, Parquet and rollups unchanged. Schedule it after
  the thirteen widgets.
- **A/B testing — split it in two.** *Measurement* belongs here and costs two columns,
  `experiment` and `variant`, which every widget can then break down by. Because `variant` is
  a free-form label, three- and four-way tests work exactly like two-way ones; nothing in the
  design assumes a control and one challenger. *Assignment* — deciding which variant a
  visitor gets — does not belong here.
- **Feature flags — a sibling service, not this one.** Assignment is a read-heavy,
  low-latency config lookup on the request path, with rules, percentage rollouts and
  targeting. Its failure mode is "the page renders wrong", against this engine's "a number is
  briefly stale". Putting them in one process couples a durability-oriented writer to a
  latency-oriented reader, and the flag service would inherit the analytics engine's restart
  profile. It should emit an `$experiment_assigned` event **into** this engine, which is what
  makes the `variant` column trustworthy.

### The one open item: heatmaps

I added a heatmaps toggle to the settings pane on the strength of PostHog providing it. We
would have to build coordinate capture and a rendering surface ourselves — a separate piece
of work, not a widget. **Recommend removing the toggle** rather than shipping a switch that
does nothing, and treating heatmaps as a later project if it is wanted.

### `AnalyticsQuery` changes meaning

Today it runs arbitrary HogQL and binds the result into the page store. It becomes "run a
named widget and bind the result" — same usefulness for custom charts, no query surface.

---

## 9. HTTP API

| method | path | auth | notes |
|---|---|---|---|
| `POST` | `/i` | none | public, batched, gzip, always 204 |
| `POST` | `/q` | authorizer (below) | `{widget, site, from, to, params}` |
| `GET` | `/healthz` `/readyz` | none | |
| `GET` | `/metrics` | internal | Prometheus, as the bridge already does |

### No write key on ingest

An earlier draft proposed a per-site write key — a public token in the snippet, like
PostHog's `phc_…`, identifying which site an event belongs to. **Dropping it**, because once
the site is resolved from the User-Agent tag, the page URL and `Origin` (section 4), a key
adds nothing: it would be a token the page carries in plain sight, readable by anyone viewing
source, identifying what we already determine from signals the page cannot choose.

Its only remaining use is revocation, and disabling a site's `analytics.enabled` in cache B
already does that, faster and without reissuing a snippet.

**Ingest honesty:** analytics ingest is spoofable in every product, this one included.
Resolving from `Origin` stops casual cross-site pollution from a browser, because browsers
set `Origin` themselves; it stops nothing at all from curl. A write key would not have
changed that. Do not describe either as a security boundary — rate limiting at the
Cloudflare Worker is the real control.

### Query authorisation

Every query must prove the caller may read that site. The check is the one
`AnalyticsService` already performs, and it should not be re-invented:

- `hasWriteAccess(appCode, clientCode)` on the **target** app — note it is write access, not
  a named authority, so a read-only role cannot open a dashboard. Worth knowing before
  somebody reports it as a bug.
- `doesClientManageClientCode(caller, clientCode)` for the tenant hierarchy.
- `analytics.enabled` on that app.

The engine stays generic the same way it does for resolution:

```go
type QueryAuthorizer interface {
    Allow(ctx context.Context, credential, site string) bool
}
```

`credential` is the bearer token or cookie as received. The default implementation is a
shared secret; the Modlix implementation calls security and caches the outcome briefly.

**Where it runs is a deployment choice, not a design one.** Either the browser calls `/q`
directly and the engine's authorizer validates the token, or `ui` authorises and proxies —
the same interface, satisfied in two places. **Recommend the proxy initially**: the logic
exists, is tested, and keeping Modlix's security semantics out of an MIT repo is worth one
network hop. Revisit if that hop ever shows up in latency.

Whichever is chosen, the engine must never be reachable from a browser without passing the
authorizer. Say so in the README, because an MIT repo gets deployed by people who never read
this plan.

Hostile-input safety is not optional on a public endpoint: cap batch size, field lengths and
per-site daily cardinality, and route unknown fields into `props` rather than erroring.

---

## 10. Modlix integration

Contained, because the query surface was already a closed set of thirteen:

- **`AnalyticsService` / `AnalyticsController`** (~380 lines) — retarget from PostHog to `/q`.
- **`HogQLTenantRewriter`** (79 lines) — **deleted**, per section 8.
- **`webAnalyticsTemplates.ts` / `productAnalyticsTemplates.ts`** (320 lines) — SQL goes, the
  widget list, display names and render hints stay.
- **The snippet** in `IndexHTMLService.java` and `ssr/render/htmlRenderer.ts` — our own SDK.
  It sends the page URL in the payload and nothing identifying beyond it; the engine resolves
  the site per section 4.
- **The Cloudflare Workers** currently fronting the PostHog instances are replaced by workers
  pointing at this engine. Worth reusing: the Worker is a natural place for the write key and
  for rate limiting, before anything reaches an origin.
- **Unchanged:** the consent page, both consent UI functions, the settings pane, the
  `analyticsLabel` property, the agent guidance, and every widget component — all written
  backend-agnostic, none of them need to know.

### Retiring PostHog

Only once the engine is serving real numbers, and in this order — the last step is
irreversible.

**Code**

- `AnalyticsService`, `AnalyticsController` — retarget, and delete the replay endpoints.
- `HogQLTenantRewriter` — delete outright.
- `SessionReplayList`, `SessionReplayPlayer` — delete both components; the pipeline was
  parked earlier and nothing has consumed them since.
- `webAnalyticsTemplates.ts`, `productAnalyticsTemplates.ts` — HogQL out, widget list stays.
- `IndexHTMLService.java`, `ssr/render/htmlRenderer.ts` — the PostHog snippet and the
  `POSTHOG_STUB`, replaced by our SDK.
- `analyticsConsent.ts` — `applyConsentToPostHog` becomes `applyConsentToEngine`; the rest of
  the consent machinery is untouched.

**Configuration** — `ui.analytics.posthog.*` in `configfiles/application-default.yml` and in
all three `oci-config/application-oci{dev,stage,prod}.yml`. Note `personalApiKey` was never
set in any of them, which is why the query side never worked anywhere.

**Infrastructure**

- `oci-config/scripts/{dev,stage,prod}-analytics/` — three compose stacks, nine containers
  each (web, worker, plugins, migrate, db, redis, kafka, zookeeper, clickhouse, caddy).
- The three analytics VMs, once the stacks are gone.
- The Cloudflare Workers fronting them, replaced by workers pointing at this engine.
- DNS for `analytics-{dev,stage}.modlix.com` and `analytics.modlix.com`.
- `dbs/posthog/` locally, plus `dbs/nginx/nginx/myconf.d/local.analytics.modlix.com.conf`.

**Documentation** — `modlix-apps/docs-content/pages/platform/services/analytics.md`.

**The existing PostHog data is not being migrated.** Confirmed: nobody reads it, in any
environment — which follows from the query side never having been configured anywhere. There
is no export step and no archive. All three environments retire together.

---

## 11. Milestones

Each ends in something runnable.

1. **Skeleton** — module, config, `/healthz`, Prometheus, Dockerfile in the bridge's shape
   (`CGO_ENABLED=0`, distroless, digest-pinned base, version via `-ldflags`).
2. **WAL** — append, rotate, group commit, recovery. Fuzz recovery against truncation and
   bit-flips before anything is built on top.
3. **Ingest** — `POST /i`, identity resolver, UA parsing including the `ModlixApp/` tag,
   referrer enrichment, `CF-IPCountry`. Load-test to 1,667/sec **here**, not at the end.
4. **Compactor** — WAL to Parquet, hourly to daily. Verify on arm64 and amd64, because
   parquet-go ships hand-written assembly per architecture.
5. **Rollups + HLL**, and the first three widgets end to end.
6. **DuckDB oracle** in tests. Do not defer past milestone 5. *(done — and mutation-tested)*
7. **The remaining rollup widgets.** *(done — eleven widgets, each checked against DuckDB)*

   One test failure worth recording, because it is the characteristic hazard of table-driven
   tests over generated data: four breakdown tests — device, browser, os, platform — passed
   while testing nothing, because the fixture generator never populated those columns and an
   empty result was compared against an empty result. `compare` now fails outright when the
   oracle returns no rows, so a vacuous pass is impossible rather than merely unlikely.
8. **Funnel, retention, stickiness, lifecycle** — one milestone, one per-visitor sort-merge,
   four accumulators. *(done)*

   Two bugs found here, both by checks beyond the unit tests, and both worth recording:

   **Time was only a partial order.** Ingest gave every event in a batch the same server
   timestamp, so a funnel of view → add → buy reported in one batch had three identical
   timestamps and whether it counted as a conversion came down to which row a file happened to
   store first. The DuckDB oracle found it: the engine counted 16 more conversions than SQL,
   because Go ordered by position and SQL by time. Ingest now spends one millisecond per event
   in a batch, making the order total, and `funnelDepth` compares timestamps strictly.

   **Period iteration stepped by 24 hours instead of by calendar day.** A "last 24 hours"
   range beginning at 16:00 therefore yielded a single period and silently dropped every event
   in the second calendar day. Every unit test used midnight-aligned ranges and missed it; the
   first end-to-end query did not. It now advances to the next calendar boundary, which also
   makes a week a week and a DST day 23 or 25 hours.
9. **Object storage** — WAL shipping, Parquet tier, retention by dropping date directories.
   *(done)*

   Proven end to end: ingest, compact, replicate, then delete every local Parquet file and
   watch the same query return the identical answer from the bucket alone. WAL segments are
   uploaded and then dropped once compacted, so the bucket holds no WAL at rest.

   One gap found only by running it: `UseSSL` was hardcoded true, so a plaintext local
   endpoint failed every upload. TLS is now derived from the endpoint's scheme — an `http://`
   prefix means plaintext, a bare hostname means TLS, since a bare hostname in a deployment is
   a real provider and defaulting that to plaintext would ship credentials in the clear.

   Worth noting what worked during that failure: the engine logged the errors, kept serving
   queries from local data, and deleted nothing. The degradation path was exercised by
   accident and behaved as designed.

   **Then a local MinIO went into `dbs/minio`, and putting the engine in front of it found
   two more things no unit test could.**

   *A page view sent as `pageview` was invisible.* Ingest stored the name verbatim, every
   traffic widget defaults to `$pageview`, and every fixture in this repo happened to spell it
   the canonical way — so each layer was right and the chain was not. The dashboard answered
   zero rows, with no error, for data sitting correctly in the Parquet file. The name now has
   one definition (`event.NamePageview`), ingest folds the obvious spellings onto it, custom
   names are still stored verbatim, and `internal/e2e` drives a real HTTP request through the
   WAL, the compactor and the query handler — the only kind of test that does not get to
   choose the spelling, which is exactly why it catches this class.

   *A port conflict hung the process instead of failing it.* `run()` waits on the compactor
   and replicator in deferred receives, and the signal context's cancel is deferred first and
   therefore runs last — so any early return blocked forever on loops nobody had told to stop.
   A second engine on a taken port stayed alive with nothing listening, `/healthz` answered by
   the other process, and the bind error surfacing only when someone killed it by hand. Each
   wait now cancels before it waits. `cmd/engine` has the regression test; it failed for 180s
   before the fix and passes in 0.01s after.

   Both are the same shape as the `UseSSL` finding: the bug was in the wiring between correct
   parts, and only running the assembled thing showed it.

   Milestones 1-5 are complete. The half-hour timezone correction is implemented and tested
   against the case that motivated it: two events in the SAME UTC hour falling on opposite
   sides of an IST day boundary, which no rollup can separate and which the raw boundary scan
   resolves exactly. `rawHoursScanned` is reported on every response, and is 0 for whole-hour
   zones and 2 for half-hour ones however long the range.
10. **Modlix resolver** — security call, the two Redis caches, invalidation hooks.
11. **Modlix integration** — SDK, `AnalyticsService` retarget, delete the rewriter, replace
    the Workers.
12. **Retire PostHog** — the inventory in section 10, all three environments together.
13. **Paths** — sequence to edge counts, with step and branching caps.
14. **Correlation** — contingency tables, chi-squared with a Fisher fallback, Benjamini-Hochberg.
    Last of the analyses because it depends on the funnel and is the one that can be
    confidently wrong.
15. **Error tracking** — fingerprinting and its rollup, reusing everything above.
16. **Second node** — hash in the Worker, proven with no engine change.

Milestones 1-5 are the product. 6 is what makes the rest trustworthy. Nothing before 11
changes anything a user sees, so the PostHog stacks keep running untouched until then.

---

## 12. Risks, and what I have not verified

- ~~parquet-go write throughput unmeasured~~ — **resolved at milestone 4.** 200k rows write
  and read back in well under a second; compaction has never been near the limiting factor.
- ~~Go assembly per architecture~~ — **resolved at milestone 4.** The full suite passes on
  linux/amd64 under emulation as well as natively on arm64. Keep it in CI: the risk returns
  the moment that check stops running.
- **HLL approximation is user-visible.** ~0.8% error on a unique-visitor count shown to a
  paying customer. Exact counts mean a raw scan and a different cost model — decide before
  the UI promises a number.
- **Compaction and query race.** A reader must never see a half-written Parquet file. Temp
  name plus atomic rename; on object storage rely on single-object atomicity, never on
  directory semantics.
- **Late and out-of-order events.** A mobile client can deliver hours late, after the target
  day compacted. Reject beyond a threshold, or write a late-arrival file the rollup pass
  re-reads. Silently dropping is the wrong default.
- **Internal-endpoint exposure.** See section 4 — the gateway rule that looks like it
  protects `/internal/` does not.
- **Hourly rollups are 24x the rows of daily ones.** They are sparse — only non-empty
  `(hour, key)` pairs are stored — so the real factor is well under 24 for high-cardinality
  dimensions like `path`. But at low volume a rollup can approach the size of the raw data it
  summarises, which defeats the purpose. Measure at milestone 5; if it bites, keep hourly only
  for the low-cardinality dimensions and derive the rest from raw with row-group pruning.
- **DST makes a timezone's day 23 or 25 hours long.** Summing "24 hourly buckets" is wrong
  twice a year for any zone that observes it. Use a real `time.Location` and the actual
  interval, never an integer offset. IST does not observe DST, which is exactly why this would
  survive local testing and fail for a customer elsewhere.
- **Bus factor.** This is a database, in-house. The DuckDB oracle, a fuzzed WAL and a written
  format spec are what make it maintainable by somebody who did not write it.

---

## 13. What needs a decision

1. **Unique visitors — approximate or exact?** Decides whether rollups alone serve the web
   widgets. Recommend approximate, labelled honestly.
2. **Late-arrival policy** — reject beyond N hours, or a late file and re-rollup. Recommend
   rejecting beyond 24h and saying so in the response.
3. **Heatmaps** — remove the settings toggle, or commit to building capture? Recommend
   removing it for now.
4. **`referrer_url` on or off by default?** Browsers usually truncate it to the origin
   anyway, and when they do not it can carry search terms. Recommend off by default,
   switchable on per site.
5. **Query authorisation in the engine, or `ui` proxying?** Recommend proxying first, per
   section 9.
6. **Where does a site's reporting timezone come from?** It has to exist for section 7 to
   mean anything. Recommend a new `analytics.timezone` on the application definition,
   defaulting to UTC rather than to the server's zone — a default that silently follows the
   host is the kind that produces numbers nobody can reproduce.

Settled in discussion, recorded here so they are not reopened: paths, stickiness, lifecycle
and correlation are all in scope; feature flags are a sibling service; the existing PostHog
data is discarded and all three environments retire together.
