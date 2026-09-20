// Package metrics exposes the engine to Prometheus.
//
// Scraped as its own job with metrics_path /metrics, the same way the WhatsApp bridge is,
// rather than pretending to be a Spring actuator like the Java services.
//
// Every series here answers a question somebody asks during an incident. Anything that only
// looks interesting on a dashboard was left out, because an unused series still costs
// cardinality and still has to be understood by whoever is paging through them at 3am.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry

	// BuildInfo carries the version as a label on a constant 1, the conventional way to make
	// "what is actually running on this node" answerable from the same place as everything
	// else during a rollout.
	BuildInfo *prometheus.GaugeVec

	// EventsReceived counts every event by what happened to it. The `outcome` label is the
	// point: an engine that silently discards is indistinguishable from one that is working,
	// and "accepted" alone cannot tell those apart.
	//
	// Deliberately labelled by outcome and not by site. Site is unbounded — it grows with
	// every customer — and an unbounded label is how a metrics endpoint becomes the outage.
	EventsReceived *prometheus.CounterVec

	// SitesResolved counts resolutions by how the site was determined, or why it was not:
	// ua_tag, path, local_hit, shared_hit, lookup, unknown_host, timeout, lookup_error.
	//
	// The reason this is worth a metric of its own is that most of those outcomes are
	// indistinguishable from the outside — a site with no traffic and a site whose host
	// stopped resolving look identical on a dashboard. Here they do not.
	SitesResolved *prometheus.CounterVec

	// WALSyncSeconds is the group-commit fsync. When ingest latency rises this says
	// immediately whether the disk is the reason, which is the first fork in that
	// investigation.
	WALSyncSeconds prometheus.Histogram

	// WALUnsyncedEvents is the current loss window expressed in events: how many acknowledged
	// events would be lost to a hard kill right now. It is the durability policy of
	// config.WALSyncInterval made observable rather than assumed.
	WALUnsyncedEvents prometheus.Gauge

	// CompactionLagSeconds is the age of the oldest event not yet in Parquet — which is
	// exactly query freshness, the number to look at when somebody says the dashboard is
	// behind. It rises on its own if compaction stalls, so it alerts without a separate
	// liveness check on the compactor.
	CompactionLagSeconds prometheus.Gauge

	// UploadedBytes shows replication actually happening. A node whose uploads have silently
	// stopped looks healthy in every other series while its local disk becomes the only copy.
	UploadedBytes *prometheus.CounterVec

	// HTTPRequests is the outermost signal, and the one that still works when the
	// interesting internals are broken.
	HTTPRequests *prometheus.CounterVec
}

func New(version string) *Metrics {
	reg := prometheus.NewRegistry()

	// Go runtime and process collectors: heap growth and fd exhaustion are the two failures
	// that present as "the engine got slow" with nothing in the engine's own series to show
	// for it.
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		registry: reg,
		BuildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "analytics_build_info",
			Help: "Always 1; the version label is the payload.",
		}, []string{"version"}),
		EventsReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "analytics_events_received_total",
			Help: "Events by outcome: accepted, or the reason they were discarded.",
		}, []string{"outcome"}),
		SitesResolved: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "analytics_sites_resolved_total",
			Help: "Site resolutions by how they were resolved, or why they were not.",
		}, []string{"outcome"}),
		WALSyncSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "analytics_wal_sync_seconds",
			Help: "Duration of a group-commit fsync.",
			// Spans a local NVMe fsync through to a slow network volume. The top bucket
			// being reached at all is itself the finding.
			Buckets: []float64{.0005, .001, .005, .01, .05, .1, .5, 1, 5},
		}),
		WALUnsyncedEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "analytics_wal_unsynced_events",
			Help: "Acknowledged events not yet fsynced: the current loss window.",
		}),
		CompactionLagSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "analytics_compaction_lag_seconds",
			Help: "Age of the oldest event not yet written to Parquet: query freshness.",
		}),
		UploadedBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "analytics_uploaded_bytes_total",
			Help: "Bytes replicated to object storage, by kind (wal, data, rollup).",
		}, []string{"kind"}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "analytics_http_requests_total",
			Help: "HTTP requests by route and status class.",
		}, []string{"route", "status"}),
	}

	reg.MustRegister(m.BuildInfo, m.EventsReceived, m.SitesResolved, m.WALSyncSeconds,
		m.WALUnsyncedEvents, m.CompactionLagSeconds, m.UploadedBytes, m.HTTPRequests)

	m.BuildInfo.WithLabelValues(version).Set(1)

	return m
}

// Handler serves the registry. Nothing else in this package touches net/http, so the server
// package stays the only place that knows about routing.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
