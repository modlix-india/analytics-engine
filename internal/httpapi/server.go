// Package httpapi serves the engine's endpoints.
//
// Four routes, and the split between two of them matters: /healthz answers "is this process
// alive", /readyz answers "should traffic be sent here". Conflating them is how a node that
// cannot write to its own disk keeps receiving events and acknowledging them.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/modlix-india/analytics-engine/internal/metrics"
)

// ReadyFunc reports whether this node can currently accept writes. It is consulted per
// request rather than cached, because the condition it reports on — a full or unwritable
// volume — appears between requests, not at boot.
type ReadyFunc func(context.Context) error

type Server struct {
	srv     *http.Server
	log     *slog.Logger
	metrics *metrics.Metrics
	ready   ReadyFunc
}

type Options struct {
	Addr    string
	Log     *slog.Logger
	Metrics *metrics.Metrics

	// Ready may be nil, in which case the node always reports ready.
	Ready ReadyFunc

	// Ingest handles POST /i. Nil leaves the route unregistered, so a query-only deployment
	// does not expose a public write endpoint it never intended to serve.
	Ingest http.HandlerFunc

	// Query handles POST /q. Nil leaves it unregistered, for an ingest-only node.
	Query http.Handler
}

func New(o Options) *Server {
	s := &Server{log: o.Log, metrics: o.Metrics, ready: o.Ready}

	mux := http.NewServeMux()
	mux.Handle("GET /healthz", s.instrument("healthz", http.HandlerFunc(s.handleHealthz)))
	mux.Handle("GET /readyz", s.instrument("readyz", http.HandlerFunc(s.handleReadyz)))
	mux.Handle("GET /metrics", o.Metrics.Handler())

	if o.Ingest != nil {
		mux.Handle("POST /i", s.instrument("ingest", o.Ingest))
	}
	if o.Query != nil {
		mux.Handle("POST /q", s.instrument("query", o.Query))
	}

	s.srv = &http.Server{
		Addr:    o.Addr,
		Handler: mux,

		// A public ingest endpoint is reachable by anything, including a client that opens a
		// connection and then stalls. Without these an idle or slow peer holds a goroutine and
		// a file descriptor indefinitely, and fd exhaustion arrives looking like a mystery.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return s
}

// Start serves until the context is cancelled, then drains in-flight requests.
//
// The drain matters more here than in an ordinary service: an ingest request that is cut off
// mid-append has been acknowledged to nobody, but one cut off after its WAL batch was synced
// has been recorded and not acknowledged. The client retries and the event arrives twice.
// Draining keeps that window to requests genuinely in flight at shutdown.
func (s *Server) Start(ctx context.Context) error {
	errc := make(chan error, 1)

	go func() {
		s.log.Info("http listening", "addr", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
			return
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		s.log.Info("http draining")
		return s.srv.Shutdown(shutdownCtx)
	}
}

// handleHealthz reports that the process is running and able to serve. It deliberately checks
// nothing else: a liveness probe that fails on a dependency restarts a process that would
// have recovered on its own.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.ready == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		return
	}
	if err := s.ready(r.Context()); err != nil {
		// The reason is returned because this is an internal endpoint and the alternative is
		// reading logs on a node that is, by definition, already misbehaving.
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not ready",
			"reason": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// instrument records the outcome of a route by status class.
//
// Class rather than exact code keeps the series bounded, and the distinction that matters
// operationally is 2xx against 4xx against 5xx, not 502 against 503.
func (s *Server) instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(rec.status/100)+"xx").Inc()
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.wroteHeader = true
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
