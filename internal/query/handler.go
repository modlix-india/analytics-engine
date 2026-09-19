package query

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Authorizer decides whether a caller may read a site.
//
// The seam that keeps this engine generic, mirroring SiteResolver on the write side. An
// embedder implements it against its own security service — in Modlix's case a write-access
// check on the target app, which is deliberately stricter than a read permission, so a
// read-only role cannot open a dashboard.
//
// The engine never invents an answer of its own: an unset Authorizer denies everything, because
// the alternative failure mode is an open analytics endpoint that nobody notices.
type Authorizer interface {
	Allow(ctx context.Context, credential, site string) bool
}

// SharedSecret is the default Authorizer: one secret for all sites.
//
// Correct for a single-tenant deployment and for local development, and NOT a substitute for
// per-site authorisation in a multi-tenant one — it says the caller is trusted, not which
// sites they may read. Anyone running this multi-tenant must supply their own.
type SharedSecret struct{ Secret string }

func (s SharedSecret) Allow(_ context.Context, credential, _ string) bool {
	if s.Secret == "" {
		return false
	}
	// Constant time, so a wrong secret cannot be discovered a byte at a time.
	return subtle.ConstantTimeCompare([]byte(credential), []byte(s.Secret)) == 1
}

// DenyAll is the zero-configuration default.
type DenyAll struct{}

func (DenyAll) Allow(context.Context, string, string) bool { return false }

type request struct {
	Widget   string `json:"widget"`
	Site     string `json:"site"`
	From     string `json:"from"`
	To       string `json:"to"`
	Timezone string `json:"timezone"`
	Event    string `json:"event"`
	Property string `json:"property"`

	Steps       []string `json:"steps"`
	WindowHours int      `json:"windowHours"`
	Period      string   `json:"period"`

	Limit int `json:"limit"`
}

type Handler struct {
	Engine *Engine
	Auth   Authorizer
	Log    *slog.Logger

	// MaxRangeDays bounds a request. Without it one query can ask for every partition a site
	// has ever written, which is a denial of service that needs only a valid credential.
	MaxRangeDays int
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	auth := h.Auth
	if auth == nil {
		auth = DenyAll{}
	}

	var req request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	// Authorised against the site being READ, not against whatever site the caller happens to
	// belong to. That distinction is the whole tenancy boundary on this endpoint.
	if !auth.Allow(r.Context(), credentialOf(r), req.Site) {
		// 403 rather than 404: the caller supplied the site, so hiding its existence achieves
		// nothing, and a distinguishable code makes a misconfigured integration diagnosable.
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	from, err := time.Parse(time.RFC3339, req.From)
	if err != nil {
		http.Error(w, "from must be RFC3339", http.StatusBadRequest)
		return
	}
	to, err := time.Parse(time.RFC3339, req.To)
	if err != nil {
		http.Error(w, "to must be RFC3339", http.StatusBadRequest)
		return
	}

	maxDays := h.MaxRangeDays
	if maxDays <= 0 {
		maxDays = 400
	}
	if to.Sub(from) > time.Duration(maxDays)*24*time.Hour {
		http.Error(w, "range too long", http.StatusBadRequest)
		return
	}

	res, err := h.Engine.Query(r.Context(), Request{
		Widget: req.Widget, Site: req.Site,
		From: from, To: to, Timezone: req.Timezone,
		Event: req.Event, Property: req.Property,
		Steps: req.Steps, WindowHours: req.WindowHours, Period: req.Period,
		Limit: req.Limit,
	})
	if err != nil {
		// The engine's errors are about the request — an unknown widget, an unknown
		// timezone — so they are safe to return and useful to whoever is integrating.
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(res); err != nil && h.Log != nil {
		h.Log.Error("query: write response", "err", err)
	}
}

// credentialOf takes the bearer token, falling back to a cookie.
//
// Passed through to the Authorizer unexamined: what a credential means is the embedder's
// business, and this engine deliberately knows nothing about token formats.
func credentialOf(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if after, ok := strings.CutPrefix(h, "Bearer "); ok {
			return after
		}
		return h
	}
	if c, err := r.Cookie("AuthToken"); err == nil {
		return c.Value
	}
	return ""
}
