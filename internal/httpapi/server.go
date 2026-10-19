package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"auditlog/internal/audit"
	"auditlog/internal/cursor"
	"auditlog/internal/store"
)

type Server struct {
	store *store.Store
	log   *slog.Logger
}

func New(st *store.Store, log *slog.Logger) http.Handler {
	s := &Server{store: st, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /v1/events", s.ingest)
	mux.HandleFunc("GET /v1/tenants/{tenant_id}/events", s.list)
	mux.HandleFunc("GET /v1/tenants/{tenant_id}/summary", s.summary)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		mux.ServeHTTP(sw, r)
		s.log.Info("request", "method", r.Method, "path", r.URL.Path, "status", sw.code, "ms", time.Since(start).Milliseconds())
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithDeadline(r, 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		s.log.Error("health", "err", err)
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	var body struct {
		Events []audit.Event `json:"events"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, decodeError(err))
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request body must be a single JSON object")
		return
	}
	if len(body.Events) == 0 {
		writeError(w, http.StatusBadRequest, "events must not be empty")
		return
	}
	if len(body.Events) > audit.MaxBatch {
		writeError(w, http.StatusBadRequest, "events exceeds the maximum batch size of 1000")
		return
	}
	for i := range body.Events {
		if err := body.Events[i].Validate(); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("events[%d]: %s", i, err))
			return
		}
	}
	original := len(body.Events)
	events := audit.Dedupe(body.Events)
	inserted, err := s.store.InsertEvents(r.Context(), events)
	if err != nil {
		s.log.Error("ingest", "err", err)
		writeError(w, http.StatusInternalServerError, "could not store events")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{
		"inserted":   inserted,
		"duplicates": int64(original) - inserted,
	})
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if err := audit.ValidateTenantID(tenantID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	q, err := listQuery(r, tenantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	page, err := s.store.ListEvents(r.Context(), q)
	if err != nil {
		s.log.Error("list", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list events")
		return
	}
	if page.Events == nil {
		page.Events = []audit.Event{}
	}
	resp := struct {
		TenantID   string        `json:"tenant_id"`
		Events     []audit.Event `json:"events"`
		NextCursor string        `json:"next_cursor,omitempty"`
	}{
		TenantID: tenantID,
		Events:   page.Events,
	}
	if page.HasMore && len(page.Events) > 0 {
		last := page.Events[len(page.Events)-1]
		token, err := cursor.Encode(cursor.Cursor{
			OccurredAt: last.OccurredAt,
			EventID:    last.EventID,
			AsOf:       page.AsOf,
			TenantID:   q.TenantID,
			Action:     q.Action,
			From:       q.From,
			To:         q.To,
			Limit:      q.Limit,
		})
		if err != nil {
			s.log.Error("cursor", "err", err)
			writeError(w, http.StatusInternalServerError, "could not list events")
			return
		}
		resp.NextCursor = token
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) summary(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if err := audit.ValidateTenantID(tenantID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	sum, err := s.store.Summary(r.Context(), tenantID)
	if err != nil {
		s.log.Error("summary", "err", err)
		writeError(w, http.StatusInternalServerError, "could not summarize events")
		return
	}
	buckets := make([]map[string]any, 0, len(sum.Buckets))
	for _, b := range sum.Buckets {
		buckets = append(buckets, map[string]any{
			"action": b.Action,
			"day":    b.Day,
			"count":  b.Count,
		})
	}
	writeJSON(w, http.StatusOK, struct {
		TenantID    string           `json:"tenant_id"`
		WindowStart *string          `json:"window_start"`
		WindowEnd   *string          `json:"window_end"`
		Buckets     []map[string]any `json:"buckets"`
	}{
		TenantID:    tenantID,
		WindowStart: sum.WindowStart,
		WindowEnd:   sum.WindowEnd,
		Buckets:     buckets,
	})
}

func listQuery(r *http.Request, tenantID string) (store.ListQuery, error) {
	q := store.ListQuery{TenantID: tenantID}
	limit, err := parseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		return store.ListQuery{}, err
	}
	q.Limit = limit
	if action := r.URL.Query().Get("action"); action != "" {
		if !audit.KnownAction(action) {
			return store.ListQuery{}, errors.New("action is not a known action")
		}
		q.Action = action
	}
	from, err := parseBound(r.URL.Query().Get("from"), "from")
	if err != nil {
		return store.ListQuery{}, err
	}
	to, err := parseBound(r.URL.Query().Get("to"), "to")
	if err != nil {
		return store.ListQuery{}, err
	}
	if from != nil && to != nil && !from.Before(*to) {
		return store.ListQuery{}, errors.New("from must be earlier than to")
	}
	q.From = from
	q.To = to
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		c, err := cursor.Decode(raw)
		if err != nil {
			return store.ListQuery{}, errors.New("cursor is not valid")
		}
		if !c.Matches(q.TenantID, q.Action, q.From, q.To, q.Limit) {
			return store.ListQuery{}, errors.New("cursor does not match this tenant, filters, or page size")
		}
		q.Cursor = &c
	}
	return q, nil
}

func parseLimit(raw string) (int, error) {
	if raw == "" {
		return 50, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 200 {
		return 0, errors.New("limit must be between 1 and 200")
	}
	return n, nil
}

func parseBound(raw, name string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be an RFC3339 timestamp", name)
	}
	u := t.UTC()
	return &u, nil
}

func decodeError(err error) string {
	var maxBytes *http.MaxBytesError
	if errors.As(err, &maxBytes) {
		return "request body too large"
	}
	return "request body must be a JSON object with an events array"
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}
