package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"auditlog/internal/audit"
	"auditlog/internal/httpapi"
	"auditlog/internal/store"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if url := os.Getenv("DATABASE_URL"); url != "" {
		_ = store.DropTestDatabase(context.Background(), url)
	}
	os.Exit(code)
}

func TestRejectsBadBatch(t *testing.T) {
	h := httpapi.New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	cases := []struct {
		body string
	}{
		{`{}`},
		{`{"events":[]}`},
		{`not-json`},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader([]byte(tc.body)))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("body %s status %d", tc.body, rr.Code)
		}
	}
}

func TestIngestListAndSummary(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	st, err := store.PrepareTestDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	h := httpapi.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	email := "ada@example.com"
	ev := audit.Event{
		EventID:      "00000000-0000-4000-8000-000000000099",
		TenantID:     "t_0001",
		ActorID:      "usr_ada",
		ActorType:    "user",
		ActorEmail:   &email,
		Action:       "invoice.delete",
		ResourceType: "invoice",
		ResourceID:   "inv_9",
		Result:       "failure",
		IP:           "198.51.100.20",
		Metadata:     []byte(`{"reason":"duplicate"}`),
		OccurredAt:   time.Date(2026, 9, 14, 8, 31, 7, 412000000, time.UTC),
	}
	payload, _ := json.Marshal(map[string]any{"events": []audit.Event{ev, ev}})
	res, err := http.Post(srv.URL+"/v1/events", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("ingest %d %s", res.StatusCode, b)
	}
	var ingested struct {
		Inserted   int64 `json:"inserted"`
		Duplicates int64 `json:"duplicates"`
	}
	if err := json.NewDecoder(res.Body).Decode(&ingested); err != nil {
		t.Fatal(err)
	}
	if ingested.Inserted != 1 || ingested.Duplicates != 1 {
		t.Fatalf("ingest counts %+v", ingested)
	}

	listRes, err := http.Get(srv.URL + "/v1/tenants/t_0002/events?limit=50")
	if err != nil {
		t.Fatal(err)
	}
	defer listRes.Body.Close()
	var other struct {
		Events []audit.Event `json:"events"`
	}
	if err := json.NewDecoder(listRes.Body).Decode(&other); err != nil {
		t.Fatal(err)
	}
	if len(other.Events) != 0 {
		t.Fatalf("other tenant saw events")
	}

	ownRes, err := http.Get(srv.URL + "/v1/tenants/t_0001/events?limit=50&action=invoice.delete&from=2026-09-01T00:00:00Z&to=2026-10-01T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	defer ownRes.Body.Close()
	var own struct {
		Events []audit.Event `json:"events"`
	}
	if err := json.NewDecoder(ownRes.Body).Decode(&own); err != nil {
		t.Fatal(err)
	}
	if len(own.Events) != 1 || own.Events[0].Result != "failure" {
		t.Fatalf("own list %+v", own.Events)
	}
}

func TestInvalidEventInsertsNothing(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	st, err := store.PrepareTestDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	h := httpapi.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	body := `{"events":[{"event_id":"00000000-0000-4000-8000-0000000000aa","tenant_id":"t_bad","actor_id":"usr_ada","actor_type":"user","actor_email":"ada@example.com","action":"invoice.delete","resource_type":"invoice","resource_id":"inv_1","result":"success","ip":"203.0.113.10","metadata":{},"occurred_at":"2026-09-01T00:00:00Z"},{"event_id":"00000000-0000-4000-8000-0000000000ab","tenant_id":"t_bad","actor_id":"usr_ada","actor_type":"user","action":"not.real","resource_type":"invoice","resource_id":"inv_1","result":"success","ip":"203.0.113.10","metadata":{},"occurred_at":"2026-09-01T00:00:00Z"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status %d %s", rr.Code, rr.Body.String())
	}
	page, err := st.ListEvents(context.Background(), store.ListQuery{TenantID: "t_bad", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 0 {
		t.Fatalf("invalid batch stored %d events", len(page.Events))
	}
}

func TestCursorIsBoundToFilters(t *testing.T) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	st, err := store.PrepareTestDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	h := httpapi.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	email := "ada@example.com"
	var events []audit.Event
	for i := 0; i < 3; i++ {
		events = append(events, audit.Event{
			EventID:      fmt.Sprintf("00000000-0000-4000-8000-%012d", 200+i),
			TenantID:     "t_page",
			ActorID:      "usr_ada",
			ActorType:    "user",
			ActorEmail:   &email,
			Action:       "invoice.delete",
			ResourceType: "invoice",
			ResourceID:   "inv_1",
			Result:       "success",
			IP:           "203.0.113.10",
			Metadata:     []byte(`{}`),
			OccurredAt:   time.Date(2026, 9, 8, i, 0, 0, 0, time.UTC),
		})
	}
	if _, err := st.InsertEvents(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/tenants/t_page/events?limit=1&action=invoice.delete", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("list %d %s", rr.Code, rr.Body.String())
	}
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.NextCursor == "" {
		t.Fatal("expected a next cursor")
	}
	reuse := httptest.NewRequest(http.MethodGet, "/v1/tenants/t_page/events?limit=1&cursor="+page.NextCursor, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, reuse)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("mismatched cursor status %d %s", rr.Code, rr.Body.String())
	}
}
