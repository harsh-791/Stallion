package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"auditlog/internal/audit"
	"auditlog/internal/cursor"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "bench: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	apiURL := strings.TrimRight(os.Getenv("API_URL"), "/")
	if apiURL == "" {
		apiURL = "http://localhost:8080"
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	ctx := context.Background()
	client := &http.Client{Timeout: 60 * time.Second}
	if err := waitHealthy(ctx, client, apiURL); err != nil {
		return err
	}

	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `SET TIME ZONE 'UTC'`); err != nil {
		return err
	}

	b2, err := benchmarkB2(ctx, client, conn, apiURL)
	if err != nil {
		return fmt.Errorf("B2: %w", err)
	}

	queries := []struct {
		name string
		url  string
	}{
		{"B1", apiURL + "/v1/tenants/t_0001/events?limit=50"},
		{"B2", b2},
		{"B3", apiURL + "/v1/tenants/t_0001/events?limit=50&action=invoice.delete&from=2026-09-01T00:00:00Z&to=2026-10-01T00:00:00Z"},
		{"B4", apiURL + "/v1/tenants/t_0001/summary"},
	}
	for _, q := range queries {
		samples, body, err := timeGET(client, q.url, 10)
		if err != nil {
			return fmt.Errorf("%s: %w", q.name, err)
		}
		if err := check(q.name, body); err != nil {
			return fmt.Errorf("%s: %w", q.name, err)
		}
		fmt.Printf("%s median_ms=%.2f samples_ms=%s\n", q.name, float64(median(samples))/float64(time.Millisecond), formatMS(samples))
	}
	return printDisk(ctx, conn)
}

func benchmarkB2(ctx context.Context, client *http.Client, conn *pgx.Conn, apiURL string) (string, error) {
	var asOf int64
	err := conn.QueryRow(ctx, `SELECT COALESCE(MAX(max_ingest_seq), 0) FROM audit_tenant_seq WHERE tenant_id = 't_0001'`).Scan(&asOf)
	if err != nil {
		return "", err
	}
	var occurred time.Time
	var eventID string
	// Match the list API snapshot: only rows with ingest_seq <= as_of.
	err = conn.QueryRow(ctx, `
		SELECT occurred_at, event_id::text
		FROM audit_events
		WHERE tenant_id = 't_0001' AND ingest_seq <= $1
		ORDER BY occurred_at DESC, event_id DESC
		OFFSET 99999 LIMIT 1`, asOf).Scan(&occurred, &eventID)
	if err != nil {
		return "", fmt.Errorf("tenant t_0001 does not have 100000 events at watermark %d: %w", asOf, err)
	}
	cursor.SetSecret(os.Getenv("CURSOR_SECRET"))
	token, err := cursor.Encode(cursor.Cursor{
		OccurredAt: occurred,
		EventID:    eventID,
		AsOf:       asOf,
		TenantID:   "t_0001",
		Limit:      50,
	})
	if err != nil {
		return "", err
	}
	q := url.Values{}
	q.Set("limit", "50")
	q.Set("cursor", token)
	pageURL := apiURL + "/v1/tenants/t_0001/events?" + q.Encode()

	body, err := getOnce(client, pageURL)
	if err != nil {
		return "", err
	}
	var page struct {
		Events []audit.Event `json:"events"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		return "", err
	}
	if len(page.Events) != 50 {
		return "", fmt.Errorf("expected 50 events, got %d", len(page.Events))
	}
	first, err := eventAt(ctx, conn, 100000, asOf)
	if err != nil {
		return "", err
	}
	last, err := eventAt(ctx, conn, 100049, asOf)
	if err != nil {
		return "", err
	}
	if page.Events[0].EventID != first || page.Events[49].EventID != last {
		return "", fmt.Errorf("page mismatch: api %s..%s sql %s..%s", page.Events[0].EventID, page.Events[49].EventID, first, last)
	}
	fmt.Printf("B2 cursor is the (occurred_at, event_id) of event 100000; page starts at %s\n", first)
	return pageURL, nil
}

func eventAt(ctx context.Context, conn *pgx.Conn, offset int, asOf int64) (string, error) {
	var id string
	err := conn.QueryRow(ctx, `
		SELECT event_id::text
		FROM audit_events
		WHERE tenant_id = 't_0001' AND ingest_seq <= $2
		ORDER BY occurred_at DESC, event_id DESC
		OFFSET $1 LIMIT 1`, offset, asOf).Scan(&id)
	return id, err
}

func timeGET(client *http.Client, rawURL string, n int) ([]time.Duration, []byte, error) {
	samples := make([]time.Duration, 0, n)
	var last []byte
	for i := 0; i < n; i++ {
		start := time.Now()
		body, err := getOnce(client, rawURL)
		if err != nil {
			return nil, nil, err
		}
		samples = append(samples, time.Since(start))
		last = body
	}
	return samples, last, nil
}

func getOnce(client *http.Client, rawURL string) ([]byte, error) {
	resp, err := client.Get(rawURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s status %d: %s", rawURL, resp.StatusCode, bytesPreview(body))
	}
	return body, nil
}

func check(name string, body []byte) error {
	switch name {
	case "B1", "B2", "B3":
		var page struct {
			Events []audit.Event `json:"events"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return err
		}
		if len(page.Events) != 50 {
			return fmt.Errorf("expected 50 events, got %d", len(page.Events))
		}
		if name == "B3" {
			from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
			to := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
			for _, ev := range page.Events {
				if ev.Action != "invoice.delete" || ev.OccurredAt.Before(from) || !ev.OccurredAt.Before(to) {
					return fmt.Errorf("event %s outside the B3 filter", ev.EventID)
				}
			}
		}
	case "B4":
		var sum struct {
			WindowStart *string `json:"window_start"`
			WindowEnd   *string `json:"window_end"`
			Buckets     []struct {
				Action string `json:"action"`
				Day    string `json:"day"`
				Count  int64  `json:"count"`
			} `json:"buckets"`
		}
		if err := json.Unmarshal(body, &sum); err != nil {
			return err
		}
		if sum.WindowStart == nil || sum.WindowEnd == nil || len(sum.Buckets) == 0 {
			return errors.New("summary window or buckets are empty")
		}
	}
	return nil
}

func printDisk(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx, `
		SELECT c.relname, pg_total_relation_size(c.oid)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind = 'r'
		ORDER BY c.relname`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var total int64
	for rows.Next() {
		var name string
		var size int64
		if err := rows.Scan(&name, &size); err != nil {
			return err
		}
		if name == "audit_events_staging" {
			continue
		}
		total += size
		fmt.Printf("disk %s_bytes=%d\n", name, size)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	fmt.Printf("disk total_bytes=%d\n", total)
	return nil
}

func waitHealthy(ctx context.Context, client *http.Client, apiURL string) error {
	var last error
	for i := 0; i < 30; i++ {
		resp, err := client.Get(apiURL + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
		last = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("api not healthy: %w", last)
}

func median(samples []time.Duration) time.Duration {
	cp := append([]time.Duration(nil), samples...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	n := len(cp)
	if n%2 == 1 {
		return cp[n/2]
	}
	return (cp[n/2-1] + cp[n/2]) / 2
}

func formatMS(samples []time.Duration) string {
	parts := make([]string, len(samples))
	for i, s := range samples {
		parts[i] = fmt.Sprintf("%.2f", float64(s)/float64(time.Millisecond))
	}
	return strings.Join(parts, ",")
}

func bytesPreview(b []byte) string {
	if len(b) > 200 {
		b = b[:200]
	}
	return string(b)
}
