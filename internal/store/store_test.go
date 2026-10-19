package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"auditlog/internal/audit"
	"auditlog/internal/cursor"
	"auditlog/internal/store"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if url := os.Getenv("DATABASE_URL"); url != "" {
		_ = store.DropTestDatabase(context.Background(), url)
	}
	os.Exit(code)
}

func TestInsertIgnoresDuplicateAndKeepsFirst(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	at := time.Date(2026, 9, 14, 8, 0, 0, 0, time.UTC)
	first := sample(1, "t_0001", "invoice.delete", at)
	second := sample(1, "t_0001", "invoice.update", at)
	n, err := st.InsertEvents(ctx, []audit.Event{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("inserted %d", n)
	}
	again, err := st.InsertEvents(ctx, []audit.Event{second})
	if err != nil {
		t.Fatal(err)
	}
	if again != 0 {
		t.Fatalf("retry inserted %d", again)
	}
	page, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_0001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].Action != "invoice.delete" {
		t.Fatalf("got %+v", page.Events)
	}
	sum, err := st.Summary(ctx, "t_0001")
	if err != nil {
		t.Fatal(err)
	}
	if got := countAction(sum, "invoice.delete"); got != 1 {
		t.Fatalf("rollup count %d", got)
	}
}

func TestDuplicateEventIDDoesNotCrossTenants(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	at := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	original := sample(2, "t_0001", "invoice.delete", at)
	if _, err := st.InsertEvents(ctx, []audit.Event{original}); err != nil {
		t.Fatal(err)
	}
	other := sample(2, "t_0002", "project.create", at)
	n, err := st.InsertEvents(ctx, []audit.Event{other})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("inserted %d", n)
	}
	page, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_0002", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 0 {
		t.Fatalf("tenant t_0002 saw %d events", len(page.Events))
	}
}

func TestTenantIsolation(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	at := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	if _, err := st.InsertEvents(ctx, []audit.Event{
		sample(3, "t_0001", "invoice.delete", at),
		sample(4, "t_0002", "user.login", at),
	}); err != nil {
		t.Fatal(err)
	}
	a, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_0001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	b, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_0002", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Events) != 1 || a.Events[0].TenantID != "t_0001" {
		t.Fatalf("tenant a: %+v", a.Events)
	}
	if len(b.Events) != 1 || b.Events[0].Action != "user.login" {
		t.Fatalf("tenant b: %+v", b.Events)
	}
}

func TestLateEventSortsByOccurredAt(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	newer := time.Date(2026, 9, 10, 12, 10, 0, 0, time.UTC)
	older := newer.Add(-10 * time.Minute)
	if _, err := st.InsertEvents(ctx, []audit.Event{sample(5, "t_0001", "invoice.delete", newer)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertEvents(ctx, []audit.Event{sample(6, "t_0001", "invoice.update", older)}); err != nil {
		t.Fatal(err)
	}
	page, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_0001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 || page.Events[0].EventID != sample(5, "", "", time.Time{}).EventID {
		t.Fatalf("order: %+v", page.Events)
	}
}

func TestPaginationSnapshotSkipsLateInsert(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	events := []audit.Event{
		sample(11, "t_0001", "invoice.delete", base),
		sample(12, "t_0001", "invoice.delete", base.Add(time.Minute)),
		sample(13, "t_0001", "invoice.delete", base.Add(2*time.Minute)),
	}
	if _, err := st.InsertEvents(ctx, events); err != nil {
		t.Fatal(err)
	}
	first, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_0001", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Events) != 1 || first.Events[0].EventID != events[2].EventID || !first.HasMore {
		t.Fatalf("first page: %+v hasMore=%v", first.Events, first.HasMore)
	}
	cur := &cursor.Cursor{OccurredAt: first.Events[0].OccurredAt, EventID: first.Events[0].EventID, AsOf: first.AsOf}

	late := sample(14, "t_0001", "invoice.update", base.Add(90*time.Second))
	if _, err := st.InsertEvents(ctx, []audit.Event{late}); err != nil {
		t.Fatal(err)
	}
	second, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_0001", Limit: 1, Cursor: cur})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Events) != 1 || second.Events[0].EventID != events[1].EventID {
		t.Fatalf("second page = %+v, want %s", second.Events, events[1].EventID)
	}
	next := &cursor.Cursor{OccurredAt: second.Events[0].OccurredAt, EventID: second.Events[0].EventID, AsOf: second.AsOf}
	third, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_0001", Limit: 1, Cursor: next})
	if err != nil {
		t.Fatal(err)
	}
	if len(third.Events) != 1 || third.Events[0].EventID != events[0].EventID || third.HasMore {
		t.Fatalf("third page: %+v hasMore=%v", third.Events, third.HasMore)
	}

	fresh, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_0001", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, len(fresh.Events))
	for i, ev := range fresh.Events {
		got[i] = ev.EventID
	}
	want := []string{events[2].EventID, late.EventID, events[1].EventID, events[0].EventID}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("fresh order %v", got)
	}
}

func TestSummaryWindowAndTenantSplit(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	sep30 := time.Date(2026, 9, 30, 23, 30, 0, 0, time.UTC)
	oct1 := time.Date(2026, 10, 1, 0, 30, 0, 0, time.UTC)
	aug1 := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	if _, err := st.InsertEvents(ctx, []audit.Event{
		sample(21, "t_0001", "invoice.delete", sep30),
		sample(22, "t_0001", "invoice.delete", sep30.Add(-time.Hour)),
		sample(23, "t_0001", "user.login", aug1),
		sample(24, "t_0001", "invoice.update", oct1),
		sample(25, "t_0002", "invoice.delete", sep30),
	}); err != nil {
		t.Fatal(err)
	}
	sum, err := st.Summary(ctx, "t_0001")
	if err != nil {
		t.Fatal(err)
	}
	if sum.WindowStart == nil || *sum.WindowStart != "2026-09-02" || sum.WindowEnd == nil || *sum.WindowEnd != "2026-10-01" {
		t.Fatalf("window %v %v", sum.WindowStart, sum.WindowEnd)
	}
	if countAction(sum, "invoice.delete") != 2 {
		t.Fatalf("invoice.delete count %+v", sum.Buckets)
	}
	if countAction(sum, "user.login") != 0 {
		t.Fatalf("old day leaked into window: %+v", sum.Buckets)
	}
	if countDay(sum, "invoice.update", "2026-10-01") != 1 || countDay(sum, "invoice.delete", "2026-09-30") != 2 {
		t.Fatalf("days %+v", sum.Buckets)
	}
	other, err := st.Summary(ctx, "t_0002")
	if err != nil {
		t.Fatal(err)
	}
	if countAction(other, "invoice.delete") != 1 {
		t.Fatalf("tenant leak %+v", other.Buckets)
	}
}

func TestLoadChunkDedupesAcrossChunks(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	url := os.Getenv("DATABASE_URL")
	testURL, err := replaceDB(url, store.TestDatabaseName())
	if err != nil {
		t.Fatal(err)
	}
	loader, err := store.NewLoader(ctx, testURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { loader.Close(ctx) })

	at := time.Date(2026, 9, 4, 1, 0, 0, 0, time.UTC)
	first := sample(31, "t_0009", "document.view", at)
	dup := first
	dup.Action = "document.download"
	other := sample(32, "t_0009", "document.share", at)
	n, err := loader.LoadChunk(ctx, []audit.Event{first, dup, other}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("inserted %d", n)
	}
	again, err := loader.LoadChunk(ctx, []audit.Event{dup}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if again != 0 {
		t.Fatalf("second chunk inserted %d", again)
	}
	page, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_0009", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 {
		t.Fatalf("listed %d", len(page.Events))
	}
	for _, ev := range page.Events {
		if ev.EventID == first.EventID && ev.Action != "document.view" {
			t.Fatalf("first line did not win: %s", ev.Action)
		}
	}
	_ = st
}

func TestLaterBatchWaitsForWatermarkLock(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	older := sample(41, "t_lock", "invoice.delete", base)
	newest := sample(43, "t_lock", "invoice.delete", base.Add(2*time.Minute))
	if _, err := st.InsertEvents(ctx, []audit.Event{older, newest}); err != nil {
		t.Fatal(err)
	}
	first, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_lock", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	cur := &cursor.Cursor{OccurredAt: first.Events[0].OccurredAt, EventID: first.Events[0].EventID, AsOf: first.AsOf}

	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	heldCtx := store.WithBeforeCommitHook(ctx, func() error {
		once.Do(func() {
			close(started)
			<-release
		})
		return nil
	})

	held := sample(42, "t_lock", "invoice.update", base.Add(time.Minute))
	errCh := make(chan error, 1)
	go func() {
		_, err := st.InsertEvents(heldCtx, []audit.Event{held})
		errCh <- err
	}()
	select {
	case <-started:
	case err := <-errCh:
		t.Fatalf("held insert: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("insert did not reach the watermark lock")
	}

	later := sample(44, "t_lock", "invoice.delete", base.Add(3*time.Minute))
	blocked := make(chan error, 1)
	go func() {
		_, err := st.InsertEvents(ctx, []audit.Event{later})
		blocked <- err
	}()
	select {
	case err := <-blocked:
		t.Fatalf("second insert committed while the first still held the lock: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	mid, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_lock", Limit: 10, Cursor: cur})
	if err != nil {
		t.Fatal(err)
	}
	if len(mid.Events) != 1 || mid.Events[0].EventID != older.EventID {
		t.Fatalf("open insert changed the snapshot: %+v", idsOf(mid.Events))
	}

	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if err := <-blocked; err != nil {
		t.Fatal(err)
	}

	after, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_lock", Limit: 10, Cursor: cur})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Events) != 1 || after.Events[0].EventID != older.EventID {
		t.Fatalf("events committed after the snapshot entered the page: %+v", idsOf(after.Events))
	}

	seqHeld := ingestSeq(t, held.EventID)
	seqLater := ingestSeq(t, later.EventID)
	if seqLater <= seqHeld {
		t.Fatalf("later batch sequence %d is not above the held batch %d", seqLater, seqHeld)
	}
}

func TestInsertRollbackLeavesNoRows(t *testing.T) {
	st := newStore(t)
	ctx := store.WithBeforeCommitHook(context.Background(), func() error {
		return errors.New("boom")
	})
	_, err := st.InsertEvents(ctx, []audit.Event{
		sample(61, "t_roll", "invoice.delete", time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)),
	})
	if err == nil {
		t.Fatal("expected the commit hook to fail the insert")
	}
	page, err := st.ListEvents(context.Background(), store.ListQuery{TenantID: "t_roll", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 0 {
		t.Fatalf("rolled-back event is visible: %+v", page.Events)
	}
	sum, err := st.Summary(context.Background(), "t_roll")
	if err != nil {
		t.Fatal(err)
	}
	if countAction(sum, "invoice.delete") != 0 {
		t.Fatalf("rollup moved on rollback: %+v", sum.Buckets)
	}
}

func TestEqualOccurredAtOrdersByEventID(t *testing.T) {
	st := newStore(t)
	at := time.Date(2026, 9, 3, 1, 2, 3, 0, time.UTC)
	low := sample(71, "t_tie", "document.view", at)
	high := sample(72, "t_tie", "document.view", at)
	if _, err := st.InsertEvents(context.Background(), []audit.Event{low, high}); err != nil {
		t.Fatal(err)
	}
	page, err := st.ListEvents(context.Background(), store.ListQuery{TenantID: "t_tie", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if idsOf(page.Events) != high.EventID+","+low.EventID {
		t.Fatalf("order %s", idsOf(page.Events))
	}
}

func TestPageWalkMatchesSnapshot(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	base := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	var events []audit.Event
	for i := 0; i < 5; i++ {
		events = append(events, sample(80+i, "t_walk", "project.create", base.Add(time.Duration(i)*time.Minute)))
	}
	if _, err := st.InsertEvents(ctx, events); err != nil {
		t.Fatal(err)
	}
	var got []string
	var cur *cursor.Cursor
	for {
		q := store.ListQuery{TenantID: "t_walk", Limit: 2, Cursor: cur}
		page, err := st.ListEvents(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range page.Events {
			got = append(got, ev.EventID)
		}
		if !page.HasMore {
			break
		}
		last := page.Events[len(page.Events)-1]
		cur = &cursor.Cursor{OccurredAt: last.OccurredAt, EventID: last.EventID, AsOf: page.AsOf}
	}
	fresh, err := st.ListEvents(ctx, store.ListQuery{TenantID: "t_walk", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != idsOf(fresh.Events) {
		t.Fatalf("walk %v full %s", got, idsOf(fresh.Events))
	}
	if len(got) != 5 {
		t.Fatalf("walk length %d", len(got))
	}
}

func TestSummaryMatchesRawAggregation(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	day := time.Date(2026, 9, 20, 15, 0, 0, 0, time.UTC)
	if _, err := st.InsertEvents(ctx, []audit.Event{
		sample(91, "t_sum", "invoice.delete", day),
		sample(92, "t_sum", "invoice.delete", day.Add(time.Hour)),
		sample(93, "t_sum", "invoice.delete", day),
		sample(93, "t_sum", "invoice.update", day),
		sample(94, "t_other", "invoice.delete", day),
	}); err != nil {
		t.Fatal(err)
	}
	sum, err := st.Summary(ctx, "t_sum")
	if err != nil {
		t.Fatal(err)
	}
	conn := testConn(t)
	defer conn.Close(ctx)
	var raw int64
	err = conn.QueryRow(ctx, `
		SELECT count(*) FROM audit_events
		WHERE tenant_id = 't_sum' AND action = 'invoice.delete'
		  AND (occurred_at AT TIME ZONE 'UTC')::date = '2026-09-20'`).Scan(&raw)
	if err != nil {
		t.Fatal(err)
	}
	if countDay(sum, "invoice.delete", "2026-09-20") != raw || raw != 3 {
		t.Fatalf("summary %d raw %d", countDay(sum, "invoice.delete", "2026-09-20"), raw)
	}
}

func TestFinishLoadRestoresIndexes(t *testing.T) {
	st := newStore(t)
	_ = st
	ctx := context.Background()
	loader, err := store.NewLoader(ctx, testDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { loader.Close(ctx) })
	if err := loader.PrepareLoad(ctx); err != nil {
		t.Fatal(err)
	}
	if err := loader.FinishLoad(ctx); err != nil {
		t.Fatal(err)
	}
	conn := testConn(t)
	defer conn.Close(ctx)
	var n int
	err = conn.QueryRow(ctx, `
		SELECT count(*) FROM pg_indexes
		WHERE schemaname = 'public'
		  AND indexname IN (
		    'audit_events_tenant_time_idx',
		    'audit_events_tenant_action_time_idx',
		    'audit_daily_counts_day_idx')`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("indexes restored = %d", n)
	}
}

func idsOf(events []audit.Event) string {
	ids := make([]string, len(events))
	for i, ev := range events {
		ids[i] = ev.EventID
	}
	return strings.Join(ids, ",")
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("DATABASE_URL")
	u, err := replaceDB(raw, store.TestDatabaseName())
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func testConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), testDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func ingestSeq(t *testing.T, eventID string) int64 {
	t.Helper()
	conn := testConn(t)
	defer conn.Close(context.Background())
	var seq int64
	err := conn.QueryRow(context.Background(), `SELECT ingest_seq FROM audit_events WHERE event_id = $1`, eventID).Scan(&seq)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	st, err := store.PrepareTestDatabase(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return st
}

func sample(n int, tenant, action string, at time.Time) audit.Event {
	email := "ada@example.com"
	resource := "invoice"
	switch {
	case strings.HasPrefix(action, "user."):
		resource = "user"
	case strings.HasPrefix(action, "document."):
		resource = "document"
	case strings.HasPrefix(action, "project."):
		resource = "project"
	case strings.HasPrefix(action, "invoice."):
		resource = "invoice"
	}
	return audit.Event{
		EventID:      fmt.Sprintf("00000000-0000-4000-8000-%012d", n),
		TenantID:     tenant,
		ActorID:      "usr_ada",
		ActorType:    "user",
		ActorEmail:   &email,
		Action:       action,
		ResourceType: resource,
		ResourceID:   "res_1",
		Result:       "success",
		IP:           "203.0.113.10",
		Metadata:     []byte(`{"n":1}`),
		OccurredAt:   at,
	}
}

func countAction(sum store.Summary, action string) int64 {
	var n int64
	for _, b := range sum.Buckets {
		if b.Action == action {
			n += b.Count
		}
	}
	return n
}

func countDay(sum store.Summary, action, day string) int64 {
	for _, b := range sum.Buckets {
		if b.Action == action && b.Day == day {
			return b.Count
		}
	}
	return 0
}

func replaceDB(raw, name string) (string, error) {
	const prefix = "postgres://"
	if !strings.HasPrefix(raw, prefix) {
		return "", fmt.Errorf("unsupported database url")
	}
	slash := strings.LastIndex(raw, "/")
	if slash < 0 {
		return "", fmt.Errorf("unsupported database url")
	}
	rest := raw[slash+1:]
	q := ""
	if i := strings.Index(rest, "?"); i >= 0 {
		q = rest[i:]
	}
	return raw[:slash+1] + name + q, nil
}
