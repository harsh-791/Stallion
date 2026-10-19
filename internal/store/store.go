package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"auditlog/internal/audit"
	"auditlog/internal/cursor"
)

type Store struct {
	pool *pgxpool.Pool
}

func Connect(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 10
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "30000"
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, `SET TIME ZONE 'UTC'`)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.dropIngestIdentity(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// dropIngestIdentity removes a leftover identity on databases created before
// sequences were assigned under the tenant lock. Existing values are kept.
func (s *Store) dropIngestIdentity(ctx context.Context) error {
	return dropIngestIdentity(ctx, s.pool)
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// InsertEvents writes a batch. Duplicate event_ids are ignored.
// The returned count is the number of new rows.
func (s *Store) InsertEvents(ctx context.Context, events []audit.Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE incoming (LIKE audit_events_staging INCLUDING DEFAULTS) ON COMMIT DROP`); err != nil {
		return 0, fmt.Errorf("create temp table: %w", err)
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"incoming"}, stageColumns, pgx.CopyFromRows(stageValues(events, 1))); err != nil {
		return 0, fmt.Errorf("copy events: %w", err)
	}
	n, err := flushInserted(ctx, tx, "incoming")
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

type ListQuery struct {
	TenantID string
	Action   string
	From     *time.Time
	To       *time.Time
	Limit    int
	Cursor   *cursor.Cursor
}

type ListPage struct {
	Events  []audit.Event
	AsOf    int64
	HasMore bool
}

// ListEvents returns one page, newest occurred_at first.
// The first page reads the committed per-tenant watermark and keeps that
// bound on later pages. Ingest publishes the watermark only while holding
// the tenant lock until commit, so a row with ingest_seq <= as_of cannot
// appear after the snapshot was taken.
func (s *Store) ListEvents(ctx context.Context, q ListQuery) (ListPage, error) {
	if q.Cursor != nil {
		return s.queryPage(ctx, s.pool, q, q.Cursor.AsOf)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return ListPage{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var asOf int64
	err = tx.QueryRow(ctx, `SELECT COALESCE(MAX(max_ingest_seq), 0) FROM audit_tenant_seq WHERE tenant_id = $1`, q.TenantID).Scan(&asOf)
	if err != nil {
		return ListPage{}, fmt.Errorf("read ingest watermark: %w", err)
	}
	page, err := s.queryPage(ctx, tx, q, asOf)
	if err != nil {
		return ListPage{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ListPage{}, err
	}
	return page, nil
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (s *Store) queryPage(ctx context.Context, qy querier, q ListQuery, asOf int64) (ListPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	sql, args := listSQL(q, asOf, limit+1)
	rows, err := qy.Query(ctx, sql, args...)
	if err != nil {
		return ListPage{}, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	events := make([]audit.Event, 0, limit)
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return ListPage{}, err
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return ListPage{}, err
	}
	page := ListPage{Events: events, AsOf: asOf}
	if len(events) > limit {
		page.HasMore = true
		page.Events = events[:limit]
	}
	return page, nil
}

func listSQL(q ListQuery, asOf int64, limit int) (string, []any) {
	var b strings.Builder
	b.WriteString(`SELECT event_id::text, tenant_id, actor_id, actor_type, actor_email, action,
		resource_type, resource_id, result, ip, metadata, occurred_at
		FROM audit_events
		WHERE tenant_id = $1 AND ingest_seq <= $2`)
	args := []any{q.TenantID, asOf}
	if q.Action != "" {
		args = append(args, q.Action)
		fmt.Fprintf(&b, " AND action = $%d", len(args))
	}
	if q.From != nil {
		args = append(args, *q.From)
		fmt.Fprintf(&b, " AND occurred_at >= $%d", len(args))
	}
	if q.To != nil {
		args = append(args, *q.To)
		fmt.Fprintf(&b, " AND occurred_at < $%d", len(args))
	}
	if q.Cursor != nil {
		args = append(args, q.Cursor.OccurredAt, q.Cursor.EventID)
		fmt.Fprintf(&b, " AND (occurred_at, event_id) < ($%d, $%d::uuid)", len(args)-1, len(args))
	}
	args = append(args, limit)
	fmt.Fprintf(&b, " ORDER BY occurred_at DESC, event_id DESC LIMIT $%d", len(args))
	return b.String(), args
}

type Summary struct {
	WindowStart *string
	WindowEnd   *string
	Buckets     []Bucket
}

type Bucket struct {
	Action string
	Day    string
	Count  int64
}

// Summary returns per-action daily counts for the 30 UTC days ending on the
// newest event day in the whole dataset.
func (s *Store) Summary(ctx context.Context, tenantID string) (Summary, error) {
	var end string
	err := s.pool.QueryRow(ctx, `
		SELECT to_char(day, 'YYYY-MM-DD')
		FROM audit_daily_counts
		ORDER BY day DESC
		LIMIT 1`).Scan(&end)
	if errors.Is(err, pgx.ErrNoRows) {
		return Summary{Buckets: []Bucket{}}, nil
	}
	if err != nil {
		return Summary{}, fmt.Errorf("summary window: %w", err)
	}
	endDay, err := time.Parse("2006-01-02", end)
	if err != nil {
		return Summary{}, err
	}
	start := endDay.AddDate(0, 0, -29).Format("2006-01-02")
	endCopy := end
	rows, err := s.pool.Query(ctx, `
		SELECT action, to_char(day, 'YYYY-MM-DD'), event_count
		FROM audit_daily_counts
		WHERE tenant_id = $1 AND day >= $2::date AND day <= $3::date
		ORDER BY action, day`, tenantID, start, end)
	if err != nil {
		return Summary{}, fmt.Errorf("summary: %w", err)
	}
	defer rows.Close()

	buckets := []Bucket{}
	for rows.Next() {
		var b Bucket
		if err := rows.Scan(&b.Action, &b.Day, &b.Count); err != nil {
			return Summary{}, err
		}
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return Summary{}, err
	}
	return Summary{WindowStart: &start, WindowEnd: &endCopy, Buckets: buckets}, nil
}

func scanEvent(rows pgx.Rows) (audit.Event, error) {
	var e audit.Event
	var meta []byte
	if err := rows.Scan(
		&e.EventID, &e.TenantID, &e.ActorID, &e.ActorType, &e.ActorEmail, &e.Action,
		&e.ResourceType, &e.ResourceID, &e.Result, &e.IP, &meta, &e.OccurredAt,
	); err != nil {
		return audit.Event{}, err
	}
	e.Metadata = meta
	e.OccurredAt = e.OccurredAt.UTC()
	return e, nil
}
