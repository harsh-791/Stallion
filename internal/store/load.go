package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"auditlog/internal/audit"
)

var stageColumns = []string{
	"seq", "event_id", "tenant_id", "actor_id", "actor_type", "actor_email",
	"action", "resource_type", "resource_id", "result", "ip", "metadata", "occurred_at",
}

const createInsertedRowsSQL = `
CREATE TEMP TABLE inserted_rows (
    tenant_id text NOT NULL,
    action text NOT NULL,
    occurred_at timestamptz NOT NULL,
    ingest_seq bigint NOT NULL
) ON COMMIT DROP
`

// insertFromStagingSQL copies the earliest row of each event_id into audit_events.
// ingest_seq continues from the locked per-tenant watermark. First write wins.
const insertFromStagingSQL = `
WITH deduped AS (
    SELECT DISTINCT ON (event_id)
        seq, event_id, tenant_id, actor_id, actor_type, actor_email, action,
        resource_type, resource_id, result, ip, metadata, occurred_at
    FROM %s
    ORDER BY event_id, seq
),
numbered AS (
    SELECT d.*, row_number() OVER (PARTITION BY tenant_id ORDER BY seq) AS rn
    FROM deduped d
),
inserted AS (
    INSERT INTO audit_events (
        event_id, tenant_id, actor_id, actor_type, actor_email, action,
        resource_type, resource_id, result, ip, metadata, occurred_at, ingest_seq
    )
    SELECT
        n.event_id::uuid,
        n.tenant_id,
        n.actor_id,
        n.actor_type,
        NULLIF(n.actor_email, ''),
        n.action,
        n.resource_type,
        n.resource_id,
        n.result,
        n.ip,
        n.metadata::jsonb,
        n.occurred_at,
        w.max_ingest_seq + n.rn
    FROM numbered n
    JOIN audit_tenant_seq w ON w.tenant_id = n.tenant_id
    ON CONFLICT (event_id) DO NOTHING
    RETURNING tenant_id, action, occurred_at, ingest_seq
)
INSERT INTO inserted_rows (tenant_id, action, occurred_at, ingest_seq)
SELECT tenant_id, action, occurred_at, ingest_seq FROM inserted
`

const rollupSQL = `
INSERT INTO audit_daily_counts (tenant_id, day, action, event_count)
SELECT tenant_id, (occurred_at AT TIME ZONE 'UTC')::date, action, COUNT(*)::bigint
FROM inserted_rows
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
ON CONFLICT (tenant_id, day, action)
DO UPDATE SET event_count = audit_daily_counts.event_count + EXCLUDED.event_count
`

const watermarkSQL = `
UPDATE audit_tenant_seq AS w
SET max_ingest_seq = s.mx
FROM (
    SELECT tenant_id, MAX(ingest_seq) AS mx
    FROM inserted_rows
    GROUP BY tenant_id
) AS s
WHERE w.tenant_id = s.tenant_id
  AND s.mx > w.max_ingest_seq
`

type beforeCommitCtxKey struct{}

// WithBeforeCommitHook attaches a test-only callback that runs while the
// watermark lock is still held. Production callers never set this.
func WithBeforeCommitHook(ctx context.Context, fn func() error) context.Context {
	return context.WithValue(ctx, beforeCommitCtxKey{}, fn)
}

func runBeforeCommit(ctx context.Context) error {
	fn, _ := ctx.Value(beforeCommitCtxKey{}).(func() error)
	if fn == nil {
		return nil
	}
	return fn()
}

type Loader struct {
	conn *pgx.Conn
}

func NewLoader(ctx context.Context, databaseURL string) (*Loader, error) {
	cfg, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.RuntimeParams["timezone"] = "UTC"
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := dropIngestIdentity(ctx, conn); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	return &Loader{conn: conn}, nil
}

func (l *Loader) Close(ctx context.Context) {
	_ = l.conn.Close(ctx)
}

// PrepareLoad drops the list indexes and relaxes commit flush for the bulk load.
// FinishLoad puts both back. Re-running the loader is safe: event_id conflicts are ignored.
func (l *Loader) PrepareLoad(ctx context.Context) error {
	stmts := []string{
		`SET synchronous_commit TO OFF`,
		`TRUNCATE audit_events_staging`,
		`DROP INDEX IF EXISTS audit_events_tenant_time_idx`,
		`DROP INDEX IF EXISTS audit_events_tenant_action_time_idx`,
		`DROP INDEX IF EXISTS audit_daily_counts_day_idx`,
	}
	for _, stmt := range stmts {
		if _, err := l.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

func (l *Loader) FinishLoad(ctx context.Context) error {
	if _, err := l.conn.Exec(ctx, `SET synchronous_commit TO ON`); err != nil {
		return err
	}
	if _, err := l.conn.Exec(ctx, `SET maintenance_work_mem TO '256MB'`); err != nil {
		return err
	}
	stmts := []string{
		`TRUNCATE audit_events_staging`,
		`CREATE INDEX IF NOT EXISTS audit_events_tenant_time_idx ON audit_events (tenant_id, occurred_at DESC, event_id DESC)`,
		`CREATE INDEX IF NOT EXISTS audit_events_tenant_action_time_idx ON audit_events (tenant_id, action, occurred_at DESC, event_id DESC)`,
		`CREATE INDEX IF NOT EXISTS audit_daily_counts_day_idx ON audit_daily_counts (day DESC)`,
		`ANALYZE audit_events`,
		`ANALYZE audit_daily_counts`,
		`ANALYZE audit_tenant_seq`,
		`CHECKPOINT`,
	}
	for _, stmt := range stmts {
		if _, err := l.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

// LoadChunk copies rows into the unlogged staging table, then inserts the
// first copy of each event_id. seq is file order, so the earliest line wins.
func (l *Loader) LoadChunk(ctx context.Context, events []audit.Event, seqStart int64) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	tx, err := l.conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `TRUNCATE audit_events_staging`); err != nil {
		return 0, err
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"audit_events_staging"}, stageColumns, pgx.CopyFromRows(stageValues(events, seqStart))); err != nil {
		return 0, fmt.Errorf("copy chunk: %w", err)
	}
	n, err := flushInserted(ctx, tx, "audit_events_staging")
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

func flushInserted(ctx context.Context, tx pgx.Tx, table string) (int64, error) {
	if table != "audit_events_staging" && table != "incoming" {
		return 0, fmt.Errorf("unexpected staging table %q", table)
	}
	if _, err := tx.Exec(ctx, createInsertedRowsSQL); err != nil {
		return 0, fmt.Errorf("create inserted_rows: %w", err)
	}
	// Lock each tenant watermark in tenant_id order before assigning sequences.
	// The lock is held until this transaction commits, so a later batch cannot
	// publish a higher ingest_seq while this one is still open.
	if err := lockTenantWatermarks(ctx, tx, table); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(insertFromStagingSQL, table)); err != nil {
		return 0, fmt.Errorf("insert events: %w", err)
	}
	if _, err := tx.Exec(ctx, rollupSQL); err != nil {
		return 0, fmt.Errorf("update daily counts: %w", err)
	}
	if _, err := tx.Exec(ctx, watermarkSQL); err != nil {
		return 0, fmt.Errorf("update ingest watermark: %w", err)
	}
	var n int64
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM inserted_rows`).Scan(&n); err != nil {
		return 0, err
	}
	if err := runBeforeCommit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

type queryExec interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func dropIngestIdentity(ctx context.Context, q queryExec) error {
	var identity string
	err := q.QueryRow(ctx, `
		SELECT a.attidentity::text
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relname = 'audit_events'
		  AND a.attname = 'ingest_seq' AND NOT a.attisdropped`).Scan(&identity)
	if err != nil {
		return fmt.Errorf("inspect ingest_seq: %w", err)
	}
	if identity == "" {
		return nil
	}
	if _, err := q.Exec(ctx, `ALTER TABLE audit_events ALTER COLUMN ingest_seq DROP IDENTITY`); err != nil {
		return fmt.Errorf("drop ingest_seq identity: %w", err)
	}
	return nil
}

func lockTenantWatermarks(ctx context.Context, tx pgx.Tx, table string) error {
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT DISTINCT tenant_id FROM %s ORDER BY tenant_id`, table))
	if err != nil {
		return fmt.Errorf("list tenants: %w", err)
	}
	var tenants []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		tenants = append(tenants, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, tenant := range tenants {
		if _, err := tx.Exec(ctx, `
			INSERT INTO audit_tenant_seq (tenant_id, max_ingest_seq) VALUES ($1, 0)
			ON CONFLICT (tenant_id) DO NOTHING`, tenant); err != nil {
			return fmt.Errorf("ensure watermark: %w", err)
		}
		var seq int64
		if err := tx.QueryRow(ctx, `
			SELECT max_ingest_seq FROM audit_tenant_seq WHERE tenant_id = $1 FOR UPDATE`, tenant).Scan(&seq); err != nil {
			return fmt.Errorf("lock watermark: %w", err)
		}
	}
	return nil
}

func stageValues(events []audit.Event, seqStart int64) [][]any {
	rows := make([][]any, len(events))
	for i, e := range events {
		var email any
		if e.ActorEmail != nil {
			email = *e.ActorEmail
		}
		rows[i] = []any{
			seqStart + int64(i),
			e.EventID,
			e.TenantID,
			e.ActorID,
			e.ActorType,
			email,
			e.Action,
			e.ResourceType,
			e.ResourceID,
			e.Result,
			e.IP,
			string(e.Metadata),
			e.OccurredAt.UTC(),
		}
	}
	return rows
}
