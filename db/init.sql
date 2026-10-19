-- Audit events are append-only. event_id is the idempotency key:
-- a retried delivery conflicts and is dropped, not updated.
CREATE TABLE audit_events (
    event_id      UUID PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    actor_id      TEXT NOT NULL,
    actor_type    TEXT NOT NULL,
    actor_email   TEXT,
    action        TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    result        TEXT NOT NULL,
    ip            TEXT NOT NULL,
    metadata      JSONB NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL,
    -- Assigned under a per-tenant lock. Not an identity: a global sequence
    -- can commit out of order and break a paging snapshot.
    ingest_seq    BIGINT NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- List order is occurred_at newest first, with event_id as a total tie-break.
CREATE INDEX audit_events_tenant_time_idx
    ON audit_events (tenant_id, occurred_at DESC, event_id DESC);

CREATE INDEX audit_events_tenant_action_time_idx
    ON audit_events (tenant_id, action, occurred_at DESC, event_id DESC);

-- One row per tenant, UTC day, and action. Updated only for rows that were inserted.
CREATE TABLE audit_daily_counts (
    tenant_id   TEXT NOT NULL,
    day         DATE NOT NULL,
    action      TEXT NOT NULL,
    event_count BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, day, action)
);

-- Newest UTC day for the summary window. The primary key starts with tenant_id,
-- so MAX(day) would otherwise scan every rollup row.
CREATE INDEX audit_daily_counts_day_idx
    ON audit_daily_counts (day DESC);

-- Highest committed ingest_seq per tenant. Avoids a scan of audit_events on each first page.
CREATE TABLE audit_tenant_seq (
    tenant_id      TEXT PRIMARY KEY,
    max_ingest_seq BIGINT NOT NULL
);

-- Bulk loader landing table. No primary key: duplicate event_ids are collapsed on insert.
CREATE UNLOGGED TABLE audit_events_staging (
    seq            BIGINT NOT NULL,
    event_id       TEXT NOT NULL,
    tenant_id      TEXT NOT NULL,
    actor_id       TEXT NOT NULL,
    actor_type     TEXT NOT NULL,
    actor_email    TEXT,
    action         TEXT NOT NULL,
    resource_type  TEXT NOT NULL,
    resource_id    TEXT NOT NULL,
    result         TEXT NOT NULL,
    ip             TEXT NOT NULL,
    metadata       TEXT NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL
);
