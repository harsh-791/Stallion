# Audit log service

Multi-tenant audit log. Calling services post batches of events. A tenant can list their own events, newest first, and can ask for a per-action daily count over the last 30 days.

The API and Postgres start together. The loader and the benchmark tool are separate commands so `docker compose up` does not try to load data.

## Setup

Docker with Compose v2 is enough. Go on the host is not required.

```bash
docker compose up
```

That builds the API image on first run, starts Postgres 16, and serves the API on port 8080. Postgres is published on port 5432. The loader runs as a container and reaches the database on the Compose network, so you do not need a Postgres client installed.

Stop it with Ctrl-C, or `docker compose down` from another shell. `docker compose down -v` deletes the database volume.

## Load the dataset

Put the provided file at `data/audit_events.ndjson.gz`. It is gitignored. The loader checks SHA-256 `e968244bc33aaf0bc13c98d253f49819da1236620eae8cf51de31b7fb34d4dc1` and stops if the file does not match.

```bash
mkdir -p data
# copy audit_events.ndjson.gz into data/
docker compose --profile load run --rm load
```

The loader streams the gzip file and bulk-inserts with `COPY`, 50,000 rows at a time. Duplicate `event_id`s are kept once (the earliest line in the file). It drops the two list indexes during the load and recreates them at the end. The `load complete` log line includes that index build. `seconds` is the load time to record below. Re-running the loader is safe: existing `event_id`s are skipped.

## Call the API

Ingest a batch. A repeated `event_id` counts as a duplicate and does not change the stored row.

```bash
curl -s -X POST http://localhost:8080/v1/events \
  -H 'content-type: application/json' \
  -d '{"events":[{"event_id":"0b6c1f0e-6d2a-4c3e-9a51-2f7d8e4b1c90","tenant_id":"t_0003","actor_id":"usr_7f2k9q","actor_type":"user","actor_email":"priya.n@northwind.example","action":"invoice.delete","resource_type":"invoice","resource_id":"inv_55120","result":"success","ip":"203.0.113.24","metadata":{"invoice_number":"INV-2026-0912","amount":1840.5,"currency":"USD","reason":"duplicate","request_id":"req_9c1d2e"},"occurred_at":"2026-09-14T08:31:07.412Z"}]}'
```

List one tenant, newest `occurred_at` first. `action`, `from`, and `to` are optional and can be combined. `from` is inclusive and `to` is exclusive. Page size defaults to 50 and cannot exceed 200. Pass `cursor` from `next_cursor` to get the next page. The cursor is signed and is bound to the tenant, the filters, and the page size. Sending it with a different tenant, filter, or limit returns 400. There is no authentication in this assignment: the tenant id in the URL is trusted.

`DATABASE_URL` is the Postgres URL. `CURSOR_SECRET` is optional and defaults to a dev value shared by the API and the bench tool. `PORT` defaults to 8080.

```bash
curl -s 'http://localhost:8080/v1/tenants/t_0001/events?limit=50'
curl -s 'http://localhost:8080/v1/tenants/t_0001/events?limit=50&action=invoice.delete&from=2026-09-01T00:00:00Z&to=2026-10-01T00:00:00Z'
```

Summary for the 30 UTC days ending on the newest event day in the database:

```bash
curl -s 'http://localhost:8080/v1/tenants/t_0001/summary'
```

Health check: `curl -s http://localhost:8080/healthz` or `curl -s http://localhost:8080/health`

To turn the page, copy `next_cursor` from the first response and repeat the same query string plus `cursor`:

```bash
curl -s 'http://localhost:8080/v1/tenants/t_0001/events?limit=50&cursor=PASTE_NEXT_CURSOR'
```

Summary response shape:

```json
{"tenant_id":"t_0001","window_start":"2026-09-01","window_end":"2026-09-30","buckets":[{"action":"invoice.delete","day":"2026-09-14","count":1}]}
```

## Benchmarks

With the stack up and the dataset loaded:

```bash
docker compose --profile bench run --rm bench
```

The tool issues 10 GETs per query and prints the median. The median is the average of the 5th and 6th sorted sample. Timing includes reading the response body. There is no discarded warmup.

| ID | Query | Median of 10 |
| --- | --- | --- |
| B1 | `GET /v1/tenants/t_0001/events?limit=50` | 0.45 ms |
| B2 | same list, the page of events 100,001–100,050 | 0.33 ms |
| B3 | `action=invoice.delete`, `from=2026-09-01T00:00:00Z`, `to=2026-10-01T00:00:00Z`, first page, limit 50 | 0.37 ms |
| B4 | `GET /v1/tenants/t_0001/summary` | 0.91 ms |

B2 is one API call. The tool finds the cursor with a single untimed query, the same order the API uses:

```sql
SELECT occurred_at, event_id
FROM audit_events
WHERE tenant_id = 't_0001' AND ingest_seq <= :as_of
ORDER BY occurred_at DESC, event_id DESC
OFFSET 99999 LIMIT 1
```

That row is event 100,000 within the list snapshot (0-based offset 99999). On this load the page starts at `bbb9e108-c27f-4a34-91b8-4772d5dd998d`. The timed request sends the previous row as `cursor`, with `as_of` set to that tenant's max `ingest_seq`, so the page is events 100,001–100,050 of the snapshot. The tool checks the first and last ids against the same `ingest_seq` filter at `OFFSET 100000` and `OFFSET 100049`. Finding the cursor is not part of the median.

The bench output also prints `disk <table>_bytes` and `disk total_bytes`. `total_bytes` is `pg_total_relation_size` of `audit_events`, `audit_daily_counts`, and `audit_tenant_seq`, which includes indexes and TOAST.

### This machine

| | |
| --- | --- |
| Machine | MacBook Air, Apple M5, 10 CPU, 16 GB RAM (`hw.memsize` 17179869184) |
| Docker memory | Docker VM `MemTotal` 8,392,605,696 bytes, 10 CPUs. `stallion-api-1` and `stallion-postgres-1` have `HostConfig.Memory` 0, so neither container has its own memory cap. |
| Load time | 34.836 s on a fresh database, including index rebuild. A second run inserted 0 rows in 23.974 s. |
| Database size | Bench `disk total_bytes` 1,169,113,088 (events + daily counts + tenant seq, including indexes). |
| B1–B4 | Measured 19 Oct 2026 on the loaded dataset, from inside the Compose network. Raw lines are in `bench-results.txt`. |

The bench tool times each HTTP call with `time.Since` after the body is read. It does not discard a warmup. The median of 10 samples is the average of the 5th and 6th when sorted. Samples, in call order:

- B1: 0.87, 0.50, 0.42, 0.36, 0.38, 0.39, 0.34, 4.21, 2.66, 0.49
- B2: 0.38, 0.39, 0.66, 0.33, 0.40, 0.32, 0.29, 0.30, 0.28, 0.31
- B3: 0.50, 0.36, 0.34, 0.34, 0.33, 0.45, 0.37, 3.36, 0.38, 0.34
- B4: 6.90, 0.93, 0.86, 4.55, 0.84, 1.06, 0.80, 1.12, 0.90, 0.78

`EXPLAIN (ANALYZE, BUFFERS)` after the load, with the pages already in shared buffers: B1, B2, and B3 are index scans (`audit_events_tenant_time_idx` or `audit_events_tenant_action_time_idx`) and returned 51 rows in about 0.02–0.03 ms inside Postgres. The summary window is `ORDER BY day DESC LIMIT 1` on `audit_daily_counts_day_idx`, an index-only scan, then the tenant's 660 buckets come from the primary key. Before that index, the same summary median was 5.33 ms because `MAX(day)` scanned all 335,307 rollup rows.

Fresh load of `data/audit_events.ndjson.gz`, checksum `e968244bc33aaf0bc13c98d253f49819da1236620eae8cf51de31b7fb34d4dc1`:

| | |
| --- | --- |
| Input lines | 2,001,032 |
| Unique `event_id`s inserted | 1,980,386 |
| Duplicate lines in the file | 20,646 |
| Rows skipped because they were already stored | 0 on the first run |
| Rollup rows | 335,307 |
| Rollup sum | 1,980,386, matching `count(*)` from `audit_events` |
| Tenants | 800 |
| `t_0001` events | 396,568 |
| Event days | 2026-08-01 through 2026-09-30 UTC |
| Summary window | 2026-09-01 through 2026-09-30 |
| `t_0001` events in that window | 198,615 in both the rollup and a raw count |

A second load inserted 0 rows. Its log says `duplicates=2001032` because that field is `lines - inserted`, so a rerun counts every line as already stored. The file itself has 20,646 duplicate lines, not 2,001,032. After the rerun, `audit_events` was still 1,980,386 rows and no rollup bucket disagreed with a raw `GROUP BY`.

## Known limitations

- Tenant ids in the URL are not authenticated.
- Ingest for one tenant takes a row lock on that tenant's watermark until the batch commits. That is what makes paging stable. A hot tenant's batches queue behind each other.
- The loader drops the two list indexes and builds them at the end. Do not expect list latency to stay low while a load is in progress. If the process is killed after the drop, run the loader again. `FinishLoad` recreates the indexes, and `ON CONFLICT DO NOTHING` skips rows already stored.
- `synchronous_commit` is off only on the loader's database session, then turned back on before the indexes are built.

## Counts and size

```bash
docker compose exec postgres psql -U audit -d audit -c "SELECT count(*) FROM audit_events;"
docker compose exec postgres psql -U audit -d audit -c "SELECT c.relname, pg_total_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind = 'r';"
```

## Tests

The database tests use a separate database name and do not truncate the loaded dataset.

Go is not required on the host. This is the command that was run:

```bash
docker run --rm -v "$PWD":/src -w /src \
  -e DATABASE_URL='postgres://audit:audit@host.docker.internal:5432/audit?sslmode=disable' \
  golang:1.23-alpine go test ./...
```

## Layout

- `cmd/api` HTTP server
- `cmd/load` bulk loader
- `cmd/bench` the four timed queries
- `internal/store` SQL
- `db/init.sql` schema created on a fresh Postgres volume
