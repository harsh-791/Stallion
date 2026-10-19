# Design

## 1. Database choice

I picked PostgreSQL 16.

What I needed was simple: unique events, list-by-tenant in time order, and counts that don't double when a retry shows up. Postgres already has a primary key, `ON CONFLICT DO NOTHING`, and real transactions, so the insert and the daily rollup can commit together. The dataset is about 2M rows. That's not exotic for Postgres. `docker compose up` also just works with the official image, which mattered for the "fresh clone" requirement.

I did look at ClickHouse first. At 20M events/day and a year of retention it sounds right. I didn't use it here. Dedup on `event_id` is async there, so a retry can be visible twice until a merge. I also didn't want to fight keyset pagination on top of that. SQLite is out: one writer, and we're supposed to think about 3k events/sec with a loader next to the API. Mongo can unique-index `event_id`, but the rollup-in-the-same-transaction story and the ordered pages felt messier. Timescale would mainly help with partitions. At 2M rows that wasn't worth the extra moving parts.

So: Postgres for this assignment. Not because it's the only thing that can ever hold a year of data, but because it fits the constraints without drama.

## 2. Schema

`audit_events` has all 12 fields from the brief. I didn't drop any. `metadata` is `jsonb`, `actor_email` can be null, `ip` is text. I added `received_at` and `ingest_seq`.

`ingest_seq` is a normal `bigint`. The insert assigns it. It is not `GENERATED ... AS IDENTITY` and not a shared global sequence. Section 5 is why that matters.

Primary key is `event_id`. That's the idempotency key: first write wins, one row per id. List order is separate: `occurred_at DESC`, then `event_id DESC` so ties don't scramble pages. Indexes:

- `(tenant_id, occurred_at DESC, event_id DESC)` for the normal list
- `(tenant_id, action, occurred_at DESC, event_id DESC)` when there's an action filter

`audit_daily_counts` is `(tenant_id, day, action)` plus a count. `day` is the UTC calendar day of `occurred_at`. Summary reads this table; it does not `GROUP BY` the raw log on every request. Only rows that were actually inserted bump the count.

There's also `audit_daily_counts_day_idx` on `(day DESC)`. The primary key starts with `tenant_id`, so "what's the newest day in the whole database?" can't use it. Summary does `ORDER BY day DESC LIMIT 1` on that index, then keeps 30 UTC days ending on that day.

`audit_tenant_seq` stores the highest committed `ingest_seq` per tenant. First page needs that. I didn't want a `MAX(ingest_seq)` over a huge tenant every time.

Loader landing table is `audit_events_staging`: unlogged, no PK. Chunks land there (duplicates included), then we insert with `DISTINCT ON (event_id)`.

No partitioning for this dataset. Two million rows don't need it, and it would make the loader uglier. For a real year of data I'd partition by month on `occurred_at` and drop old partitions.

## 3. Pagination

Keyset, not `OFFSET`, for the API.

Cursor is opaque: base64url JSON, HMAC-SHA256 with `CURSOR_SECRET` (dev default in compose). It stores the last row's time + id, the watermark `as_of`, and the tenant / action / from / to / limit that produced it. Wrong signature or reused with different filters → 400.

Next page is basically:

```sql
WHERE tenant_id = $1
  AND ingest_seq <= as_of
  AND (occurred_at, event_id) < (cursor_time, cursor_id)
ORDER BY occurred_at DESC, event_id DESC
LIMIT 50
```

We fetch limit+1 so we know whether to return `next_cursor`.

First page has no cursor. One `REPEATABLE READ` transaction: read that tenant's `max_ingest_seq`, then the page. Later pages are new HTTP requests that keep the same `as_of`. RR on request 1 does not protect request 2. The write-path lock is what makes the watermark real.

Why not `OFFSET`? Newest-first list + insert at the top = every page shifts. You get duplicates or gaps. Keyset sticks to the sort key. Watermark keeps mid-walk inserts out until the client starts over at page one.

B2 still uses one untimed `OFFSET 99999` (with `ingest_seq <= as_of`) just to find the cursor for events 100,001–100,050. That's a bench helper, not how the API pages, and it's not in the B2 median.

`from` inclusive, `to` exclusive, UTC. Same filters and limit on every page or 400.

## 4. Write path

Ingest is `POST /v1/events`, max 1000 events, body capped at 2 MiB. Validate the whole batch first. Bad JSON / action / id / IP → 400, nothing written. Dedup repeated ids inside the batch in Go (keep first). Postgres hates the same conflict key twice in one insert.

Then: copy into a temp table, one transaction:

1. Lock each tenant's watermark row (`FOR UPDATE`), tenants sorted by id so multi-tenant batches don't deadlock.
2. Insert with `ON CONFLICT DO NOTHING`. New rows get `ingest_seq = locked_max + row_number`. Conflicts don't take a sequence. Gaps are fine.
3. Roll up only the rows that came back from the insert.
4. Bump `audit_tenant_seq` if the new max is higher.

Response is `inserted` / `duplicates`. Different body or tenant on the same id does not overwrite the stored row.

Loader does not call HTTP. Check SHA-256 first (`e968244bc33aaf0bc13c98d253f49819da1236620eae8cf51de31b7fb34d4dc1`), stream gzip NDJSON, `COPY` 50k rows into staging, same flush as the API. File line order is `seq`, so earliest line wins across chunks.

For speed it drops the two list indexes and the day index, then `FinishLoad` rebuilds them (even if the stream failed). `synchronous_commit` off only on that session, on again before indexes. Don't run list-heavy traffic during a load: inserts are still correct, list latency isn't.

## 5. Constraints

**Retries.** PK + `ON CONFLICT DO NOTHING`. Second delivery doesn't change the row or the count. Loader uses the same rule.

**Late / out of order (up to 10 minutes).** We store and sort by `occurred_at`, not arrival time. I don't reject "old" timestamps. The file is Aug–Sep 2026; a 10-minute cutoff would refuse the load. I treat 10 minutes as how producers behave, not as an ingest validator.

**Stable paging.** After page one, anything that commits later has a higher `ingest_seq` than `as_of`, so it stays out of the walk. Refresh page one to see it.

A global identity sequence would break this: A takes 100 and stalls, B takes 101 and commits, a reader snapshotted at 101 later sees 100 appear. So we lock the tenant watermark before assigning sequences and hold the lock until commit. Same tenant's batches queue. The big tenants (~60% of events) pay for that. I kept it. Silently skipping an audit row felt worse.

**Tenant isolation.** List and summary always filter `tenant_id` from the path. No cross-tenant route. Same `event_id` can't be rewritten onto another tenant. No auth in this build: URL tenant is trusted. Production would take tenant from the credential and (I'd add) RLS so a missing `WHERE` can't leak.

## 6. At production scale

Rough math from the brief: 20M/day ≈ 231/sec average, peak 3k/sec, ~7.3B events/year. Projections. I did not load-test 3k/sec.

What I actually ran is the 2M file. Fresh load ~34.8s, 2,001,032 lines → 1,980,386 rows kept (20,646 duplicate lines), 335,307 rollup rows, 800 tenants, DB ~1.18 GB. Medians of 10 (no warmup discard; B2 is final page only): B1 0.45 ms, B2 0.33 ms, B3 0.37 ms, B4 0.91 ms.

B4 was the surprise. Before the day index it was 5.33 ms because "newest day" scanned every rollup row. One index fixed it. Don't treat 1.18 GB @ 2M rows as a year of data.

From the code, first pain points are probably: heap + PK + two list indexes per insert, one watermark lock per tenant until commit, and hot `audit_daily_counts` rows. Latest page should stay cheap. Scanning raw events for summary would not.

What I'd change if this had to grow: queue in front (Kafka-ish) so ingest ack doesn't wait on indexes; monthly partitions + drop for retention; flush rollups about once a minute instead of every batch; split the hottest tenants if one primary still can't keep up; only then maybe ClickHouse for heavy analytics reads, with Postgres (or the log + unique id) still owning idempotency. Read replicas help reads once writes are the bottleneck; they don't fix hot rollup rows or disk growth.

## 7. Assumptions

Stuff the brief didn't spell out:

- No authentication; path `tenant_id` is trusted.
- Append-only. No update/delete API.
- Batch all-or-nothing.
- Max batch 1000, default page 50, max page 200, body 2 MiB, metadata JSON object ≤ 16 KiB.
- Newest = `occurred_at`, tie-break `event_id` DESC.
- "Last 30 days" = 30 UTC days ending on the newest day in `audit_daily_counts` (global, not per tenant). For this file: 1–30 Sep 2026. Sparse buckets; success and failure both count.
- Duplicate id with different body: ignore; first stored body wins.
- `actor_email` null ok; non-null only for `actor_type=user`.
- IPv4 only.
- Old timestamps are allowed; lateness is a sort problem, not a reject rule.
- Client must resend the same filters/limit with the cursor.

## 8. AI usage

I used Cursor (agent / Grok) a lot for the code: API, schema, compose, loader, tests, bench tool. It also drafted early versions of this doc. Later we (me + the agent) fixed the identity/`ingest_seq` race with the watermark lock, signed cursors, loaded the dataset, recorded benches, and added the day index after `EXPLAIN` showed the summary scan.

I still need to be able to explain the choices without the tool: why Postgres, why `event_id` as PK, why keyset + lock, why a rollup table. Load time, counts, and B1–B4 numbers are from commands on this machine, not invented.
