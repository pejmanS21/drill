# divar-scraper

Fetches Divar listings into an append-only raw JSONL store plus mergeable CSV batches.

## Why v0.2 exists

The first run (`divar_data/`, 9,386 rows, Tehran + Karaj) has two defects:

1. **The category filter was silently dropped.** Only 918 rows have کارکرد and 1,160 have
   برند و مدل — the rest is Divar's unfiltered general feed (the 225-column union includes
   ودیعه, نوع کفش, نوع جونده, وضعیت سربازی). Cause: each search page echoed the server's
   `search_data` back verbatim, and the category fell out of it.
2. **Failures were unrecoverable.** Permanent failures went to stderr and nowhere else, so
   the ~700 listings lost to rate limits were never written down and cannot be retried.

Both are fixed. `divar_data/` is kept as-is for reference; v0.2 writes to `divar_data_iran/`.

## Output layout

| File | Role |
|---|---|
| `raw.jsonl` | Append-only. Full unmodified detail JSON per listing. Source of truth. |
| `failures.jsonl` | Outstanding permanent failures. Rewritten each run, not appended. |
| `batch_NNNNN.csv` | Rebuilt from `raw.jsonl` every run, **identical header in every file**. |

Storing the raw payload means a new field never requires a re-scrape — change the flattener
and re-run `--export-only` offline.

## Usage

`--category auto` is the whole vehicle tree — cars, motorcycles, heavy, classic, parts.

```sh
# 1. Confirm the filter is applied (one request, no detail fetches)
divar-scraper --dry-run --category auto --city-ids iran

# 2. Full run. Pilots 200 listings, verifies them, then fetches the rest and
#    sweeps its own failures up to 3 times.
divar-scraper --count 20000 --category auto --city-ids iran

# 3. Keep working anything still outstanding
divar-scraper --retry-failures

# 4. Offline: rebuild CSVs / re-audit an existing raw.jsonl
divar-scraper --export-only
divar-scraper --audit-only
```

Re-running step 2 is safe: tokens already in `raw.jsonl` are skipped.

## Price history: `--observe` (run daily)

Skipping seen tokens makes re-runs safe, but it also means a re-run records **no history**.
`--observe` is the other half: it re-visits every listing already in `raw.jsonl` and appends
one small dated line per listing to `observations.jsonl`. It never writes to `raw.jsonl`.

```sh
divar-scraper --observe                     # every known listing
divar-scraper --observe --observe-max 20000 # a fixed panel: the first 20k, same ones every day
```

```json
{"token":"gaimMkek","observed_at":1789649189,"status":"live","price":550000000}
{"token":"gXyz1234","observed_at":1789649189,"status":"removed","reason":"post removed (http 404 Not Found)"}
```

- `removed` is only written on a direct 404/410 for that token. Absence from the search feed
  proves nothing, because Divar caps a single search stream.
- A listing that could not be reached (429, timeout) gets **no line**, so a network problem is
  never read as a removal. If more than half the panel is unreachable the run exits non-zero:
  treat that day as a gap.
- Once removed, a token is not visited again.
- `price` is toman (it matches the "… تومان" text Divar displays), `null` when the listing has no fixed price. Divar round-trips prices through
  a 32-bit float (`2150000128` for 2.15 billion), so round when reading; the file stores what
  Divar sent.
- Safe to run while a crawl is appending to `raw.jsonl`: an incomplete final line is ignored.

History cannot be backfilled. A missed day is gone.

## How failures are handled

1. Transient errors retry in-request (8×, exponential backoff + jitter, `Retry-After` honoured).
2. 404/410 are recognised as dead posts and not retried.
3. Whatever still fails is written to `failures.jsonl`.
4. The run then sweeps that queue automatically, up to `--retry-passes` times, each sweep
   starting at 2ⁿ× the current throttle — these tokens already lost a race with the rate
   limiter, so repeating the pace that caused it would just repeat it.
5. Anything left survives in `failures.jsonl` for a later `--retry-failures`.

## Rate limiting

A single shared throttle adapts to the server instead of fighting it: +50% on every HTTP 429
(capped by `--max-delay-ms`), -10% after 50 clean fetches. Backoff is exponential with jitter,
`Retry-After` is honoured, and 404/410 are treated as dead posts rather than retried 8 times.

## Category audit

Each result's own `web_info.category_slug` is compared against an accepted **set**, and the
run aborts below `--min-category-purity` (default 0.9). This is the guard the first run lacked.

Divar's slugs form a tree and `auto` is the parent of the vehicle subtree, so `--category auto`
accepts `light`, `heavy`, `classic`, `motorcycles`, `auto-parts`… Substring matching would get
this backwards — `light` shares no substring with `auto` while `auto-parts` does. Override the
set with `--accept-categories light,heavy`, or skip the check with `--min-category-purity 0`.

Because those slug names are Divar's private vocabulary and unverified here, the slug check
**warns by default** (`--min-category-purity 0`). Raise it once `--dry-run` shows the real slugs.

## Payload audit — the guard that actually bites

Slug names change; payloads do not. Before the full run, a `--pilot` batch (200 listings) is
fetched and checked for vehicle spec keys (کارکرد, حجم موتور, گیربکس, برند و مدل…). Below
`--min-vehicle-purity` (0.7) the run aborts and prints sample non-vehicle titles. A broken
filter costs 200 requests instead of 20,000.

Verified against the first run's own data: it scores 12.5% and aborts. The check is spec-based,
not slug-based, so motorcycles, heavy vehicles and classics pass alongside cars.

`برند و مدل` is treated as a *weak* signal: Divar uses it for handsets too. It counts as a
vehicle only when the payload does not also carry مقدار رم / تعداد سیم‌کارت / حافظهٔ داخلی —
which keeps rental-car listings (no کارکرد) in and iPhones out.

On a resumed run the pilot is scored on the pilot tokens alone, never the whole `raw.jsonl`,
so thousands of earlier good rows cannot mask a newly-broken filter.

## Failure queue semantics

`failures.jsonl` is the outstanding-work queue, deduped by token:

- A **normal run** merges: earlier failures are kept unless they now appear in `raw.jsonl`.
  It never truncates the queue — that is what lost the first run's ~700 records.
- A **`--retry-failures` run** rewrites: recovered tokens leave, so the file can drain.
