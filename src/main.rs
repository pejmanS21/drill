use anyhow::{bail, Context, Result};
use clap::Parser;
use futures::stream::{self, StreamExt};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) drill/0.2";
const SEARCH_URL: &str = "https://api.divar.ir/v8/postlist/w/search";
const POST_URL: &str = "https://api.divar.ir/v8/posts-v2/web";

const RAW_FILE: &str = "raw.jsonl";
const FAIL_FILE: &str = "failures.jsonl";
const OBS_FILE: &str = "observations.jsonl";
/// Error prefix for a 404/410 post. --observe reads it to tell "removed" from
/// "could not reach", so both sides share this constant.
const DEAD_PREFIX: &str = "post removed";

#[derive(Parser, Debug)]
#[command(name = "drill", about = "Fetch Divar listings into raw JSONL + batched CSV")]
struct Args {
    /// Total number of listings (آگهی) to fetch
    #[arg(short, long, default_value_t = 10000)]
    count: usize,

    /// Rows per CSV batch file
    #[arg(short, long, default_value_t = 500)]
    batch_size: usize,

    /// Output directory (holds raw.jsonl, failures.jsonl, batch_*.csv)
    #[arg(short, long, default_value = "divar_data_iran")]
    out_dir: String,

    /// Divar city ids, comma separated. "iran" = whole country.
    #[arg(long, default_value = "iran")]
    city_ids: String,

    /// Divar API category value(s), comma separated. NOT the URL slug --
    /// run --probe-categories to find valid values.
    #[arg(long, default_value = "cars")]
    category: String,

    /// Concurrent detail-page fetches
    #[arg(long, default_value_t = 5)]
    concurrency: usize,

    /// Delay between search-page requests, ms
    #[arg(long, default_value_t = 400)]
    page_delay_ms: u64,

    /// Starting per-request throttle delay, ms. Adapts upward on 429.
    #[arg(long, default_value_t = 350)]
    detail_delay_ms: u64,

    /// Ceiling for the adaptive throttle, ms
    #[arg(long, default_value_t = 8000)]
    max_delay_ms: u64,

    /// Max retries per request before recording a permanent failure
    #[arg(long, default_value_t = 8)]
    max_retries: u32,

    /// Base backoff on failure/429, ms (grows exponentially, with jitter)
    #[arg(long, default_value_t = 1500)]
    backoff_ms: u64,

    /// Re-fetch only the tokens in failures.jsonl, then exit
    #[arg(long)]
    retry_failures: bool,

    /// Rebuild the CSV batches from raw.jsonl and exit (no network)
    #[arg(long)]
    export_only: bool,

    /// Run the vehicle-payload audit against an existing raw.jsonl and exit
    #[arg(long)]
    audit_only: bool,

    /// Print Divar's city id list and exit (fallback if whole-country fails)
    #[arg(long)]
    list_cities: bool,

    /// Try candidate category values against the API and exit. Divar's URL slugs
    /// are not its API enum, so this is how the valid values are found.
    #[arg(long)]
    probe_categories: bool,

    /// Fetch one search page, print the category breakdown, and exit
    #[arg(long)]
    dry_run: bool,

    /// Abort if fewer than this fraction of collected tokens match a known
    /// vehicle slug. Default 0 = warn only, because Divar's child-slug names
    /// are unverified; --min-vehicle-purity is the guard that actually bites.
    #[arg(long, default_value_t = 0.0)]
    min_category_purity: f64,

    /// Category slugs to accept, comma separated. Empty = derive from --category
    /// (for a parent slug like "auto" that means its whole subtree).
    #[arg(long, default_value = "")]
    accept_categories: String,

    /// Verify the filter on this many listings before the full run. 0 disables.
    #[arg(long, default_value_t = 200)]
    pilot: usize,

    /// Abort if fewer than this fraction of fetched listings carry vehicle
    /// specs. Vocabulary-independent, unlike the slug check. 0 disables.
    #[arg(long, default_value_t = 0.7)]
    min_vehicle_purity: f64,

    /// Automatic retry sweeps over failures.jsonl after the main pass
    #[arg(long, default_value_t = 3)]
    retry_passes: u32,

    /// Re-visit every listing already in raw.jsonl and append one dated line
    /// (live + price, or removed) to observations.jsonl, then exit. Run daily:
    /// this is the only source of price history and time-on-market.
    #[arg(long)]
    observe: bool,

    /// Observe only the first N listings of raw.jsonl (file order, so the same
    /// panel is followed every day). 0 = all.
    #[arg(long, default_value_t = 0)]
    observe_max: usize,
}

// ---------------------------------------------------------------- throttle

/// One shared delay every detail request waits out. Grows on 429 (additive
/// increase), decays on sustained success — so a country-wide run finds the
/// rate limit instead of burning 700 records against it like the last one did.
struct Throttle {
    delay_ms: AtomicU64,
    max_ms: u64,
    ok_streak: AtomicU64,
}

impl Throttle {
    fn new(start_ms: u64, max_ms: u64) -> Self {
        Self { delay_ms: AtomicU64::new(start_ms), max_ms, ok_streak: AtomicU64::new(0) }
    }

    fn current(&self) -> u64 {
        self.delay_ms.load(Ordering::Relaxed)
    }

    async fn wait(&self) {
        tokio::time::sleep(Duration::from_millis(self.current())).await;
    }

    /// Hit a 429: back the whole run off by 50%, capped.
    fn penalize(&self) {
        self.ok_streak.store(0, Ordering::Relaxed);
        let cur = self.current();
        let next = ((cur * 3) / 2).clamp(50, self.max_ms);
        if next != cur {
            self.delay_ms.store(next, Ordering::Relaxed);
            eprintln!("  [throttle] 429 -> delay {cur}ms => {next}ms");
        }
    }

    /// Start a retry sweep deliberately slow -- these tokens already lost a race
    /// with the rate limiter, so hammering them at the same pace repeats it.
    fn slow_start(&self, pass: u32) {
        let next = (self.current() * 2u64.pow(pass.min(3))).min(self.max_ms);
        self.delay_ms.store(next, Ordering::Relaxed);
        self.ok_streak.store(0, Ordering::Relaxed);
    }

    /// 50 clean fetches in a row: shave 10% off, never below 50ms.
    fn relax(&self) {
        if self.ok_streak.fetch_add(1, Ordering::Relaxed) < 50 {
            return;
        }
        self.ok_streak.store(0, Ordering::Relaxed);
        let cur = self.current();
        let next = ((cur * 9) / 10).max(50);
        if next != cur {
            self.delay_ms.store(next, Ordering::Relaxed);
        }
    }
}

/// Exponential backoff with jitter. ponytail: nanos-as-jitter, no rand dep.
fn backoff_delay(base_ms: u64, attempt: u32) -> Duration {
    let exp = base_ms.saturating_mul(1u64 << attempt.min(6));
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 % (exp / 4 + 1))
        .unwrap_or(0);
    Duration::from_millis((exp + jitter).min(120_000))
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

// ------------------------------------------------------------------ search

/// Build a search body that ALWAYS carries the category filter. The previous
/// version echoed the server's `search_data` back verbatim; the filter fell out
/// and 88% of the last run was not cars. Re-injecting it every page makes that
/// impossible.
/// Divar rejects `city_ids: ["iran"]` with 400, so the whole-country request
/// addresses cities some other way. Rather than guess, the candidates are tried
/// once at startup and the first that answers 200 is used for the whole run.
#[derive(Clone, Debug, PartialEq)]
enum CityMode {
    /// Explicit numeric city ids.
    Ids(Vec<String>),
    /// No city field at all.
    Omit,
    /// A `cities` field carrying slugs rather than ids.
    Slugs(Vec<String>),
}

impl CityMode {
    fn apply(&self, body: &mut Value) {
        match self {
            CityMode::Ids(ids) => body["city_ids"] = json!(ids),
            CityMode::Omit => {}
            CityMode::Slugs(s) => body["cities"] = json!(s),
        }
    }

    fn describe(&self) -> String {
        match self {
            CityMode::Ids(ids) if ids.len() > 6 => format!("city_ids: {} ids", ids.len()),
            CityMode::Ids(ids) => format!("city_ids: {ids:?}"),
            CityMode::Omit => "no city field (whole country)".into(),
            CityMode::Slugs(s) => format!("cities: {s:?}"),
        }
    }
}

/// `--city-ids iran` / `all` means "everywhere"; anything else is taken literally.
fn city_candidates(raw: &str) -> Vec<CityMode> {
    let ids: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let whole_country = ids.len() == 1 && matches!(ids[0].as_str(), "iran" | "all" | "0");
    if !whole_country {
        return vec![CityMode::Ids(ids)];
    }
    vec![
        CityMode::Omit,
        CityMode::Ids(vec!["0".into()]),
        CityMode::Slugs(vec!["iran".into()]),
    ]
}

/// Divar's URL slugs and its API category enum are different vocabularies:
/// `/s/iran/auto` exists as a URL, but the API answers `invalid category: auto`.
/// Since the server validates the value and rejects instantly, the cheapest way
/// to learn the vocabulary is to ask it.
const CATEGORY_CANDIDATES: &[&str] = &[
    "vehicles", "auto", "cars", "car", "light", "heavy", "classic", "motorcycles",
    "auto-parts", "car-rental", "rent-car", "vans", "trucks", "buses",
    "light-car", "heavy-car", "automobile", "vehicle",
];

/// Try each candidate against the live API and report which are accepted, with
/// the page-1 slug breakdown so a parent can be told from a leaf.
async fn probe_categories(client: &Client, city: &CityMode, extra: &str) -> Result<()> {
    let mut candidates: Vec<String> = CATEGORY_CANDIDATES.iter().map(|s| s.to_string()).collect();
    for c in extra.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if !candidates.iter().any(|x| x == c) {
            candidates.push(c.to_string());
        }
    }

    let mut valid: Vec<String> = Vec::new();
    for cat in &candidates {
        let body = search_body(city, cat, None);
        match search_page(client, &body, 0, 0).await {
            Ok(resp) => {
                let toks: Vec<(String, String)> = resp
                    .get("list_widgets")
                    .and_then(|v| v.as_array())
                    .map(|ws| ws.iter().filter_map(token_and_category).collect())
                    .unwrap_or_default();
                let mut breakdown: BTreeMap<String, usize> = BTreeMap::new();
                for (_, slug) in &toks {
                    *breakdown.entry(if slug.is_empty() { "?".into() } else { slug.clone() }).or_default() += 1;
                }
                println!("  OK   {cat:14} {:3} listings  {breakdown:?}", toks.len());
                valid.push(cat.clone());
            }
            Err(e) => {
                let msg = e.to_string();
                let reason = if msg.contains("invalid category") { "invalid category" } else { "error" };
                println!("  no   {cat:14} {reason}");
            }
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    if valid.is_empty() {
        bail!("no candidate category was accepted; check divar.ir's own network tab");
    }
    eprintln!("\naccepted: {}", valid.join(","));
    eprintln!(
        "Pick the ones whose breakdown is vehicles, then run:\n  --category {}",
        valid.join(",")
    );
    Ok(())
}

/// Try each candidate once; keep the first that the API accepts.
async fn resolve_city_mode(client: &Client, raw: &str, category: &str) -> Result<CityMode> {
    let candidates = city_candidates(raw);
    if candidates.len() == 1 {
        return Ok(candidates.into_iter().next().unwrap());
    }

    eprintln!("resolving how to ask for the whole country...");
    let mut errors = Vec::new();
    for mode in candidates {
        let body = search_body(&mode, category, None);
        match search_page(client, &body, 0, 0).await {
            Ok(resp) => {
                let n = resp.get("list_widgets").and_then(|v| v.as_array()).map_or(0, |a| a.len());
                if n == 0 {
                    errors.push(format!("{}: accepted but returned 0 listings", mode.describe()));
                    continue;
                }
                eprintln!("  OK -> {} ({n} listings on page 1)", mode.describe());
                return Ok(mode);
            }
            Err(e) => {
                eprintln!("  no  -> {}: {}", mode.describe(), e.to_string().lines().next().unwrap_or(""));
                errors.push(format!("{}: {e}", mode.describe()));
            }
        }
    }
    bail!(
        "could not work out how to request the whole country. Tried:\n  {}\n\
         Fall back to explicit ids: --list-cities to dump them, then \
         --city-ids 1,2,3,...",
        errors.join("\n  ")
    )
}

fn search_body(city: &CityMode, category: &str, prev: Option<(&Value, &Value)>) -> Value {
    let mut search_data = match prev {
        Some((sd, _)) if !sd.is_null() => sd.clone(),
        _ => json!({ "form_data": { "data": {} } }),
    };
    force_category(&mut search_data, category);

    let mut body = json!({ "search_data": search_data });
    city.apply(&mut body);
    if let Some((_, pd)) = prev {
        if !pd.is_null() {
            body["pagination_data"] = pd.clone();
        }
    }
    body
}

fn force_category(search_data: &mut Value, category: &str) {
    if !search_data.is_object() {
        *search_data = json!({});
    }
    let data = search_data
        .as_object_mut()
        .unwrap()
        .entry("form_data")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .map(|fd| fd.entry("data").or_insert_with(|| json!({})));
    if let Some(data) = data {
        if !data.is_object() {
            *data = json!({});
        }
        data["category"] = json!({ "str": { "value": category } });
    }
}

/// Each result carries its own category; that is the ground truth we audit
/// against, not what we asked for.
fn token_and_category(widget: &Value) -> Option<(String, String)> {
    let payload = widget.get("data")?.get("action")?.get("payload")?;
    let token = payload.get("token")?.as_str()?.to_string();
    let cat = payload
        .get("web_info")
        .and_then(|w| w.get("category_slug").or_else(|| w.get("category")))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    Some((token, cat))
}

async fn search_page(client: &Client, body: &Value, max_retries: u32, backoff_ms: u64) -> Result<Value> {
    let mut attempt = 0u32;
    loop {
        let outcome = async {
            let resp = client.post(SEARCH_URL).json(body).send().await?;
            let status = resp.status();
            if status == StatusCode::TOO_MANY_REQUESTS {
                bail!("rate limited (429) on search page");
            }
            // A 4xx means OUR request is malformed. Retrying it eight times
            // just prints the same error eight times.
            if status.is_client_error() {
                let detail = resp.text().await.unwrap_or_default();
                bail!(
                    "search page http status {status} -- the request body is wrong, not the \
                     network; retrying will not help. Server said: {}",
                    detail.chars().take(300).collect::<String>()
                );
            }
            if !status.is_success() {
                bail!("search page http status {status}");
            }
            resp.json::<Value>().await.context("search response not JSON")
        }
        .await;

        match outcome {
            Ok(json) => return Ok(json),
            Err(e) if e.to_string().contains("request body is wrong") => return Err(e),
            Err(e) => {
                attempt += 1;
                if attempt > max_retries {
                    return Err(e);
                }
                let wait = backoff_delay(backoff_ms, attempt);
                eprintln!("search retry {attempt}/{max_retries}: {e:#} (waiting {}ms)", wait.as_millis());
                tokio::time::sleep(wait).await;
            }
        }
    }
}

/// Page the search feed until we have `count` tokens or the feed runs dry.
/// Returns (token, category_slug) pairs, deduped.
async fn collect_tokens(
    client: &Client,
    city: &CityMode,
    category: &str,
    count: usize,
    args: &Args,
) -> Result<Vec<(String, String)>> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut prev: Option<(Value, Value)> = None;
    let mut empty_pages = 0u32;

    loop {
        let body = search_body(city, category, prev.as_ref().map(|(a, b)| (a, b)));
        let resp = search_page(client, &body, args.max_retries, args.backoff_ms).await?;

        let widgets = resp.get("list_widgets").and_then(|v| v.as_array()).cloned().unwrap_or_default();

        let mut got_any = false;
        for w in &widgets {
            if let Some((tok, cat)) = token_and_category(w) {
                if seen.insert(tok.clone()) {
                    out.push((tok, cat));
                    got_any = true;
                }
            }
        }

        eprintln!("  collected {} / {} tokens", out.len(), count);

        if out.len() >= count {
            break;
        }

        let has_next = resp
            .get("pagination")
            .and_then(|p| p.get("has_next_page"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // A page of pure duplicates is not proof the feed ended; tolerate a few.
        if !got_any {
            empty_pages += 1;
        } else {
            empty_pages = 0;
        }

        if !has_next || empty_pages >= 3 {
            eprintln!(
                "WARNING: feed exhausted at {} tokens, {} were requested. \
                 Divar caps a single search stream; shard by city or by brand/year to go deeper.",
                out.len(),
                count
            );
            break;
        }

        prev = Some((
            resp.get("search_data").cloned().unwrap_or(Value::Null),
            resp.get("pagination").and_then(|p| p.get("data")).cloned().unwrap_or(Value::Null),
        ));

        tokio::time::sleep(Duration::from_millis(args.page_delay_ms)).await;
    }

    out.truncate(count);
    Ok(out)
}

/// Divar category slugs are a tree. `/s/iran/auto` is the PARENT of the whole
/// vehicle subtree, so its results legitimately carry child slugs. Substring
/// matching gets this exactly backwards: "light" shares no substring with
/// "auto" (rejected, though it is what we want) while "auto-parts" does
/// (accepted, though it is tyres and mirrors). So: an explicit set.
fn accepted_slugs(category: &str) -> BTreeSet<String> {
    const VEHICLE_SUBTREE: &[&str] = &[
        "auto", "light", "heavy", "classic", "motorcycles", "auto-parts",
        "car-rental", "vehicles", "cars",
    ];
    let mut set = BTreeSet::new();
    set.insert(category.to_string());
    if category == "auto" || category == "vehicles" {
        set.extend(VEHICLE_SUBTREE.iter().map(|s| s.to_string()));
    }
    set
}

/// Fail loudly if the feed handed back something other than what we filtered
/// for. This is the check the last run did not have.
fn audit_categories(
    tokens: &[(String, String)],
    wanted: &str,
    accept: &BTreeSet<String>,
    min_purity: f64,
) -> Result<()> {
    if tokens.is_empty() {
        bail!("no tokens collected");
    }
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, c) in tokens {
        *counts.entry(c.as_str()).or_default() += 1;
    }
    eprintln!("category breakdown of collected tokens:");
    for (c, n) in &counts {
        let label = if c.is_empty() { "<unknown>" } else { c };
        eprintln!("  {label:20} {n}");
    }

    // Unknown means the feed did not label results, not that they are wrong.
    let unknown = counts.get("").copied().unwrap_or(0);
    if unknown == tokens.len() {
        eprintln!("NOTE: feed returned no category labels; purity cannot be checked here.");
        return Ok(());
    }

    let matched: usize = counts
        .iter()
        .filter(|(c, _)| accept.contains(**c))
        .map(|(_, n)| *n)
        .sum();
    let purity = matched as f64 / (tokens.len() - unknown).max(1) as f64;
    eprintln!("accepting slugs: {:?}", accept);
    eprintln!("category purity for '{wanted}': {:.1}%", purity * 100.0);

    if purity < min_purity {
        bail!(
            "category filter is not being applied: only {:.1}% of results are '{wanted}' \
             (threshold {:.0}%). This is exactly the bug that made the previous run 88% \
             non-car. Fix --category before scraping; --min-category-purity 0 overrides.",
            purity * 100.0,
            min_purity * 100.0
        );
    }
    Ok(())
}

/// Slug names are Divar's private vocabulary and change without notice, so the
/// slug audit alone is not something to stake a 20k run on. This check needs no
/// vocabulary: a vehicle listing carries vehicle specs. It is exactly the test
/// that exposed the first run (918 of 9,386 rows had کارکرد).
const VEHICLE_SPEC_KEYS: &[&str] = &[
    "کارکرد",
    "مدل (سال تولید)",
    "مدل (سال ساخت)",
    "نوع سوخت",
    "گیربکس",
    "وضعیت بدنه",
    "حجم موتور",
    "نوع موتور",
    "وضعیت فنی موتور و گیربکس",
    "بیمهٔ شخص ثالث",
    "مهلت بیمهٔ شخص ثالث",
    "سال تولید",
    "نوع کلاچ",
    "میزان فابریک بودن قطعات",
    "نوع کمک فنر",
    "ظرفیت بار/صندوق",
    "نوع وسیلهٔ نقلیه",
];

/// `برند و مدل` alone is NOT proof: Divar uses it for phones and laptops too
/// (verified against the first run -- of 248 rows carrying it without any strong
/// key, roughly half were handsets). But rental-car listings legitimately carry
/// it with no کارکرد, so it cannot simply be dropped. Accept it only when the
/// payload does not also look like consumer electronics.
const WEAK_VEHICLE_KEYS: &[&str] = &["برند و مدل"];
const NOT_VEHICLE_KEYS: &[&str] = &[
    "تعداد سیم\u{200c}کارت",
    "مقدار رم",
    "حافظهٔ داخلی",
    "حافظه داخلی",
    "سیستم عامل",
    "پردازنده",
    "اندازهٔ صفحه",
    "کارت حافظه",
    "متراژ",
    "ودیعه",
    "تعداد اتاق",
];

fn looks_like_a_vehicle(row: &Row) -> bool {
    let has = |keys: &[&str]| keys.iter().any(|k| row.dynamic.contains_key(*k));
    has(VEHICLE_SPEC_KEYS) || (has(WEAK_VEHICLE_KEYS) && !has(NOT_VEHICLE_KEYS))
}

/// Audit what was actually fetched, not what the feed claimed. Runs on a small
/// pilot batch so a broken filter costs 200 requests, not 20,000.
fn audit_fetched_payloads(
    out_dir: &str,
    min_purity: f64,
    only: Option<&HashSet<String>>,
) -> Result<()> {
    if min_purity <= 0.0 {
        return Ok(());
    }
    let records = read_jsonl(&format!("{out_dir}/{RAW_FILE}"))?;
    // On a resumed run raw.jsonl already holds thousands of good rows; scoring
    // all of them would drown a bad pilot and silently disable the guard.
    let rows: Vec<Row> = records
        .iter()
        .filter_map(|r| {
            let token = r.get("token")?.as_str()?;
            if only.is_some_and(|set| !set.contains(token)) {
                return None;
            }
            Some(flatten(token, r.get("detail")?, 0))
        })
        .collect();
    if rows.is_empty() {
        bail!("pilot batch fetched nothing -- cannot verify the category filter");
    }

    let vehicles = rows.iter().filter(|r| looks_like_a_vehicle(r)).count();
    let purity = vehicles as f64 / rows.len() as f64;
    eprintln!(
        "payload audit: {vehicles}/{} listings carry vehicle specs ({:.1}%)",
        rows.len(),
        purity * 100.0
    );

    if purity < min_purity {
        let sample: Vec<&str> = rows
            .iter()
            .filter(|r| !looks_like_a_vehicle(r))
            .take(3)
            .map(|r| r.fixed.get("title").map(|s| s.as_str()).unwrap_or("?"))
            .collect();
        bail!(
            "only {:.1}% of fetched listings are vehicles (need {:.0}%). \
             The category filter is not being applied -- this is the bug that made the \
             previous run 88% non-vehicle. Non-vehicle samples: {sample:?}. \
             Check `--dry-run --category <slug>`, or pass --min-vehicle-purity 0 to override.",
            purity * 100.0,
            min_purity * 100.0
        );
    }
    Ok(())
}

// ------------------------------------------------------------------ detail

enum FetchErr {
    /// Post is gone (404/410) — retrying will never help.
    Dead(String),
    Transient(String),
}

/// Fetch one ad's FULL detail JSON. No field selection happens here; whatever
/// Divar returns is what gets stored.
async fn fetch_detail(
    client: &Client,
    token: &str,
    throttle: &Throttle,
    max_retries: u32,
    backoff_ms: u64,
) -> std::result::Result<Value, String> {
    let url = format!("{POST_URL}/{token}");
    let mut attempt = 0u32;

    loop {
        throttle.wait().await;

        let outcome: std::result::Result<Value, FetchErr> = async {
            let resp = client
                .get(&url)
                .send()
                .await
                .map_err(|e| FetchErr::Transient(e.to_string()))?;
            let status = resp.status();

            if status == StatusCode::TOO_MANY_REQUESTS {
                throttle.penalize();
                // Honour Retry-After when the server bothers to send it.
                let ra = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok());
                if let Some(secs) = ra {
                    tokio::time::sleep(Duration::from_secs(secs.min(120))).await;
                }
                return Err(FetchErr::Transient("rate limited (429)".into()));
            }
            if status == StatusCode::NOT_FOUND || status == StatusCode::GONE {
                return Err(FetchErr::Dead(format!("{DEAD_PREFIX} (http {status})")));
            }
            if !status.is_success() {
                return Err(FetchErr::Transient(format!("http status {status}")));
            }
            resp.json::<Value>()
                .await
                .map_err(|e| FetchErr::Transient(format!("detail response not JSON: {e}")))
        }
        .await;

        match outcome {
            Ok(json) => {
                throttle.relax();
                return Ok(json);
            }
            Err(FetchErr::Dead(msg)) => return Err(msg),
            Err(FetchErr::Transient(msg)) => {
                attempt += 1;
                if attempt > max_retries {
                    return Err(format!("{msg} (gave up after {attempt} attempts)"));
                }
                let wait = backoff_delay(backoff_ms, attempt);
                eprintln!(
                    "  retry {attempt}/{max_retries} for {token}: {msg} (waiting {}ms, throttle {}ms)",
                    wait.as_millis(),
                    throttle.current()
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

// ----------------------------------------------------------------- parsing

/// Columns that always exist, in this order. Everything else becomes a
/// dynamic column harvested from the payload.
const FIXED_COLUMNS: &[&str] = &[
    "token",
    "url",
    "title",
    "subtitle",
    "description",
    "seo_title",
    "seo_description",
    "category",
    "city",
    "district",
    "posted_raw",
    "price_raw",
    "latitude",
    "longitude",
    "map_radius_m",
    "image_count",
    "image_urls",
    "thumbnail_urls",
    "tags",
    "fetched_at",
];

struct Row {
    fixed: BTreeMap<String, String>,
    dynamic: BTreeMap<String, String>,
}

fn s(v: Option<&Value>) -> String {
    match v {
        Some(Value::String(x)) => x.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

/// Walk the whole payload and harvest every {title, value} pair, whatever
/// widget or nesting level it lives at. Divar keeps adding widget types; this
/// picks up new ones for free instead of needing a new match arm each time.
fn harvest_pairs(v: &Value, out: &mut BTreeMap<String, String>) {
    match v {
        Value::Object(map) => {
            let title = map.get("title").and_then(|t| t.as_str());
            let value = map.get("value");
            if let (Some(k), Some(val)) = (title, value) {
                let text = match val {
                    Value::String(x) => x.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => String::new(),
                };
                if !k.trim().is_empty() && !text.trim().is_empty() {
                    out.entry(k.trim().to_string()).or_insert(text);
                }
            }
            for (_, child) in map {
                harvest_pairs(child, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                harvest_pairs(item, out);
            }
        }
        _ => {}
    }
}

/// Flatten a scalar-valued object into `prefix_key` columns (webengage and
/// friends carry numeric price/year/mileage that the display strings lose).
fn flatten_scalars(v: &Value, prefix: &str, out: &mut BTreeMap<String, String>) {
    if let Some(map) = v.as_object() {
        for (k, val) in map {
            match val {
                Value::String(_) | Value::Number(_) | Value::Bool(_) => {
                    out.insert(format!("{prefix}{k}"), s(Some(val)));
                }
                Value::Object(_) => flatten_scalars(val, &format!("{prefix}{k}_"), out),
                _ => {}
            }
        }
    }
}

fn flatten(token: &str, detail: &Value, fetched_at: u64) -> Row {
    let empty: Vec<Value> = Vec::new();
    let sections = detail.get("sections").and_then(|v| v.as_array()).unwrap_or(&empty);

    let mut fixed: BTreeMap<String, String> = BTreeMap::new();
    let mut dynamic: BTreeMap<String, String> = BTreeMap::new();

    fixed.insert("token".into(), token.to_string());
    fixed.insert("url".into(), format!("https://divar.ir/v/-/{token}"));
    fixed.insert("fetched_at".into(), fetched_at.to_string());
    fixed.insert("seo_title".into(), s(detail.pointer("/seo/title")));
    fixed.insert("seo_description".into(), s(detail.pointer("/seo/description")));
    fixed.insert("city".into(), s(detail.pointer("/city/name")));
    fixed.insert("district".into(), s(detail.pointer("/district/name")));
    let category = [
        detail.pointer("/category/slug"),
        detail.pointer("/web_info/category_slug"),
        detail.pointer("/category"),
    ]
    .into_iter()
    .map(s)
    .find(|v| !v.is_empty())
    .unwrap_or_default();
    fixed.insert("category".into(), category);

    let mut image_urls = Vec::new();
    let mut thumbnail_urls = Vec::new();
    let mut tags = Vec::new();

    for sec in sections {
        let name = s(sec.get("section_name"));
        let widgets = sec.get("widgets").and_then(|w| w.as_array()).cloned().unwrap_or_default();

        for w in &widgets {
            let wt = s(w.get("widget_type"));
            let d = w.get("data").cloned().unwrap_or(Value::Null);

            match (name.as_str(), wt.as_str()) {
                ("TITLE", "LEGEND_TITLE_ROW") => {
                    fixed.insert("title".into(), s(d.get("title")));
                    fixed.insert("subtitle".into(), s(d.get("subtitle")));
                }
                ("TITLE", "EXPANDABLE_SECTION") => {
                    fixed.insert("posted_raw".into(), s(d.get("title")));
                }
                ("DESCRIPTION", _) => {
                    let text = s(d.get("text"));
                    if !text.is_empty() {
                        fixed.insert("description".into(), text);
                    }
                }
                _ => {}
            }

            if name == "IMAGE" {
                if let Some(items) = d.get("items").and_then(|v| v.as_array()) {
                    for item in items {
                        if let Some(img) = item.get("image") {
                            let u = s(img.get("url"));
                            if !u.is_empty() {
                                image_urls.push(u);
                            }
                            let t = s(img.get("thumbnail_url"));
                            if !t.is_empty() {
                                thumbnail_urls.push(t);
                            }
                        }
                    }
                }
            }

            if let Some(chips) = d.pointer("/chip_list/chips").and_then(|v| v.as_array()) {
                for chip in chips {
                    let t = s(chip.get("text"));
                    if !t.is_empty() {
                        tags.push(t);
                    }
                }
            }

            if name == "MAP" {
                if let Some(loc) = d.get("location") {
                    let pt = loc.pointer("/fuzzy_data/point").or_else(|| loc.pointer("/exact_data/point"));
                    if let Some(pt) = pt {
                        fixed.insert("latitude".into(), s(pt.get("latitude")));
                        fixed.insert("longitude".into(), s(pt.get("longitude")));
                    }
                    if let Some(r) = loc.pointer("/fuzzy_data/radius") {
                        fixed.insert("map_radius_m".into(), s(Some(r)));
                    }
                }
            }
        }
    }

    // Everything else, generically.
    for sec in sections {
        harvest_pairs(sec, &mut dynamic);
    }
    for key in ["webengage", "analytics"] {
        if let Some(v) = detail.get(key) {
            flatten_scalars(v, &format!("{key}_"), &mut dynamic);
        }
    }

    // Price is worth a stable column even though it is also a harvested pair.
    for k in ["قیمت", "قیمت کل", "قیمت هر متر", "مبلغ اجاره"] {
        if let Some(v) = dynamic.get(k) {
            fixed.insert("price_raw".into(), v.clone());
            break;
        }
    }

    fixed.insert("image_count".into(), image_urls.len().to_string());
    fixed.insert("image_urls".into(), image_urls.join(" | "));
    fixed.insert("thumbnail_urls".into(), thumbnail_urls.join(" | "));
    fixed.insert("tags".into(), tags.join(" | "));

    // Never let a harvested pair shadow a fixed column.
    for c in FIXED_COLUMNS {
        dynamic.remove(*c);
    }

    Row { fixed, dynamic }
}

// ---------------------------------------------------------------------- io

/// The outstanding-failure queue after a run: everything still unresolved,
/// deduped by token, newest error winning. Anything now present in raw.jsonl
/// has been recovered and drops out.
///
/// In retry mode the previous queue IS the input, so it is not re-merged --
/// otherwise nothing could ever leave the file.
fn merge_failures(
    previous: &[Value],
    still_failing: Vec<Value>,
    have: &HashSet<String>,
    retry_mode: bool,
) -> Vec<Value> {
    let tok = |v: &Value| v.get("token").and_then(|t| t.as_str()).map(String::from);

    let mut by_token: BTreeMap<String, Value> = BTreeMap::new();
    if !retry_mode {
        for v in previous {
            if let Some(t) = tok(v) {
                if !have.contains(&t) {
                    by_token.insert(t, v.clone());
                }
            }
        }
    }
    for v in still_failing {
        if let Some(t) = tok(&v) {
            if !have.contains(&t) {
                by_token.insert(t, v);
            }
        }
    }
    by_token.into_values().collect()
}

/// Tokens still owed: in failures.jsonl and not yet in raw.jsonl.
fn outstanding_failures(out_dir: &str) -> Result<Vec<String>> {
    let have = tokens_in(&format!("{out_dir}/{RAW_FILE}"))?;
    Ok(read_jsonl(&format!("{out_dir}/{FAIL_FILE}"))?
        .iter()
        .filter_map(|v| v.get("token").and_then(|t| t.as_str()).map(String::from))
        .filter(|t| !have.contains(t))
        .collect())
}

/// Fallback path: if the whole country cannot be asked for in one query, the
/// run has to fan out over explicit ids, and those have to come from somewhere.
async fn list_cities(client: &Client) -> Result<()> {
    const ENDPOINTS: &[&str] = &[
        "https://api.divar.ir/v8/places/cities",
        "https://api.divar.ir/v5/place/cities",
        "https://api.divar.ir/v8/place/cities",
    ];
    for url in ENDPOINTS {
        match client.get(*url).send().await {
            Ok(r) if r.status().is_success() => {
                let body: Value = r.json().await?;
                let mut ids: Vec<String> = Vec::new();
                collect_city_ids(&body, &mut ids);
                eprintln!("{url} -> {} cities", ids.len());
                println!("{}", ids.join(","));
                return Ok(());
            }
            Ok(r) => eprintln!("  {url}: http {}", r.status()),
            Err(e) => eprintln!("  {url}: {e}"),
        }
    }
    bail!("no city-list endpoint responded; get the ids from divar.ir's own network tab")
}

/// City payloads are nested differently per endpoint; pull every numeric `id`
/// that sits next to a name rather than hard-coding one shape.
fn collect_city_ids(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(map) => {
            let has_name = map.contains_key("name") || map.contains_key("slug");
            if has_name {
                if let Some(id) = map.get("id") {
                    let id = match id {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        _ => String::new(),
                    };
                    if !id.is_empty() && !out.contains(&id) {
                        out.push(id);
                    }
                }
            }
            for (_, child) in map {
                collect_city_ids(child, out);
            }
        }
        Value::Array(items) => items.iter().for_each(|i| collect_city_ids(i, out)),
        _ => {}
    }
}

fn append_jsonl(path: &str, value: &Value) -> Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("cannot open {path}"))?;
    writeln!(f, "{}", serde_json::to_string(value)?)?;
    Ok(())
}

fn read_jsonl(path: &str) -> Result<Vec<Value>> {
    if !std::path::Path::new(path).exists() {
        return Ok(Vec::new());
    }
    let f = File::open(path).with_context(|| format!("cannot read {path}"))?;
    let mut out = Vec::new();
    for line in BufReader::new(f).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        out.push(serde_json::from_str(&line).with_context(|| format!("bad JSON line in {path}"))?);
    }
    Ok(out)
}

fn tokens_in(path: &str) -> Result<HashSet<String>> {
    Ok(read_jsonl(path)?
        .iter()
        .filter_map(|v| v.get("token").and_then(|t| t.as_str()).map(String::from))
        .collect())
}

/// Rebuild every CSV batch from raw.jsonl with ONE header shared by all files,
/// so the batches can actually be concatenated. The old run wrote a different
/// header per batch (131-169 columns) and could not be merged.
fn export_csv(out_dir: &str, batch_size: usize) -> Result<()> {
    let raw_path = format!("{out_dir}/{RAW_FILE}");
    let records = read_jsonl(&raw_path)?;
    if records.is_empty() {
        eprintln!("nothing to export: {raw_path} is empty");
        return Ok(());
    }

    let rows: Vec<Row> = records
        .iter()
        .filter_map(|r| {
            let token = r.get("token")?.as_str()?;
            let detail = r.get("detail")?;
            let at = r.get("fetched_at").and_then(|v| v.as_u64()).unwrap_or(0);
            Some(flatten(token, detail, at))
        })
        .collect();

    let mut dyn_keys: BTreeSet<String> = BTreeSet::new();
    for r in &rows {
        dyn_keys.extend(r.dynamic.keys().cloned());
    }

    let mut header: Vec<String> = FIXED_COLUMNS.iter().map(|s| s.to_string()).collect();
    header.extend(dyn_keys.iter().cloned());

    for old in fs::read_dir(out_dir)? {
        let p = old?.path();
        if p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with("batch_") && n.ends_with(".csv")) {
            fs::remove_file(p)?;
        }
    }

    for (i, chunk) in rows.chunks(batch_size.max(1)).enumerate() {
        let path = format!("{out_dir}/batch_{:05}.csv", i + 1);
        let mut w = csv::WriterBuilder::new().from_path(&path)?;
        w.write_record(&header)?;
        for r in chunk {
            let mut line: Vec<String> = FIXED_COLUMNS
                .iter()
                .map(|c| r.fixed.get(*c).cloned().unwrap_or_default())
                .collect();
            for k in &dyn_keys {
                line.push(r.dynamic.get(k).cloned().unwrap_or_default());
            }
            w.write_record(&line)?;
        }
        w.flush()?;
        eprintln!("wrote {path} ({} rows)", chunk.len());
    }

    eprintln!("exported {} rows, {} columns (identical header in every batch)", rows.len(), header.len());
    Ok(())
}

// -------------------------------------------------------------------- main

/// Fetch every token, streaming successes to raw.jsonl and permanent failures
/// to failures.jsonl so a later --retry-failures can pick them up.
async fn fetch_all(
    client: &Client,
    tokens: Vec<String>,
    args: &Args,
    throttle: Arc<Throttle>,
) -> Result<(usize, usize)> {
    let raw_path = format!("{}/{RAW_FILE}", args.out_dir);
    let fail_path = format!("{}/{FAIL_FILE}", args.out_dir);

    let total = tokens.len();
    let mut done = 0usize;
    let mut failed = 0usize;
    let mut still_failing: Vec<Value> = Vec::new();

    let mut results = stream::iter(tokens.into_iter().map(|tok| {
        let client = client.clone();
        let throttle = throttle.clone();
        let (max_retries, backoff_ms) = (args.max_retries, args.backoff_ms);
        async move {
            let r = fetch_detail(&client, &tok, &throttle, max_retries, backoff_ms).await;
            (tok, r)
        }
    }))
    .buffer_unordered(args.concurrency.max(1));

    while let Some((token, res)) = results.next().await {
        match res {
            Ok(detail) => {
                append_jsonl(&raw_path, &json!({
                    "token": token,
                    "fetched_at": now_secs(),
                    "detail": detail,
                }))?;
                done += 1;
            }
            Err(msg) => {
                failed += 1;
                eprintln!("PERMANENT FAILURE {token}: {msg}");
                still_failing.push(json!({ "token": token, "error": msg, "at": now_secs() }));
            }
        }

        if (done + failed) % 25 == 0 || done + failed == total {
            eprintln!(
                "progress: {done} ok, {failed} failed, {total} total (throttle {}ms)",
                throttle.current()
            );
        }
    }

    // This file is the outstanding-work queue, so it is rewritten rather than
    // appended -- but a normal run must NOT drop failures recorded by an
    // earlier run. Only tokens we have actually recovered leave the queue.
    let have = tokens_in(&raw_path)?;
    let previous = read_jsonl(&fail_path).unwrap_or_default();
    let queue = merge_failures(&previous, still_failing, &have, args.retry_failures);

    let mut f = File::create(&fail_path).with_context(|| format!("cannot write {fail_path}"))?;
    for v in &queue {
        writeln!(f, "{}", serde_json::to_string(v)?)?;
    }
    if queue.len() > failed {
        eprintln!("failures.jsonl now holds {} outstanding listings ({} from earlier runs)",
            queue.len(), queue.len() - failed);
    }

    Ok((done, failed))
}

// ----------------------------------------------------------------- observe

/// One dated sighting. `None` for a transient failure: "could not reach" says
/// nothing about the listing, so nothing is recorded and tomorrow tries again.
fn observation(token: &str, result: &std::result::Result<Value, String>, at: u64) -> Option<Value> {
    match result {
        Ok(detail) => Some(json!({
            "token": token,
            "observed_at": at,
            "status": "live",
            // Toman, as displayed on the site. Null for listings with no fixed price (توافقی).
            "price": detail.pointer("/webengage/price").cloned().unwrap_or(Value::Null),
        })),
        Err(msg) if msg.starts_with(DEAD_PREFIX) => Some(json!({
            "token": token,
            "observed_at": at,
            "status": "removed",
            "reason": msg,
        })),
        Err(_) => None,
    }
}

/// Which listings to visit: raw.jsonl order (a stable panel), each token once,
/// skipping those already confirmed removed, capped at `max` (0 = no cap).
fn observe_panel(raw_tokens: Vec<String>, removed: &HashSet<String>, max: usize) -> Vec<String> {
    let mut seen = HashSet::new();
    let panel = raw_tokens
        .into_iter()
        .filter(|t| seen.insert(t.clone()) && !removed.contains(t));
    if max == 0 { panel.collect() } else { panel.take(max).collect() }
}

/// Tokens of a JSONL file in file order, keeping only lines where `keep` holds.
/// Streams line by line: raw.jsonl is ~70 KB per listing and must not be held
/// in memory whole.
fn tokens_where(path: &str, keep: impl Fn(&Value) -> bool) -> Result<Vec<String>> {
    if !std::path::Path::new(path).exists() {
        return Ok(Vec::new());
    }
    let f = File::open(path).with_context(|| format!("cannot read {path}"))?;
    let mut out = Vec::new();
    let mut lines = BufReader::new(f).lines().peekable();
    while let Some(line) = lines.next() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            // A crawl appending to this file right now leaves a half-written
            // LAST line. Anywhere else, a bad line is corruption.
            Err(_) if lines.peek().is_none() => {
                eprintln!("note: ignoring incomplete final line of {path} (file is being written?)");
                break;
            }
            Err(e) => return Err(e).with_context(|| format!("bad JSON line in {path}")),
        };
        if keep(&v) {
            if let Some(t) = v.get("token").and_then(|t| t.as_str()) {
                out.push(t.to_string());
            }
        }
    }
    Ok(out)
}

// ponytail: one detail fetch per listing per day. If that gets too slow, the
// search feed's list widgets may carry the price for free -- but the feed is
// capped, so removal must still be confirmed by a direct fetch like this one.
async fn observe_all(client: &Client, args: &Args, throttle: Arc<Throttle>) -> Result<()> {
    let raw_path = format!("{}/{RAW_FILE}", args.out_dir);
    let obs_path = format!("{}/{OBS_FILE}", args.out_dir);

    let removed: HashSet<String> = tokens_where(&obs_path, |v| v["status"] == "removed")?.into_iter().collect();
    let panel = observe_panel(tokens_where(&raw_path, |_| true)?, &removed, args.observe_max);
    if panel.is_empty() {
        bail!("nothing to observe: no live listings in {raw_path}");
    }
    eprintln!("observing {} listings ({} already confirmed removed)", panel.len(), removed.len());

    let total = panel.len();
    let (mut live, mut gone, mut unreachable) = (0usize, 0usize, 0usize);

    let mut results = stream::iter(panel.into_iter().map(|tok| {
        let client = client.clone();
        let throttle = throttle.clone();
        let (max_retries, backoff_ms) = (args.max_retries, args.backoff_ms);
        async move {
            let r = fetch_detail(&client, &tok, &throttle, max_retries, backoff_ms).await;
            (tok, r)
        }
    }))
    .buffer_unordered(args.concurrency.max(1));

    while let Some((token, res)) = results.next().await {
        match observation(&token, &res, now_secs()) {
            Some(obs) => {
                if obs["status"] == "live" { live += 1 } else { gone += 1 }
                append_jsonl(&obs_path, &obs)?;
            }
            None => unreachable += 1,
        }
        let n = live + gone + unreachable;
        if n % 100 == 0 || n == total {
            eprintln!("observe: {live} live, {gone} removed, {unreachable} unreachable, {total} total (throttle {}ms)", throttle.current());
        }
    }

    // A day with mostly unreachable listings is a gap in the history, not a quiet day.
    if unreachable * 2 > total {
        bail!("observe pass unreliable: {unreachable} of {total} listings unreachable");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("cannot create output dir {}", args.out_dir))?;

    if args.audit_only {
        return audit_fetched_payloads(&args.out_dir, args.min_vehicle_purity, None);
    }

    if args.export_only {
        return export_csv(&args.out_dir, args.batch_size);
    }

    let client = Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(25))
        .build()?;

    if args.list_cities {
        return list_cities(&client).await;
    }

    if args.observe {
        let throttle = Arc::new(Throttle::new(args.detail_delay_ms, args.max_delay_ms));
        return observe_all(&client, &args, throttle).await;
    }

    let categories: Vec<String> = args
        .category
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if categories.is_empty() {
        bail!("no category given");
    }

    let city = resolve_city_mode(&client, &args.city_ids, &categories[0]).await?;

    if args.probe_categories {
        return probe_categories(&client, &city, &args.category).await;
    }

    let accept: BTreeSet<String> = if args.accept_categories.trim().is_empty() {
        categories.iter().flat_map(|c| accepted_slugs(c)).collect()
    } else {
        args.accept_categories.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
    };

    let throttle = Arc::new(Throttle::new(args.detail_delay_ms, args.max_delay_ms));
    let raw_path = format!("{}/{RAW_FILE}", args.out_dir);
    let fail_path = format!("{}/{FAIL_FILE}", args.out_dir);

    // ---- retry mode: only the tokens that failed before ----
    if args.retry_failures {
        let have = tokens_in(&raw_path)?;
        let todo: Vec<String> = read_jsonl(&fail_path)?
            .iter()
            .filter_map(|v| v.get("token").and_then(|t| t.as_str()).map(String::from))
            .filter(|t| !have.contains(t))
            .collect();

        if todo.is_empty() {
            eprintln!("no outstanding failures in {fail_path}");
            return export_csv(&args.out_dir, args.batch_size);
        }

        eprintln!("retrying {} failed listings", todo.len());
        let (ok, bad) = fetch_all(&client, todo, &args, throttle).await?;
        eprintln!("retry done: {ok} recovered, {bad} still failing");
        return export_csv(&args.out_dir, args.batch_size);
    }

    // ---- dry run: one page, category breakdown, no detail fetches ----
    if args.dry_run {
        let mut tokens: Vec<(String, String)> = Vec::new();
        for cat in &categories {
            let body = search_body(&city, cat, None);
            eprintln!("request body: {}", serde_json::to_string(&body)?);
            let resp = search_page(&client, &body, args.max_retries, args.backoff_ms).await?;
            let page: Vec<(String, String)> = resp
                .get("list_widgets")
                .and_then(|v| v.as_array())
                .map(|ws| ws.iter().filter_map(token_and_category).collect())
                .unwrap_or_default();
            eprintln!("  '{cat}' page 1 returned {} listings", page.len());
            tokens.extend(page);
        }
        audit_categories(&tokens, &args.category, &accept, args.min_category_purity)?;
        eprintln!(
            "\n--- read the breakdown above yourself; a zero exit code is NOT a verdict. ---\n\
             Expect vehicle slugs only. If you see real-estate/phones/jobs, the filter is\n\
             not applied and '{}' is the wrong value for {}.",
            args.category, city.describe()
        );
        return Ok(());
    }

    // ---- full scrape ----
    eprintln!(
        "fetching up to {} listings, category '{}', {}",
        args.count, args.category, city.describe()
    );

    // Split the budget across categories, then hand any shortfall to the rest --
    // a small category should not cap a large one.
    let mut collected: Vec<(String, String)> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for (i, cat) in categories.iter().enumerate() {
        let remaining = args.count.saturating_sub(collected.len());
        if remaining == 0 {
            break;
        }
        let share = remaining / (categories.len() - i).max(1);
        let want = if i + 1 == categories.len() { remaining } else { share.max(1) };
        eprintln!("collecting '{cat}' (up to {want})...");
        let got = collect_tokens(&client, &city, cat, want, &args).await?;
        for (tok, slug) in got {
            if seen.insert(tok.clone()) {
                collected.push((tok, slug));
            }
        }
    }
    eprintln!("token collection done: {} unique tokens", collected.len());
    audit_categories(&collected, &args.category, &accept, args.min_category_purity)?;

    // Resume: never re-fetch what raw.jsonl already has.
    let have = tokens_in(&raw_path)?;
    let todo: Vec<String> = collected
        .into_iter()
        .map(|(t, _)| t)
        .filter(|t| !have.contains(t))
        .collect();
    if !have.is_empty() {
        eprintln!("resuming: {} already in {raw_path}, {} to fetch", have.len(), todo.len());
    }

    // Pilot first: verify the filter against real payloads before committing to
    // the full run.
    let pilot_n = args.pilot.min(todo.len());
    let mut todo = todo;
    if pilot_n > 0 && args.min_vehicle_purity > 0.0 {
        let pilot: Vec<String> = todo.drain(..pilot_n).collect();
        let pilot_set: HashSet<String> = pilot.iter().cloned().collect();
        eprintln!("pilot batch: fetching {pilot_n} listings to verify the filter");
        fetch_all(&client, pilot, &args, throttle.clone()).await?;
        audit_fetched_payloads(&args.out_dir, args.min_vehicle_purity, Some(&pilot_set))?;
        eprintln!("pilot OK -- continuing with the remaining {} listings", todo.len());
    }

    let (mut done, mut failed) = fetch_all(&client, todo, &args, throttle.clone()).await?;
    eprintln!("main pass: {done} written, {failed} failed");

    // Sweep the failure queue automatically. The first run lost ~700 listings
    // because this was a manual step nobody took.
    for pass in 1..=args.retry_passes {
        let outstanding = outstanding_failures(&args.out_dir)?;
        if outstanding.is_empty() {
            break;
        }
        // Each sweep starts slower than the last; these tokens already lost a
        // race with the rate limiter.
        throttle.slow_start(pass);
        eprintln!(
            "retry sweep {pass}/{}: {} outstanding (throttle {}ms)",
            args.retry_passes,
            outstanding.len(),
            throttle.current()
        );
        let (ok, bad) = fetch_all(&client, outstanding, &args, throttle.clone()).await?;
        done += ok;
        failed = bad;
        eprintln!("retry sweep {pass}: {ok} recovered, {bad} still failing");
    }

    eprintln!("done: {done} listings written, {failed} unrecovered");
    if failed > 0 {
        eprintln!("rerun with --retry-failures to keep working the {failed} in failures.jsonl");
    }

    export_csv(&args.out_dir, args.batch_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bug that cost the last run: the category must survive the round-trip
    /// through the server's echoed search_data.
    /// `city_ids: ["iran"]` gets a 400, so "everywhere" must be probed, while an
    /// explicit numeric list must be taken at face value and not probed.
    #[test]
    fn whole_country_is_probed_but_explicit_ids_are_not() {
        for word in ["iran", "all", "0"] {
            let c = city_candidates(word);
            assert!(c.len() > 1, "{word} should produce candidates to try");
            assert_eq!(c[0], CityMode::Omit);
            assert!(!c.contains(&CityMode::Ids(vec!["iran".into()])), "the shape that 400s");
        }

        let c = city_candidates("1,2");
        assert_eq!(c, vec![CityMode::Ids(vec!["1".into(), "2".into()])]);
    }

    #[test]
    fn city_mode_shapes_the_body_correctly() {
        let mut b = json!({});
        CityMode::Omit.apply(&mut b);
        assert!(b.get("city_ids").is_none() && b.get("cities").is_none());

        let mut b = json!({});
        CityMode::Ids(vec!["1".into()]).apply(&mut b);
        assert_eq!(b["city_ids"], json!(["1"]));

        let mut b = json!({});
        CityMode::Slugs(vec!["iran".into()]).apply(&mut b);
        assert_eq!(b["cities"], json!(["iran"]));
    }

    #[test]
    fn city_ids_are_scraped_from_any_payload_shape() {
        let payload = json!({
            "cities": [ { "id": 1, "name": "تهران" }, { "id": "2", "slug": "karaj" } ],
            "nested": { "districts": [ { "id": 99, "name": "ونک" } ] },
            "noise": { "id": 7 }
        });
        let mut ids = Vec::new();
        collect_city_ids(&payload, &mut ids);
        assert!(ids.contains(&"1".to_string()) && ids.contains(&"2".to_string()));
        assert!(!ids.contains(&"7".to_string()), "an id with no name is not a city");
    }

    #[test]
    fn category_survives_pagination_echo() {
        let body = search_body(&CityMode::Ids(vec!["1".into()]), "auto", None);
        assert_eq!(body.pointer("/search_data/form_data/data/category/str/value").unwrap(), "auto");

        // Server echoes search_data back WITHOUT the category (what Divar did).
        let echoed = json!({ "form_data": { "data": { "sort": { "str": { "value": "new" } } } } });
        let page_data = json!({ "last_post_date": "123" });
        let body2 = search_body(&CityMode::Ids(vec!["1".into()]), "auto", Some((&echoed, &page_data)));
        assert_eq!(body2.pointer("/search_data/form_data/data/category/str/value").unwrap(), "auto");
        assert_eq!(body2.pointer("/search_data/form_data/data/sort/str/value").unwrap(), "new");
        assert_eq!(body2.pointer("/pagination_data/last_post_date").unwrap(), "123");
    }

    fn toks(pairs: &[(&str, usize)]) -> Vec<(String, String)> {
        let mut v = Vec::new();
        for (slug, n) in pairs {
            for i in 0..*n {
                v.push((format!("{slug}{i}"), slug.to_string()));
            }
        }
        v
    }

    #[test]
    fn audit_rejects_a_polluted_feed() {
        // 2 of 10 are vehicles: the shape of the previous run. Must bail.
        let bad = toks(&[("real-estate", 8), ("light", 2)]);
        let accept = accepted_slugs("auto");
        assert!(audit_categories(&bad, "auto", &accept, 0.9).is_err());
    }

    /// `/s/iran/auto` is a PARENT slug: its results come back tagged with child
    /// slugs. A substring matcher rejects "light" (wanted) and accepts
    /// "auto-parts" (dubious) -- this asserts we do neither.
    #[test]
    fn audit_accepts_the_whole_vehicle_subtree_under_a_parent_slug() {
        let accept = accepted_slugs("auto");
        for child in ["light", "heavy", "classic", "motorcycles"] {
            assert!(accept.contains(child), "{child} must be accepted under 'auto'");
        }
        let real_feed = toks(&[("light", 60), ("heavy", 15), ("motorcycles", 20), ("classic", 5)]);
        assert!(
            audit_categories(&real_feed, "auto", &accept, 0.9).is_ok(),
            "a genuine whole-vehicle feed must not be rejected"
        );

        // Narrowing to a leaf slug must NOT drag the siblings in.
        let leaf = accepted_slugs("light");
        assert!(leaf.contains("light"));
        assert!(!leaf.contains("motorcycles"));
        assert!(audit_categories(&real_feed, "light", &leaf, 0.9).is_err());
    }

    /// The whole point of the task: a later run must never erase the failures
    /// an earlier run recorded.
    #[test]
    fn failure_queue_survives_a_later_run() {
        let old_fails: Vec<Value> = (0..700)
            .map(|i| json!({ "token": format!("old{i}"), "error": "rate limited (429)" }))
            .collect();
        let new_fails: Vec<Value> = (0..5)
            .map(|i| json!({ "token": format!("new{i}"), "error": "http status 500" }))
            .collect();

        // Normal run, nothing recovered: all 705 must still be queued.
        let have = HashSet::new();
        let q = merge_failures(&old_fails, new_fails.clone(), &have, false);
        assert_eq!(q.len(), 705, "a normal run must not truncate earlier failures");

        // Tokens now present in raw.jsonl have been recovered and drop out.
        let have: HashSet<String> = (0..700).map(|i| format!("old{i}")).collect();
        let q = merge_failures(&old_fails, new_fails.clone(), &have, false);
        assert_eq!(q.len(), 5);

        // Retry mode rewrites: the previous queue is the input, so only what
        // still failed stays -- otherwise the file could never drain.
        let q = merge_failures(&old_fails, new_fails, &HashSet::new(), true);
        assert_eq!(q.len(), 5);

        // Same token failing twice is queued once, with the newer error.
        let q = merge_failures(
            &[json!({ "token": "x", "error": "old" })],
            vec![json!({ "token": "x", "error": "new" })],
            &HashSet::new(),
            false,
        );
        assert_eq!(q.len(), 1);
        assert_eq!(q[0]["error"], "new");
    }

    #[test]
    fn harvest_picks_up_nested_and_unknown_widgets() {
        let detail = json!({
            "sections": [
                { "section_name": "TITLE", "widgets": [
                    { "widget_type": "LEGEND_TITLE_ROW", "data": { "title": "پراید ۱۳۹۰", "subtitle": "۲۰۰٬۰۰۰ کیلومتر" } },
                    { "widget_type": "EXPANDABLE_SECTION", "data": { "title": "دیروز در تهران" } }
                ]},
                { "section_name": "LIST_DATA", "widgets": [
                    { "widget_type": "GROUP_INFO_ROW", "data": { "items": [
                        { "title": "کارکرد", "value": "۲۰۰٬۰۰۰" },
                        { "title": "مدل", "value": "۱۳۹۰" }
                    ]}},
                    // A widget type the parser has never seen: harvested anyway.
                    { "widget_type": "SOME_NEW_ROW_2027", "data": { "title": "نوع سوخت", "value": "بنزین" } }
                ]},
                { "section_name": "DESCRIPTION", "widgets": [ { "data": { "text": "سالم" } } ] }
            ],
            "city": { "name": "تهران" },
            "webengage": { "price": 450000000, "brand_model": "Pride" }
        });

        let row = flatten("abc123", &detail, 99);
        assert_eq!(row.fixed["title"], "پراید ۱۳۹۰");
        assert_eq!(row.fixed["subtitle"], "۲۰۰٬۰۰۰ کیلومتر");
        assert_eq!(row.fixed["description"], "سالم");
        assert_eq!(row.fixed["city"], "تهران");
        assert_eq!(row.fixed["url"], "https://divar.ir/v/-/abc123");
        assert_eq!(row.dynamic["کارکرد"], "۲۰۰٬۰۰۰");
        assert_eq!(row.dynamic["نوع سوخت"], "بنزین");
        assert_eq!(row.dynamic["webengage_price"], "450000000");
        assert_eq!(row.dynamic["webengage_brand_model"], "Pride");
        // A harvested pair must never shadow a fixed column.
        assert!(!row.dynamic.contains_key("title"));
    }

    fn listing(specs: &[(&str, &str)]) -> Value {
        json!({ "sections": [ { "section_name": "LIST_DATA", "widgets":
            specs.iter().map(|(k, v)| json!({ "data": { "title": k, "value": v } }))
                 .collect::<Vec<_>>() } ] })
    }

    /// The vocabulary-independent guard: does the payload look like a vehicle?
    #[test]
    fn vehicle_detection_ignores_slug_vocabulary() {
        let car = flatten("a", &listing(&[("کارکرد", "۱۲۰۰۰۰"), ("گیربکس", "دنده‌ای")]), 0);
        assert!(looks_like_a_vehicle(&car));

        // A motorcycle: different specs, still a vehicle. "All kinds" must pass.
        let bike = flatten("b", &listing(&[("حجم موتور", "۱۲۵"), ("برند و مدل", "هوندا")]), 0);
        assert!(looks_like_a_vehicle(&bike));

        // The pollution from the first run.
        let flat = flatten("c", &listing(&[("ودیعه", "۱۰۰"), ("متراژ", "۸۰")]), 0);
        assert!(!looks_like_a_vehicle(&flat));
        let shoe = flatten("d", &listing(&[("نوع کفش", "کتانی"), ("جنس", "چرم")]), 0);
        assert!(!looks_like_a_vehicle(&shoe));
    }

    /// `برند و مدل` is shared between cars and handsets. A rental car carries it
    /// with no کارکرد and must pass; a phone carries it with رم/سیم‌کارت and
    /// must not. Both shapes are real rows from the first run.
    #[test]
    fn weak_brand_key_does_not_let_phones_through() {
        let rental = flatten("a", &listing(&[("برند و مدل", "پژو 207i پانوراما اتوماتیک")]), 0);
        assert!(looks_like_a_vehicle(&rental), "rental cars have no کارکرد but are vehicles");

        let phone = flatten(
            "b",
            &listing(&[
                ("برند و مدل", "اپل iPhone 17 Pro Max"),
                ("تعداد سیم\u{200c}کارت", "۲"),
                ("مقدار رم", "۴ گیگابایت"),
            ]),
            0,
        );
        assert!(!looks_like_a_vehicle(&phone), "a handset must not count as a vehicle");
    }

    /// The history feature depends on three distinctions: live carries a price,
    /// a 404/410 is a removal, and "could not reach" records nothing at all.
    #[test]
    fn observation_separates_live_removed_and_unreachable() {
        let live = observation("a", &Ok(json!({ "webengage": { "price": 550000000u64 } })), 7).unwrap();
        assert_eq!(live["status"], "live");
        assert_eq!(live["price"], 550000000u64);
        assert_eq!(live["observed_at"], 7);

        let negotiable = observation("a", &Ok(json!({})), 7).unwrap();
        assert!(negotiable["price"].is_null(), "no fixed price is null, not 0");

        let dead = observation("a", &Err(format!("{DEAD_PREFIX} (http 404 Not Found)")), 7).unwrap();
        assert_eq!(dead["status"], "removed");

        let flaky = observation("a", &Err("rate limited (429) (gave up after 9 attempts)".into()), 7);
        assert!(flaky.is_none(), "a transient failure must not be read as a removal");
    }

    /// Same panel every day: file order, no duplicates, removed ones dropped
    /// BEFORE the cap so the cap is not wasted on dead listings.
    #[test]
    fn observe_panel_is_stable_and_skips_removed() {
        let raw: Vec<String> = ["a", "b", "a", "c", "d"].iter().map(|s| s.to_string()).collect();
        let removed: HashSet<String> = ["b".to_string()].into_iter().collect();
        assert_eq!(observe_panel(raw.clone(), &removed, 0), vec!["a", "c", "d"]);
        assert_eq!(observe_panel(raw, &removed, 2), vec!["a", "c"]);
    }

    /// A retry sweep must not re-run at the pace that caused the failures.
    #[test]
    fn retry_sweeps_start_slower_each_pass() {
        let t = Throttle::new(400, 8000);
        t.slow_start(1);
        assert_eq!(t.current(), 800);
        t.slow_start(2);
        assert_eq!(t.current(), 3200);
        for p in 1..=9 {
            t.slow_start(p);
            assert!(t.current() <= 8000, "must stay under max_delay_ms");
        }
    }

    #[test]
    fn backoff_grows_and_stays_bounded() {
        assert!(backoff_delay(1000, 1) >= Duration::from_millis(2000));
        assert!(backoff_delay(1000, 3) >= Duration::from_millis(8000));
        assert!(backoff_delay(1000, 50) <= Duration::from_millis(120_000));
    }

    #[test]
    fn throttle_backs_off_on_429_and_is_capped() {
        let t = Throttle::new(400, 2000);
        t.penalize();
        assert_eq!(t.current(), 600);
        for _ in 0..20 {
            t.penalize();
        }
        assert_eq!(t.current(), 2000, "must not exceed max_delay_ms");
    }
}
