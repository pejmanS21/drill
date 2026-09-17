use anyhow::{bail, Context, Result};
use clap::Parser;
use futures::stream::{self, StreamExt};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::time::Duration;

const USER_AGENT: &str = "Mozilla/5.0 (X11; Linux x86_64) divar-scraper/0.1";
const SEARCH_URL: &str = "https://api.divar.ir/v8/postlist/w/search";
const POST_URL: &str = "https://api.divar.ir/v8/posts-v2/web";

#[derive(Parser, Debug)]
#[command(name = "divar-scraper", about = "Fetch Divar car listings into batched CSV files")]
struct Args {
    /// Total number of listings (اگهی) to fetch, split across cities
    #[arg(short, long)]
    count: usize,

    /// Rows per CSV batch file
    #[arg(short, long, default_value_t = 500)]
    batch_size: usize,

    /// Output directory for CSV batches
    #[arg(short, long, default_value = "divar_data")]
    out_dir: String,

    /// Comma-separated Divar city ids (1 = tehran, 2 = karaj)
    #[arg(long, default_value = "1,2")]
    city_ids: String,

    /// Divar category slug
    #[arg(long, default_value = "cars")]
    category: String,

    /// Concurrent detail-page fetches
    #[arg(long, default_value_t = 5)]
    concurrency: usize,

    /// Delay between search-page requests, ms
    #[arg(long, default_value_t = 400)]
    page_delay_ms: u64,

    /// Delay before each detail-page request, ms (rate-limit throttle)
    #[arg(long, default_value_t = 350)]
    detail_delay_ms: u64,

    /// Max retries per request on failure/rate-limit before giving up
    #[arg(long, default_value_t = 5)]
    max_retries: u32,

    /// Base backoff delay on failure/429, ms (multiplied by attempt number)
    #[arg(long, default_value_t = 1500)]
    backoff_ms: u64,
}

#[derive(Debug, Clone)]
struct AdRecord {
    token: String,
    url: String,
    title: String,
    description: String,
    seo_title: String,
    seo_description: String,
    city: String,
    posted_raw: String,
    latitude: String,
    longitude: String,
    map_radius_m: String,
    image_count: String,
    image_urls: String,
    thumbnail_urls: String,
    tags: String,
    specs: HashMap<String, String>,
}

const FIXED_COLUMNS: &[&str] = &[
    "token",
    "url",
    "title",
    "description",
    "seo_title",
    "seo_description",
    "city",
    "posted_raw",
    "latitude",
    "longitude",
    "map_radius_m",
    "image_count",
    "image_urls",
    "thumbnail_urls",
    "tags",
];

fn fixed_value(rec: &AdRecord, col: &str) -> String {
    match col {
        "token" => rec.token.clone(),
        "url" => rec.url.clone(),
        "title" => rec.title.clone(),
        "description" => rec.description.clone(),
        "seo_title" => rec.seo_title.clone(),
        "seo_description" => rec.seo_description.clone(),
        "city" => rec.city.clone(),
        "posted_raw" => rec.posted_raw.clone(),
        "latitude" => rec.latitude.clone(),
        "longitude" => rec.longitude.clone(),
        "map_radius_m" => rec.map_radius_m.clone(),
        "image_count" => rec.image_count.clone(),
        "image_urls" => rec.image_urls.clone(),
        "thumbnail_urls" => rec.thumbnail_urls.clone(),
        "tags" => rec.tags.clone(),
        _ => String::new(),
    }
}

fn slug_url(token: &str) -> String {
    format!("https://divar.ir/v/-/{token}")
}

fn section<'a>(sections: &'a [Value], name: &str) -> Option<&'a Value> {
    sections
        .iter()
        .find(|s| s.get("section_name").and_then(|v| v.as_str()) == Some(name))
}

fn widgets_of(sec: &Value) -> &[Value] {
    sec.get("widgets").and_then(|w| w.as_array()).map(|v| v.as_slice()).unwrap_or(&[])
}

fn parse_detail(token: &str, root: &Value) -> AdRecord {
    let empty: Vec<Value> = Vec::new();
    let sections = root.get("sections").and_then(|v| v.as_array()).unwrap_or(&empty);

    let seo = root.get("seo").cloned().unwrap_or(Value::Null);
    let seo_title = seo.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let seo_description = seo.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();

    let city = root
        .get("city")
        .and_then(|c| c.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let mut title = String::new();
    let mut posted_raw = String::new();
    if let Some(sec) = section(sections, "TITLE") {
        for w in widgets_of(sec) {
            let wt = w.get("widget_type").and_then(|v| v.as_str()).unwrap_or("");
            let wd = w.get("data").cloned().unwrap_or(Value::Null);
            if wt == "LEGEND_TITLE_ROW" {
                title = wd.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
            }
            if wt == "EXPANDABLE_SECTION" {
                posted_raw = wd.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
            }
        }
    }

    let mut description = String::new();
    if let Some(sec) = section(sections, "DESCRIPTION") {
        for w in widgets_of(sec) {
            if let Some(text) = w.get("data").and_then(|d| d.get("text")).and_then(|v| v.as_str()) {
                description = text.to_string();
            }
        }
    }

    let mut image_urls = Vec::new();
    let mut thumbnail_urls = Vec::new();
    if let Some(sec) = section(sections, "IMAGE") {
        for w in widgets_of(sec) {
            if let Some(items) = w.get("data").and_then(|d| d.get("items")).and_then(|v| v.as_array()) {
                for item in items {
                    if let Some(img) = item.get("image") {
                        if let Some(u) = img.get("url").and_then(|v| v.as_str()) {
                            image_urls.push(u.to_string());
                        }
                        if let Some(u) = img.get("thumbnail_url").and_then(|v| v.as_str()) {
                            thumbnail_urls.push(u.to_string());
                        }
                    }
                }
            }
        }
    }
    let image_count = image_urls.len().to_string();

    let mut tags = Vec::new();
    if let Some(sec) = section(sections, "TAGS") {
        for w in widgets_of(sec) {
            if let Some(chips) = w
                .get("data")
                .and_then(|d| d.get("chip_list"))
                .and_then(|c| c.get("chips"))
                .and_then(|v| v.as_array())
            {
                for chip in chips {
                    if let Some(t) = chip.get("text").and_then(|v| v.as_str()) {
                        tags.push(t.to_string());
                    }
                }
            }
        }
    }

    let mut latitude = String::new();
    let mut longitude = String::new();
    let mut map_radius_m = String::new();
    if let Some(sec) = section(sections, "MAP") {
        for w in widgets_of(sec) {
            let loc = w.get("data").and_then(|d| d.get("location"));
            if let Some(loc) = loc {
                let pt = loc
                    .get("fuzzy_data")
                    .and_then(|f| f.get("point"))
                    .or_else(|| loc.get("exact_data").and_then(|f| f.get("point")));
                if let Some(pt) = pt {
                    latitude = pt.get("latitude").map(|v| v.to_string()).unwrap_or_default();
                    longitude = pt.get("longitude").map(|v| v.to_string()).unwrap_or_default();
                }
                if let Some(r) = loc.get("fuzzy_data").and_then(|f| f.get("radius")) {
                    map_radius_m = r.to_string();
                }
            }
        }
    }

    let mut specs: HashMap<String, String> = HashMap::new();
    if let Some(sec) = section(sections, "LIST_DATA") {
        for w in widgets_of(sec) {
            let wt = w.get("widget_type").and_then(|v| v.as_str()).unwrap_or("");
            let wd = w.get("data").cloned().unwrap_or(Value::Null);
            if wt == "GROUP_INFO_ROW" {
                if let Some(items) = wd.get("items").and_then(|v| v.as_array()) {
                    for item in items {
                        let k = item.get("title").and_then(|v| v.as_str()).unwrap_or("");
                        let v = item.get("value").and_then(|v| v.as_str()).unwrap_or("");
                        if !k.is_empty() {
                            specs.insert(k.to_string(), v.to_string());
                        }
                    }
                }
            } else if let (Some(k), Some(v)) = (
                wd.get("title").and_then(|v| v.as_str()),
                wd.get("value").and_then(|v| v.as_str()),
            ) {
                specs.insert(k.to_string(), v.to_string());
            }
        }
    }

    AdRecord {
        token: token.to_string(),
        url: slug_url(token),
        title,
        description,
        seo_title,
        seo_description,
        city,
        posted_raw,
        latitude,
        longitude,
        map_radius_m,
        image_count,
        image_urls: image_urls.join(" | "),
        thumbnail_urls: thumbnail_urls.join(" | "),
        tags: tags.join(" | "),
        specs,
    }
}

/// POST the search-listing API once, with retry + backoff on failure or 429.
async fn search_page(client: &Client, body: &Value, max_retries: u32, backoff_ms: u64) -> Result<Value> {
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let outcome = async {
            let resp = client.post(SEARCH_URL).json(body).send().await?;
            let status = resp.status();
            if status == StatusCode::TOO_MANY_REQUESTS {
                bail!("rate limited (429) on search page");
            }
            if !status.is_success() {
                bail!("search page http status {status}");
            }
            let json: Value = resp.json().await.context("search response not JSON")?;
            Ok::<Value, anyhow::Error>(json)
        }
        .await;

        match outcome {
            Ok(json) => return Ok(json),
            Err(e) => {
                if attempt > max_retries {
                    return Err(e.context("search page: giving up after max retries"));
                }
                let wait = backoff_ms * attempt as u64;
                eprintln!("search retry {attempt}/{max_retries}: {e:#} (waiting {wait}ms)");
                tokio::time::sleep(Duration::from_millis(wait)).await;
            }
        }
    }
}

/// Collect up to `count` post tokens for a single city, paginating the search API.
async fn collect_tokens_for_city(
    client: &Client,
    city_id: &str,
    category: &str,
    count: usize,
    page_delay_ms: u64,
    max_retries: u32,
    backoff_ms: u64,
) -> Result<Vec<String>> {
    let mut tokens: Vec<String> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();

    let mut body = serde_json::json!({
        "city_ids": [city_id],
        "search_data": {
            "form_data": { "data": { "category": { "str": { "value": category } } } }
        }
    });

    loop {
        let resp = search_page(client, &body, max_retries, backoff_ms).await?;

        let widgets = resp
            .get("list_widgets")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut got_any = false;
        for w in &widgets {
            if let Some(tok) = w
                .get("data")
                .and_then(|d| d.get("action"))
                .and_then(|a| a.get("payload"))
                .and_then(|p| p.get("token"))
                .and_then(|v| v.as_str())
            {
                if seen.insert(tok.to_string()) {
                    tokens.push(tok.to_string());
                    got_any = true;
                }
            }
        }

        eprintln!("  city {city_id}: collected {} / {} tokens", tokens.len(), count);

        if tokens.len() >= count {
            break;
        }

        let has_next = resp
            .get("pagination")
            .and_then(|p| p.get("has_next_page"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if !has_next || !got_any {
            eprintln!("  city {city_id}: no more pages, stopping with {} tokens", tokens.len());
            break;
        }

        let next_search_data = resp.get("search_data").cloned().unwrap_or(Value::Null);
        let next_pagination_data = resp
            .get("pagination")
            .and_then(|p| p.get("data"))
            .cloned()
            .unwrap_or(Value::Null);

        body = serde_json::json!({
            "city_ids": [city_id],
            "search_data": next_search_data,
            "pagination_data": next_pagination_data,
        });

        tokio::time::sleep(Duration::from_millis(page_delay_ms)).await;
    }

    tokens.truncate(count);
    Ok(tokens)
}

/// Fetch one ad's detail page, with a throttle delay plus retry + backoff on
/// failure or rate-limiting (HTTP 429).
async fn fetch_detail(
    client: &Client,
    token: String,
    delay_ms: u64,
    max_retries: u32,
    backoff_ms: u64,
) -> Result<AdRecord> {
    let url = format!("{POST_URL}/{token}");
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;

        let outcome = async {
            let resp = client.get(&url).send().await?;
            let status = resp.status();
            if status == StatusCode::TOO_MANY_REQUESTS {
                bail!("rate limited (429)");
            }
            if !status.is_success() {
                bail!("http status {status}");
            }
            let json: Value = resp.json().await.context("detail response not JSON")?;
            Ok::<Value, anyhow::Error>(json)
        }
        .await;

        match outcome {
            Ok(json) => return Ok(parse_detail(&token, &json)),
            Err(e) => {
                if attempt > max_retries {
                    return Err(e.context(format!("{token}: giving up after {attempt} attempts")));
                }
                let wait = backoff_ms * attempt as u64;
                eprintln!("  retry {attempt}/{max_retries} for {token}: {e:#} (waiting {wait}ms)");
                tokio::time::sleep(Duration::from_millis(wait)).await;
            }
        }
    }
}

fn write_batch(out_dir: &str, batch_num: usize, records: &[AdRecord]) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let mut spec_keys: BTreeSet<String> = BTreeSet::new();
    for r in records {
        for k in r.specs.keys() {
            spec_keys.insert(k.clone());
        }
    }

    let mut header: Vec<String> = FIXED_COLUMNS.iter().map(|s| s.to_string()).collect();
    header.extend(spec_keys.iter().cloned());

    let path = format!("{out_dir}/batch_{:05}.csv", batch_num);
    let mut wtr = csv::WriterBuilder::new()
        .from_path(&path)
        .with_context(|| format!("cannot create {path}"))?;
    wtr.write_record(&header)?;

    for r in records {
        let mut row: Vec<String> = FIXED_COLUMNS.iter().map(|c| fixed_value(r, c)).collect();
        for k in spec_keys.iter() {
            row.push(r.specs.get(k).cloned().unwrap_or_default());
        }
        wtr.write_record(&row)?;
    }
    wtr.flush()?;
    eprintln!("wrote {path} ({} rows, {} columns)", records.len(), header.len());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let city_ids: Vec<String> = args
        .city_ids
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if city_ids.is_empty() {
        bail!("no city ids given");
    }

    fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("cannot create output dir {}", args.out_dir))?;

    let client = Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(20))
        .build()?;

    eprintln!(
        "fetching up to {} listings in category '{}', cities {:?}",
        args.count, args.category, city_ids
    );

    // Split the total count evenly across cities.
    let per_city = (args.count + city_ids.len() - 1) / city_ids.len();
    let mut tokens: Vec<String> = Vec::new();
    for city_id in &city_ids {
        eprintln!("collecting tokens for city {city_id}...");
        let mut city_tokens = collect_tokens_for_city(
            &client,
            city_id,
            &args.category,
            per_city,
            args.page_delay_ms,
            args.max_retries,
            args.backoff_ms,
        )
        .await?;
        tokens.append(&mut city_tokens);
    }
    tokens.truncate(args.count);
    eprintln!("token collection done: {} tokens total", tokens.len());

    let mut batch: Vec<AdRecord> = Vec::with_capacity(args.batch_size);
    let mut batch_num = 1usize;
    let mut done = 0usize;
    let mut failed = 0usize;
    let total = tokens.len();

    let detail_delay_ms = args.detail_delay_ms;
    let max_retries = args.max_retries;
    let backoff_ms = args.backoff_ms;

    let mut results = stream::iter(tokens.into_iter().map(|tok| {
        let client = client.clone();
        async move { fetch_detail(&client, tok, detail_delay_ms, max_retries, backoff_ms).await }
    }))
    .buffer_unordered(args.concurrency);

    while let Some(res) = results.next().await {
        match res {
            Ok(rec) => {
                batch.push(rec);
                done += 1;
            }
            Err(e) => {
                failed += 1;
                eprintln!("fetch failed permanently: {e:#}");
            }
        }

        if done % 20 == 0 || done + failed == total {
            eprintln!("progress: {done} fetched, {failed} failed, {total} total");
        }

        if batch.len() >= args.batch_size {
            write_batch(&args.out_dir, batch_num, &batch)?;
            batch_num += 1;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        write_batch(&args.out_dir, batch_num, &batch)?;
    }

    eprintln!("done: {done} listings written, {failed} failed, output dir: {}", args.out_dir);
    Ok(())
}
