#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# ///
"""Crawl bama.ir, karnameh.com and hamrah-mechanic.com into one CSV per source, in the
column layout torob-car's ingest (`backend/ingest/row_mapper.py`) reads from Divar batches.

Closed-vocabulary columns (گیربکس, نوع سوخت, وضعیت بدنه) only ever hold Divar's own
wording: the ingest aborts the whole run on an unknown value. Anything we cannot translate
confidently is left blank and the site's original text goes to a `*_raw` column.

    uv run scripts/crawl_sources.py                      # all three, 300 each
    uv run scripts/crawl_sources.py --source bama --count 50
"""

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
REQUEST_DELAY_S = 0.5
MAX_ATTEMPTS = 4
URL_SEPARATOR = "|"
NEXT_DATA = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)
LOCATION_SPLIT = re.compile(r"\s*[/،,]\s*")

HEADER = [
    "token", "url", "source", "title", "description", "city", "posted_raw",
    "latitude", "longitude", "image_urls", "thumbnail_urls", "fetched_at",
    "webengage_cat_2", "webengage_cat_3", "webengage_business_type",
    "برند و مدل", "مدل (سال تولید)", "کارکرد", "قیمت پایه", "گیربکس", "نوع سوخت",
    "وضعیت بدنه", "رنگ", "مهلت بیمهٔ شخص ثالث", "وضعیت سند و مدارک",
    "gearbox_raw", "fuel_raw", "body_raw", "price_type_raw",
]  # fmt: skip

# Site wording → Divar wording. Keys missing here are blanked (original kept in *_raw).
GEARBOX = {
    "اتوماتیک": "اتوماتیک", "اتومات": "اتوماتیک", "automatic": "اتوماتیک",
    "دنده ای": "دنده ای", "دنده‌ای": "دنده ای", "manual": "دنده ای",
}  # fmt: skip
FUEL = {
    "بنزینی": "بنزین", "بنزین": "بنزین", "هیبرید": "هیبرید",
    "هیبرید ملایم": "هیبرید", "هیبریدی": "هیبرید", "پلاگین هیبرید": "پلاگین هیبرید", "برقی": "برق",
    "برق": "برق", "دیزلی": "گازوئیل", "گازوئیل": "گازوئیل", "گازوئیلی": "گازوئیل",
    "دوگانه سوز شرکتی": "دوگانه سوز شرکتی", "دوگانه سوز دستی": "دوگانه سوز دستی",
}  # fmt: skip
BODY = {
    "بدون رنگ": "بدون رنگ", "خط و خش جزئی": "خط و خش جزئی",
    "یک لکه رنگ": "رنگ شدگی جزئی", "دو لکه رنگ": "رنگ شدگی جزئی",
    "چند لکه رنگ": "رنگ شدگی جزئی", "صافکاری بدون رنگ": "رنگ شدگی جزئی",
    "دور رنگ": "رنگ شدگی زیاد", "تمام رنگ": "رنگ شدگی زیاد",
    "گلگیر رنگ": "رنگ شدگی جزئی", "کاپوت رنگ": "رنگ شدگی جزئی",
    "یک درب رنگ": "رنگ شدگی جزئی", "1 قطعه رنگ": "رنگ شدگی جزئی",
    "2 قطعه رنگ": "رنگ شدگی جزئی", "کامل رنگ": "رنگ شدگی زیاد",
    "تصادفی": "تصادفی", "سالم": "کاملا سالم", "intact": "کاملا سالم",
}  # fmt: skip
# Brand spellings that differ from Divar's «برند و مدل». Checked against every bama and
# hamrah brand name; the rest either match or are brands absent from Divar's data.
BRAND_ALIASES = {"ب ام و": "بی ام و"}
# Ingest strips posted_raw, so « در …» with no age in front loses its marker.
UNKNOWN_AGE = "نامشخص"
Row = dict[str, str]


class FetchError(Exception):
    """A URL still failed after every retry."""


def fetch(url: str) -> bytes:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        time.sleep(REQUEST_DELAY_S)
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read()
        except (urllib.error.URLError, TimeoutError) as error:
            if isinstance(error, urllib.error.HTTPError) and error.code == 404:
                raise FetchError(f"{url}: 404") from error
            if attempt == MAX_ATTEMPTS:
                raise FetchError(f"{url}: {error}") from error
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def fetch_json(url: str) -> Any:
    return json.loads(fetch(url))


def next_data(url: str) -> dict[str, Any]:
    found = NEXT_DATA.search(fetch(url).decode())
    if found is None:
        raise FetchError(f"{url}: no __NEXT_DATA__")
    return json.loads(found.group(1))["props"]["pageProps"]


def digits(raw: Any) -> str:
    return re.sub(r"\D", "", str(raw or ""))


def mileage(raw: str) -> str:
    return "0" if "صفر" in raw else digits(raw)


def translate(raw: str, vocabulary: dict[str, str]) -> str:
    return vocabulary.get(raw.strip(), "")


def city_and_district(location: str) -> tuple[str, str]:
    parts = [part for part in LOCATION_SPLIT.split(location.strip()) if part]
    return (parts[0] if parts else "", parts[1] if len(parts) > 1 else "")


def posted_raw(age: str, city: str, district: str) -> str:
    """Divar's `<age> در <city>، <district>`: the only place ingest reads district from."""
    age = age or UNKNOWN_AGE
    return f"{age} در {city}، {district}" if district else f"{age} در {city}"


def trim_name(*parts: str) -> str:
    """Divar-style «برند و مدل»: single-spaced, with Divar's brand spelling."""
    name = " ".join(" ".join(parts).split())
    for alias, brand in BRAND_ALIASES.items():
        if name == alias or name.startswith(alias + " "):
            return brand + name[len(alias) :]
    return name


def base_row(source: str, business_type: str) -> Row:
    row = dict.fromkeys(HEADER, "")
    row.update(
        source=source,
        fetched_at=str(int(time.time())),
        webengage_cat_2="cars",
        webengage_cat_3="light",
        webengage_business_type=business_type,
    )
    return row


def with_vocab(row: Row, gearbox: str, fuel: str, body: str) -> Row:
    row.update(
        gearbox_raw=gearbox, fuel_raw=fuel, body_raw=body,
        گیربکس=translate(gearbox, GEARBOX),
        **{"نوع سوخت": translate(fuel, FUEL), "وضعیت بدنه": translate(body, BODY)},
    )  # fmt: skip
    return row


# --- bama.ir: JSON search API, paged by pageIndex; total_pages grows as you page, so
#     stop on has_next=false or a page with nothing new.
def bama(count: int) -> Iterator[Row]:
    seen: set[str] = set()
    for page in range(10_000):
        data = fetch_json(
            f"https://bama.ir/cad/api/search?pageIndex={page}&pageSize=30"
        )
        fresh = [
            ad for ad in data["data"]["ads"]
            if ad["type"] == "ad" and ad["detail"]["code"] not in seen
        ]  # fmt: skip
        for ad in fresh:
            seen.add(ad["detail"]["code"])
            yield bama_row(ad)
            if len(seen) >= count:
                return
        if not fresh or not data["metadata"]["has_next"]:
            return


def bama_row(ad: dict[str, Any]) -> Row:
    detail, price = ad["detail"], ad["price"]
    city, district = city_and_district(detail["location"] or "")
    images = ad.get("images") or []
    row = base_row("bama", "dealer" if ad.get("dealer") else "personal")
    # «تویوتا، راوفور» → «تویوتا راوفور»: a trailing comma would make «تویوتا،» a brand.
    name = trim_name(detail["title"].replace("،", " "))
    trim = trim_name(name, detail["trim"] or "")
    row.update(
        token=f"bama-{detail['code']}",
        url=f"https://bama.ir{detail['url']}",
        title=f"{name} {detail['subtitle'] or ''}".strip(),
        description=detail.get("description") or "",
        city=city,
        posted_raw=posted_raw(detail.get("time") or "", city, district),
        image_urls=URL_SEPARATOR.join(image["large"] for image in images),
        thumbnail_urls=URL_SEPARATOR.join(image["thumb"] for image in images),
        price_type_raw=price.get("type") or "",
        رنگ=detail.get("body_color") or "",
        **{
            "برند و مدل": trim,
            "مدل (سال تولید)": detail.get("year") or "",
            "کارکرد": mileage(detail.get("mileage") or ""),
            "قیمت پایه": digits(price.get("price")).lstrip("0"),
        },
    )
    return with_vocab(
        row, detail.get("transmission") or "", detail.get("fuel") or "",
        detail.get("body_status") or "",
    )  # fmt: skip


# --- karnameh.com: SSR list pages (?page=N) + JSON detail for colour/insurance/body.
KARNAMEH_DETAIL = (
    "https://api-gw.karnameh.com/post-storage/car-posts/car-post-detail/{}/"
)


def karnameh(count: int) -> Iterator[Row]:
    produced = 0
    for page in range(1, 10_000):
        listing = next_data(f"https://karnameh.com/buy-used-cars?page={page}")[
            "firstPage"
        ]
        for post in listing["car_posts"]:
            # Detail adds colour/insurance/body but drops gearbox, so layer it over the list card.
            detail = fetch_json(KARNAMEH_DETAIL.format(post["concierge_sale_token"]))
            yield karnameh_row(
                post | {k: v for k, v in detail.items() if v is not None}
            )
            produced += 1
            if produced >= count:
                return
        if page >= listing["pages"]:
            return


def karnameh_row(post: dict[str, Any]) -> Row:
    widget = {item["name"]: item["value"] for item in post.get("widget") or []}
    images = [image["url"] for image in post.get("car_images") or []]
    city = post["city_name_fa"]
    row = base_row("karnameh", "karnameh")
    row.update(
        token=f"karnameh-{post['token']}",
        url=f"https://karnameh.com/buy-used-cars/{post['token']}",
        title=post["title"],
        description=post.get("description") or "",
        city=city,
        posted_raw=posted_raw("", city, ""),
        image_urls=URL_SEPARATOR.join(images),
        thumbnail_urls=URL_SEPARATOR.join(images),
        رنگ=widget.get("رنگ", ""),
        **{
            "برند و مدل": trim_name(
                post["brand_name_fa"], post["model_name_fa"], post["type_name_fa"]
            ),
            "مدل (سال تولید)": str(
                post.get("displayable_year") or post.get("year") or ""
            ),
            "کارکرد": digits(post.get("usage")),
            "قیمت پایه": digits(post.get("advertisement_price")),
            "مهلت بیمهٔ شخص ثالث": widget.get("بیمه شخص ثالث", ""),
        },
    )
    return with_vocab(row, post.get("gearbox") or "", "", post.get("body_status") or "")


# --- hamrah-mechanic.com: SSR list pages (used cars, kmStatus=1) + SSR detail page.
HAMRAH = "https://www.hamrah-mechanic.com"


def hamrah_url(car: dict[str, Any]) -> str:
    """Some slugs carry raw spaces (`/toyota/corolla cross/`)."""
    return HAMRAH + urllib.parse.quote(car["exhibitionDetailUrl"])


def hamrah(count: int) -> Iterator[Row]:
    produced = 0
    for page in range(1, 10_000):
        cars = next_data(f"{HAMRAH}/cars-for-sale/?kmStatus=1&page={page}")["cars"]
        for car in cars["list"]:
            if car["isSold"] or car["comingSoon"]:
                continue
            yield hamrah_row(car, next_data(hamrah_url(car)))
            produced += 1
            if produced >= count:
                return
        if not cars["list"] or page * cars["count"] >= cars["totalCount"]:
            return


def hamrah_row(car: dict[str, Any], page: dict[str, Any]) -> Row:
    details = page["orderDetails"]
    specs = {
        spec["key"]: spec["value"] for spec in details["carSpecifications"]["specs"]
    }
    images = page.get("gallery") or []
    city, district = city_and_district(car.get("carLocation") or "")
    row = base_row("hamrah-mechanic", "hamrah-mechanic")
    row.update(
        token=f"hamrah-{car['orderId']}",
        url=hamrah_url(car),
        title=f"{car['carNamePersian']} {car['carTypeName']} مدل {car['carYear']}",
        description=details["descriptionPart"].get("description") or "",
        city=city,
        posted_raw=posted_raw("", city, district),
        image_urls=URL_SEPARATOR.join(image["largeImage"] for image in images),
        thumbnail_urls=URL_SEPARATOR.join(image["thumbnailImage"] for image in images),
        رنگ=car.get("carColorName") or "",
        **{
            "برند و مدل": trim_name(car["carNamePersian"], car["carTypeName"]),
            "مدل (سال تولید)": str(car["carYear"]),
            "کارکرد": digits(car["km"]),
            "قیمت پایه": digits(car.get("offerPrice") or car["price"]),
            "مهلت بیمهٔ شخص ثالث": specs.get("remainingInsurance", ""),
            "وضعیت سند و مدارک": specs.get("document", ""),
        },
    )
    return with_vocab(
        row, car.get("gearBoxPersian") or "", specs.get("fuelType", ""),
        specs.get("bodyCondition", ""),
    )  # fmt: skip


SOURCES: dict[str, Callable[[int], Iterator[Row]]] = {
    "bama": bama, "karnameh": karnameh, "hamrah-mechanic": hamrah,
}  # fmt: skip


def write_csv(path: Path, rows: Iterator[Row]) -> int:
    """Rows go to a temp file first so an aborted crawl never leaves a half CSV behind."""
    tmp = path.with_suffix(".csv.part")
    written = 0
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            written += 1
            if written % 25 == 0:
                print(f"  {path.stem}: {written}", file=sys.stderr)
    tmp.replace(path)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", choices=[*SOURCES, "all"], default="all")
    parser.add_argument("--count", type=int, default=300, help="listings per source")
    parser.add_argument("--out-dir", type=Path, default=Path("other_sources"))
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    names = list(SOURCES) if args.source == "all" else [args.source]
    for name in names:
        written = write_csv(args.out_dir / f"{name}.csv", SOURCES[name](args.count))
        print(f"{name}: {written} rows → {args.out_dir / name}.csv")


if __name__ == "__main__":
    main()
