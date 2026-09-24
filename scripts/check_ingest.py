"""Acceptance check: feed crawled CSVs through torob-car's own `map_row`.

Run from torob-car's backend so its ingest package is importable:

    cd ../torob-car/backend
    PYTHONPATH=. uv run python ../../drill/scripts/check_ingest.py \
        ../../drill/other_sources/*.csv

Exits non-zero if any CSV has a value the ingest would abort on, or no usable rows.
"""

import csv
import sys
from collections import Counter

from ingest.column_maps import UnknownValueError
from ingest.row_mapper import NormalizedListing, RowRejectedError, map_row

SAMPLE_ROWS = 3


def check(path: str) -> bool:
    rejected: Counter[str] = Counter()
    nulled: Counter[str] = Counter()
    unknown: Counter[str] = Counter()
    listings: list[NormalizedListing] = []
    with open(path, encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            try:
                mapped = map_row(row)
            except RowRejectedError as error:
                rejected[error.reason] += 1
                continue
            except UnknownValueError as error:
                unknown[str(error.args)] += 1
                continue
            listings.append(mapped.listing)
            nulled.update(mapped.nulled)
    print(f"== {path}: ok={len(listings)} rejected={dict(rejected)} "
          f"nulled={dict(nulled)} unknown={dict(unknown)}")  # fmt: skip
    for listing in listings[:SAMPLE_ROWS]:
        print("  ", listing.token, listing.brand, "|", listing.trim, "|",
              listing.year, listing.km, listing.price, listing.gearbox,
              listing.fuel, listing.body_condition, listing.city, listing.district)  # fmt: skip
    return bool(listings) and not unknown


def main() -> None:
    csv.field_size_limit(sys.maxsize)
    results = [check(path) for path in sys.argv[1:]]
    sys.exit(0 if results and all(results) else 1)


if __name__ == "__main__":
    main()
