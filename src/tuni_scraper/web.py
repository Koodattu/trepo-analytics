from pathlib import Path

from flask import Flask, jsonify, render_template, request

from tuni_scraper.config import DEFAULT_DB_PATH
from tuni_scraper.database import Database


VALID_SORT_FIELDS = {"downloads", "downloads_per_day", "interest", "accepted"}
VALID_SORT_DIRECTIONS = {"asc", "desc"}


def parse_int_arg(name: str, default: int | None = None) -> int | None:
    raw_value = request.args.get(name, "").strip()
    if not raw_value:
        return default

    try:
        return int(raw_value)
    except ValueError:
        return default


def parse_str_arg(name: str, default: str = "") -> str:
    return request.args.get(name, default).strip()


def build_work_filters() -> dict[str, str | int | None]:
    sort_by = parse_str_arg("sort_by", "downloads")
    sort_direction = parse_str_arg("sort_direction", "desc").lower()
    if sort_by not in VALID_SORT_FIELDS:
        sort_by = "downloads"
    if sort_direction not in VALID_SORT_DIRECTIONS:
        sort_direction = "desc"

    return {
        "title": parse_str_arg("title"),
        "author": parse_str_arg("author"),
        "year": parse_int_arg("year"),
        "work_type": parse_str_arg("work_type"),
        "min_downloads": parse_int_arg("min_downloads"),
        "max_downloads": parse_int_arg("max_downloads"),
        "min_interest": parse_int_arg("min_interest"),
        "max_interest": parse_int_arg("max_interest"),
        "accepted_start": parse_str_arg("accepted_start"),
        "accepted_end": parse_str_arg("accepted_end"),
        "sort_by": sort_by,
        "sort_direction": sort_direction,
        "limit": max(1, min(parse_int_arg("limit", 25) or 25, 500)),
    }


def build_random_gems_payload(database: Database) -> dict[str, list[dict[str, object]]]:
    return {
        "interesting": [dict(row) for row in database.get_random_interesting_works(2, 80)],
        "signal_rich": [dict(row) for row in database.get_random_signal_rich_works(2, 90)],
        "interest_heavy_mismatch": [
            dict(row)
            for row in database.get_random_interest_heavy_mismatch_works(2, 25.0, 80)
        ],
    }


def create_app(db_path: Path = DEFAULT_DB_PATH) -> Flask:
    app = Flask(__name__, template_folder="templates")
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.jinja_env.auto_reload = True
    database = Database(db_path)

    @app.route("/")
    def index() -> str:
        filters = build_work_filters()
        random_gems = build_random_gems_payload(database)

        works = database.search_works(
            title=filters["title"] or None,
            author=filters["author"] or None,
            year=filters["year"],
            work_type=filters["work_type"] or None,
            min_downloads=filters["min_downloads"],
            max_downloads=filters["max_downloads"],
            min_interest=filters["min_interest"],
            max_interest=filters["max_interest"],
            accepted_start=filters["accepted_start"] or None,
            accepted_end=filters["accepted_end"] or None,
            sort_by=str(filters["sort_by"]),
            sort_direction=str(filters["sort_direction"]),
            limit=filters["limit"],
        )

        pub_counts_by_month = [
            dict(row) for row in database.get_publication_counts_by_accepted_month()
        ]

        return render_template(
            "index.html",
            overview=database.get_overview(),
            random_interesting=random_gems["interesting"],
            random_signal_rich=random_gems["signal_rich"],
            random_interest_heavy_mismatch=random_gems["interest_heavy_mismatch"],
            top_downloads=database.get_top_downloads(10),
            top_trending=database.get_top_by_downloads_per_day(10),
            top_interesting=database.get_top_by_interestingness(10),
            least_trending=database.get_least_trending(10),
            top_signal_rich=database.get_top_signal_rich_works(10),
            least_interesting=database.get_least_interesting_works(10),
            least_signal_rich=database.get_least_signal_rich_works(10),
            download_heavy_outliers=database.get_download_heavy_outliers(10),
            interest_heavy_outliers=database.get_interest_heavy_outliers(10),
            publication_counts_by_year=database.get_publication_counts_by_year(),
            download_counts_by_year=database.get_download_counts_by_year(),
            type_counts_overall=database.get_type_counts_overall(),
            interest_bucket_summary=database.get_interest_bucket_summary(),
            work_type_signal_summary=database.get_work_type_signal_summary(),
            pub_counts_by_month=pub_counts_by_month,
            works=works,
            filters=filters,
            available_years=database.get_available_years(),
            available_types=database.get_available_types(),
        )

    @app.route("/api/works")
    def api_works():
        filters = build_work_filters()
        works = database.search_works(
            title=filters["title"] or None,
            author=filters["author"] or None,
            year=filters["year"],
            work_type=filters["work_type"] or None,
            min_downloads=filters["min_downloads"],
            max_downloads=filters["max_downloads"],
            min_interest=filters["min_interest"],
            max_interest=filters["max_interest"],
            accepted_start=filters["accepted_start"] or None,
            accepted_end=filters["accepted_end"] or None,
            sort_by=str(filters["sort_by"]),
            sort_direction=str(filters["sort_direction"]),
            limit=filters["limit"],
        )
        return jsonify({"works": [dict(row) for row in works]})

    @app.route("/api/random-gems")
    def api_random_gems():
        return jsonify(build_random_gems_payload(database))

    return app