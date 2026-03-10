from pathlib import Path

from flask import Flask, jsonify, render_template, request

from tuni_scraper.config import DEFAULT_DB_PATH
from tuni_scraper.database import Database


def parse_int_arg(name: str, default: int | None = None) -> int | None:
    raw_value = request.args.get(name, "").strip()
    if not raw_value:
        return default

    try:
        return int(raw_value)
    except ValueError:
        return default


def create_app(db_path: Path = DEFAULT_DB_PATH) -> Flask:
    app = Flask(__name__, template_folder="templates")
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.jinja_env.auto_reload = True
    database = Database(db_path)

    @app.route("/")
    def index() -> str:
        filters = {
            "title": request.args.get("title", "").strip(),
            "author": request.args.get("author", "").strip(),
            "year": parse_int_arg("year"),
            "work_type": request.args.get("work_type", "").strip(),
            "min_downloads": parse_int_arg("min_downloads"),
            "max_downloads": parse_int_arg("max_downloads"),
            "limit": max(1, min(parse_int_arg("limit", 10) or 10, 500)),
        }

        works = database.search_works(
            title=filters["title"] or None,
            author=filters["author"] or None,
            year=filters["year"],
            work_type=filters["work_type"] or None,
            min_downloads=filters["min_downloads"],
            max_downloads=filters["max_downloads"],
            limit=filters["limit"],
        )

        pub_counts_by_month = [
            dict(row) for row in database.get_publication_counts_by_accepted_month()
        ]

        return render_template(
            "index.html",
            overview=database.get_overview(),
            top_downloads=database.get_top_downloads(10),
            top_trending=database.get_top_by_downloads_per_day(10),
            least_downloads=database.get_least_downloads(10),
            least_trending=database.get_least_trending(10),
            publication_counts_by_year=database.get_publication_counts_by_year(),
            download_counts_by_year=database.get_download_counts_by_year(),
            type_counts_overall=database.get_type_counts_overall(),
            pub_counts_by_month=pub_counts_by_month,
            works=works,
            filters=filters,
            available_years=database.get_available_years(),
            available_types=database.get_available_types(),
        )

    @app.route("/api/works")
    def api_works():
        filters = {
            "title": request.args.get("title", "").strip(),
            "author": request.args.get("author", "").strip(),
            "year": parse_int_arg("year"),
            "work_type": request.args.get("work_type", "").strip(),
            "min_downloads": parse_int_arg("min_downloads"),
            "max_downloads": parse_int_arg("max_downloads"),
            "limit": max(1, min(parse_int_arg("limit", 10) or 10, 500)),
        }
        works = database.search_works(
            title=filters["title"] or None,
            author=filters["author"] or None,
            year=filters["year"],
            work_type=filters["work_type"] or None,
            min_downloads=filters["min_downloads"],
            max_downloads=filters["max_downloads"],
            limit=filters["limit"],
        )
        return jsonify({"works": [dict(row) for row in works]})

    return app