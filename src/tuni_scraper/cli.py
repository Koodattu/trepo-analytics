import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

from tuni_scraper.config import (
    DEFAULT_DB_PATH,
    DEFAULT_DELAY_SECONDS,
    DEFAULT_EXPORT_PATH,
    DEFAULT_LLM_BATCH_SIZE,
    DEFAULT_LLM_MAX_RETRIES,
    DEFAULT_MAX_OFFSET,
    DEFAULT_OPENAI_MODEL,
    DEFAULT_PAGE_SIZE,
    OPENAI_MODEL_ENV,
    PROJECT_ROOT,
)
from tuni_scraper.database import Database
from tuni_scraper.llm_rating import run_interest_rating
from tuni_scraper.reports import render_table
from tuni_scraper.scraper import run_scrape
from tuni_scraper.web import create_app


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def configure_environment() -> None:
    load_dotenv(PROJECT_ROOT / ".env")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape TREPO recent submissions and analyze download counts.")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="SQLite database path.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    scrape_parser = subparsers.add_parser("scrape", help="Scrape recent submissions and publication download counts.")
    scrape_parser.add_argument("--start-offset", type=int, default=None, help="Override the saved starting offset.")
    scrape_parser.add_argument("--max-offset", type=int, default=DEFAULT_MAX_OFFSET, help="Maximum recent-submissions offset.")
    scrape_parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Listing page size.")
    scrape_parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Delay in seconds between requests.")
    scrape_parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds.")
    scrape_parser.add_argument("--limit-pages", type=int, default=None, help="Only scrape this many listing pages.")
    scrape_parser.add_argument("--refresh-downloads", action="store_true", help="Re-fetch downloads even if the detail page was already scraped.")

    report_parser = subparsers.add_parser("report", help="Print summary reports.")
    report_parser.add_argument("--limit", type=int, default=10, help="Limit for top lists.")

    export_parser = subparsers.add_parser("export-json", help="Export all stored records to JSON.")
    export_parser.add_argument("--output", type=Path, default=DEFAULT_EXPORT_PATH, help="JSON export path.")

    rate_parser = subparsers.add_parser(
        "rate-interest",
        help="Use OpenAI to rate publication titles by how unusually interesting they sound.",
    )
    rate_parser.add_argument(
        "--model",
        default=None,
        help=f"OpenAI model. Defaults to ${OPENAI_MODEL_ENV} or {DEFAULT_OPENAI_MODEL}.",
    )
    rate_parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_LLM_BATCH_SIZE,
        help="Number of publication titles to send in each OpenAI request.",
    )
    rate_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of publications to rate during this run.",
    )
    rate_parser.add_argument(
        "--rerate",
        action="store_true",
        help="Include publications that already have an interestingness rating.",
    )
    rate_parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_LLM_MAX_RETRIES,
        help="Retry count for invalid or incomplete structured model responses.",
    )

    update_parser = subparsers.add_parser(
        "update-new",
        help="Fetch only newly appeared submissions from the front page, rate them, and stop when only known publications remain.",
    )
    update_parser.add_argument("--start-offset", type=int, default=0, help="Starting offset for the incremental scan.")
    update_parser.add_argument("--max-offset", type=int, default=DEFAULT_MAX_OFFSET, help="Maximum recent-submissions offset.")
    update_parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Listing page size.")
    update_parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS, help="Delay in seconds between requests.")
    update_parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds.")
    update_parser.add_argument("--limit-pages", type=int, default=None, help="Only scan this many listing pages.")
    update_parser.add_argument(
        "--model",
        default=None,
        help=f"OpenAI model. Defaults to ${OPENAI_MODEL_ENV} or {DEFAULT_OPENAI_MODEL}.",
    )
    update_parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_LLM_BATCH_SIZE,
        help="Number of publication titles to send in each OpenAI request.",
    )
    update_parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_LLM_MAX_RETRIES,
        help="Retry count for invalid or incomplete structured model responses.",
    )

    reset_parser = subparsers.add_parser("reset-progress", help="Reset the saved scraping offset.")
    reset_parser.add_argument("--offset", type=int, default=0, help="Offset to store as the next page to scrape.")
    reset_parser.add_argument("--max-offset", type=int, default=DEFAULT_MAX_OFFSET, help="Maximum offset to store.")
    reset_parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="Page size to store.")

    serve_parser = subparsers.add_parser("serve", help="Run the Flask dashboard.")
    serve_parser.add_argument("--host", default="127.0.0.1", help="Host interface.")
    serve_parser.add_argument("--port", type=int, default=5000, help="Port number.")
    serve_parser.add_argument("--debug", action="store_true", help="Run Flask in debug mode.")

    return parser


def print_report(database: Database, limit: int) -> None:
    overview = database.get_overview()
    print("Overview")
    print(f"Total publications: {overview['total_publications']}")
    print(f"Publications with downloads: {overview['publications_with_downloads']}")
    print(f"Total downloads: {overview['total_downloads']}")
    print(f"Next offset: {overview['next_offset']} / {overview['max_offset']}")
    print()

    top_downloads = [
        (row["downloads"], row["title"], row["author"], row["year"], row["work_type"])
        for row in database.get_top_downloads(limit)
    ]
    print(render_table("Top downloads", ["Downloads", "Title", "Author", "Year", "Type"], top_downloads))
    print()

    top_authors = [(row["author"], row["publication_count"]) for row in database.get_top_authors(limit)]
    print(render_table("Top authors", ["Author", "Publications"], top_authors))
    print()

    publications_by_year = [(row["year"], row["publication_count"]) for row in database.get_publication_counts_by_year()]
    print(render_table("Publications by year", ["Year", "Publications"], publications_by_year))
    print()

    downloads_by_year = [(row["year"], row["total_downloads"]) for row in database.get_download_counts_by_year()]
    print(render_table("Downloads by year", ["Year", "Downloads"], downloads_by_year))
    print()

    downloads_by_type = [(row["work_type"], row["total_downloads"]) for row in database.get_download_counts_by_type()]
    print(render_table("Downloads by type", ["Type", "Downloads"], downloads_by_type))
    print()

    type_counts = [(row["work_type"], row["publication_count"]) for row in database.get_type_counts_overall()]
    print(render_table("Type counts overall", ["Type", "Publications"], type_counts))
    print()

    type_counts_by_year = [(row["year"], row["work_type"], row["publication_count"]) for row in database.get_type_counts_by_year()]
    print(render_table("Type counts by year", ["Year", "Type", "Publications"], type_counts_by_year))


def main() -> None:
    configure_environment()
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    database = Database(args.db_path)

    if args.command == "scrape":
        result = run_scrape(
            database=database,
            start_offset=args.start_offset,
            max_offset=args.max_offset,
            page_size=args.page_size,
            delay_seconds=args.delay,
            timeout=args.timeout,
            limit_pages=args.limit_pages,
            refresh_downloads=args.refresh_downloads,
        )
        print(f"Pages processed: {result.pages_processed}")
        print(f"Works seen: {result.works_seen}")
        print(f"Detail requests: {result.detail_requests}")
        print(f"Detail updates: {result.detail_updates}")
        print(f"Next offset: {result.ending_offset}")
        return

    if args.command == "report":
        print_report(database, limit=args.limit)
        return

    if args.command == "export-json":
        database.export_json(args.output)
        print(f"Exported data to {args.output}")
        return

    if args.command == "rate-interest":
        result = run_interest_rating(
            database=database,
            model=args.model,
            batch_size=args.batch_size,
            limit=args.limit,
            include_rated=args.rerate,
            max_retries=args.max_retries,
        )
        print(f"Model: {result.model}")
        print(f"Works considered: {result.works_considered}")
        print(f"Works rated: {result.works_rated}")
        print(f"Batches sent: {result.batches_sent}")
        return

    if args.command == "update-new":
        scrape_result = run_scrape(
            database=database,
            start_offset=args.start_offset,
            max_offset=args.max_offset,
            page_size=args.page_size,
            delay_seconds=args.delay,
            timeout=args.timeout,
            limit_pages=args.limit_pages,
            refresh_downloads=False,
            stop_on_known_page=True,
            persist_progress=False,
        )
        print(f"Pages processed: {scrape_result.pages_processed}")
        print(f"Works seen: {scrape_result.works_seen}")
        print(f"New works: {scrape_result.new_works}")
        print(f"Detail requests: {scrape_result.detail_requests}")
        print(f"Detail updates: {scrape_result.detail_updates}")
        print(f"Stopping offset: {scrape_result.ending_offset}")

        rating_result = run_interest_rating(
            database=database,
            model=args.model,
            batch_size=args.batch_size,
            include_rated=False,
            max_retries=args.max_retries,
            handle_urls=scrape_result.new_handles,
        )
        print(f"Model: {rating_result.model}")
        print(f"Works considered for rating: {rating_result.works_considered}")
        print(f"Works rated: {rating_result.works_rated}")
        print(f"Batches sent: {rating_result.batches_sent}")
        return

    if args.command == "reset-progress":
        database.reset_progress(offset=args.offset, max_offset=args.max_offset, page_size=args.page_size)
        print(f"Progress reset to offset {args.offset}")
        return

    if args.command == "serve":
        app = create_app(args.db_path)
        app.run(host=args.host, port=args.port, debug=args.debug)
        return

    parser.error(f"Unknown command: {args.command}")