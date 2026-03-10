import argparse
import logging
from pathlib import Path

from tuni_scraper.config import DEFAULT_DB_PATH, DEFAULT_DELAY_SECONDS, DEFAULT_EXPORT_PATH, DEFAULT_MAX_OFFSET, DEFAULT_PAGE_SIZE
from tuni_scraper.database import Database
from tuni_scraper.reports import render_table
from tuni_scraper.scraper import run_scrape
from tuni_scraper.web import create_app


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


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

    if args.command == "reset-progress":
        database.reset_progress(offset=args.offset, max_offset=args.max_offset, page_size=args.page_size)
        print(f"Progress reset to offset {args.offset}")
        return

    if args.command == "serve":
        app = create_app(args.db_path)
        app.run(host=args.host, port=args.port, debug=args.debug)
        return

    parser.error(f"Unknown command: {args.command}")