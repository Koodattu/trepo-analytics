import logging
import time
from collections.abc import Sequence
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from tuni_scraper.config import (
    BASE_URL,
    DEFAULT_DELAY_SECONDS,
    DEFAULT_MAX_OFFSET,
    DEFAULT_PAGE_SIZE,
    DEFAULT_TIMEOUT_SECONDS,
    build_recent_submissions_url,
)
from tuni_scraper.database import Database
from tuni_scraper.models import DownloadRefreshResult, ScrapeResult
from tuni_scraper.parsing import parse_accepted_date, parse_download_stats_payload, parse_recent_submissions


LOGGER = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "tuni-msc-downloads-counts-scraper/0.1 (+https://trepo.tuni.fi)",
            "Accept-Language": "en-US,en;q=0.8,fi;q=0.7",
        }
    )

    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=RETRYABLE_STATUS_CODES,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def fetch_html(session: requests.Session, url: str, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def extract_handle(handle_url: str) -> str | None:
    parsed = urlparse(handle_url)
    path = parsed.path.strip("/")
    if not path.startswith("handle/"):
        return None

    handle = path.removeprefix("handle/")
    return handle or None


def fetch_download_count(session: requests.Session, handle_url: str, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> int | None:
    handle = extract_handle(handle_url)
    if handle is None:
        raise ValueError(f"Unsupported TREPO handle URL: {handle_url}")

    response = session.get(
        f"{BASE_URL}/simplestats/rest",
        params={"handle": handle},
        timeout=timeout,
    )
    response.raise_for_status()
    return parse_download_stats_payload(response.json())


def run_scrape(
    database: Database,
    start_offset: int | None = None,
    max_offset: int = DEFAULT_MAX_OFFSET,
    page_size: int = DEFAULT_PAGE_SIZE,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    limit_pages: int | None = None,
    refresh_downloads: bool = False,
    stop_on_known_page: bool = False,
    persist_progress: bool = True,
) -> ScrapeResult:
    if start_offset is not None:
        current_offset = start_offset
    elif persist_progress:
        progress = database.get_progress(max_offset=max_offset, page_size=page_size)
        current_offset = progress.next_offset
    else:
        current_offset = 0

    session = build_session()

    pages_processed = 0
    works_seen = 0
    new_handles: list[str] = []
    detail_requests = 0
    detail_updates = 0

    while current_offset <= max_offset:
        if limit_pages is not None and pages_processed >= limit_pages:
            break

        listing_url = build_recent_submissions_url(current_offset)
        LOGGER.info("Fetching recent submissions page: %s", listing_url)
        try:
            html = fetch_html(session, listing_url, timeout=timeout)
        except requests.RequestException as error:
            raise RuntimeError(f"Failed to fetch recent submissions page at offset {current_offset}.") from error

        publications = parse_recent_submissions(html, base_url=BASE_URL)
        if stop_on_known_page and not publications:
            raise RuntimeError(f"TREPO returned no publications at offset {current_offset}.")

        page_contains_only_known_works = bool(publications)

        for publication in publications:
            publication.listing_offset = current_offset
            publication.listing_url = listing_url
            already_seen = database.has_work(publication.handle_url)
            database.upsert_work(publication)
            works_seen += 1
            needs_detail_fetch = database.needs_detail_fetch(publication.handle_url)

            if already_seen:
                if stop_on_known_page and not needs_detail_fetch:
                    continue
            else:
                page_contains_only_known_works = False
                new_handles.append(publication.handle_url)

            if not refresh_downloads and not needs_detail_fetch:
                continue

            if delay_seconds > 0:
                time.sleep(delay_seconds)

            LOGGER.info("Fetching page and download stats: %s", publication.handle_url)
            detail_requests += 1

            # Fetch the handle page HTML to extract the accepted date
            accepted_date: str | None = None
            try:
                page_html = fetch_html(session, publication.handle_url, timeout=timeout)
                accepted_date = parse_accepted_date(page_html)
            except requests.RequestException as error:
                LOGGER.warning("Failed to fetch page HTML for %s: %s", publication.handle_url, error)

            # Fetch download count from the stats API
            downloads: int | None = None
            try:
                downloads = fetch_download_count(session, publication.handle_url, timeout=timeout)
            except requests.RequestException as error:
                LOGGER.warning("Failed to fetch download stats for %s: %s", publication.handle_url, error)

                if delay_seconds > 0:
                    time.sleep(max(delay_seconds, 2.0))

                continue

            database.update_detail(publication.handle_url, downloads, accepted_date)
            detail_updates += 1

        pages_processed += 1
        current_offset += page_size

        if persist_progress:
            database.save_progress(current_offset, max_offset, page_size)

        if stop_on_known_page and page_contains_only_known_works:
            LOGGER.info("Encountered a recent submissions page with only known publications at offset %s.", current_offset - page_size)
            break

        if delay_seconds > 0 and current_offset <= max_offset:
            time.sleep(delay_seconds)

    return ScrapeResult(
        pages_processed=pages_processed,
        works_seen=works_seen,
        new_works=len(new_handles),
        detail_requests=detail_requests,
        detail_updates=detail_updates,
        ending_offset=current_offset,
        new_handles=tuple(new_handles),
    )


def run_download_refresh(
    database: Database,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    limit: int | None = None,
    handle_urls: Sequence[str] | None = None,
    max_consecutive_failures: int = 10,
) -> DownloadRefreshResult:
    if delay_seconds < 0:
        raise ValueError("delay_seconds must not be negative")
    if max_consecutive_failures <= 0:
        raise ValueError("max_consecutive_failures must be greater than zero")

    handles = list(handle_urls) if handle_urls is not None else database.get_work_handles(limit=limit)
    session = build_session()
    downloads_updated = 0
    failed_handles: list[str] = []
    consecutive_failures = 0

    for index, handle_url in enumerate(handles):
        if index > 0 and delay_seconds > 0:
            time.sleep(delay_seconds)

        LOGGER.info("Refreshing download count %s/%s: %s", index + 1, len(handles), handle_url)
        try:
            downloads = fetch_download_count(session, handle_url, timeout=timeout)
        except (requests.RequestException, ValueError) as error:
            LOGGER.warning("Failed to refresh download count for %s: %s", handle_url, error)
            failed_handles.append(handle_url)
        else:
            if downloads is not None:
                database.update_download_count(handle_url, downloads)
                downloads_updated += 1
                consecutive_failures = 0
                continue

            LOGGER.warning("TREPO returned no download count for %s; preserving the stored value.", handle_url)
            failed_handles.append(handle_url)

        consecutive_failures += 1
        if consecutive_failures >= max_consecutive_failures:
            remaining_handles = handles[index + 1 :]
            failed_handles = [*remaining_handles, *failed_handles]
            LOGGER.error(
                "Aborting the refresh after %s consecutive failures; %s handles remain queued.",
                consecutive_failures,
                len(remaining_handles),
            )
            break

    return DownloadRefreshResult(
        works_considered=len(handles),
        downloads_updated=downloads_updated,
        failures=len(failed_handles),
        failed_handles=tuple(failed_handles),
    )
