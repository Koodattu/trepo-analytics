from pathlib import Path

import pytest
import requests

from tuni_scraper.database import Database
from tuni_scraper.models import PublicationRecord
from tuni_scraper.scraper import run_download_refresh, run_scrape


def test_run_scrape_stops_when_page_contains_only_known_works(tmp_path: Path, monkeypatch) -> None:
    database = Database(tmp_path / "test.db")
    database.save_progress(next_offset=200, max_offset=999, page_size=20)
    database.upsert_work(
        PublicationRecord(
            handle_url="https://example.test/existing",
            title="Existing work",
            author="Author A",
            year=2024,
            work_type="Thesis",
            accepted_date="2024-01-01",
        )
    )
    database.update_detail("https://example.test/existing", downloads=5, accepted_date="2024-01-01")

    publications_by_offset = {
        0: [
            PublicationRecord(
                handle_url="https://example.test/new",
                title="New work",
                author="Author B",
                year=2025,
                work_type="Thesis",
            )
        ],
        20: [
            PublicationRecord(
                handle_url="https://example.test/existing",
                title="Existing work",
                author="Author A",
                year=2024,
                work_type="Thesis",
            )
        ],
    }
    requested_urls: list[str] = []

    monkeypatch.setattr("tuni_scraper.scraper.build_session", lambda: object())
    monkeypatch.setattr(
        "tuni_scraper.scraper.fetch_html",
        lambda session, url, timeout=30: requested_urls.append(url) or url,
    )
    monkeypatch.setattr(
        "tuni_scraper.scraper.parse_recent_submissions",
        lambda html, base_url: publications_by_offset[int(html.rsplit("offset=", maxsplit=1)[1])],
    )
    monkeypatch.setattr("tuni_scraper.scraper.parse_accepted_date", lambda html: "2025-02-01")
    monkeypatch.setattr("tuni_scraper.scraper.fetch_download_count", lambda session, handle_url, timeout=30: 42)

    result = run_scrape(
        database=database,
        start_offset=0,
        max_offset=40,
        page_size=20,
        delay_seconds=0,
        timeout=30,
        stop_on_known_page=True,
        persist_progress=False,
    )

    assert result.pages_processed == 2
    assert result.works_seen == 2
    assert result.new_works == 1
    assert result.new_handles == ("https://example.test/new",)
    assert result.detail_requests == 1
    assert result.detail_updates == 1
    assert result.ending_offset == 40
    assert database.get_progress(max_offset=999, page_size=20).next_offset == 200
    assert requested_urls == [
        "https://trepo.tuni.fi/handle/10024/105882/recent-submissions?offset=0",
        "https://example.test/new",
        "https://trepo.tuni.fi/handle/10024/105882/recent-submissions?offset=20",
    ]


def test_incremental_scrape_retries_missing_details_for_known_work(tmp_path: Path, monkeypatch) -> None:
    database = Database(tmp_path / "test.db")
    handle_url = "https://example.test/incomplete"
    database.upsert_work(
        PublicationRecord(
            handle_url=handle_url,
            title="Incomplete work",
            author="Author A",
            year=2025,
            work_type="Thesis",
        )
    )

    monkeypatch.setattr("tuni_scraper.scraper.build_session", lambda: object())
    monkeypatch.setattr("tuni_scraper.scraper.fetch_html", lambda session, url, timeout=30: url)
    monkeypatch.setattr(
        "tuni_scraper.scraper.parse_recent_submissions",
        lambda html, base_url: [
            PublicationRecord(
                handle_url=handle_url,
                title="Incomplete work",
                author="Author A",
                year=2025,
                work_type="Thesis",
            )
        ],
    )
    monkeypatch.setattr("tuni_scraper.scraper.parse_accepted_date", lambda html: "2025-03-01")
    monkeypatch.setattr("tuni_scraper.scraper.fetch_download_count", lambda session, url, timeout=30: 9)

    result = run_scrape(
        database=database,
        start_offset=0,
        max_offset=20,
        page_size=20,
        delay_seconds=0,
        stop_on_known_page=True,
        persist_progress=False,
    )

    assert result.pages_processed == 1
    assert result.new_works == 0
    assert result.detail_updates == 1
    assert database.needs_detail_fetch(handle_url) is False


def test_incremental_scrape_raises_when_listing_request_fails(tmp_path: Path, monkeypatch) -> None:
    database = Database(tmp_path / "test.db")
    monkeypatch.setattr("tuni_scraper.scraper.build_session", lambda: object())
    monkeypatch.setattr(
        "tuni_scraper.scraper.fetch_html",
        lambda session, url, timeout=30: (_ for _ in ()).throw(requests.ConnectionError("offline")),
    )

    with pytest.raises(RuntimeError, match="offset 0"):
        run_scrape(
            database=database,
            start_offset=0,
            max_offset=20,
            page_size=20,
            delay_seconds=0,
            stop_on_known_page=True,
            persist_progress=False,
        )


def test_download_refresh_uses_stored_handles_and_preserves_failed_values(tmp_path: Path, monkeypatch) -> None:
    database = Database(tmp_path / "test.db")
    for index in range(1, 4):
        handle_url = f"https://example.test/{index}"
        database.upsert_work(
            PublicationRecord(
                handle_url=handle_url,
                title=f"Work {index}",
                author=None,
                year=2025,
                work_type="Thesis",
                downloads=index * 10,
                accepted_date=f"2025-01-0{index}",
            )
        )

    responses = {
        "https://example.test/1": 111,
        "https://example.test/2": None,
        "https://example.test/3": requests.ConnectionError("offline"),
    }
    requested_handles: list[str] = []

    def fake_fetch_download_count(session, handle_url: str, timeout: int = 30) -> int | None:
        requested_handles.append(handle_url)
        response = responses[handle_url]
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr("tuni_scraper.scraper.build_session", lambda: object())
    monkeypatch.setattr("tuni_scraper.scraper.fetch_download_count", fake_fetch_download_count)

    result = run_download_refresh(database=database, delay_seconds=0)

    assert result.works_considered == 3
    assert result.downloads_updated == 1
    assert result.failures == 2
    assert result.failed_handles == (
        "https://example.test/2",
        "https://example.test/3",
    )
    assert requested_handles == sorted(responses)
    rows = database.fetch_rows("SELECT handle_url, downloads, accepted_date FROM works ORDER BY handle_url")
    assert [(row["downloads"], row["accepted_date"]) for row in rows] == [
        (111, "2025-01-01"),
        (20, "2025-01-02"),
        (30, "2025-01-03"),
    ]


def test_download_refresh_queues_remaining_handles_after_repeated_failures(tmp_path: Path, monkeypatch) -> None:
    database = Database(tmp_path / "test.db")
    handles = [f"https://example.test/{index}" for index in range(1, 5)]
    for index, handle_url in enumerate(handles, start=1):
        database.upsert_work(
            PublicationRecord(
                handle_url=handle_url,
                title=f"Work {index}",
                author=None,
                year=2025,
                work_type="Thesis",
            )
        )

    requested_handles: list[str] = []
    monkeypatch.setattr("tuni_scraper.scraper.build_session", lambda: object())

    def fail_download(session, handle_url: str, timeout: int = 30) -> int:
        requested_handles.append(handle_url)
        raise requests.ConnectionError("offline")

    monkeypatch.setattr("tuni_scraper.scraper.fetch_download_count", fail_download)

    result = run_download_refresh(
        database=database,
        delay_seconds=0,
        max_consecutive_failures=2,
    )

    assert requested_handles == handles[:2]
    assert result.works_considered == 4
    assert result.downloads_updated == 0
    assert result.failed_handles == tuple([*handles[2:], *handles[:2]])
