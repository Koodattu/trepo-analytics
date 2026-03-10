from pathlib import Path

from tuni_scraper.database import Database
from tuni_scraper.models import PublicationRecord
from tuni_scraper.scraper import run_scrape


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