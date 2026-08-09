from pathlib import Path

from tuni_scraper.database import Database
from tuni_scraper.jobs import run_monthly_download_refresh, run_weekly_update
from tuni_scraper.llm_rating import InterestRatingResult
from tuni_scraper.models import DownloadRefreshResult, ScrapeResult


def test_weekly_update_scrapes_before_rating_the_unrated_backlog(tmp_path: Path, monkeypatch) -> None:
    database = Database(tmp_path / "test.db")
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_scrape(**kwargs) -> ScrapeResult:
        calls.append(("scrape", kwargs))
        return ScrapeResult(
            pages_processed=1,
            works_seen=1,
            new_works=1,
            detail_requests=1,
            detail_updates=1,
            ending_offset=20,
            new_handles=("https://example.test/new",),
        )

    def fake_rating(**kwargs) -> InterestRatingResult:
        calls.append(("rating", kwargs))
        return InterestRatingResult(
            model="gpt-5.6-luna",
            reasoning_effort="low",
            works_considered=2,
            works_rated=2,
            batches_sent=1,
        )

    monkeypatch.setattr("tuni_scraper.jobs.run_scrape", fake_scrape)
    monkeypatch.setattr("tuni_scraper.jobs.run_interest_rating", fake_rating)

    result = run_weekly_update(
        database=database,
        start_offset=0,
        max_offset=100,
        page_size=20,
        delay_seconds=0,
        timeout=30,
        limit_pages=None,
        model=None,
        reasoning_effort=None,
        batch_size=100,
        max_retries=3,
    )

    assert [name for name, _ in calls] == ["scrape", "rating"]
    assert calls[0][1]["stop_on_known_page"] is True
    assert calls[0][1]["persist_progress"] is False
    assert "handle_urls" not in calls[1][1]
    assert calls[1][1]["include_rated"] is False
    assert result.rating.works_rated == 2


def test_monthly_refresh_retries_only_queued_failures(tmp_path: Path, monkeypatch) -> None:
    database = Database(tmp_path / "test.db")
    failed_handles = ("https://example.test/2", "https://example.test/3")
    calls: list[dict[str, object]] = []

    def fake_refresh(**kwargs) -> DownloadRefreshResult:
        calls.append(kwargs)
        if len(calls) == 1:
            return DownloadRefreshResult(
                works_considered=3,
                downloads_updated=1,
                failures=2,
                failed_handles=failed_handles,
            )
        return DownloadRefreshResult(
            works_considered=2,
            downloads_updated=2,
            failures=0,
            failed_handles=(),
        )

    monkeypatch.setattr("tuni_scraper.jobs.run_download_refresh", fake_refresh)

    first_result = run_monthly_download_refresh(
        database,
        delay_seconds=0,
        timeout=30,
        cycle="2026-08",
    )

    assert first_result.refresh.failures == 2
    assert first_result.state_path is not None
    assert first_result.state_path.exists()

    second_result = run_monthly_download_refresh(
        database,
        delay_seconds=0,
        timeout=30,
        cycle="2026-08",
    )

    assert second_result.resumed_failures is True
    assert calls[1]["handle_urls"] == list(failed_handles)
    assert second_result.refresh.failures == 0
    assert first_result.state_path.exists() is False


def test_new_month_ignores_an_older_failure_queue(tmp_path: Path, monkeypatch) -> None:
    database = Database(tmp_path / "test.db")
    calls: list[dict[str, object]] = []

    def fake_refresh(**kwargs) -> DownloadRefreshResult:
        calls.append(kwargs)
        if len(calls) == 1:
            return DownloadRefreshResult(
                works_considered=1,
                downloads_updated=0,
                failures=1,
                failed_handles=("https://example.test/stale",),
            )
        return DownloadRefreshResult(
            works_considered=10,
            downloads_updated=10,
            failures=0,
            failed_handles=(),
        )

    monkeypatch.setattr("tuni_scraper.jobs.run_download_refresh", fake_refresh)

    august_result = run_monthly_download_refresh(
        database,
        delay_seconds=0,
        timeout=30,
        cycle="2026-08",
    )
    september_result = run_monthly_download_refresh(
        database,
        delay_seconds=0,
        timeout=30,
        cycle="2026-09",
    )

    assert august_result.refresh.failures == 1
    assert calls[1]["handle_urls"] is None
    assert september_result.resumed_failures is False
    assert september_result.refresh.failures == 0
    assert august_result.state_path is not None
    assert august_result.state_path.exists() is False
