import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from tuni_scraper.database import Database
from tuni_scraper.llm_rating import InterestRatingResult, run_interest_rating
from tuni_scraper.models import DownloadRefreshResult, ScrapeResult
from tuni_scraper.scraper import run_download_refresh, run_scrape


@dataclass(slots=True)
class WeeklyUpdateResult:
    scrape: ScrapeResult
    rating: InterestRatingResult


@dataclass(slots=True)
class MonthlyRefreshResult:
    refresh: DownloadRefreshResult
    resumed_failures: bool
    state_path: Path | None


def _load_failed_handles(state_path: Path, cycle: str) -> list[str] | None:
    if not state_path.exists():
        return None

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    stored_cycle = payload.get("cycle") if isinstance(payload, dict) else None
    if not isinstance(stored_cycle, str):
        raise RuntimeError(f"Invalid download refresh state file: {state_path}")
    if stored_cycle != cycle:
        return None

    handles = payload.get("failed_handles") if isinstance(payload, dict) else None
    if not isinstance(handles, list) or not all(isinstance(handle, str) and handle for handle in handles):
        raise RuntimeError(f"Invalid download refresh state file: {state_path}")
    return handles


def _save_failed_handles(state_path: Path, cycle: str, handles: tuple[str, ...]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    temporary_path.write_text(
        json.dumps({"cycle": cycle, "failed_handles": handles}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_path.replace(state_path)


def run_weekly_update(
    database: Database,
    start_offset: int,
    max_offset: int,
    page_size: int,
    delay_seconds: float,
    timeout: int,
    limit_pages: int | None,
    model: str | None,
    reasoning_effort: str | None,
    batch_size: int,
    max_retries: int,
) -> WeeklyUpdateResult:
    scrape_result = run_scrape(
        database=database,
        start_offset=start_offset,
        max_offset=max_offset,
        page_size=page_size,
        delay_seconds=delay_seconds,
        timeout=timeout,
        limit_pages=limit_pages,
        refresh_downloads=False,
        stop_on_known_page=True,
        persist_progress=False,
    )

    rating_result = run_interest_rating(
        database=database,
        model=model,
        reasoning_effort=reasoning_effort,
        batch_size=batch_size,
        include_rated=False,
        max_retries=max_retries,
    )
    return WeeklyUpdateResult(scrape=scrape_result, rating=rating_result)


def run_monthly_download_refresh(
    database: Database,
    delay_seconds: float,
    timeout: int,
    limit: int | None = None,
    cycle: str | None = None,
) -> MonthlyRefreshResult:
    if limit is not None:
        return MonthlyRefreshResult(
            refresh=run_download_refresh(
                database=database,
                delay_seconds=delay_seconds,
                timeout=timeout,
                limit=limit,
            ),
            resumed_failures=False,
            state_path=None,
        )

    state_path = database.db_path.with_name(f"{database.db_path.stem}.download-refresh-pending.json")
    resolved_cycle = cycle or datetime.now().astimezone().strftime("%Y-%m")
    pending_handles = _load_failed_handles(state_path, resolved_cycle)
    result = run_download_refresh(
        database=database,
        delay_seconds=delay_seconds,
        timeout=timeout,
        handle_urls=pending_handles,
    )

    if result.failed_handles:
        _save_failed_handles(state_path, resolved_cycle, result.failed_handles)
    elif state_path.exists():
        state_path.unlink()

    return MonthlyRefreshResult(
        refresh=result,
        resumed_failures=pending_handles is not None,
        state_path=state_path,
    )
