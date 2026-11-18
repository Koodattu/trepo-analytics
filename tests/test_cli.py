from pathlib import Path

import pytest

from tuni_scraper.cli import build_parser, main, print_report
from tuni_scraper.database import Database
from tuni_scraper.jobs import MonthlyRefreshResult
from tuni_scraper.models import DownloadRefreshResult, PublicationRecord


def test_report_counts_works_with_known_downloads(tmp_path: Path, capsys) -> None:
    database = Database(tmp_path / "report.db")
    for title, downloads in [("Known", 0), ("Missing", None)]:
        database.upsert_work(PublicationRecord(
            handle_url=f"https://example.test/{title}", title=title, downloads=downloads,
            author=None, year=None, work_type=None,
        ))

    print_report(database, limit=1)

    output = capsys.readouterr().out
    assert "Total publications: 2\n" in output
    assert "Publications with downloads: 1\n" in output


def test_parser_resolves_path_defaults_from_current_environment(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "configured.db"
    export_path = tmp_path / "configured.json"
    monkeypatch.setenv("TUNI_SCRAPER_DB_PATH", str(database_path))
    monkeypatch.setenv("TUNI_SCRAPER_EXPORT_PATH", str(export_path))

    parser = build_parser()

    assert parser.parse_args(["report"]).db_path == database_path
    assert parser.parse_args(["export-json"]).output == export_path


def test_refresh_downloads_exits_nonzero_when_failures_remain(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "test.db"
    state_path = tmp_path / "pending.json"
    result = MonthlyRefreshResult(
        refresh=DownloadRefreshResult(
            works_considered=2,
            downloads_updated=1,
            failures=1,
            failed_handles=("https://example.test/2",),
        ),
        resumed_failures=False,
        state_path=state_path,
    )
    monkeypatch.setattr("tuni_scraper.cli.run_monthly_download_refresh", lambda **kwargs: result)
    monkeypatch.setattr("tuni_scraper.cli.configure_environment", lambda: None)
    monkeypatch.setattr(
        "sys.argv",
        ["trepo-scraper", "--db-path", str(database_path), "refresh-downloads"],
    )

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 1
    assert "exit status 1" in (tmp_path / "logs" / "trepo-errors.log").read_text(encoding="utf-8")
