from pathlib import Path

from tuni_scraper.database import Database
from tuni_scraper.models import PublicationRecord


def build_database(tmp_path: Path) -> Database:
    database = Database(tmp_path / "test.db")

    database.upsert_work(
        PublicationRecord(
            handle_url="https://example.test/1",
            title="Alpha",
            author="Author A",
            year=2024,
            work_type="Thesis",
            downloads=100,
            accepted_date="2024-01-01",
        )
    )
    database.update_interest_ratings([(20, "https://example.test/1")])

    database.upsert_work(
        PublicationRecord(
            handle_url="https://example.test/2",
            title="Beta",
            author="Author B",
            year=2024,
            work_type="Thesis",
            downloads=50,
            accepted_date="2024-06-01",
        )
    )
    database.update_interest_ratings([(95, "https://example.test/2")])

    database.upsert_work(
        PublicationRecord(
            handle_url="https://example.test/3",
            title="Gamma",
            author="Author C",
            year=2023,
            work_type="Article",
            downloads=10,
            accepted_date=None,
        )
    )
    database.update_interest_ratings([(60, "https://example.test/3")])

    return database


def test_search_works_ranks_by_selected_download_metric(tmp_path: Path) -> None:
    database = build_database(tmp_path)

    rows = database.search_works(sort_by="downloads", sort_direction="desc", limit=10)

    assert [row["title"] for row in rows] == ["Alpha", "Beta", "Gamma"]
    assert [row["sort_rank"] for row in rows] == [1, 2, 3]


def test_search_works_ranks_by_selected_interest_metric(tmp_path: Path) -> None:
    database = build_database(tmp_path)

    rows = database.search_works(sort_by="interest", sort_direction="desc", limit=10)

    assert [row["title"] for row in rows] == ["Beta", "Gamma", "Alpha"]
    assert [row["sort_rank"] for row in rows] == [1, 2, 3]


def test_search_works_null_rank_follows_selected_metric(tmp_path: Path) -> None:
    database = build_database(tmp_path)

    rows = database.search_works(sort_by="accepted", sort_direction="asc", limit=10)

    assert [row["title"] for row in rows] == ["Alpha", "Beta", "Gamma"]
    assert [row["sort_rank"] for row in rows] == [1, 2, None]


def test_search_works_preserves_global_rank_when_filtered(tmp_path: Path) -> None:
    database = build_database(tmp_path)

    rows = database.search_works(title="Beta", sort_by="downloads", sort_direction="desc", limit=10)

    assert len(rows) == 1
    assert rows[0]["title"] == "Beta"
    assert rows[0]["sort_rank"] == 2


def test_get_works_for_interest_rating_can_filter_to_specific_handles(tmp_path: Path) -> None:
    database = build_database(tmp_path)

    database.upsert_work(
        PublicationRecord(
            handle_url="https://example.test/4",
            title="Delta",
            author="Author D",
            year=2025,
            work_type="Thesis",
        )
    )

    rows = database.get_works_for_interest_rating(handle_urls=["https://example.test/4", "https://example.test/2"])

    assert [row["handle_url"] for row in rows] == ["https://example.test/4"]