import json
import sqlite3
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from tuni_scraper.config import DEFAULT_DB_PATH, DEFAULT_MAX_OFFSET, DEFAULT_PAGE_SIZE
from tuni_scraper.models import PublicationRecord, ScrapeProgress


PROGRESS_KEY = "recent_submissions"

WORK_SORT_FIELDS = {
    "downloads": {
        "sort_expr": "COALESCE(downloads, -1)",
        "rank_expr": "downloads",
        "missing_expr": "downloads IS NULL",
    },
    "downloads_per_day": {
        "sort_expr": "COALESCE(downloads_per_day, -1)",
        "rank_expr": "downloads_per_day",
        "missing_expr": "downloads_per_day IS NULL",
    },
    "interest": {
        "sort_expr": "COALESCE(interestingness_rating, -1)",
        "rank_expr": "interestingness_rating",
        "missing_expr": "interestingness_rating IS NULL",
    },
    "accepted": {
        "sort_expr": "COALESCE(date(accepted_date), '')",
        "rank_expr": "date(accepted_date)",
        "missing_expr": "accepted_date IS NULL OR accepted_date = ''",
    },
}


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def format_year_month_label(year_month: str | None) -> str | None:
    if not year_month:
        return None

    return datetime.strptime(year_month, "%Y-%m").strftime("%b %Y")


class Database:
    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @staticmethod
    def _downloads_per_day_sql(
        downloads_expr: str = "downloads",
        accepted_date_expr: str = "accepted_date",
    ) -> str:
        return f"""
            CASE
                WHEN {accepted_date_expr} IS NOT NULL
                     AND {accepted_date_expr} != ''
                     AND {downloads_expr} IS NOT NULL
                     AND (julianday('now') - julianday({accepted_date_expr})) > 0
                THEN ROUND(
                    CAST({downloads_expr} AS REAL) / (julianday('now') - julianday({accepted_date_expr})),
                    2
                )
                ELSE NULL
            END
        """

    def _signal_scored_cte(self, min_downloads: int = 5) -> str:
        downloads_per_day_sql = self._downloads_per_day_sql()
        return f"""
            WITH ranked AS (
                SELECT
                    title,
                    author,
                    year,
                    work_type,
                    interestingness_rating,
                    downloads,
                    handle_url,
                    accepted_date,
                    {downloads_per_day_sql} AS downloads_per_day,
                    ROW_NUMBER() OVER (
                        ORDER BY downloads DESC, interestingness_rating DESC, title ASC
                    ) AS download_rank,
                    ROW_NUMBER() OVER (
                        ORDER BY interestingness_rating DESC, downloads DESC, title ASC
                    ) AS interest_rank,
                    COUNT(*) OVER () AS candidate_count
                FROM works
                WHERE downloads IS NOT NULL
                  AND downloads >= {min_downloads}
                  AND interestingness_rating IS NOT NULL
            ),
            scored AS (
                SELECT
                    title,
                    author,
                    year,
                    work_type,
                    interestingness_rating,
                    downloads,
                    handle_url,
                    accepted_date,
                    downloads_per_day,
                    ROUND(
                        CASE
                            WHEN candidate_count = 0 THEN 0
                            ELSE ((candidate_count - download_rank + 1.0) / candidate_count) * 100.0
                        END,
                        1
                    ) AS download_percentile,
                    ROUND(
                        CASE
                            WHEN candidate_count = 0 THEN 0
                            ELSE ((candidate_count - interest_rank + 1.0) / candidate_count) * 100.0
                        END,
                        1
                    ) AS interest_percentile,
                    ROUND(
                        CASE
                            WHEN candidate_count = 0 THEN 0
                            ELSE (
                                2.0
                                * ((candidate_count - download_rank + 1.0) / candidate_count)
                                * ((candidate_count - interest_rank + 1.0) / candidate_count)
                            ) / (
                                ((candidate_count - download_rank + 1.0) / candidate_count)
                                + ((candidate_count - interest_rank + 1.0) / candidate_count)
                            ) * 100.0
                        END,
                        1
                    ) AS signal_score
                FROM ranked
            )
        """

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS works (
                    handle_url TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    author TEXT,
                    year INTEGER,
                    work_type TEXT,
                    interestingness_rating INTEGER,
                    downloads INTEGER,
                    accepted_date TEXT,
                    listing_offset INTEGER,
                    listing_url TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    detail_scraped_at TEXT
                );

                CREATE TABLE IF NOT EXISTS progress (
                    name TEXT PRIMARY KEY,
                    next_offset INTEGER NOT NULL,
                    max_offset INTEGER NOT NULL,
                    page_size INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            # Migration: add accepted_date column to existing databases
            existing_cols = {row[1] for row in connection.execute("PRAGMA table_info(works)")}
            if "accepted_date" not in existing_cols:
                connection.execute("ALTER TABLE works ADD COLUMN accepted_date TEXT")
            if "interestingness_rating" not in existing_cols:
                connection.execute("ALTER TABLE works ADD COLUMN interestingness_rating INTEGER")

    def get_progress(
        self,
        max_offset: int = DEFAULT_MAX_OFFSET,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> ScrapeProgress:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT next_offset, max_offset, page_size, updated_at FROM progress WHERE name = ?",
                (PROGRESS_KEY,),
            ).fetchone()

            if row is None:
                created = ScrapeProgress(
                    next_offset=0,
                    max_offset=max_offset,
                    page_size=page_size,
                    updated_at=utc_now_iso(),
                )
                connection.execute(
                    """
                    INSERT INTO progress (name, next_offset, max_offset, page_size, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (PROGRESS_KEY, created.next_offset, created.max_offset, created.page_size, created.updated_at),
                )
                return created

        return ScrapeProgress(
            next_offset=row["next_offset"],
            max_offset=row["max_offset"],
            page_size=row["page_size"],
            updated_at=row["updated_at"],
        )

    def save_progress(self, next_offset: int, max_offset: int, page_size: int) -> None:
        timestamp = utc_now_iso()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO progress (name, next_offset, max_offset, page_size, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    next_offset = excluded.next_offset,
                    max_offset = excluded.max_offset,
                    page_size = excluded.page_size,
                    updated_at = excluded.updated_at
                """,
                (PROGRESS_KEY, next_offset, max_offset, page_size, timestamp),
            )

    def reset_progress(
        self,
        offset: int = 0,
        max_offset: int = DEFAULT_MAX_OFFSET,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> None:
        self.save_progress(offset, max_offset, page_size)

    def has_work(self, handle_url: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM works WHERE handle_url = ?",
                (handle_url,),
            ).fetchone()

        return row is not None

    def upsert_work(self, record: PublicationRecord) -> None:
        timestamp = utc_now_iso()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO works (
                    handle_url,
                    title,
                    author,
                    year,
                    work_type,
                    interestingness_rating,
                    downloads,
                    accepted_date,
                    listing_offset,
                    listing_url,
                    first_seen_at,
                    last_seen_at,
                    detail_scraped_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(handle_url) DO UPDATE SET
                    title = excluded.title,
                    author = excluded.author,
                    year = excluded.year,
                    work_type = excluded.work_type,
                    listing_offset = excluded.listing_offset,
                    listing_url = excluded.listing_url,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    record.handle_url,
                    record.title,
                    record.author,
                    record.year,
                    record.work_type,
                    None,
                    record.downloads,
                    record.accepted_date,
                    record.listing_offset,
                    record.listing_url,
                    timestamp,
                    timestamp,
                    None,
                ),
            )

    def needs_detail_fetch(self, handle_url: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT detail_scraped_at, accepted_date FROM works WHERE handle_url = ?",
                (handle_url,),
            ).fetchone()

        if row is None:
            return True

        return row["detail_scraped_at"] is None or row["accepted_date"] is None

    def update_detail(
        self,
        handle_url: str,
        downloads: int | None,
        accepted_date: str | None,
    ) -> None:
        timestamp = utc_now_iso()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE works
                SET downloads = ?, accepted_date = ?, detail_scraped_at = ?, last_seen_at = ?
                WHERE handle_url = ?
                """,
                (downloads, accepted_date, timestamp, timestamp, handle_url),
            )

    def get_works_for_interest_rating(
        self,
        limit: int | None = None,
        include_rated: bool = False,
        handle_urls: Sequence[str] | None = None,
    ) -> list[sqlite3.Row]:
        if handle_urls is not None and not handle_urls:
            return []

        query = """
            SELECT handle_url, title, interestingness_rating
            FROM works
            WHERE title IS NOT NULL AND TRIM(title) != ''
        """
        params: list[object] = []

        if handle_urls is not None:
            placeholders = ", ".join("?" for _ in handle_urls)
            query += f" AND handle_url IN ({placeholders})"
            params.extend(handle_urls)

        if not include_rated:
            query += " AND interestingness_rating IS NULL"

        query += " ORDER BY year DESC, title ASC"

        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        return self.fetch_rows(query, params)

    def update_interest_ratings(self, ratings: Sequence[tuple[int, str]]) -> None:
        with self._connect() as connection:
            connection.executemany(
                """
                UPDATE works
                SET interestingness_rating = ?
                WHERE handle_url = ?
                """,
                ratings,
            )

    def fetch_rows(self, query: str, params: Sequence[object] = ()) -> list[sqlite3.Row]:
        with self._connect() as connection:
            return connection.execute(query, params).fetchall()

    def fetch_one(self, query: str, params: Sequence[object] = ()) -> sqlite3.Row | None:
        with self._connect() as connection:
            return connection.execute(query, params).fetchone()

    def get_overview(self) -> dict[str, object]:
        row = self.fetch_one(
            """
            SELECT
                COUNT(*) AS total_publications,
                COALESCE(SUM(downloads), 0) AS total_downloads,
                ROUND(COALESCE(AVG(COALESCE(downloads, 0)), 0), 1) AS average_downloads_per_work,
                (
                    SELECT year
                    FROM works
                    WHERE year IS NOT NULL AND year > 0
                    GROUP BY year
                    ORDER BY COUNT(*) DESC, year DESC
                    LIMIT 1
                ) AS busiest_year,
                (
                    SELECT strftime('%Y-%m', accepted_date)
                    FROM works
                    WHERE accepted_date IS NOT NULL AND accepted_date != ''
                    GROUP BY strftime('%Y-%m', accepted_date)
                    ORDER BY COUNT(*) DESC, strftime('%Y-%m', accepted_date) DESC
                    LIMIT 1
                ) AS peak_month
            FROM works
            """
        )
        progress = self.get_progress()
        return {
            "total_publications": row["total_publications"] if row else 0,
            "total_downloads": row["total_downloads"] if row else 0,
            "average_downloads_per_work": row["average_downloads_per_work"] if row else 0,
            "busiest_year": row["busiest_year"] if row else None,
            "peak_month": format_year_month_label(row["peak_month"]) if row else None,
            "next_offset": progress.next_offset,
            "max_offset": progress.max_offset,
            "page_size": progress.page_size,
            "updated_at": progress.updated_at,
        }

    def get_top_downloads(self, limit: int = 10) -> list[sqlite3.Row]:
        downloads_per_day_sql = self._downloads_per_day_sql()
        return self.fetch_rows(
            f"""
            SELECT
                title, author, year, work_type, downloads, handle_url, accepted_date,
                {downloads_per_day_sql} AS downloads_per_day
            FROM works
            WHERE downloads IS NOT NULL
            ORDER BY downloads DESC, title ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_top_by_downloads_per_day(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            SELECT
                title, author, year, work_type, downloads, handle_url, accepted_date,
                ROUND(
                    CAST(downloads AS REAL) / (julianday('now') - julianday(accepted_date)),
                    2
                ) AS downloads_per_day
            FROM works
            WHERE downloads IS NOT NULL
              AND accepted_date IS NOT NULL
              AND (julianday('now') - julianday(accepted_date)) > 0
            ORDER BY downloads_per_day DESC, downloads DESC, title ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_top_by_interestingness(self, limit: int = 10) -> list[sqlite3.Row]:
        downloads_per_day_sql = self._downloads_per_day_sql()
        return self.fetch_rows(
            f"""
            SELECT
                title, author, year, work_type, interestingness_rating, downloads, handle_url, accepted_date,
                {downloads_per_day_sql} AS downloads_per_day
            FROM works
            WHERE interestingness_rating IS NOT NULL
            ORDER BY interestingness_rating DESC, downloads DESC, title ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_least_interesting_works(self, limit: int = 10) -> list[sqlite3.Row]:
        downloads_per_day_sql = self._downloads_per_day_sql()
        return self.fetch_rows(
            f"""
            SELECT
                title,
                author,
                year,
                work_type,
                interestingness_rating,
                downloads,
                handle_url,
                accepted_date,
                {downloads_per_day_sql} AS downloads_per_day
            FROM works
            WHERE interestingness_rating IS NOT NULL
            ORDER BY interestingness_rating ASC, downloads DESC, title ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_least_downloads(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            SELECT
                title, author, year, work_type, downloads, handle_url, accepted_date,
                RANK() OVER (ORDER BY COALESCE(downloads, 0) DESC) AS dl_rank,
                CASE
                    WHEN accepted_date IS NOT NULL
                         AND (julianday('now') - julianday(accepted_date)) > 0
                    THEN ROUND(
                        CAST(downloads AS REAL) / (julianday('now') - julianday(accepted_date)),
                        2
                    )
                    ELSE NULL
                END AS downloads_per_day
            FROM works
            WHERE downloads IS NOT NULL
              AND downloads >= 5
              AND year IS NOT NULL AND year > 0
              AND accepted_date IS NOT NULL AND accepted_date != ''
            ORDER BY downloads ASC, title ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_least_trending(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            SELECT
                title, author, year, work_type, downloads, handle_url, accepted_date,
                ROUND(
                    CAST(downloads AS REAL) / (julianday('now') - julianday(accepted_date)),
                    2
                ) AS downloads_per_day
            FROM works
            WHERE downloads IS NOT NULL
              AND downloads >= 5
              AND year IS NOT NULL AND year > 0
              AND accepted_date IS NOT NULL
              AND (julianday('now') - julianday(accepted_date)) > 0
            ORDER BY downloads_per_day ASC, downloads ASC, title ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_top_signal_rich_works(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.fetch_rows(
            self._signal_scored_cte()
            + """
            SELECT
                title,
                author,
                year,
                work_type,
                interestingness_rating,
                downloads,
                handle_url,
                accepted_date,
                downloads_per_day,
                signal_score
            FROM scored
            ORDER BY signal_score DESC, downloads DESC, interestingness_rating DESC, title ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_least_signal_rich_works(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.fetch_rows(
            self._signal_scored_cte()
            + """
            SELECT
                title,
                author,
                year,
                work_type,
                interestingness_rating,
                downloads,
                handle_url,
                accepted_date,
                downloads_per_day,
                signal_score
            FROM scored
            ORDER BY signal_score ASC, downloads DESC, interestingness_rating ASC, title ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_download_heavy_outliers(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.fetch_rows(
            self._signal_scored_cte()
            + """
            SELECT
                title,
                author,
                year,
                work_type,
                interestingness_rating,
                downloads,
                handle_url,
                accepted_date,
                downloads_per_day,
                signal_score,
                ROUND(download_percentile - interest_percentile, 1) AS gap_score
            FROM scored
            ORDER BY gap_score DESC, downloads DESC, interestingness_rating ASC, title ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_interest_heavy_outliers(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.fetch_rows(
            self._signal_scored_cte()
            + """
            SELECT
                title,
                author,
                year,
                work_type,
                interestingness_rating,
                downloads,
                handle_url,
                accepted_date,
                downloads_per_day,
                signal_score,
                ROUND(interest_percentile - download_percentile, 1) AS gap_score
            FROM scored
            ORDER BY gap_score DESC, interestingness_rating DESC, downloads ASC, title ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_random_interesting_works(
        self, limit: int = 3, min_rating: int = 80
    ) -> list[sqlite3.Row]:
        downloads_per_day_sql = self._downloads_per_day_sql()
        return self.fetch_rows(
            f"""
            SELECT
                title, author, year, work_type, interestingness_rating, downloads, handle_url, accepted_date,
                {downloads_per_day_sql} AS downloads_per_day
            FROM works
            WHERE interestingness_rating IS NOT NULL
              AND interestingness_rating >= ?
            ORDER BY RANDOM()
            LIMIT ?
            """,
            (min_rating, limit),
        )

    def get_random_signal_rich_works(
        self, limit: int = 3, min_score: float = 90.0
    ) -> list[sqlite3.Row]:
        return self.fetch_rows(
            self._signal_scored_cte()
            + """
            SELECT
                title,
                author,
                year,
                work_type,
                interestingness_rating,
                downloads,
                handle_url,
                accepted_date,
                downloads_per_day,
                signal_score
            FROM scored
            WHERE signal_score >= ?
            ORDER BY RANDOM()
            LIMIT ?
            """,
            (min_score, limit),
        )

    def get_interest_bucket_summary(self) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            WITH RECURSIVE buckets AS (
                SELECT 0 AS bucket_index, '0-10' AS bucket_label
                UNION ALL
                SELECT
                    bucket_index + 1,
                    printf(
                        '%d-%d',
                        (bucket_index + 1) * 10,
                        CASE
                            WHEN bucket_index + 1 = 9 THEN 100
                            ELSE (bucket_index + 2) * 10
                        END
                    )
                FROM buckets
                WHERE bucket_index < 9
            ),
            bucketed AS (
                SELECT
                    CASE
                        WHEN interestingness_rating >= 100 THEN 9
                        ELSE CAST(interestingness_rating / 10 AS INTEGER)
                    END AS bucket_index,
                    COUNT(*) AS publication_count,
                    COALESCE(SUM(downloads), 0) AS total_downloads
                FROM works
                WHERE interestingness_rating IS NOT NULL
                  AND interestingness_rating BETWEEN 0 AND 100
                GROUP BY 1
            )
            SELECT
                buckets.bucket_label,
                COALESCE(bucketed.publication_count, 0) AS publication_count,
                COALESCE(bucketed.total_downloads, 0) AS total_downloads
            FROM buckets
            LEFT JOIN bucketed ON bucketed.bucket_index = buckets.bucket_index
            ORDER BY buckets.bucket_index ASC
            """
        )

    def get_work_type_signal_summary(self) -> list[sqlite3.Row]:
        return self.fetch_rows(
            self._signal_scored_cte()
            + """
            SELECT
                COALESCE(work_type, 'Unknown') AS work_type,
                ROUND(AVG(signal_score), 1) AS average_signal_score,
                ROUND(AVG(interestingness_rating), 1) AS average_interest,
                ROUND(AVG(downloads), 1) AS average_downloads,
                COUNT(*) AS publication_count
            FROM scored
            GROUP BY COALESCE(work_type, 'Unknown')
            ORDER BY average_signal_score DESC, publication_count DESC, work_type ASC
            """
        )

    def get_top_authors(self, limit: int = 10) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            SELECT author, COUNT(*) AS publication_count
            FROM works
            WHERE author IS NOT NULL AND TRIM(author) != ''
            GROUP BY author
            ORDER BY publication_count DESC, author ASC
            LIMIT ?
            """,
            (limit,),
        )

    def get_publication_counts_by_year(self) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            SELECT year, COUNT(*) AS publication_count
            FROM works
            WHERE year IS NOT NULL AND year > 0
            GROUP BY year
            ORDER BY year ASC
            """
        )

    def get_download_counts_by_year(self) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            SELECT year, COALESCE(SUM(downloads), 0) AS total_downloads
            FROM works
            WHERE year IS NOT NULL AND year > 0
            GROUP BY year
            ORDER BY year ASC
            """
        )

    def get_download_counts_by_type(self) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            SELECT COALESCE(work_type, 'Unknown') AS work_type, COALESCE(SUM(downloads), 0) AS total_downloads
            FROM works
            GROUP BY COALESCE(work_type, 'Unknown')
            ORDER BY total_downloads DESC, work_type ASC
            """
        )

    def get_type_counts_overall(self) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            SELECT COALESCE(work_type, 'Unknown') AS work_type, COUNT(*) AS publication_count
            FROM works
            GROUP BY COALESCE(work_type, 'Unknown')
            ORDER BY publication_count DESC, work_type ASC
            """
        )

    def get_type_counts_by_year(self) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            SELECT year, COALESCE(work_type, 'Unknown') AS work_type, COUNT(*) AS publication_count
            FROM works
            WHERE year IS NOT NULL AND year > 0
            GROUP BY year, COALESCE(work_type, 'Unknown')
            ORDER BY year ASC, publication_count DESC, work_type ASC
            """
        )

    def get_available_years(self) -> list[int]:
        rows = self.fetch_rows(
            """
            SELECT DISTINCT year
            FROM works
            WHERE year IS NOT NULL AND year > 0
            ORDER BY year DESC
            """
        )
        return [row["year"] for row in rows]

    def get_available_types(self) -> list[str]:
        rows = self.fetch_rows(
            """
            SELECT DISTINCT COALESCE(work_type, 'Unknown') AS work_type
            FROM works
            ORDER BY work_type ASC
            """
        )
        return [row["work_type"] for row in rows]

    def search_works(
        self,
        title: str | None = None,
        author: str | None = None,
        year: int | None = None,
        work_type: str | None = None,
        min_downloads: int | None = None,
        max_downloads: int | None = None,
        min_interest: int | None = None,
        max_interest: int | None = None,
        accepted_start: str | None = None,
        accepted_end: str | None = None,
        sort_by: str = "downloads",
        sort_direction: str = "desc",
        limit: int = 10,
    ) -> list[sqlite3.Row]:
        resolved_sort = WORK_SORT_FIELDS.get(sort_by, WORK_SORT_FIELDS["downloads"])
        resolved_sort_direction = "ASC" if sort_direction.lower() == "asc" else "DESC"

        query = """
            WITH scored AS (
                SELECT
                    w.handle_url,
                    w.title,
                    w.author,
                    w.year,
                    w.work_type,
                    w.interestingness_rating,
                    w.downloads,
                    w.accepted_date,
                    CASE
                        WHEN w.accepted_date IS NOT NULL
                             AND w.downloads IS NOT NULL
                             AND (julianday('now') - julianday(w.accepted_date)) > 0
                        THEN ROUND(
                            CAST(w.downloads AS REAL) / (julianday('now') - julianday(w.accepted_date)),
                            2
                        )
                        ELSE NULL
                    END AS downloads_per_day
                FROM works w
            ), ranked AS (
                SELECT
                    scored.*,
                    CASE
                        WHEN {missing_expr} THEN NULL
                        ELSE RANK() OVER (
                            ORDER BY
                                CASE WHEN {missing_expr} THEN 1 ELSE 0 END,
                                {rank_expr} {sort_direction}
                        )
                    END AS sort_rank
                FROM scored
            )
            SELECT
                handle_url,
                title,
                author,
                year,
                work_type,
                interestingness_rating,
                downloads,
                accepted_date,
                sort_rank,
                downloads_per_day
            FROM ranked
            WHERE 1 = 1
        """
        query = query.format(
            missing_expr=resolved_sort["missing_expr"],
            rank_expr=resolved_sort["rank_expr"],
            sort_direction=resolved_sort_direction,
        )
        params: list[object] = []

        if title:
            query += " AND title LIKE ? COLLATE NOCASE"
            params.append(f"%{title}%")

        if author:
            query += " AND author LIKE ? COLLATE NOCASE"
            params.append(f"%{author}%")

        if year is not None:
            query += " AND year = ?"
            params.append(year)

        if work_type:
            if work_type == "Unknown":
                query += " AND COALESCE(work_type, 'Unknown') = 'Unknown'"
            else:
                query += " AND work_type = ?"
                params.append(work_type)

        if min_downloads is not None:
            query += " AND COALESCE(downloads, 0) >= ?"
            params.append(min_downloads)

        if max_downloads is not None:
            query += " AND COALESCE(downloads, 0) <= ?"
            params.append(max_downloads)

        if min_interest is not None:
            query += " AND interestingness_rating IS NOT NULL AND interestingness_rating >= ?"
            params.append(min_interest)

        if max_interest is not None:
            query += " AND interestingness_rating IS NOT NULL AND interestingness_rating <= ?"
            params.append(max_interest)

        if accepted_start:
            query += " AND accepted_date IS NOT NULL AND date(accepted_date) >= date(?)"
            params.append(accepted_start)

        if accepted_end:
            query += " AND accepted_date IS NOT NULL AND date(accepted_date) <= date(?)"
            params.append(accepted_end)

        query += """
            ORDER BY
                CASE WHEN sort_rank IS NULL THEN 1 ELSE 0 END,
                sort_rank ASC,
                year DESC,
                title ASC
            LIMIT ?
        """
        params.append(limit)

        return self.fetch_rows(query, params)

    def get_all_works(self) -> list[dict[str, object]]:
        rows = self.fetch_rows(
            """
            SELECT handle_url, title, author, year, work_type, interestingness_rating, downloads, accepted_date,
                   listing_offset, listing_url, first_seen_at, last_seen_at, detail_scraped_at
            FROM works
            ORDER BY year DESC, title ASC
            """
        )
        return [dict(row) for row in rows]

    def get_publication_counts_by_accepted_month(self) -> list[sqlite3.Row]:
        return self.fetch_rows(
            """
            SELECT
                CAST(strftime('%Y', accepted_date) AS INTEGER) AS year,
                CAST(strftime('%m', accepted_date) AS INTEGER) AS month,
                COUNT(*) AS publication_count
            FROM works
            WHERE accepted_date IS NOT NULL
              AND accepted_date != ''
              AND CAST(strftime('%Y', accepted_date) AS INTEGER) > 0
            GROUP BY year, month
            ORDER BY year ASC, month ASC
            """
        )

    def export_json(self, output_path: Path) -> None:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "overview": self.get_overview(),
            "works": self.get_all_works(),
        }
        output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")