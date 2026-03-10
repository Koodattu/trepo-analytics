import json
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from tuni_scraper.config import BASE_URL
from tuni_scraper.models import PublicationRecord


def normalize_whitespace(value: str | None) -> str | None:
    if value is None:
        return None
    collapsed = " ".join(value.split())
    return collapsed or None


def parse_recent_submissions(html: str, base_url: str = BASE_URL) -> list[PublicationRecord]:
    soup = BeautifulSoup(html, "html.parser")
    records: list[PublicationRecord] = []

    for block in soup.select("div.artifact-description"):
        title_link = block.select_one("h4.artifact-title a[href]")
        if title_link is None:
            continue

        artifact_info = block.select_one("div.artifact-info")
        author = None
        year = None
        work_type = None

        if artifact_info is not None:
            author_tag = artifact_info.select_one(".author span")
            year_tag = artifact_info.select_one(".date")

            author = normalize_whitespace(author_tag.get_text(" ", strip=True)) if author_tag else None

            if year_tag is not None:
                year_text = normalize_whitespace(year_tag.get_text(" ", strip=True))
                if year_text and year_text.isdigit():
                    year = int(year_text)

            info_parts = [normalize_whitespace(part) for part in artifact_info.stripped_strings]
            info_parts = [part for part in info_parts if part]
            if info_parts:
                last_part = info_parts[-1]
                if not re.fullmatch(r"\(?\d{4}\)?", last_part):
                    work_type = last_part

        title = normalize_whitespace(title_link.get_text(" ", strip=True)) or ""
        handle_url = urljoin(base_url, title_link.get("href", ""))

        records.append(
            PublicationRecord(
                handle_url=handle_url,
                title=title,
                author=author,
                year=year,
                work_type=work_type,
            )
        )

    return records


def parse_download_count(html: str) -> int | None:
    soup = BeautifulSoup(html, "html.parser")
    downloads_tag = soup.find(id="file_downloads")
    if downloads_tag is None:
        return None

    raw_value = normalize_whitespace(downloads_tag.get_text(" ", strip=True))
    if raw_value is None:
        return None

    digits = re.sub(r"\D", "", raw_value)
    if not digits:
        return None

    return int(digits)


def parse_accepted_date(html: str) -> str | None:
    """Extract the accepted/approval date from a TREPO handle page.

    Looks for the pattern: <h5>Hyväksymispäivämäärä</h5>2026-03-03
    Returns the date as a yyyy-mm-dd string, or None if not found.
    """
    soup = BeautifulSoup(html, "html.parser")
    for heading in soup.find_all("h5"):
        if "hyväksymispäivämäärä" in heading.get_text().lower():
            # The date is a direct text sibling immediately after the <h5>
            sibling = heading.next_sibling
            if sibling is not None:
                date_text = normalize_whitespace(str(sibling).strip())
                if date_text and re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
                    return date_text
    return None


def parse_download_stats_payload(payload: object) -> int | None:
    if not isinstance(payload, dict):
        return None

    sum_value = payload.get("sum")
    if isinstance(sum_value, int):
        return sum_value

    if isinstance(sum_value, str) and sum_value.isdigit():
        return int(sum_value)

    return None


def parse_download_stats_json(raw_json: str) -> int | None:
    try:
        payload = json.loads(raw_json)
    except json.JSONDecodeError:
        return None

    return parse_download_stats_payload(payload)