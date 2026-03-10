from dataclasses import dataclass


@dataclass(slots=True)
class PublicationRecord:
    handle_url: str
    title: str
    author: str | None
    year: int | None
    work_type: str | None
    listing_offset: int | None = None
    listing_url: str | None = None
    downloads: int | None = None
    accepted_date: str | None = None


@dataclass(slots=True)
class ScrapeProgress:
    next_offset: int
    max_offset: int
    page_size: int
    updated_at: str


@dataclass(slots=True)
class ScrapeResult:
    pages_processed: int
    works_seen: int
    detail_requests: int
    detail_updates: int
    ending_offset: int