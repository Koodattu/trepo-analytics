from tuni_scraper.parsing import parse_download_count, parse_download_stats_json, parse_download_stats_payload, parse_recent_submissions
from tuni_scraper.scraper import extract_handle


RECENT_SUBMISSIONS_HTML = """
<div class="artifact-description">
  <h4 class="artifact-title">
    <a href="/handle/10024/234402">Suomen vesihuollon fyysisen turvallisuuden nykytila ja kehittäminen</a>
  </h4>
  <div class="artifact-info">
    <span class="author h4"><small><span>Vasama, Venla</span></small></span>
    <span class="publisher-date h4"><small>(<span class="date">2026</span>)</small></span>
    <br> Diplomityö
  </div>
  <div class="artifact-abstract">Example</div>
</div>
"""


def test_parse_recent_submissions_extracts_expected_fields() -> None:
    records = parse_recent_submissions(RECENT_SUBMISSIONS_HTML)

    assert len(records) == 1
    assert records[0].handle_url == "https://trepo.tuni.fi/handle/10024/234402"
    assert records[0].title == "Suomen vesihuollon fyysisen turvallisuuden nykytila ja kehittäminen"
    assert records[0].author == "Vasama, Venla"
    assert records[0].year == 2026
    assert records[0].work_type == "Diplomityö"


def test_parse_download_count_handles_missing_value() -> None:
    assert parse_download_count("<html><body><span id='file_downloads'>123</span></body></html>") == 123
    assert parse_download_count("<html><body>No downloads</body></html>") is None


def test_parse_download_stats_payload_extracts_sum() -> None:
  assert parse_download_stats_payload({"sum": 5, "202603": 5}) == 5
  assert parse_download_stats_payload({"sum": "123"}) == 123


def test_parse_download_stats_payload_handles_missing_sum() -> None:
  assert parse_download_stats_payload({"202603": 5}) is None
  assert parse_download_stats_payload([]) is None


def test_parse_download_stats_json_handles_invalid_json() -> None:
  assert parse_download_stats_json('{"sum": 7, "202603": 7}') == 7
  assert parse_download_stats_json("not json") is None


def test_extract_handle_returns_handle_identifier() -> None:
  assert extract_handle("https://trepo.tuni.fi/handle/10024/234747") == "10024/234747"
  assert extract_handle("https://trepo.tuni.fi/other/10024/234747") is None