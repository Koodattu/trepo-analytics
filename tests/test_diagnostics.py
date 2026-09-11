import json
import logging
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest
import requests

from tuni_scraper.cli import main
from tuni_scraper.diagnostics import DiagnosticFormatter, log_response_error, redact
from tuni_scraper.scraper import build_session, fetch_html, fetch_download_count


@pytest.fixture
def http_server():
    statuses = []
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            self.send_response(statuses.pop(0))
            self.send_header("Content-Type", "text/html")
            self.send_header("CF-Mitigated", "challenge")
            self.send_header("Set-Cookie", "private-cookie-value")
            self.end_headers()
            self.wfile.write(b'<title>Just a moment</title><script>private-script-value</script>')

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", statuses, calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_exhausted_503_preserves_response_and_retry_history(http_server, caplog):
    url, statuses, calls = http_server
    statuses.extend([503] * 6)
    with build_session() as session:
        session.get_adapter(url).max_retries.backoff_factor = 0
        with pytest.raises(requests.HTTPError) as error:
            fetch_html(session, url)
    assert error.value.response.status_code == 503
    assert len(calls) == 6
    records = [r for r in caplog.records if r.name == "tuni_scraper.diagnostics"]
    assert len(records) == 1
    details = json.loads(records[0].getMessage().split(": ", 1)[1])
    assert details["retry_count"] == 5
    assert details["headers"]["CF-Mitigated"] == "challenge"
    assert details["body_excerpt"] == "Just a moment"
    assert "private-cookie-value" not in caplog.text
    assert "private-script-value" not in caplog.text


def test_recovered_request_does_not_log_terminal_failure(http_server, caplog):
    url, statuses, calls = http_server
    statuses.extend([503, 200])
    with build_session() as session:
        assert "Just a moment" in fetch_html(session, url)
    assert len(calls) == 2
    assert "TREPO response diagnostic" not in caplog.text


def test_invalid_json_response_is_logged(http_server, monkeypatch, caplog):
    url, statuses, calls = http_server
    statuses.append(200)
    monkeypatch.setattr("tuni_scraper.scraper.BASE_URL", url)
    with build_session() as session, pytest.raises(ValueError):
        fetch_download_count(session, "https://trepo.tuni.fi/handle/10024/234747")
    assert calls == ["/simplestats/rest?handle=10024%2F234747"]
    assert "Invalid download statistics payload" in caplog.text
    assert "Just a moment" in caplog.text


def test_response_excerpt_is_bounded_and_redacted(caplog):
    response = requests.Response()
    response.status_code = 503
    response.url = "https://user:private-password@trepo.tuni.fi/?offset=20&token=private-query"
    response.headers["Content-Type"] = "text/html"
    response._content = (
        b'<input value="private-form"><textarea>private-textarea</textarea>'
        b'<p>api_key="private-key" Bearer private-bearer</p>' + b"maintenance " * 1000
    )
    log_response_error(response, "HTTP request failed")
    details = json.loads(caplog.records[-1].getMessage().split(": ", 1)[1])
    assert len(details["body_excerpt"]) <= 2000
    assert "offset=20" in details["url"]
    assert "maintenance" in details["body_excerpt"]
    assert "private-" not in caplog.text


@pytest.mark.parametrize("failure", [requests.ConnectionError, RuntimeError])
def test_cli_persists_failure_traceback_and_closes_log(tmp_path, monkeypatch, failure):
    monkeypatch.setattr("tuni_scraper.cli.configure_environment", lambda: None)
    monkeypatch.setattr("sys.argv", ["trepo-scraper", "--db-path", str(tmp_path / "test.db"), "update-new"])

    def fail(**kwargs):
        raise failure("offline; api_key=private-key")

    monkeypatch.setattr("tuni_scraper.cli.run_weekly_update", fail)
    for _ in range(2):
        with pytest.raises(SystemExit) as error:
            main()
        assert error.value.code == 1
    log_path = tmp_path / "logs" / "trepo-errors.log"
    text = log_path.read_text(encoding="utf-8")
    assert text.count("Command failed.") == 2
    assert "command=update-new" in text
    assert "Traceback" in text
    assert failure.__name__ in text
    assert "private-key" not in text
    log_path.rename(log_path.with_suffix(".closed"))


def test_formatter_redacts_exception_details():
    try:
        raise ValueError("Authorization: Bearer private-bearer; OPENAI_API_KEY=private-key")
    except ValueError:
        record = logging.LogRecord("test", logging.ERROR, __file__, 1, "Failed", (), sys.exc_info())
    formatted = DiagnosticFormatter().format(record)
    assert "ValueError" in formatted
    assert "private-" not in formatted
    assert "sk-private-key" not in redact("sk-private-key")
    assert "invalid URL omitted" in redact("Invalid URL: https://[broken")
