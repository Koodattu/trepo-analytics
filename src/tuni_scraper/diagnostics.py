import json
import logging
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup


LOGGER = logging.getLogger(__name__)


def redact(text: str) -> str:
    def redact_url(match: re.Match) -> str:
        try:
            url = urlsplit(match.group(0))
        except ValueError:
            return "[invalid URL omitted]"
        query = urlencode([
            (key, value if key in {"offset", "handle"} else "[redacted]")
            for key, value in parse_qsl(url.query, keep_blank_values=True)
        ])
        return urlunsplit((url.scheme, url.netloc.rsplit("@", 1)[-1], url.path, query, ""))

    text = re.sub(r"https?://[^\s<>\"']+", redact_url, text)
    text = re.sub(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+", r"\1 [redacted]", text)
    text = re.sub(r"\bsk-[A-Za-z0-9_-]+", "[redacted]", text)
    return re.sub(
        r"(?i)(\b(?:[\w-]*(?:token|secret|password|api[_-]?key)|authorization|cookie|sessionid)"
        r"[\"']?\s*[:=]\s*)(?:\"[^\"]*\"|'[^']*'|[^\s,;<>]+)",
        r"\1[redacted]",
        text,
    )


class DiagnosticFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))


def log_response_error(response: requests.Response, reason: str) -> None:
    content_type = response.headers.get("Content-Type", "").lower()
    excerpt = "[non-text response omitted]"
    if not content_type or any(kind in content_type for kind in ("text/", "json", "xml")):
        # Keep visible error text without scripts, form values, or full HTML dumps.
        soup = BeautifulSoup(response.text[:16384], "html.parser")
        for tag in soup(["script", "style", "input", "textarea"]):
            tag.decompose()
        excerpt = redact(" ".join(soup.stripped_strings))[:2000]

    retries = getattr(response.raw, "retries", None)
    details = {
        "reason": reason,
        "method": response.request.method if response.request is not None else "GET",
        "url": redact(response.url or ""),
        "status": response.status_code,
        "retry_count": len(retries.history) if retries is not None else 0,
        "headers": {
            key: redact(response.headers[key])[:256]
            for key in ("Server", "Content-Type", "Retry-After", "CF-Ray", "CF-Mitigated", "Via", "X-Cache")
            if key in response.headers
        },
        "body_excerpt": excerpt,
    }
    LOGGER.warning("TREPO response diagnostic: %s", json.dumps(details, ensure_ascii=True))


def log_http_error(response: requests.Response, **kwargs) -> None:
    if response.status_code >= 400:
        log_response_error(response, "HTTP request failed")
