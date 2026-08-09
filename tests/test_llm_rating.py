from pathlib import Path
from types import SimpleNamespace

from tuni_scraper.database import Database
from tuni_scraper.llm_rating import (
    SYSTEM_PROMPT,
    PublicationInterestRating,
    PublicationInterestRatings,
    _request_batch_ratings,
    run_interest_rating,
)


def test_rating_request_uses_luna_low_reasoning_and_unchanged_prompt() -> None:
    captured: dict[str, object] = {}
    parsed = PublicationInterestRatings(
        ratings=[PublicationInterestRating(batch_id="1", rating=72)]
    )

    class FakeCompletions:
        @staticmethod
        def parse(**kwargs):
            captured.update(kwargs)
            message = SimpleNamespace(refusal=None, parsed=parsed)
            return SimpleNamespace(choices=[SimpleNamespace(message=message)])

    client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))
    ratings = _request_batch_ratings(
        client=client,
        model="gpt-5.6-luna",
        reasoning_effort="low",
        batch=[{"batch_id": "1", "handle_url": "https://example.test/1", "title": "A title"}],
        max_retries=1,
    )

    assert ratings == {"1": 72}
    assert captured["model"] == "gpt-5.6-luna"
    assert captured["reasoning_effort"] == "low"
    assert captured["response_format"] is PublicationInterestRatings
    assert captured["messages"][0] == {"role": "system", "content": SYSTEM_PROMPT}
    assert "temperature" not in captured


def test_rating_noop_does_not_require_api_key(tmp_path: Path, monkeypatch) -> None:
    database = Database(tmp_path / "test.db")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_REASONING_EFFORT", raising=False)

    result = run_interest_rating(database)

    assert result.model == "gpt-5.6-luna"
    assert result.reasoning_effort == "low"
    assert result.works_rated == 0
    assert result.batches_sent == 0
