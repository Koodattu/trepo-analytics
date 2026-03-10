import json
import logging
import os
from dataclasses import dataclass

from openai import OpenAI
from pydantic import BaseModel, Field

from tuni_scraper.config import DEFAULT_LLM_BATCH_SIZE, DEFAULT_LLM_MAX_RETRIES, DEFAULT_OPENAI_MODEL, OPENAI_API_KEY_ENV, OPENAI_MODEL_ENV
from tuni_scraper.database import Database


LOGGER = logging.getLogger(__name__)

SYSTEM_PROMPT = """You rate publication titles by how unusually interesting, mold-breaking, and curiosity-inducing they sound.

Judge only the title text. Do not infer the actual quality of the work, its results, or its academic importance.

Scoring rubric:
- 0-20: very routine, formulaic, generic, expected academic phrasing
- 21-40: somewhat specific but still conventional and unsurprising
- 41-60: moderately distinctive or interesting, but not a strong outlier
- 61-80: clearly unusual, striking, or notably fresh in framing
- 81-100: exceptional outlier, weird in a good way, boldly distinctive, or highly curiosity-provoking

What to reward:
- unexpected combinations of concepts
- vivid specificity
- unusual framing, tension, paradox, or conceptual contrast
- titles that feel unlike the typical thesis or paper title

What to penalize:
- standard templates like analyses, surveys, evaluations, case studies, or comparisons phrased conventionally
- bland corporate, administrative, or routine technical wording
- titles that sound interchangeable with many other academic works

Important constraints:
- Use the full scale, but keep most ordinary titles below 50.
- Return exactly one rating for every provided batch_id.
- Never omit an id, duplicate an id, invent an id, or include the title text in the output.
- Output ratings only, with no explanations.
"""


class PublicationInterestRating(BaseModel):
    batch_id: str
    rating: int = Field(ge=0, le=100)


class PublicationInterestRatings(BaseModel):
    ratings: list[PublicationInterestRating]


@dataclass(slots=True)
class InterestRatingResult:
    model: str
    works_considered: int
    works_rated: int
    batches_sent: int


def _chunk_rows(rows: list[dict[str, str]], batch_size: int) -> list[list[dict[str, str]]]:
    return [rows[index : index + batch_size] for index in range(0, len(rows), batch_size)]


def _build_user_prompt(batch: list[dict[str, str]]) -> str:
    payload = {
        "task": "Rate each publication title for unusual interestingness using only the title.",
        "items": [{"batch_id": item["batch_id"], "title": item["title"]} for item in batch],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _validate_ratings(
    expected_ids: set[str],
    response: PublicationInterestRatings,
) -> dict[str, int] | None:
    returned_ids = [item.batch_id for item in response.ratings]
    if len(returned_ids) != len(expected_ids):
        return None

    if len(set(returned_ids)) != len(returned_ids):
        return None

    if set(returned_ids) != expected_ids:
        return None

    return {item.batch_id: item.rating for item in response.ratings}


def _request_batch_ratings(
    client: OpenAI,
    model: str,
    batch: list[dict[str, str]],
    max_retries: int,
) -> dict[str, int]:
    expected_ids = {item["batch_id"] for item in batch}
    prompt = _build_user_prompt(batch)

    for attempt in range(1, max_retries + 1):
        completion = client.chat.completions.parse(
            model=model,
            temperature=0.2,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            response_format=PublicationInterestRatings,
        )
        message = completion.choices[0].message
        if message.refusal:
            raise RuntimeError(f"OpenAI refused the rating request: {message.refusal}")

        if message.parsed is None:
            LOGGER.warning("OpenAI returned no parsed payload on attempt %s/%s.", attempt, max_retries)
            continue

        ratings = _validate_ratings(expected_ids, message.parsed)
        if ratings is not None:
            return ratings

        LOGGER.warning(
            "OpenAI returned an invalid rating payload on attempt %s/%s for batch of %s titles.",
            attempt,
            max_retries,
            len(batch),
        )

    raise RuntimeError("OpenAI did not return a valid structured rating payload after all retry attempts.")


def run_interest_rating(
    database: Database,
    model: str | None = None,
    batch_size: int = DEFAULT_LLM_BATCH_SIZE,
    limit: int | None = None,
    include_rated: bool = False,
    max_retries: int = DEFAULT_LLM_MAX_RETRIES,
) -> InterestRatingResult:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    if max_retries <= 0:
        raise ValueError("max_retries must be greater than zero")

    api_key = os.getenv(OPENAI_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(f"{OPENAI_API_KEY_ENV} is not set. Add it to the environment or the project .env file.")

    resolved_model = model or os.getenv(OPENAI_MODEL_ENV) or DEFAULT_OPENAI_MODEL
    rows = database.get_works_for_interest_rating(limit=limit, include_rated=include_rated)
    if not rows:
        return InterestRatingResult(
            model=resolved_model,
            works_considered=0,
            works_rated=0,
            batches_sent=0,
        )

    client = OpenAI(api_key=api_key)
    work_items = []
    for row in rows:
        title = row["title"].strip()
        if not title:
            continue
        work_items.append({"handle_url": row["handle_url"], "title": title})

    if not work_items:
        return InterestRatingResult(
            model=resolved_model,
            works_considered=0,
            works_rated=0,
            batches_sent=0,
        )

    batches_sent = 0
    works_rated = 0
    batches = []
    for raw_batch in _chunk_rows(work_items, batch_size):
        batches.append(
            [
                {
                    "batch_id": str(batch_index),
                    "handle_url": item["handle_url"],
                    "title": item["title"],
                }
                for batch_index, item in enumerate(raw_batch, start=1)
            ]
        )

    total_batches = len(batches)
    for batch_number, batch in enumerate(batches, start=1):
        LOGGER.info(
            "Requesting interestingness ratings for batch %s/%s (%s titles) with model %s.",
            batch_number,
            total_batches,
            len(batch),
            resolved_model,
        )
        ratings = _request_batch_ratings(client=client, model=resolved_model, batch=batch, max_retries=max_retries)
        database.update_interest_ratings(
            [(ratings[item["batch_id"]], item["handle_url"]) for item in batch]
        )
        batches_sent += 1
        works_rated += len(batch)

    return InterestRatingResult(
        model=resolved_model,
        works_considered=len(work_items),
        works_rated=works_rated,
        batches_sent=batches_sent,
    )