# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.14.6
ARG UV_VERSION=0.12.3

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim-trixie AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=0 \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /build

COPY --from=uv /uv /bin/uv
COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable --no-install-project --extra server

COPY README.md LICENSE ./
COPY src ./src

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable --extra server

FROM python:${PYTHON_VERSION}-slim-trixie AS runtime

ARG APP_UID=1000
ARG APP_GID=1000

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TUNI_SCRAPER_DB_PATH=/app/data/trepo_scraper.db \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

RUN groupadd --gid "$APP_GID" appuser \
    && useradd --no-log-init --uid "$APP_UID" --gid "$APP_GID" --no-create-home --shell /usr/sbin/nologin appuser \
    && install --directory --owner=appuser --group=appuser /app /app/data

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

USER appuser

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "from urllib.request import urlopen; urlopen('http://127.0.0.1:5000/api/random-gems').read()"

STOPSIGNAL SIGTERM

CMD ["gunicorn", "--bind=0.0.0.0:5000", "--workers=2", "--threads=2", "--timeout=60", "--access-logfile=-", "--error-logfile=-", "tuni_scraper.wsgi:app"]
