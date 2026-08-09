# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.14.6

FROM python:${PYTHON_VERSION}-slim-trixie AS builder

ARG PIP_VERSION=26.2.1

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

RUN python -m venv "$VIRTUAL_ENV"

WORKDIR /build

COPY pyproject.toml README.md ./
COPY src ./src

RUN --mount=type=cache,target=/root/.cache/pip \
    python -m pip install --upgrade "pip==$PIP_VERSION" \
    && python -m pip install ".[server]" \
    && python -m pip uninstall --yes pip

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
