import os
from pathlib import Path
from urllib.parse import urlencode

BASE_URL = "https://trepo.tuni.fi"
RECENT_SUBMISSIONS_PATH = "/handle/10024/105882/recent-submissions"
DEFAULT_PAGE_SIZE = 20
DEFAULT_MAX_OFFSET = 41870
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_LLM_BATCH_SIZE = 100
DEFAULT_LLM_MAX_RETRIES = 3
DEFAULT_OPENAI_MODEL = "gpt-5.6-luna"
DEFAULT_OPENAI_REASONING_EFFORT = "low"
OPENAI_API_KEY_ENV = "OPENAI_API_KEY"
OPENAI_MODEL_ENV = "OPENAI_MODEL"
OPENAI_REASONING_EFFORT_ENV = "OPENAI_REASONING_EFFORT"
DATA_DIR_ENV = "TUNI_SCRAPER_DATA_DIR"
DB_PATH_ENV = "TUNI_SCRAPER_DB_PATH"
EXPORT_PATH_ENV = "TUNI_SCRAPER_EXPORT_PATH"


def resolve_project_root() -> Path:
    configured_root = os.getenv("TUNI_SCRAPER_PROJECT_ROOT")
    if configured_root:
        return Path(configured_root).expanduser().resolve()

    candidate_root = Path(__file__).resolve().parents[2]
    if (candidate_root / "pyproject.toml").exists():
        return candidate_root

    return Path.cwd()


def resolve_data_dir() -> Path:
    return Path(os.getenv(DATA_DIR_ENV, resolve_project_root() / "data")).expanduser()


def resolve_db_path() -> Path:
    return Path(os.getenv(DB_PATH_ENV, resolve_data_dir() / "trepo_scraper.db")).expanduser()


def resolve_export_path() -> Path:
    return Path(os.getenv(EXPORT_PATH_ENV, resolve_data_dir() / "publications.json")).expanduser()


PROJECT_ROOT = resolve_project_root()
DATA_DIR = resolve_data_dir()
DEFAULT_DB_PATH = resolve_db_path()
DEFAULT_EXPORT_PATH = resolve_export_path()


def build_recent_submissions_url(offset: int) -> str:
    query = urlencode({"offset": offset})
    return f"{BASE_URL}{RECENT_SUBMISSIONS_PATH}?{query}"
