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
DEFAULT_OPENAI_MODEL = "gpt-4.1-mini"
OPENAI_API_KEY_ENV = "OPENAI_API_KEY"
OPENAI_MODEL_ENV = "OPENAI_MODEL"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_DB_PATH = DATA_DIR / "trepo_scraper.db"
DEFAULT_EXPORT_PATH = DATA_DIR / "publications.json"


def build_recent_submissions_url(offset: int) -> str:
    query = urlencode({"offset": offset})
    return f"{BASE_URL}{RECENT_SUBMISSIONS_PATH}?{query}"