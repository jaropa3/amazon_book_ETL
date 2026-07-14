import os
import re
import random
import time
from datetime import datetime, timezone
import pandas as pd
import requests
from bs4 import BeautifulSoup
from config import CONFIG
from logger import setup_logger

logger = setup_logger("Amazon_books_ETL")
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(PROJECT_DIR, CONFIG.storage.raw_data_dir)
SCRAPER_CFG = CONFIG.scraper

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

def build_headers() -> dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

def _is_challenge_page(response: requests.Response) -> bool:
    return "bm-verify" in response.text or len(response.content) < 5000

# Statusy, które ponawiamy: 503 (chwilowo niedostępny), 429 (rate limit — zwolnij).
RETRIABLE_STATUS = {429, 503}

def _retry_wait(response: requests.Response | None, attempt: int, backoff_base: float) -> float:
    """Ile czekać przed kolejną próbą. Przy 429 szanujemy Retry-After (serwer mówi 'zwolnij'),
    poza tym exponential backoff + jitter. Obsługujemy formę Retry-After w sekundach."""
    if response is not None and response.status_code == 429:
        retry_after = response.headers.get("Retry-After", "")
        if retry_after.isdigit():
            logger.debug("Retry-After: %s s", retry_after)
            return float(retry_after)
    return backoff_base ** attempt + random.uniform(0, 1)

def _get_with_retry(url: str, max_retries: int = SCRAPER_CFG.max_retries, backoff_base: float = SCRAPER_CFG.backoff_base) -> requests.Response | None:
    response = None
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, timeout=(5, 30), headers=build_headers())  # (connect=5s, read=30s)
            if response.status_code not in RETRIABLE_STATUS and not _is_challenge_page(response):
                return response
            reason = response.status_code if response.status_code in RETRIABLE_STATUS else "challenge"
        except requests.RequestException as exc:
            response = None
            reason = f"błąd sieci ({exc.__class__.__name__})"

        if attempt < max_retries:
            wait = _retry_wait(response, attempt, backoff_base)
            logger.warning("%s, retry %d/%d za %.1fs", reason, attempt + 1, max_retries, wait)
            time.sleep(wait)

    return response

def fetched_to_csv(books: list[dict]) -> None:
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    scraped_at = datetime.now(timezone.utc)  # UTC-aware — isoformat() dokłada offset +00:00
    timestamp = scraped_at.strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(RAW_DATA_DIR, f"books_{timestamp}.csv")
    df = pd.DataFrame(books)
    df["scraped_at"] = scraped_at.isoformat()
    df.to_csv(filepath, index=False)
    logger.info("zapisano %d wierszy do %s", len(df), filepath)

def parse_books(html: bytes | str) -> list[dict]:
    """Parsuje HTML strony wyników → lista książek. Czysta funkcja: bez sieci i I/O."""
    soup = BeautifulSoup(html, "html.parser")
    containers = soup.find_all("div", {"data-component-type": "s-search-result"})
    books = []
    for book in containers:
        title = book.find("h2")
        author = book.find("a", href=lambda h: h and re.search(r'^/[^/]+/e/[A-Z0-9]{10}', h))
        price = book.find("span", {"class": "a-offscreen"})
        rating = book.find("span", {"class": "a-icon-alt"})
        if title and author:
            books.append({
                "asin": book.get("data-asin"),
                "title": title.get_text(strip=True),
                "author": author.get_text(strip=True),
                "price": price.get_text(strip=True) if price else None,
                "rating": rating.get_text(strip=True) if rating else None,
            })
    return books


def fetch_books(num_pages: int = SCRAPER_CFG.num_pages) -> int:
    base_url = f"{SCRAPER_CFG.base_url}?k={SCRAPER_CFG.keyword.replace(' ', '+')}"
    books = []

    for page in range(1, num_pages + 1):
        url = f"{base_url}&page={page}"
        response = _get_with_retry(url)

        if response is None:
            logger.error("strona %d: brak odpowiedzi po wszystkich próbach (błąd sieci).", page)
            continue
        if _is_challenge_page(response):
            logger.error("strona %d: Amazon zablokował zapytanie po wszystkich próbach. Odczekuje 10s", page)
            time.sleep(10)
            continue
        if response.status_code != 200:
            logger.error("strona %d: błąd: %s", page, response.status_code)
            continue

        books.extend(parse_books(response.content))

        if page < num_pages:
            delay = SCRAPER_CFG.delay_between_pages
            time.sleep(random.uniform(delay.min, delay.max))
    if len(books) < 1:
        raise RuntimeError("Nie udało się zebrać żadnych danych z Amazona.")
    else:
        fetched_to_csv(books)
        return len(books)

def main() -> None:
    fetch_books()


if __name__ == "__main__":
    main()
    

