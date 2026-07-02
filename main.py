import os
import re
import random
import time
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from config import CONFIG
from ingest import ingest_books

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(PROJECT_DIR, CONFIG["storage"]["raw_data_dir"])
SCRAPER_CFG = CONFIG["scraper"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

def build_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

def _is_challenge_page(response):
    return "bm-verify" in response.text or len(response.content) < 5000

def _get_with_retry(url, max_retries=SCRAPER_CFG["max_retries"], backoff_base=SCRAPER_CFG["backoff_base"]):
    for attempt in range(max_retries + 1):
        response = requests.get(url, headers=build_headers())
        if response.status_code != 503 and not _is_challenge_page(response):
            return response

        if attempt < max_retries:
            wait = backoff_base ** attempt + random.uniform(0, 1)
            reason = response.status_code if response.status_code == 503 else "challenge"
            print(f"{reason}, retry {attempt + 1}/{max_retries} za {wait:.1f}s")
            time.sleep(wait)

    return response

def fetched_to_csv(books):
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    scraped_at = datetime.now()
    timestamp = scraped_at.strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(RAW_DATA_DIR, f"books_{timestamp}.csv")
    df = pd.DataFrame(books)
    df["scraped_at"] = scraped_at.isoformat()
    df.to_csv(filepath, index=False)
    print(f"zapisano {len(df)} wierszy do {filepath}")

def fetch_books(num_pages=SCRAPER_CFG["num_pages"]):
    base_url = f"{SCRAPER_CFG['base_url']}?k={SCRAPER_CFG['keyword'].replace(' ', '+')}"
    books = []

    for page in range(1, num_pages + 1):
        url = f"{base_url}&page={page}"
        response = _get_with_retry(url)
        #zapis dokąd?

        if _is_challenge_page(response):
            print(f"strona {page}: Amazon zablokował zapytanie po wszystkich próbach.")
            continue
        elif response.status_code != 200:
            print(f"strona {page}: błąd:", response.status_code)
            continue

        soup = BeautifulSoup(response.content, "html.parser")
        book_cointainer = soup.find_all("div", {"data-component-type": "s-search-result"})
        for book in book_cointainer:
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

        if page < num_pages:
            delay = SCRAPER_CFG["delay_between_pages"]
            time.sleep(random.uniform(delay["min"], delay["max"]))
    if len(books) < 1:
        raise RuntimeError("Nie udało się zebrać żadnych danych z Amazona.")
    else:
        fetched_to_csv(books)
        return len(books)

def inspect_divs(url: str = None) -> None: # funkcja wyszukiwania kontenerów <div> na stronie www i pierwsze 300 znaków dla każdego
    if url is None:
        base_url = f"{SCRAPER_CFG['base_url']}?k={SCRAPER_CFG['keyword'].replace(' ', '+')}"
        url = f"{base_url}&page=1"
    response = _get_with_retry(url)
    if _is_challenge_page(response):
        print("Amazon zablokował zapytanie.")
        return
    soup = BeautifulSoup(response.content, "html.parser") 
    types = sorted(set(
        div.get("data-component-type")
        for div in soup.find_all("div", attrs={"data-component-type": True})
    ))
    print(f"Znalezione data-component-type ({len(types)}):")
    for t in types:
        divs = soup.find_all("div", {"data-component-type": t})
        print(f"\n  {len(divs):3}x  [{t}]")
        print(f"       {str(divs[0])[:300]}...")


def main():
    fetch_books()


if __name__ == "__main__":
    main()

#dodać gita
#przepisać to pod AWS i zacząć naukę AWS
# ogarnąć co to snowflake
#projekty do porfolio.
#scraping z hendonmob

