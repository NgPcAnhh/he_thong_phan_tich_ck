import concurrent.futures
import logging
import os
import random
import time
from datetime import datetime, date
from typing import List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Crawl configuration
MAX_WORKERS = 10
MIN_SLEEP = 1.5
MAX_SLEEP = 3.5

TARGET_SITES = [
    {"name": "cafef", "url": "https://cafef.vn/latest-news-sitemap.xml", "type": "sitemap_xml"},
    {"name": "vneconomy", "url": "https://vneconomy.vn/sitemap/latest-news.xml", "type": "sitemap_xml"},
    {"name": "vnexpress", "url": "https://vnexpress.net/rss/tin-moi-nhat.rss", "type": "rss"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}


def _date_tokens(target_date: date) -> List[str]:
    """Tokens used to detect target date strings in article publish text."""
    return [
        target_date.strftime("%Y-%m-%d"),
        target_date.strftime("%d/%m"),
        target_date.strftime("%d-%m"),
        f"{target_date.day}/{target_date.month}",
        f"{target_date.day}-{target_date.month}",
    ]


def _matches_target_day(text: str | None, tokens: List[str]) -> bool:
    if not text:
        return False
    return any(token in text for token in tokens)


def fetch_content(url: str) -> bytes | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.content
        if resp.status_code == 403:
            logger.warning("403 blocked at %s", url)
        if resp.status_code == 429:
            logger.warning("429 too many requests, slowing down 10s")
            time.sleep(10)
    except Exception:
        pass
    return None


def _parse_article(html_content: bytes | None, site_name: str, url: str) -> dict | None:
    if html_content is None:
        return None

    soup = BeautifulSoup(html_content, "html.parser")
    item = {"source": site_name, "url": url, "title": "", "sapo": "", "content": "", "publish_time": ""}

    try:
        if site_name == "cafef":
            item["title"] = soup.select_one("h1.title").get_text(strip=True) if soup.select_one("h1.title") else ""
            item["sapo"] = soup.select_one("h2.sapo").get_text(strip=True) if soup.select_one("h2.sapo") else ""
            item["content"] = soup.select_one(".detail-content").get_text(strip=True) if soup.select_one(".detail-content") else ""
            item["publish_time"] = soup.select_one(".pdate").get_text(strip=True) if soup.select_one(".pdate") else ""
        elif site_name == "vneconomy":
            item["title"] = soup.select_one("h1.detail-title").get_text(strip=True) if soup.select_one("h1.detail-title") else ""
            item["sapo"] = soup.select_one("h2.detail-sapo").get_text(strip=True) if soup.select_one("h2.detail-sapo") else ""
            item["content"] = soup.select_one(".detail-content").get_text(strip=True) if soup.select_one(".detail-content") else ""
            item["publish_time"] = soup.select_one(".detail-time").get_text(strip=True) if soup.select_one(".detail-time") else ""
        elif site_name == "vnexpress":
            item["title"] = soup.select_one("h1.title-detail").get_text(strip=True) if soup.select_one("h1.title-detail") else ""
            item["sapo"] = soup.select_one("p.description").get_text(strip=True) if soup.select_one("p.description") else ""
            item["content"] = soup.select_one("article.fck_detail").get_text(strip=True) if soup.select_one("article.fck_detail") else ""
            item["publish_time"] = soup.select_one(".date").get_text(strip=True) if soup.select_one(".date") else ""
    except Exception:
        return None

    return item


def _process_single_url(pack: Tuple[str, str], tokens: List[str]) -> dict | None:
    url, site_name = pack
    time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))
    html = fetch_content(url)
    article = _parse_article(html, site_name, url)
    if article and _matches_target_day(article.get("publish_time"), tokens):
        return article
    return None


def _collect_urls(target_date: date) -> List[Tuple[str, str]]:
    """Collect candidate article URLs; if date-filtered set is empty, fall back to all entries."""
    urls: List[Tuple[str, str]] = []
    tokens = _date_tokens(target_date)

    for site in TARGET_SITES:
        raw = fetch_content(site["url"])
        if not raw:
            logger.warning("No response when fetching sitemap/rss for %s", site["name"])
            continue

        soup = BeautifulSoup(raw, "xml")
        site_urls: List[Tuple[str, str]] = []

        if site["type"] == "sitemap_xml":
            for tag in soup.find_all("url"):
                loc = tag.find("loc")
                pub = tag.find("news:publication_date")
                if not loc or not loc.text:
                    continue
                if pub and _matches_target_day(pub.text, tokens):
                    site_urls.append((loc.text, site["name"]))
                elif not pub:
                    site_urls.append((loc.text, site["name"]))

            if not site_urls:
                # Fallback: keep all URLs to avoid missing news when publication_date format changes
                site_urls = [(tag.find("loc").text, site["name"]) for tag in soup.find_all("url") if tag.find("loc") and tag.find("loc").text]
        else:
            for item in soup.find_all("item"):
                link = item.find("link")
                if link and link.text:
                    site_urls.append((link.text, site["name"]))

        urls.extend(site_urls)
        logger.info("Collected %d urls from %s", len(site_urls), site["name"])

    return urls


def crawl_news_df(target_date_str: str | None = None, max_workers: int = MAX_WORKERS) -> pd.DataFrame:
    target_date = pd.to_datetime(target_date_str or datetime.now().date()).date()
    tokens = _date_tokens(target_date)
    urls = _collect_urls(target_date)
    logger.info("Target date %s - total candidate urls: %d", target_date.isoformat(), len(urls))

    articles: List[dict] = []
    if not urls:
        return pd.DataFrame()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_process_single_url, pack, tokens) for pack in urls]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                articles.append(result)

    logger.info("First pass matched %d articles", len(articles))

    # Fallback: if nothing matched target_date, relax filter to accept all crawled items
    if not articles:
        tokens_relaxed: List[str] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_process_single_url, pack, tokens_relaxed) for pack in urls]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    articles.append(result)
        logger.info("Fallback pass matched %d articles", len(articles))

    if not articles:
        return pd.DataFrame()

    df = pd.DataFrame(articles)
    df.insert(0, "target_date", target_date.isoformat())
    df["fetched_at"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    return df
