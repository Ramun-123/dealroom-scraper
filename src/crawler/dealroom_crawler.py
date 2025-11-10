thonimport logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests

logger = logging.getLogger("dealroom-crawler")

@dataclass
class HTTPSettings:
    timeout: int
    max_retries: int
    user_agent: str

@dataclass
class CrawlerSettings:
    dealroom_base_url: str
    sleep_between_requests: float

class DealroomCrawler:
    """
    Small HTTP client responsible for turning a domain or URL into a Dealroom company page
    and fetching its HTML.

    This does not try to outsmart Dealroom's routing – ideally, callers should provide a
    proper Dealroom URL, but plain domains are supported with a simple heuristic.
    """

    def __init__(self, settings: Dict[str, Any]) -> None:
        http_conf = settings.get("http", {})
        crawler_conf = settings.get("crawler", {})

        self.http = HTTPSettings(
            timeout=int(http_conf.get("timeout", 10)),
            max_retries=int(http_conf.get("max_retries", 2)),
            user_agent=str(http_conf.get("user_agent", "dealroom-scraper/1.0")),
        )
        self.crawler = CrawlerSettings(
            dealroom_base_url=str(
                crawler_conf.get("dealroom_base_url", "https://app.dealroom.co/companies")
            ),
            sleep_between_requests=float(crawler_conf.get("sleep_between_requests", 0.5)),
        )

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.http.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

    def build_profile_url(self, identifier: str) -> str:
        """
        If identifier is already an http(s) URL, return as-is.
        Otherwise, treat it as a company slug and append to Dealroom base URL.
        """
        parsed = urlparse(identifier)
        if parsed.scheme in ("http", "https"):
            return identifier

        # Heuristic: if identifier looks like a domain, just prepend https://
        if "." in identifier and " " not in identifier:
            return f"https://{identifier}"

        # Otherwise assume it's a Dealroom slug
        identifier = identifier.strip().strip("/")
        return f"{self.crawler.dealroom_base_url}/{identifier}"

    def fetch_company_page(self, identifier: str) -> Tuple[Optional[str], str]:
        """
        Fetch a company page and return (html, final_url).
        On repeated failure, returns (None, attempted_url).
        """
        url = self.build_profile_url(identifier)
        logger.info("Fetching Dealroom page for '%s' -> %s", identifier, url)

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.http.max_retries + 1):
            try:
                response = self.session.get(url, timeout=self.http.timeout)
                if response.status_code >= 400:
                    logger.warning(
                        "HTTP %s for %s (attempt %d/%d)",
                        response.status_code,
                        url,
                        attempt,
                        self.http.max_retries,
                    )
                    time.sleep(self.crawler.sleep_between_requests)
                    continue

                logger.debug("Fetched %d bytes from %s", len(response.text), url)
                time.sleep(self.crawler.sleep_between_requests)
                return response.text, response.url
            except Exception as exc:  # pylint: disable=broad-except
                last_exc = exc
                logger.warning(
                    "Error fetching %s (attempt %d/%d): %s",
                    url,
                    attempt,
                    self.http.max_retries,
                    exc,
                )
                time.sleep(self.crawler.sleep_between_requests)

        logger.error("Failed to fetch %s after %d attempts: %s", url, self.http.max_retries, last_exc)
        return None, url