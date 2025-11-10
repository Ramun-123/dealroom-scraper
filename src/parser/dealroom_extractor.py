thonimport json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger("dealroom-extractor")

@dataclass
class CompanyRecord:
    about: Optional[str] = None
    company_status: Optional[str] = None
    funding_rounds: List[Dict[str, Any]] = field(default_factory=list)
    growth_stage: Optional[str] = None
    employees: Optional[str] = None
    similarweb_traffic: Optional[str] = None
    social_links: Dict[str, str] = field(default_factory=dict)
    investors: List[Dict[str, Any]] = field(default_factory=list)
    industries: List[str] = field(default_factory=list)
    team: List[Dict[str, Any]] = field(default_factory=list)
    hq_locations: List[Dict[str, Any]] = field(default_factory=list)
    kpi_summary: Dict[str, Any] = field(default_factory=dict)
    nearby_companies: List[Dict[str, Any]] = field(default_factory=list)
    related_companies: List[Dict[str, Any]] = field(default_factory=list)
    news: List[Dict[str, Any]] = field(default_factory=list)
    website_url: Optional[str] = None
    linkedin_url: Optional[str] = None
    twitter_url: Optional[str] = None
    instagram_url: Optional[str] = None
    raw_source_url: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "about": self.about,
            "company_status": self.company_status,
            "funding_rounds": self.funding_rounds,
            "growth_stage": self.growth_stage,
            "employees": self.employees,
            "similarweb_traffic": self.similarweb_traffic,
            "social_links": self.social_links,
            "investors": self.investors,
            "industries": self.industries,
            "team": self.team,
            "hq_locations": self.hq_locations,
            "kpi_summary": self.kpi_summary,
            "nearby_companies": self.nearby_companies,
            "related_companies": self.related_companies,
            "news": self.news,
            "website_url": self.website_url,
            "linkedin_url": self.linkedin_url,
            "twitter_url": self.twitter_url,
            "instagram_url": self.instagram_url,
            "raw_source_url": self.raw_source_url,
        }
        # Remove keys with None or empty containers, but keep falsey scalars like 0
        cleaned: Dict[str, Any] = {}
        for key, value in data.items():
            if value is None:
                continue
            if isinstance(value, (list, dict)) and len(value) == 0:
                continue
            cleaned[key] = value
        return cleaned

class DealroomExtractor:
    """
    Extract structured company information from a Dealroom company profile HTML.

    This implementation tries to be robust:
    - Looks for JSON-LD scripts describing an Organization.
    - Falls back to meta tags and visible content where reasonable.
    - Never raises on missing fields; instead, it returns partial data.
    """

    def extract_company_data(self, html: str, source_url: Optional[str] = None) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "lxml")
        record = CompanyRecord(raw_source_url=source_url)

        ld_org = self._extract_ld_organization(soup)
        if ld_org:
            self._populate_from_ld_json(ld_org, record)

        self._populate_social_links(soup, record)
        self._populate_meta_fallbacks(soup, record)
        self._populate_status_and_stage(soup, record)
        self._populate_funding_and_investors(soup, record)
        self._populate_locations(soup, record)

        return record.to_dict()

    # ---------------- JSON-LD helpers ---------------- #

    def _extract_ld_organization(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                payload_text = script.string or script.get_text()
                if not payload_text:
                    continue
                data = json.loads(payload_text)
            except (json.JSONDecodeError, TypeError):
                continue

            # JSON-LD can be a list or a dict
            candidates: List[Dict[str, Any]] = []
            if isinstance(data, dict):
                candidates = [data]
            elif isinstance(data, list):
                candidates = [x for x in data if isinstance(x, dict)]

            for obj in candidates:
                obj_type = obj.get("@type")
                if isinstance(obj_type, list):
                    is_org = any(t.lower() in ("organization", "corp", "corporation") for t in obj_type)
                else:
                    is_org = isinstance(obj_type, str) and obj_type.lower() in (
                        "organization",
                        "corp",
                        "corporation",
                        "company",
                    )
                if is_org:
                    logger.debug("Found Organization JSON-LD block")
                    return obj

        return None

    def _populate_from_ld_json(self, ld: Dict[str, Any], record: CompanyRecord) -> None:
        record.about = ld.get("description") or record.about

        # Website
        url = ld.get("url") or ld.get("sameAs") or None
        if isinstance(url, list):
            for candidate in url:
                if isinstance(candidate, str) and candidate.startswith("http"):
                    url = candidate
                    break
        if isinstance(url, str):
            record.website_url = url

        # Industry / industries
        industries: List[str] = []
        industry = ld.get("industry")
        if isinstance(industry, list):
            industries.extend([str(x) for x in industry])
        elif isinstance(industry, str):
            industries.append(industry)
        if industries:
            record.industries = list(sorted(set(record.industries + industries)))

        # Employees
        employees = ld.get("numberOfEmployees") or ld.get("employee", {}).get("count")
        if isinstance(employees, (int, float)):
            record.employees = str(int(employees))
        elif isinstance(employees, str):
            record.employees = employees

        # Social links in JSON-LD
        same_as = ld.get("sameAs")
        links = same_as if isinstance(same_as, list) else [same_as] if isinstance(same_as, str) else []
        for link in links:
            self._assign_social_link(str(link), record)

    # ---------------- Social links & meta fallbacks ---------------- #

    def _assign_social_link(self, href: str, record: CompanyRecord) -> None:
        href_lower = href.lower()
        if "linkedin.com" in href_lower:
            record.linkedin_url = href
            record.social_links.setdefault("linkedin", href)
        elif "twitter.com" in href_lower or "x.com" in href_lower:
            record.twitter_url = href
            record.social_links.setdefault("twitter", href)
        elif "instagram.com" in href_lower:
            record.instagram_url = href
            record.social_links.setdefault("instagram", href)
        elif "facebook.com" in href_lower:
            record.social_links.setdefault("facebook", href)
        elif "youtube.com" in href_lower or "youtu.be" in href_lower:
            record.social_links.setdefault("youtube", href)

    def _populate_social_links(self, soup: BeautifulSoup, record: CompanyRecord) -> None:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            self._assign_social_link(href, record)

    def _populate_meta_fallbacks(self, soup: BeautifulSoup, record: CompanyRecord) -> None:
        # Description fallback from meta
        if not record.about:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if not meta_desc:
                meta_desc = soup.find("meta", attrs={"property": "og:description"})
            if meta_desc and meta_desc.get("content"):
                record.about = meta_desc["content"].strip()

        # Website URL fallback from canonical or og:url
        if not record.website_url:
            canonical = soup.find("link", rel="canonical")
            if canonical and canonical.get("href"):
                record.website_url = canonical["href"]
            else:
                og_url = soup.find("meta", attrs={"property": "og:url"})
                if og_url and og_url.get("content"):
                    record.website_url = og_url["content"]

    # ---------------- Status, funding, locations ---------------- #

    def _populate_status_and_stage(self, soup: BeautifulSoup, record: CompanyRecord) -> None:
        text = soup.get_text(separator=" ", strip=True).lower()

        if record.company_status is None:
            if "closed" in text or "defunct" in text:
                record.company_status = "closed"
            elif "acquired" in text:
                record.company_status = "acquired"
            elif "ipo" in text:
                record.company_status = "public"
            else:
                record.company_status = "operational"  # optimistic default

        # Stage: look for common stage keywords
        if record.growth_stage is None:
            stage_keywords = [
                ("seed", "seed"),
                ("series a", "early growth"),
                ("series b", "growth"),
                ("series c", "late growth"),
                ("series d", "late growth"),
                ("pre-seed", "pre-seed"),
                ("early stage", "early growth"),
                ("late stage", "late growth"),
                ("scaleup", "scaleup"),
            ]
            for key, stage in stage_keywords:
                if key in text:
                    record.growth_stage = stage
                    break

    def _populate_funding_and_investors(self, soup: BeautifulSoup, record: CompanyRecord) -> None:
        """
        Try to detect structured JSON blobs with funding/investor info.
        Dealroom is a SPA, so funding may be in embedded JSON.
        """
        scripts = soup.find_all("script")
        for script in scripts:
            raw = script.string or script.get_text()
            if not raw:
                continue

            if "funding" not in raw and "investor" not in raw:
                continue

            try:
                data = json.loads(raw)
            except Exception:  # broad on purpose - HTML may contain many non-JSON scripts
                continue

            self._walk_for_funding_and_investors(data, record)

        # Remove duplicates and normalize
        unique_rounds = []
        seen_rounds = set()
        for fr in record.funding_rounds:
            key = (
                fr.get("year"),
                fr.get("round"),
                fr.get("amount"),
                fr.get("currency"),
            )
            if key in seen_rounds:
                continue
            seen_rounds.add(key)
            unique_rounds.append(fr)
        record.funding_rounds = unique_rounds

        unique_investors = []
        seen_investors = set()
        for inv in record.investors:
            name = inv.get("name")
            if not name or name in seen_investors:
                continue
            seen_investors.add(name)
            unique_investors.append(inv)
        record.investors = unique_investors

    def _walk_for_funding_and_investors(self, node: Any, record: CompanyRecord) -> None:
        if isinstance(node, dict):
            # Funding rounds
            if "funding_rounds" in node and isinstance(node["funding_rounds"], list):
                for fr in node["funding_rounds"]:
                    if not isinstance(fr, dict):
                        continue
                    clean = {
                        "year": fr.get("year") or fr.get("date") or fr.get("round_year"),
                        "round": fr.get("round") or fr.get("type"),
                        "amount": fr.get("amount") or fr.get("raised"),
                        "currency": fr.get("currency") or fr.get("currency_code"),
                        "investors": fr.get("investors") or [],
                    }
                    record.funding_rounds.append(clean)

            # Investors
            if "investors" in node and isinstance(node["investors"], list):
                for inv in node["investors"]:
                    if not isinstance(inv, dict):
                        continue
                    clean = {
                        "name": inv.get("name"),
                        "type": inv.get("type"),
                        "path": inv.get("path") or inv.get("slug"),
                    }
                    record.investors.append(clean)

            for value in node.values():
                self._walk_for_funding_and_investors(value, record)
        elif isinstance(node, list):
            for item in node:
                self._walk_for_funding_and_investors(item, record)

    def _populate_locations(self, soup: BeautifulSoup, record: CompanyRecord) -> None:
        """
        A very loose heuristic: if there is JSON with address-like fields, capture them.
        """
        scripts = soup.find_all("script")
        for script in scripts:
            raw = script.string or script.get_text()
            if not raw or "address" not in raw:
                continue

            try:
                data = json.loads(raw)
            except Exception:
                continue

            self._walk_for_locations(data, record)

        # Deduplicate
        unique_locations = []
        seen = set()
        for loc in record.hq_locations:
            key = (loc.get("address"), loc.get("country"))
            if key in seen:
                continue
            seen.add(key)
            unique_locations.append(loc)
        record.hq_locations = unique_locations

    def _walk_for_locations(self, node: Any, record: CompanyRecord) -> None:
        if isinstance(node, dict):
            addr = node.get("address")
            if isinstance(addr, dict):
                address = addr.get("streetAddress") or addr.get("address") or addr.get("full")
                country = addr.get("addressCountry") or addr.get("country")
                if address or country:
                    record.hq_locations.append(
                        {
                            "address": address,
                            "country": country,
                        }
                    )
            for value in node.values():
                self._walk_for_locations(value, record)
        elif isinstance(node, list):
            for item in node:
                self._walk_for_locations(item, record)