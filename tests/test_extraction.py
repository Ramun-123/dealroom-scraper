thonimport json
import sys
from pathlib import Path

import pytest

# Ensure src/ is on sys.path so we can import parser modules
ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from parser.dealroom_extractor import DealroomExtractor  # type: ignore  # noqa: E402

@pytest.fixture
def sample_html() -> str:
    ld_json = {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": "Vibe.co",
        "url": "http://vibe.co",
        "description": "Vibe.co is a digital advertising platform that specializes in streaming apps and TV channels.",
        "industry": ["marketing", "adtech"],
        "numberOfEmployees": "51-200",
        "sameAs": [
            "https://www.linkedin.com/company/vibe-ctv-ott/",
            "https://twitter.com/vibe_ads"
        ],
        "address": {
            "@type": "PostalAddress",
            "streetAddress": "Chicago, Cook County, Illinois, United States",
            "addressCountry": "United States"
        }
    }

    html = f"""
    <html>
      <head>
        <title>Vibe.co - Dealroom</title>
        <meta name="description" content="Meta description that should be overridden by JSON-LD.">
        <script type="application/ld+json">
        {json.dumps(ld_json)}
        </script>
      </head>
      <body>
        <a href="https://www.linkedin.com/company/vibe-ctv-ott/">LinkedIn</a>
        <a href="https://twitter.com/vibe_ads">Twitter</a>
      </body>
    </html>
    """
    return html

def test_json_ld_extraction(sample_html: str) -> None:
    extractor = DealroomExtractor()
    result = extractor.extract_company_data(sample_html, source_url="https://app.dealroom.co/companies/vibe_ctv_ott")

    assert result["about"].startswith("Vibe.co is a digital advertising platform")
    assert result["website_url"] == "http://vibe.co"
    assert "marketing" in result["industries"]
    assert "adtech" in result["industries"]
    assert result["employees"] == "51-200"

    # Social links
    assert result["linkedin_url"] == "https://www.linkedin.com/company/vibe-ctv-ott/"
    assert result["twitter_url"] == "https://twitter.com/vibe_ads"
    assert result["social_links"]["linkedin"] == result["linkedin_url"]
    assert result["social_links"]["twitter"] == result["twitter_url"]

    # Locations
    assert result["hq_locations"][0]["country"] == "United States"
    assert "Chicago" in result["hq_locations"][0]["address"]

def test_missing_fields_are_handled_gracefully() -> None:
    html = "<html><head><title>No data</title></head><body><p>Empty page</p></body></html>"
    extractor = DealroomExtractor()
    result = extractor.extract_company_data(html, source_url="https://example.com/empty")

    # Should not crash, and at least return operational as default status
    assert result["company_status"] == "operational"
    assert result["raw_source_url"] == "https://example.com/empty"