"""Extract domain and brand name from a URL, bare domain, or brand name."""

import re
from urllib.parse import urlparse

import tldextract


def extract(input_str: str) -> dict:
    """Accept a URL, bare domain, or brand name and return domain + brand info.

    Returns:
        {"domain": "nike.com", "brand_name": "Nike", "raw_input": "..."}
    """
    raw = input_str.strip()

    # If it looks like a URL or domain (has a dot), parse it
    if "." in raw:
        # Ensure it has a scheme so urlparse works
        url = raw if "://" in raw else f"https://{raw}"
        parsed = urlparse(url)
        hostname = parsed.hostname or raw

        ext = tldextract.extract(hostname)
        domain = ext.registered_domain  # e.g. "nike.com"
        brand = ext.domain              # e.g. "nike"

        if not domain:
            # tldextract couldn't parse — treat as plain text
            domain = raw
            brand = raw.split(".")[0]
    else:
        # No dot — treat as a brand name, construct a likely domain
        brand = raw.lower().replace(" ", "")
        domain = f"{brand}.com"

    brand_name = _humanize(brand)

    return {
        "domain": domain.lower(),
        "brand_name": brand_name,
        "raw_input": raw,
    }


def _humanize(brand_slug: str) -> str:
    """Turn a slug like 'warby-parker' or 'warbyparker' into 'Warby Parker'."""
    # Split on hyphens/underscores
    if "-" in brand_slug or "_" in brand_slug:
        parts = re.split(r"[-_]", brand_slug)
        return " ".join(p.capitalize() for p in parts if p)

    # Try splitting camelCase-style runs (uppercase boundaries)
    parts = re.findall(r"[A-Z][a-z]+|[a-z]+", brand_slug)
    if len(parts) > 1:
        return " ".join(p.capitalize() for p in parts)

    return brand_slug.capitalize()
