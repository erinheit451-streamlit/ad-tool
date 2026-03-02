"""Detect whether ads are managed by an agency or in-house.

Compares advertiser/payer names against the brand name and a curated
list of known agencies.
"""

import json
import logging
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_AGENCIES_FILE = _DATA_DIR / "known_agencies.json"

# Cache loaded data
_known_agencies: Optional[dict] = None


def _load_agencies() -> dict:
    global _known_agencies
    if _known_agencies is None:
        try:
            with open(_AGENCIES_FILE, "r", encoding="utf-8") as f:
                _known_agencies = json.load(f)
        except FileNotFoundError:
            logger.warning("known_agencies.json not found")
            _known_agencies = {"agencies": [], "patterns": []}
    return _known_agencies


def detect_agency(
    advertiser_name: Optional[str],
    brand_name: str,
    paid_for_by: Optional[str] = None,
) -> dict:
    """Determine if the advertiser/payer is an agency or the brand itself.

    Args:
        advertiser_name: Name from ad platform (e.g. Google advertiser name)
        brand_name: Expected brand name (e.g. "Nike")
        paid_for_by: "Paid for by" text from Facebook (if available)

    Returns:
        {
            "is_agency": bool,
            "confidence": "high" | "medium" | "low",
            "reason": str,
            "advertiser_name": str | None,
            "paid_for_by": str | None,
        }
    """
    data = _load_agencies()
    agency_list = [a.lower() for a in data.get("agencies", [])]
    patterns = [p.lower() for p in data.get("patterns", [])]

    # Check both names — advertiser_name (Google) and paid_for_by (Facebook)
    names_to_check = []
    if advertiser_name:
        names_to_check.append(("advertiser_name", advertiser_name))
    if paid_for_by:
        names_to_check.append(("paid_for_by", paid_for_by))

    if not names_to_check:
        return {
            "is_agency": False,
            "confidence": "low",
            "reason": "No advertiser or payer name available to analyze.",
            "advertiser_name": advertiser_name,
            "paid_for_by": paid_for_by,
        }

    brand_lower = brand_name.lower().strip()

    for source, name in names_to_check:
        name_lower = name.lower().strip()

        # 1. Exact or near-exact match to brand = in-house (high confidence)
        similarity = SequenceMatcher(None, brand_lower, name_lower).ratio()
        if similarity >= 0.8:
            continue  # Looks like the brand itself, check next name

        # 2. Brand name is contained within the advertiser name
        if brand_lower in name_lower or name_lower in brand_lower:
            continue  # Likely a subsidiary or variant

        # 3. Check against known agency list
        for agency in agency_list:
            if agency in name_lower or name_lower in agency:
                return {
                    "is_agency": True,
                    "confidence": "high",
                    "reason": (
                        f'"{name}" ({source}) matches known agency "{agency}".'
                    ),
                    "advertiser_name": advertiser_name,
                    "paid_for_by": paid_for_by,
                }
            # Check partial match (e.g. "GroupM" in "GroupM North America")
            if SequenceMatcher(None, agency, name_lower).ratio() >= 0.7:
                return {
                    "is_agency": True,
                    "confidence": "medium",
                    "reason": (
                        f'"{name}" ({source}) is similar to known agency "{agency}".'
                    ),
                    "advertiser_name": advertiser_name,
                    "paid_for_by": paid_for_by,
                }

        # 4. Check against generic agency patterns
        for pattern in patterns:
            if pattern in name_lower:
                return {
                    "is_agency": True,
                    "confidence": "medium",
                    "reason": (
                        f'"{name}" ({source}) contains agency pattern "{pattern}".'
                    ),
                    "advertiser_name": advertiser_name,
                    "paid_for_by": paid_for_by,
                }

        # 5. Low similarity to brand and not a known agency
        if similarity < 0.4:
            return {
                "is_agency": True,
                "confidence": "low",
                "reason": (
                    f'"{name}" ({source}) has low similarity '
                    f'to brand "{brand_name}" ({similarity:.0%}). '
                    f"May be an agency or parent company."
                ),
                "advertiser_name": advertiser_name,
                "paid_for_by": paid_for_by,
            }

    # All names checked, all matched the brand
    return {
        "is_agency": False,
        "confidence": "high",
        "reason": "Advertiser name closely matches the brand — likely in-house.",
        "advertiser_name": advertiser_name,
        "paid_for_by": paid_for_by,
    }


def analyze_all_ads(ads: list, brand_name: str, source: str = "google") -> list:
    """Run agency detection on a list of ads.

    Returns the ads list with an "agency_flag" dict added to each.
    """
    for ad in ads:
        if source == "google":
            flag = detect_agency(
                advertiser_name=ad.get("advertiser_name"),
                brand_name=brand_name,
            )
        else:  # facebook
            flag = detect_agency(
                advertiser_name=ad.get("page_name"),
                brand_name=brand_name,
                paid_for_by=ad.get("paid_for_by"),
            )
        ad["agency_flag"] = flag

    return ads
