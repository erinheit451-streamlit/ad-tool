"""Scrape Google Ads Transparency Center for ad creatives by domain.

Primary: HTTP RPC to Google's internal protobuf-like JSON endpoints.
Fallback: Playwright browser automation with network interception.
"""

import json
import logging
import re
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Google Ads Transparency Center RPC endpoints
_BASE_URL = "https://adstransparency.google.com"
_SUGGESTIONS_URL = f"{_BASE_URL}/anji/_/rpc/SearchService/SearchSuggestions"
_CREATIVES_URL = f"{_BASE_URL}/anji/_/rpc/SearchService/SearchCreatives"

_HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": f"{_BASE_URL}/",
    "Origin": _BASE_URL,
}

MAX_ADS = 200
PAGE_SIZE = 40


def scrape_google_ads(domain: str, max_ads: int = MAX_ADS, progress_cb=None) -> dict:
    """Scrape Google Ads Transparency Center for a domain.

    Returns:
        {
            "advertiser_id": str | None,
            "advertiser_name": str | None,
            "ads": [{creative_id, format, title, body, image_url, last_shown, advertiser_name}],
            "total_found": int,
            "source": "rpc" | "playwright" | "error",
            "error": str | None,
        }
    """
    _status(progress_cb, f"Searching Google Ads Transparency for {domain}...")

    result = _scrape_via_rpc(domain, max_ads, progress_cb)
    if result["source"] != "error":
        return result

    logger.warning("RPC scrape failed: %s — trying Playwright fallback", result["error"])
    _status(progress_cb, "RPC failed, trying browser fallback...")
    pw_result = _scrape_via_playwright(domain, max_ads, progress_cb)
    if pw_result["source"] != "error":
        return pw_result

    result["error"] = f"RPC: {result['error']} | Playwright: {pw_result['error']}"
    return result


# ---------------------------------------------------------------------------
# RPC approach — correct protobuf-like JSON format
# ---------------------------------------------------------------------------

def _scrape_via_rpc(domain: str, max_ads: int, progress_cb) -> dict:
    """Use Google's internal RPC endpoints to fetch ad creatives."""
    try:
        session = requests.Session()
        session.headers.update(_HEADERS)

        # Step 0: Initialize session cookies by visiting the homepage
        _status(progress_cb, "Initializing Google session...")
        try:
            session.get(
                f"{_BASE_URL}/?region=US",
                timeout=15,
                headers={"User-Agent": _HEADERS["User-Agent"]},
            )
        except requests.RequestException:
            pass  # Continue anyway — cookies are nice to have, not required

        # Step 1: Search suggestions to find advertiser ID
        _status(progress_cb, f"Looking up advertiser for {domain}...")
        advertiser_id, advertiser_name = _find_advertiser(session, domain)

        # Step 2: Fetch creatives — by advertiser ID if found, else by domain
        ads = []
        page_token = None
        total_found = 0
        page_num = 0

        while len(ads) < max_ads:
            page_num += 1
            _status(progress_cb, f"Fetching Google ads page {page_num}...")

            payload = _build_creatives_payload(
                domain=domain,
                advertiser_id=advertiser_id,
                page_token=page_token,
            )

            resp = session.post(_CREATIVES_URL, data=payload, timeout=20)

            if resp.status_code == 429:
                logger.warning("Rate limited on page %d, backing off", page_num)
                time.sleep(5)
                resp = session.post(_CREATIVES_URL, data=payload, timeout=20)

            if resp.status_code != 200:
                if page_num == 1:
                    return _error_result(f"HTTP {resp.status_code} on creatives request")
                break

            parsed = _parse_creatives_response(resp.text, advertiser_name)
            if parsed is None:
                if page_num == 1:
                    return _error_result("Could not parse creatives response")
                break

            if page_num == 1:
                total_found = parsed.get("total_count", 0)
                # Update advertiser name if we got it from the response
                if not advertiser_name and parsed.get("advertiser_name"):
                    advertiser_name = parsed["advertiser_name"]

            new_ads = parsed.get("ads", [])
            if not new_ads:
                break

            ads.extend(new_ads)
            page_token = parsed.get("next_page_token")
            if not page_token:
                break

            time.sleep(1.5)

        if not ads:
            return _error_result("No ads found via RPC")

        return {
            "advertiser_id": advertiser_id,
            "advertiser_name": advertiser_name,
            "ads": ads[:max_ads],
            "total_found": total_found or len(ads),
            "source": "rpc",
            "error": None,
        }

    except requests.RequestException as e:
        return _error_result(f"Request error: {e}")
    except Exception as e:
        return _error_result(f"RPC error: {e}")


def _find_advertiser(session: requests.Session, domain: str) -> tuple:
    """Call SearchSuggestions to find the advertiser ID for a domain.

    Returns (advertiser_id, advertiser_name) or (None, None).
    """
    payload_obj = {"1": domain, "2": 10, "3": 10}
    payload = {"f.req": json.dumps(payload_obj)}

    try:
        resp = session.post(_SUGGESTIONS_URL, data=payload, timeout=15)
        if resp.status_code != 200:
            logger.debug("Suggestions returned %d", resp.status_code)
            return None, None

        data = _parse_json_response(resp.text)
        if not data:
            return None, None

        # Response structure: {"1": [{"1": {"1": name, "2": AR_id, ...}, "2": {"1": domain}}]}
        # Navigate to find the advertiser
        suggestions = _get_nested(data, "1")
        if not isinstance(suggestions, list) or not suggestions:
            return None, None

        for suggestion in suggestions:
            advertiser_info = _get_nested(suggestion, "1")
            if not advertiser_info:
                continue
            adv_id = _get_nested(advertiser_info, "2")  # "AR..." string
            adv_name = _get_nested(advertiser_info, "1")  # display name
            if adv_id and isinstance(adv_id, str) and adv_id.startswith("AR"):
                return adv_id, adv_name

        return None, None

    except Exception as e:
        logger.debug("Suggestions lookup failed: %s", e)
        return None, None


def _build_creatives_payload(
    domain: str,
    advertiser_id: Optional[str] = None,
    page_token: Optional[str] = None,
) -> dict:
    """Build the f.req payload for SearchCreatives.

    If we have an advertiser_id, search by that (more reliable).
    Otherwise fall back to domain-based search.
    """
    req = {"2": PAGE_SIZE, "3": {}}

    if advertiser_id:
        # Search by advertiser ID with US region
        req["3"]["13"] = {"1": [advertiser_id]}
        req["3"]["8"] = [2840]  # US region code
    else:
        # Search by domain
        req["3"]["12"] = {"1": domain}

    if page_token:
        req["4"] = page_token

    # Request format/date info
    req["7"] = {"1": 1}

    return {"f.req": json.dumps(req)}


def _parse_creatives_response(text: str, default_advertiser: Optional[str] = None) -> Optional[dict]:
    """Parse the SearchCreatives response.

    Verified response structure:
        "1": list of creative objects
        "2": next page token (str)
        "4": total count (str of number)
        "5": secondary count (str of number)
    """
    data = _parse_json_response(text)
    if data is None:
        return None

    ads = []
    advertiser_name = default_advertiser
    next_page_token = None
    total_count = 0

    # The creatives are in key "1" as a list
    creatives_list = _get_nested(data, "1")
    if isinstance(creatives_list, list):
        for creative in creatives_list:
            ad = _parse_single_creative(creative)
            if ad:
                if not advertiser_name and ad.get("advertiser_name"):
                    advertiser_name = ad["advertiser_name"]
                ads.append(ad)

    # Next page token is in key "2"
    next_page_token = _get_nested(data, "2")
    if not isinstance(next_page_token, str):
        next_page_token = None

    # Total count is in key "4" as a string number
    count_val = _get_nested(data, "4")
    if isinstance(count_val, str) and count_val.isdigit():
        total_count = int(count_val)
    elif isinstance(count_val, (int, float)):
        total_count = int(count_val)

    return {
        "ads": ads,
        "advertiser_name": advertiser_name,
        "total_count": total_count,
        "next_page_token": next_page_token,
    }


def _parse_single_creative(creative) -> Optional[dict]:
    """Parse a single creative entry from the response.

    Verified structure (numbered keys):
        "1"  = advertiser ID (AR...)
        "2"  = creative ID (CR...)
        "3"  = content object:
               "3"."3"."2" = HTML with <img> tag (image ads)
               "3"."1"."4" = display ad preview URL
               "3"."5"    = bool (has content?)
        "4"  = format type (1=image, 2=text, 3=video)
        "6"  = first shown timestamp {"1": epoch_sec, "2": nanos}
        "7"  = last shown timestamp {"1": epoch_sec, "2": nanos}
        "12" = advertiser name (str)
        "13" = ad count for this advertiser (int)
        "14" = domain (str)
    """
    if not isinstance(creative, dict):
        return None

    creative_id = _get_nested(creative, "2")
    if not creative_id or not isinstance(creative_id, str):
        return None

    # Advertiser info
    advertiser_name = _get_nested(creative, "12")
    if not isinstance(advertiser_name, str):
        advertiser_name = None

    domain = _get_nested(creative, "14")

    # Format: 1=image, 2=text, 3=video
    fmt_code = _get_nested(creative, "4")
    fmt_map = {1: "IMAGE", 2: "TEXT", 3: "VIDEO"}
    fmt = fmt_map.get(fmt_code, "IMAGE")

    # Extract image URL from content
    image_url = None
    title = None
    body = None

    content = _get_nested(creative, "3")
    if isinstance(content, dict):
        # Image ads: "3"."3"."2" contains <img> HTML
        img_html = _get_nested(content, "3", "2")
        if isinstance(img_html, str):
            img_match = re.search(r'src="([^"]+)"', img_html)
            if img_match:
                image_url = img_match.group(1)

        # Display ad preview URL: "3"."1"."4"
        if not image_url:
            preview_url = _get_nested(content, "1", "4")
            if isinstance(preview_url, str) and preview_url.startswith("http"):
                image_url = preview_url

        # Text ads may have content in "3"."2" directly or in "3"."1"
        text_content = _get_nested(content, "2")
        if isinstance(text_content, str) and not text_content.startswith("<"):
            title = text_content

        # Some ads have title in "3"."1"."1"
        if not title:
            t = _get_nested(content, "1", "1")
            if isinstance(t, str) and len(t) > 2 and not t.startswith("http"):
                title = t

    # Last shown date from epoch timestamp
    last_shown = None
    ts_obj = _get_nested(creative, "7")
    if isinstance(ts_obj, dict):
        epoch = _get_nested(ts_obj, "1")
        if epoch and isinstance(epoch, (int, str)):
            try:
                import datetime
                dt = datetime.datetime.fromtimestamp(int(epoch))
                last_shown = dt.strftime("%Y-%m-%d")
            except (ValueError, OSError):
                pass

    # First shown date
    first_shown = None
    ts_obj_first = _get_nested(creative, "6")
    if isinstance(ts_obj_first, dict):
        epoch = _get_nested(ts_obj_first, "1")
        if epoch and isinstance(epoch, (int, str)):
            try:
                import datetime
                dt = datetime.datetime.fromtimestamp(int(epoch))
                first_shown = dt.strftime("%Y-%m-%d")
            except (ValueError, OSError):
                pass

    return {
        "creative_id": creative_id,
        "format": fmt,
        "title": title,
        "body": body,
        "image_url": image_url,
        "last_shown": last_shown,
        "first_shown": first_shown,
        "advertiser_name": advertiser_name,
        "domain": domain,
    }


# ---------------------------------------------------------------------------
# Playwright fallback — intercepts RPC responses from the browser
# ---------------------------------------------------------------------------

def _scrape_via_playwright(domain: str, max_ads: int, progress_cb) -> dict:
    """Playwright fallback: load the page and intercept RPC network responses."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return _error_result("Playwright not installed")

    ads = []
    advertiser_name = None
    captured_responses = []

    try:
        _status(progress_cb, "Launching browser for Google Ads Transparency...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
                ),
            )
            page = ctx.new_page()

            # Intercept RPC responses to capture ad data
            def on_response(response):
                if "SearchCreatives" in response.url or "SearchSuggestions" in response.url:
                    try:
                        captured_responses.append({
                            "url": response.url,
                            "body": response.text(),
                        })
                    except Exception:
                        pass

            page.on("response", on_response)

            # Navigate to the transparency page for this domain
            url = f"{_BASE_URL}/?region=US&domain={domain}"
            _status(progress_cb, f"Loading {url}...")
            page.goto(url, wait_until="networkidle", timeout=45000)
            time.sleep(4)

            # Scroll to trigger more ad loads
            for i in range(3):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)

            browser.close()

        # Parse captured RPC responses
        _status(progress_cb, "Parsing captured responses...")
        for resp_data in captured_responses:
            if "SearchCreatives" in resp_data["url"]:
                parsed = _parse_creatives_response(resp_data["body"])
                if parsed and parsed.get("ads"):
                    ads.extend(parsed["ads"])
                    if not advertiser_name and parsed.get("advertiser_name"):
                        advertiser_name = parsed["advertiser_name"]

            elif "SearchSuggestions" in resp_data["url"]:
                data = _parse_json_response(resp_data["body"])
                if data:
                    suggestions = _get_nested(data, "1")
                    if isinstance(suggestions, list) and suggestions:
                        info = _get_nested(suggestions[0], "1")
                        if info and not advertiser_name:
                            advertiser_name = _get_nested(info, "1")

    except Exception as e:
        if not ads:
            return _error_result(f"Playwright: {e}")
        logger.warning("Playwright error after %d ads: %s", len(ads), e)

    if not ads:
        return _error_result("No ads found via Playwright (no RPC responses captured)")

    return {
        "advertiser_id": None,
        "advertiser_name": advertiser_name,
        "ads": ads[:max_ads],
        "total_found": len(ads),
        "source": "playwright",
        "error": None,
    }


# ---------------------------------------------------------------------------
# JSON parsing helpers
# ---------------------------------------------------------------------------

def _parse_json_response(text: str):
    """Parse a Google RPC response (may have )]}' prefix)."""
    try:
        cleaned = text
        if cleaned.startswith(")]}'"):
            cleaned = cleaned[4:]
        cleaned = cleaned.strip()
        return json.loads(cleaned)
    except (json.JSONDecodeError, TypeError):
        return None


def _get_nested(data, *keys):
    """Safely navigate nested dicts/lists by string or int keys."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list):
            try:
                idx = int(key)
                current = current[idx]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if current is None:
            return None
    return current


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _error_result(msg: str) -> dict:
    return {
        "advertiser_id": None,
        "advertiser_name": None,
        "ads": [],
        "total_found": 0,
        "source": "error",
        "error": msg,
    }


def _status(cb, msg: str):
    if cb:
        cb(msg)
    logger.info(msg)
