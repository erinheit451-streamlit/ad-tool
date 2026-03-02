"""Scrape Facebook/Meta Ad Library for active ads by brand name.

Uses Playwright to interact with the Ad Library's required flow:
1. Load the Ad Library page
2. Select "All ads" from the Ad Category dropdown
3. Type search term and click the advertiser suggestion (or press Enter)
4. Scroll to load ads, extract card data
"""

import logging
import random
import re
import time
from typing import Optional
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

AD_LIBRARY_URL = "https://www.facebook.com/ads/library/"

SCROLL_CYCLES = 5
SCROLL_DELAY_MIN = 2.0
SCROLL_DELAY_MAX = 4.0


def scrape_facebook_ads(
    brand_name: str,
    domain: str,
    max_ads: int = 50,
    progress_cb=None,
    google_advertiser_name: str = None,
) -> dict:
    """Scrape Facebook Ad Library for a brand.

    Args:
        brand_name: e.g. "Nike"
        domain: e.g. "nike.com"
        max_ads: cap on ads to return
        progress_cb: optional callback(status_str)
        google_advertiser_name: advertiser name from Google (used as search hint)

    Returns:
        {
            "ads": [{page_name, paid_for_by, body, start_date, platforms,
                      library_id, image_url, screenshot_bytes}],
            "total_found": int,
            "source": "playwright",
            "error": str | None,
            "search_term_used": str | None,
            "fb_page_id": str | None,
        }
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return _error_result("Playwright not installed")

    search_terms = _build_search_terms(brand_name, domain, google_advertiser_name)
    _status(progress_cb, f"Will try Facebook searches: {search_terms}")

    for search_term in search_terms:
        _status(progress_cb, f"Searching Facebook Ad Library for '{search_term}'...")
        result = _try_facebook_search(search_term, max_ads, progress_cb)

        if result.get("ads"):
            result["search_term_used"] = search_term
            return result

        # Login wall = stop trying
        if result.get("error") and "login" in result["error"].lower():
            result["search_term_used"] = search_term
            return result

    all_terms = ", ".join(f'"{t}"' for t in search_terms)
    return {
        "ads": [],
        "total_found": 0,
        "source": "playwright",
        "search_term_used": None,
        "fb_page_id": None,
        "error": (
            f"No ads found after trying: {all_terms}. "
            f"Try manually: {AD_LIBRARY_URL}"
        ),
    }


def _build_search_terms(brand_name: str, domain: str, google_advertiser_name: str = None) -> list:
    """Build a prioritized list of search terms to try."""
    terms = []
    seen = set()

    def _add(term):
        normalized = term.strip().lower()
        if normalized and normalized not in seen and len(normalized) > 1:
            seen.add(normalized)
            terms.append(term.strip())

    # 1. DBA name from Google advertiser
    if google_advertiser_name:
        dba_match = re.search(r'dba\s+(.+)', google_advertiser_name, re.IGNORECASE)
        if dba_match:
            _add(dba_match.group(1).strip())
        else:
            clean = re.sub(
                r'\s*(,?\s*Inc\.?|,?\s*LLC|,?\s*Corp\.?|,?\s*Ltd\.?|,?\s*Co\.?)\s*$',
                '', google_advertiser_name, flags=re.IGNORECASE,
            ).rstrip(",. ")
            if clean:
                _add(clean)

    # 2. Brand with spaces inserted
    spaced = _space_out_brand(brand_name)
    if spaced != brand_name:
        _add(spaced)

    # 3. Original brand name
    _add(brand_name)

    # 4. Domain without TLD
    domain_name = domain.split(".")[0] if "." in domain else domain
    _add(domain_name)

    return terms[:4]


def _space_out_brand(name: str) -> str:
    """Add spaces to concatenated brand names: 'deepsouthkawasaki' -> 'Deep South Kawasaki'."""
    if " " in name:
        return name

    parts = re.findall(r'[A-Z][a-z]+|[a-z]+', name)
    if len(parts) > 1:
        return " ".join(p.capitalize() for p in parts)

    lower = name.lower()
    split_words = [
        "deep", "south", "north", "east", "west", "central", "mid", "upper", "lower",
        "golden", "silver", "green", "blue", "red", "black", "white", "grand",
        "kawasaki", "yamaha", "honda", "suzuki", "polaris", "harley", "davidson",
        "indian", "triumph", "ducati", "arctic",
        "motor", "cycle", "cycles", "sport", "sports", "power", "auto", "car", "cars",
        "truck", "marine", "outdoor", "outdoors", "adventure", "performance",
        "country", "valley", "mountain", "lake", "river", "bay", "coast", "island",
        "city", "town", "hill", "hills", "creek", "ridge", "park", "pine", "oak",
        "warby", "parker", "home", "depot", "best", "buy", "target", "dollar",
        "general", "family", "first", "american", "national", "pacific", "atlantic",
    ]

    result = lower
    for word in sorted(split_words, key=len, reverse=True):
        result = result.replace(word, f" {word} ")
    result = " ".join(result.split())
    if result != lower:
        return result.title()

    return name


def _try_facebook_search(search_term: str, max_ads: int, progress_cb) -> dict:
    """Execute a full Facebook Ad Library search with the proper UI flow."""
    from playwright.sync_api import sync_playwright
    from pathlib import Path

    # Debug log file for diagnosing Streamlit issues
    debug_log = Path(__file__).resolve().parent.parent / "fb_debug.log"

    def _dbg(msg):
        with open(debug_log, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%H:%M:%S')} {msg}\n")

    _dbg(f"=== Starting search for '{search_term}' ===")

    ads = []
    fb_page_id = None

    try:
        with sync_playwright() as p:
            _dbg("Playwright started")
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                locale="en-US",
            )
            ctx.add_init_script(
                'Object.defineProperty(navigator, "webdriver", {get: () => false});'
            )
            page = ctx.new_page()

            # Step 1: Load the Ad Library
            _status(progress_cb, "Loading Facebook Ad Library...")
            page.goto(AD_LIBRARY_URL, wait_until="domcontentloaded", timeout=30000)
            time.sleep(4)
            _dbg(f"Page loaded, URL: {page.url}")

            # Step 2: Select "All ads" from the Ad Category dropdown
            _status(progress_cb, "Selecting 'All ads' category...")
            cat_ok = _select_all_ads_category(page)
            _dbg(f"Category select result: {cat_ok}")
            if not cat_ok:
                browser.close()
                return _error_result("Could not find Ad Category dropdown")

            time.sleep(2)

            # Step 3: Type search term and look for advertiser autocomplete
            _status(progress_cb, f"Searching for '{search_term}'...")
            search_input = page.query_selector('input[type="search"]')
            if not search_input:
                search_input = page.query_selector(
                    'input[placeholder*="keyword"], input[placeholder*="advertiser"]'
                )
            _dbg(f"Search input found: {search_input is not None}")
            if not search_input:
                browser.close()
                return _error_result("Search box not found after selecting category")

            search_input.click()
            time.sleep(0.5)
            search_input.fill(search_term)
            time.sleep(3)

            # Step 4: Try to click the advertiser suggestion
            clicked_advertiser = _click_advertiser_suggestion(page, search_term)
            _dbg(f"Clicked advertiser suggestion: {clicked_advertiser}")

            if not clicked_advertiser:
                _status(progress_cb, "No advertiser match, doing keyword search...")
                search_input.press("Enter")

            time.sleep(5)

            url = page.url
            _dbg(f"URL after search: {url}")
            pid_match = re.search(r'view_all_page_id=(\d+)', url)
            if pid_match:
                fb_page_id = pid_match.group(1)

            body_text = page.inner_text("body")
            _dbg(f"Body text length: {len(body_text)}")
            _dbg(f"Has 'Started running': {'Started running' in body_text}")
            _dbg(f"Has 'no results': {'no results' in body_text.lower()}")
            _dbg(f"First 200 chars: {body_text[:200]}")

            if _is_login_blocked(body_text):
                _dbg("LOGIN BLOCKED")
                browser.close()
                return _error_result("Facebook requires login to view results")

            if "no results" in body_text.lower() or (
                "0 results" in body_text.lower()
                and "Started running" not in body_text
            ):
                _dbg("NO RESULTS")
                browser.close()
                return _error_result(f"No results for '{search_term}'")

            # Step 5: Scroll to load more ads
            _status(progress_cb, "Scrolling to load ads...")
            for cycle in range(SCROLL_CYCLES):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(random.uniform(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX))

            # Step 6: Extract ad cards
            _status(progress_cb, "Extracting ad data...")
            ad_cards = _find_ad_cards(page)
            _dbg(f"Ad cards found: {len(ad_cards) if ad_cards else 0}")

            if ad_cards:
                for i, card in enumerate(ad_cards[:max_ads]):
                    ad = _extract_ad_from_card(card, i)
                    if ad and (ad.get("body") or ad.get("page_name")):
                        try:
                            ad["screenshot_bytes"] = card.screenshot()
                        except Exception:
                            ad["screenshot_bytes"] = None
                        ads.append(ad)
            else:
                _status(progress_cb, "Trying text extraction fallback...")
                ads = _extract_ads_from_page_text(page)

            _dbg(f"Total ads extracted: {len(ads)}")
            browser.close()

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        _dbg(f"EXCEPTION: {e}\n{tb}")
        logger.error("Facebook scraping error: %s\n%s", e, tb)
        if not ads:
            return _error_result(f"Facebook scraping failed: {e}")
        logger.warning("Facebook error after %d ads: %s", len(ads), e)

    return {
        "ads": ads,
        "total_found": len(ads),
        "source": "playwright",
        "error": None if ads else f"No ads extracted for '{search_term}'",
        "fb_page_id": fb_page_id,
    }


def _select_all_ads_category(page) -> bool:
    """Click the Ad Category dropdown and select 'All ads'."""
    try:
        comboboxes = page.query_selector_all('[role="combobox"]')
        if len(comboboxes) < 2:
            return False

        # The second combobox is Ad Category
        comboboxes[1].click()
        time.sleep(1)

        # Find and click "All ads"
        all_ads = page.query_selector('text="All ads"')
        if all_ads:
            all_ads.click()
            return True

        # Fallback: look in listbox options
        options = page.query_selector_all('[role="option"]')
        for opt in options:
            if "All ads" in opt.inner_text():
                opt.click()
                return True

        return False
    except Exception as e:
        logger.debug("Failed to select All ads: %s", e)
        return False


def _click_advertiser_suggestion(page, search_term: str) -> bool:
    """Try to click the advertiser autocomplete suggestion.

    The advertiser suggestion contains '@username' and 'follow' text.
    Clicking it navigates to the page-level view with all ads.
    """
    try:
        suggestions = page.query_selector_all(
            '[role="option"], [role="listbox"] > div'
        )
        for s in suggestions:
            try:
                txt = s.inner_text().strip()
                # Advertiser suggestions have @username or follower count
                if "@" in txt or "follow" in txt.lower():
                    s.click()
                    return True
            except Exception:
                continue

        # Try a broader selector: look for the "Advertisers" section header
        all_items = page.query_selector_all("ul > li, [role='listbox'] > *")
        in_advertisers_section = False
        for item in all_items:
            try:
                txt = item.inner_text().strip()
                if "Advertisers" in txt:
                    in_advertisers_section = True
                    # This item might contain the clickable advertiser
                    if "@" in txt or "follow" in txt.lower():
                        item.click()
                        return True
                    continue
                if in_advertisers_section and len(txt) > 3:
                    item.click()
                    return True
            except Exception:
                continue

        return False
    except Exception:
        return False


def _is_login_blocked(text: str) -> bool:
    """Check if stuck on a login page."""
    login_indicators = ["Log in to Facebook", "Log Into Facebook", "Create new account"]
    has_login = any(ind in text for ind in login_indicators)
    has_content = "Started running" in text or "Active" in text
    return has_login and not has_content


def _find_ad_cards(page) -> list:
    """Find individual ad card elements.

    Facebook's Ad Library renders ads in a grid. Each ad card contains
    'Library ID:' text. We walk up from that text node to find the
    container that includes the full ad (body, images) — typically ~7
    levels up, where 'Sponsored' text appears and length is 200-3000.
    """
    # Primary: JS-based walk from "Library ID:" text to find card containers
    try:
        card_handles = page.evaluate_handle('''() => {
            const cards = [];
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
            while (walker.nextNode()) {
                if (walker.currentNode.textContent.includes('Library ID:')) {
                    let el = walker.currentNode.parentElement;
                    for (let i = 0; i < 10; i++) {
                        if (!el || !el.parentElement) break;
                        const text = el.innerText || '';
                        if (text.includes('Sponsored') && text.length > 200 && text.length < 3000) {
                            cards.push(el);
                            break;
                        }
                        el = el.parentElement;
                    }
                }
            }
            return cards;
        }''')

        cards = []
        length = card_handles.evaluate("arr => arr.length")
        for i in range(length):
            el = card_handles.evaluate_handle(f"arr => arr[{i}]").as_element()
            if el:
                cards.append(el)

        if cards:
            logger.info("Found %d ad cards via Library ID walk-up", len(cards))
            return cards
    except Exception as e:
        logger.debug("JS card walk failed: %s", e)

    # Fallback: CSS selector strategies
    for sel in ['div[role="article"]', 'div._99s5', 'div[class*="_7jvw"]']:
        try:
            cards = page.query_selector_all(sel)
            if cards:
                logger.info("Found %d cards with selector: %s", len(cards), sel)
                return cards
        except Exception:
            continue

    return []


def _extract_ads_from_page_text(page) -> list:
    """Last-resort: parse ads from the full page text."""
    try:
        text = page.inner_text("body")
        ads = []

        parts = text.split("Started running on")
        for i, part in enumerate(parts[1:], 1):
            prev_part = parts[i - 1] if i - 1 < len(parts) else ""
            lines = [l.strip() for l in prev_part.split("\n") if l.strip()]

            date_match = re.match(r"\s*(\w+ \d+, \d{4})", part)
            start_date = date_match.group(1) if date_match else None

            # Extract Library ID from current part
            lib_id_match = re.search(r"Library ID:\s*(\d+)", prev_part)
            library_id = lib_id_match.group(1) if lib_id_match else None

            page_name = None
            body = None
            for line in reversed(lines[-15:]):
                if len(line) > 5 and not any(
                    skip in line.lower()
                    for skip in ["see ad details", "about this ad", "active", "library id",
                                 "platforms", "filters", "sort by"]
                ):
                    if not body and len(line) > 20:
                        body = line
                    elif not page_name and len(line) < 80:
                        page_name = line

            if body or page_name:
                ads.append({
                    "page_name": page_name,
                    "paid_for_by": None,
                    "body": body,
                    "start_date": start_date,
                    "library_id": library_id,
                    "platforms": None,
                    "image_url": None,
                    "screenshot_bytes": None,
                })

            if len(ads) >= 50:
                break

        return ads
    except Exception:
        return []


def _extract_ad_from_card(card, index: int) -> Optional[dict]:
    """Extract ad information from a single card element."""
    try:
        text = card.inner_text().strip()
        if not text:
            return None

        lines = [l.strip() for l in text.split("\n") if l.strip()]

        page_name = None
        paid_for_by = None
        body = None
        start_date = None
        platforms = None
        library_id = None

        for line in lines:
            if "Paid for by" in line:
                paid_for_by = line.replace("Paid for by", "").strip().strip(chr(183)).strip()
            elif "Started running on" in line:
                start_date = line.replace("Started running on", "").strip()
            elif line.startswith("Library ID:"):
                library_id = line.replace("Library ID:", "").strip()
            elif "Sponsored" in line:
                # The line before "Sponsored" is usually the page name
                idx = lines.index(line)
                if idx > 0:
                    page_name = lines[idx - 1]

        # Find platforms line (contains platform icons text)
        for line in lines:
            if line == "Platforms":
                continue
            if any(p in line.lower() for p in ["facebook", "instagram", "messenger", "audience network"]):
                if len(line) < 100:
                    platforms = line

        # Body = longest line that isn't metadata
        skip_patterns = [
            "Paid for by", "Started running", "Library ID:", "Sponsored",
            "See ad details", "About this ad", "Active", "Inactive",
            "Platforms", "Categories", "Open Dropdown",
        ]
        non_meta = [
            l for l in lines
            if l != page_name
            and not any(sp in l for sp in skip_patterns)
            and len(l) > 15
            and l != platforms
        ]
        if non_meta:
            body = max(non_meta, key=len)

        # Try to get an image
        image_url = None
        try:
            img = card.query_selector("img[src*='scontent'], img[src*='fbcdn']")
            if img:
                image_url = img.get_attribute("src")
        except Exception:
            pass

        return {
            "page_name": page_name,
            "paid_for_by": paid_for_by,
            "body": body,
            "start_date": start_date,
            "library_id": library_id,
            "platforms": platforms,
            "image_url": image_url,
            "screenshot_bytes": None,
        }
    except Exception as e:
        logger.debug("Card extraction %d failed: %s", index, e)
        return None


def _error_result(msg: str) -> dict:
    return {
        "ads": [],
        "total_found": 0,
        "source": "playwright",
        "error": msg,
    }


def _status(cb, msg: str):
    if cb:
        cb(msg)
    logger.info(msg)
