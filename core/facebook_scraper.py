"""Scrape Facebook/Meta Ad Library for active ads by brand name.

Uses Playwright to interact with the Ad Library's required flow:
1. Load the Ad Library page
2. Select "All ads" from the Ad Category dropdown
3. Type search term and click the advertiser suggestion (or press Enter)
4. Scroll to load ads, extract card data

NOTE: Playwright's sync_playwright() cannot run inside Streamlit's event loop
on Windows (asyncio subprocess creation raises NotImplementedError). To work
around this, we spawn the Playwright work in a child process via
multiprocessing.
"""

import json
import logging
import multiprocessing
import random
import re
import tempfile
import time
from pathlib import Path
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

    Spawns a child process to run Playwright (avoids Streamlit's asyncio
    event-loop conflict on Windows).
    """
    search_terms = _build_search_terms(brand_name, domain, google_advertiser_name)
    _status(progress_cb, f"Will try Facebook searches: {search_terms}")

    per_term_errors = []
    for search_term in search_terms:
        _status(progress_cb, f"Searching Facebook Ad Library for '{search_term}'...")
        result = _run_in_subprocess(search_term, max_ads, progress_cb)

        if result.get("ads"):
            result["search_term_used"] = search_term
            return result

        # Login wall = stop trying
        if result.get("error") and "login" in result["error"].lower():
            result["search_term_used"] = search_term
            return result

        per_term_errors.append(f'"{search_term}": {result.get("error", "no ads")}')

    all_terms = ", ".join(f'"{t}"' for t in search_terms)
    detail = " | ".join(per_term_errors)
    return {
        "ads": [],
        "total_found": 0,
        "source": "playwright",
        "search_term_used": None,
        "fb_page_id": None,
        "error": (
            f"No ads found after trying: {all_terms}. "
            f"Per-term errors: {detail}. "
            f"Try manually: {AD_LIBRARY_URL}"
        ),
    }


def _run_in_subprocess(search_term: str, max_ads: int, progress_cb) -> dict:
    """Run the Playwright scraper in a completely separate Python process.

    This avoids the Windows asyncio NotImplementedError that occurs when
    Playwright tries to spawn a subprocess inside Streamlit's event loop.
    We use subprocess.Popen (not multiprocessing) for maximum isolation.

    Card screenshots are saved as PNG files in a temp directory and their
    paths are returned in each ad dict.
    """
    import subprocess, sys

    result_file = Path(tempfile.mktemp(suffix=".json", prefix="fb_result_"))
    screenshot_dir = Path(tempfile.mkdtemp(prefix="fb_screenshots_"))
    this_file = Path(__file__).resolve()

    # Build worker script that runs the search and saves screenshots
    script = (
        f"import sys, json, base64\n"
        f"from pathlib import Path\n"
        f"sys.path.insert(0, {str(this_file.parent.parent)!r})\n"
        f"from core.facebook_scraper import _try_facebook_search\n"
        f"r = _try_facebook_search({search_term!r}, {max_ads!r}, None)\n"
        f"sd = Path({str(screenshot_dir)!r})\n"
        f"for i, ad in enumerate(r.get('ads', [])):\n"
        f"    sb = ad.pop('screenshot_bytes', None)\n"
        f"    ad.pop('thumbnail_bytes', None)\n"
        f"    if sb:\n"
        f"        p = sd / f'card_{{i}}.png'\n"
        f"        p.write_bytes(sb)\n"
        f"        ad['screenshot_path'] = str(p)\n"
        f"Path({str(result_file)!r}).write_text(\n"
        f"    json.dumps(r, default=str, ensure_ascii=False), encoding='utf-8')\n"
    )

    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        elapsed = 0
        while proc.poll() is None and elapsed < 120:
            time.sleep(3)
            elapsed += 3
            if progress_cb:
                _status(progress_cb, f"Facebook: scraping '{search_term}' ({elapsed}s)...")

        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
            return _error_result("Facebook scraping timed out after 120s")

        if not result_file.exists():
            stderr = proc.stderr.read().decode("utf-8", errors="replace")[:500]
            return _error_result(f"Facebook subprocess failed (exit {proc.returncode}): {stderr}")

        data = json.loads(result_file.read_text(encoding="utf-8"))

        # Load screenshot PNGs back into bytes for Streamlit display
        for ad in data.get("ads", []):
            sp = ad.pop("screenshot_path", None)
            if sp and Path(sp).exists():
                ad["screenshot_bytes"] = Path(sp).read_bytes()

        return data

    except Exception as e:
        return _error_result(f"Failed to launch Facebook subprocess: {e}")
    finally:
        result_file.unlink(missing_ok=True)
        # Clean up screenshot temp files
        import shutil
        shutil.rmtree(screenshot_dir, ignore_errors=True)


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
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
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

            # Navigate directly to keyword search results URL
            # (bypasses fragile dropdown / autocomplete UI interaction)
            _status(progress_cb, f"Searching Facebook Ad Library for '{search_term}'...")
            search_url = (
                f"{AD_LIBRARY_URL}"
                f"?active_status=active&ad_type=all&country=US"
                f"&q={quote_plus(search_term)}&search_type=keyword_unordered"
            )
            _dbg(f"Navigating to: {search_url}")
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(6)
            _dbg(f"Page loaded, URL: {page.url}")

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
    container that includes the full ad (body, images, carousel) —
    typically ~7 levels up, where 'Sponsored' text appears.
    Cards with large carousels can be up to ~5000 chars.
    """
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
                        if (text.includes('Sponsored') && text.length > 200 && text.length < 6000) {
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


def _extract_ad_from_card(card, index: int) -> Optional[dict]:
    """Extract ad information from a single card element.

    Captures: page name, body text, start date, library ID, platforms,
    creative image URLs (not the profile pic), and carousel slide data
    (headline, description, CTA, destination link).
    """
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
                idx = lines.index(line)
                if idx > 0:
                    page_name = lines[idx - 1]

        # Find platforms line
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
            "Platforms", "Categories", "Open Dropdown", "Shop Now",
            "Learn More", "Sign Up", "Download", "Book Now",
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

        # ----- Extract creative images (not profile pic) -----
        # Profile pics: class contains "_8nqq", rendered ~32x32
        # Creative images: rendered ~175x175, no alt text
        creative_images = _extract_creative_images(card)

        # ----- Extract carousel slides (headline, description, CTA, link) -----
        carousel_slides = _extract_carousel_slides(card)

        return {
            "page_name": page_name,
            "paid_for_by": paid_for_by,
            "body": body,
            "start_date": start_date,
            "library_id": library_id,
            "platforms": platforms,
            "image_url": creative_images[0] if creative_images else None,
            "creative_images": creative_images,
            "carousel_cards": carousel_slides,
            "screenshot_bytes": None,
        }
    except Exception as e:
        logger.debug("Card extraction %d failed: %s", index, e)
        return None


def _extract_creative_images(card) -> list:
    """Extract ad creative image URLs from a card, filtering out profile pics.

    Profile pics: class contains '_8nqq', natural size ~60x60
    Creative images: natural size ~600x600, served from scontent/fbcdn
    """
    try:
        img_data = card.evaluate('''(el) => {
            const imgs = el.querySelectorAll('img');
            const creatives = [];
            for (const img of imgs) {
                const isProfilePic = (
                    img.className.includes('_8nqq') ||
                    img.naturalWidth <= 80 ||
                    (img.getBoundingClientRect().width <= 50 && img.alt)
                );
                if (!isProfilePic && img.src && (
                    img.src.includes('scontent') || img.src.includes('fbcdn')
                )) {
                    creatives.push(img.src);
                }
            }
            return creatives;
        }''')
        return img_data or []
    except Exception:
        return []


def _extract_carousel_slides(card) -> list:
    """Extract carousel slide data from <a> links in the card.

    Each carousel slide is an <a> with text like:
        DOMAIN.COM
        Headline Text
        Description Text
        Shop Now

    Returns list of {headline, description, cta, link_url}.
    """
    try:
        slides = card.evaluate('''(el) => {
            const links = el.querySelectorAll('a[href]');
            const ctas = ['Shop Now', 'Learn More', 'Sign Up', 'Download',
                          'Book Now', 'Contact Us', 'Get Offer', 'Apply Now',
                          'Watch More', 'See Menu', 'Get Quote'];
            const slides = [];
            for (const link of links) {
                const text = link.innerText.trim();
                const lines = text.split('\\n').map(l => l.trim()).filter(l => l);
                // A carousel slide link typically has 3-5 lines:
                // domain, headline, description, CTA
                const hasCTA = lines.some(l => ctas.includes(l));
                if (hasCTA && lines.length >= 2) {
                    const ctaLine = lines.find(l => ctas.includes(l));
                    const nonCTA = lines.filter(l => !ctas.includes(l));
                    // First line is often the domain, skip it if it looks like a URL
                    let headline = null;
                    let description = null;
                    for (const line of nonCTA) {
                        if (line.includes('.COM') || line.includes('.com') ||
                            line.includes('.NET') || line.includes('.ORG') ||
                            line.length < 4) continue;
                        if (!headline) {
                            headline = line;
                        } else if (!description) {
                            description = line;
                        }
                    }
                    if (headline) {
                        slides.push({
                            headline: headline,
                            description: description,
                            cta: ctaLine || null,
                            link_url: link.href || null,
                        });
                    }
                }
            }
            return slides;
        }''')
        return slides or []
    except Exception:
        return []


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
