"""Website technology scanner — combines webtech with Playwright JS tag detection.

webtech catches header-level fingerprints (CMS, server, frameworks).
Playwright catches JS-loaded tags that only appear after full page execution
(ad pixels, analytics, tag managers, chat widgets, marketing automation).

Results are categorized by BDR relevance:
  - Ad Platforms (Google Ads, Facebook Pixel, Bing Ads, etc.)
  - Analytics (GA4, Adobe Analytics, Hotjar, etc.)
  - Tag Management (GTM, Tealium, Adobe Launch, etc.)
  - Marketing / CRM (HubSpot, Mailchimp, Salesforce, etc.)
  - Chat / Support (Intercom, Drift, Zendesk, etc.)
  - CMS / E-commerce (WordPress, Shopify, etc.)
  - Agency Indicators (ReachLocal, Dealer Spike, etc.)
  - Other Tech (server, CDN, frameworks)
"""

import json
import logging
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Category definitions: pattern -> {name, category, bdr_note}
# Each pattern is checked against script src URLs, inline JS, and network
# request URLs captured by Playwright.
# ---------------------------------------------------------------------------

AD_TECH_SIGNATURES = [
    # --- Ad Platforms ---
    {"pattern": "googleads.g.doubleclick.net", "name": "Google Ads", "category": "Ad Platforms",
     "bdr_note": "Actively running Google Ads campaigns"},
    {"pattern": "google_conversion", "name": "Google Ads Conversion", "category": "Ad Platforms",
     "bdr_note": "Tracking Google Ads conversions"},
    {"pattern": "googlesyndication.com", "name": "Google AdSense", "category": "Ad Platforms",
     "bdr_note": "Running Google display ads on site"},
    {"pattern": "connect.facebook.net/en_US/fbevents", "name": "Facebook Pixel", "category": "Ad Platforms",
     "bdr_note": "Facebook/Meta advertising active"},
    {"pattern": "fbevents.js", "name": "Facebook Pixel", "category": "Ad Platforms",
     "bdr_note": "Facebook/Meta advertising active"},
    {"pattern": "bat.bing.com", "name": "Microsoft/Bing Ads", "category": "Ad Platforms",
     "bdr_note": "Running Bing/Microsoft Ads campaigns"},
    {"pattern": "snap.licdn.com/li.lms-analytics", "name": "LinkedIn Insight Tag", "category": "Ad Platforms",
     "bdr_note": "LinkedIn advertising active"},
    {"pattern": "linkedin.com/px", "name": "LinkedIn Pixel", "category": "Ad Platforms",
     "bdr_note": "LinkedIn advertising active"},
    {"pattern": "analytics.tiktok.com", "name": "TikTok Pixel", "category": "Ad Platforms",
     "bdr_note": "TikTok advertising active"},
    {"pattern": "ads.pinterest.com", "name": "Pinterest Tag", "category": "Ad Platforms",
     "bdr_note": "Pinterest advertising active"},
    {"pattern": "ads-twitter.com", "name": "X/Twitter Ads", "category": "Ad Platforms",
     "bdr_note": "X/Twitter advertising active"},
    {"pattern": "amazon-adsystem.com", "name": "Amazon Ads", "category": "Ad Platforms",
     "bdr_note": "Amazon advertising active"},
    {"pattern": "ad.doubleclick.net", "name": "Google DV360/Campaign Manager", "category": "Ad Platforms",
     "bdr_note": "Using Google programmatic display (DV360)"},
    {"pattern": "pixel.tapad.com", "name": "Tapad", "category": "Ad Platforms",
     "bdr_note": "Cross-device ad targeting active"},
    {"pattern": "adsrvr.org", "name": "The Trade Desk", "category": "Ad Platforms",
     "bdr_note": "Using The Trade Desk programmatic ads"},
    {"pattern": "criteo.com", "name": "Criteo", "category": "Ad Platforms",
     "bdr_note": "Criteo retargeting active"},
    {"pattern": "taboola.com", "name": "Taboola", "category": "Ad Platforms",
     "bdr_note": "Running Taboola native ads"},
    {"pattern": "outbrain.com", "name": "Outbrain", "category": "Ad Platforms",
     "bdr_note": "Running Outbrain native ads"},

    # --- Analytics ---
    {"pattern": "google-analytics.com/analytics.js", "name": "Google Analytics (UA)", "category": "Analytics",
     "bdr_note": "Legacy Universal Analytics — may need GA4 migration"},
    {"pattern": "googletagmanager.com/gtag", "name": "Google Analytics 4 (GA4)", "category": "Analytics",
     "bdr_note": "GA4 analytics active"},
    {"pattern": "analytics.google.com", "name": "Google Analytics", "category": "Analytics",
     "bdr_note": "Google Analytics active"},
    {"pattern": "hotjar.com", "name": "Hotjar", "category": "Analytics",
     "bdr_note": "Using Hotjar for heatmaps/session recording"},
    {"pattern": "clarity.ms", "name": "Microsoft Clarity", "category": "Analytics",
     "bdr_note": "Using Microsoft Clarity for behavior analytics"},
    {"pattern": "fullstory.com", "name": "FullStory", "category": "Analytics",
     "bdr_note": "Using FullStory for session replay"},
    {"pattern": "mouseflow.com", "name": "Mouseflow", "category": "Analytics",
     "bdr_note": "Session recording/analytics active"},
    {"pattern": "crazyegg.com", "name": "Crazy Egg", "category": "Analytics",
     "bdr_note": "Heatmap/click tracking active"},
    {"pattern": "assets.adobedtm.com", "name": "Adobe Analytics", "category": "Analytics",
     "bdr_note": "Enterprise-level Adobe Analytics"},
    {"pattern": "cdn.segment.com", "name": "Segment", "category": "Analytics",
     "bdr_note": "Using Segment CDP for data routing"},
    {"pattern": "plausible.io", "name": "Plausible", "category": "Analytics",
     "bdr_note": "Privacy-focused analytics"},
    {"pattern": "mixpanel.com", "name": "Mixpanel", "category": "Analytics",
     "bdr_note": "Product analytics active"},

    # --- Tag Management ---
    {"pattern": "googletagmanager.com/gtm.js", "name": "Google Tag Manager", "category": "Tag Management",
     "bdr_note": "GTM manages their tracking tags"},
    {"pattern": "tags.tiqcdn.com", "name": "Tealium", "category": "Tag Management",
     "bdr_note": "Tealium tag management — check for agency tags"},
    {"pattern": "cdn.optimizely.com", "name": "Optimizely", "category": "Tag Management",
     "bdr_note": "A/B testing active"},
    {"pattern": "assets.adobedtm.com/launch", "name": "Adobe Launch", "category": "Tag Management",
     "bdr_note": "Adobe tag management"},

    # --- Marketing / CRM ---
    {"pattern": "js.hs-scripts.com", "name": "HubSpot", "category": "Marketing / CRM",
     "bdr_note": "HubSpot marketing automation active"},
    {"pattern": "js.hsforms.net", "name": "HubSpot Forms", "category": "Marketing / CRM",
     "bdr_note": "Using HubSpot for lead capture"},
    {"pattern": "mktdpcdn.com", "name": "Marketo", "category": "Marketing / CRM",
     "bdr_note": "Marketo marketing automation (enterprise)"},
    {"pattern": "pardot.com", "name": "Salesforce Pardot", "category": "Marketing / CRM",
     "bdr_note": "Salesforce Pardot B2B marketing"},
    {"pattern": "mc.yandex.ru", "name": "Yandex Metrica", "category": "Marketing / CRM",
     "bdr_note": "Yandex analytics (international focus)"},
    {"pattern": "cdn.callrail.com", "name": "CallRail", "category": "Marketing / CRM",
     "bdr_note": "Call tracking active — measuring phone leads"},
    {"pattern": "calltrackingmetrics.com", "name": "CallTrackingMetrics", "category": "Marketing / CRM",
     "bdr_note": "Call tracking active"},
    {"pattern": "invoca.net", "name": "Invoca", "category": "Marketing / CRM",
     "bdr_note": "Invoca call intelligence"},
    {"pattern": "marchex.io", "name": "Marchex", "category": "Marketing / CRM",
     "bdr_note": "Marchex call analytics"},
    {"pattern": "chimpstatic.com", "name": "Mailchimp", "category": "Marketing / CRM",
     "bdr_note": "Using Mailchimp for email marketing"},
    {"pattern": "klaviyo.com", "name": "Klaviyo", "category": "Marketing / CRM",
     "bdr_note": "Klaviyo email/SMS marketing (e-commerce focused)"},
    {"pattern": "activecampaign.com", "name": "ActiveCampaign", "category": "Marketing / CRM",
     "bdr_note": "ActiveCampaign marketing automation"},
    {"pattern": "convertkit.com", "name": "ConvertKit", "category": "Marketing / CRM",
     "bdr_note": "ConvertKit email marketing"},
    {"pattern": "birdeye.com", "name": "Birdeye", "category": "Marketing / CRM",
     "bdr_note": "Birdeye review/reputation management"},
    {"pattern": "yext.com", "name": "Yext", "category": "Marketing / CRM",
     "bdr_note": "Yext listings/reputation management"},
    {"pattern": "podium.com", "name": "Podium", "category": "Marketing / CRM",
     "bdr_note": "Podium messaging/reviews platform"},

    # --- Chat / Support ---
    {"pattern": "widget.intercom.io", "name": "Intercom", "category": "Chat / Support",
     "bdr_note": "Intercom live chat/support active"},
    {"pattern": "js.driftt.com", "name": "Drift", "category": "Chat / Support",
     "bdr_note": "Drift conversational marketing"},
    {"pattern": "static.zdassets.com", "name": "Zendesk", "category": "Chat / Support",
     "bdr_note": "Zendesk support active"},
    {"pattern": "cdn.livechatinc.com", "name": "LiveChat", "category": "Chat / Support",
     "bdr_note": "LiveChat widget active"},
    {"pattern": "embed.tawk.to", "name": "Tawk.to", "category": "Chat / Support",
     "bdr_note": "Free live chat — may want upgrade"},
    {"pattern": "wchat.freshchat.com", "name": "Freshchat", "category": "Chat / Support",
     "bdr_note": "Freshworks chat active"},
    {"pattern": "tidio.co", "name": "Tidio", "category": "Chat / Support",
     "bdr_note": "Tidio chatbot active"},

    # --- Agency Indicators ---
    {"pattern": "capture-api.reachlocalservices.com", "name": "ReachLocal/LocaliQ", "category": "Agency Indicators",
     "bdr_note": "Currently or previously a LocaliQ/Gannett client"},
    {"pattern": "reachlocal", "name": "ReachLocal/LocaliQ", "category": "Agency Indicators",
     "bdr_note": "Currently or previously a LocaliQ/Gannett client"},
    {"pattern": "dealerspike", "name": "Dealer Spike", "category": "Agency Indicators",
     "bdr_note": "Website/marketing managed by Dealer Spike"},
    {"pattern": "dealer.com", "name": "Dealer.com", "category": "Agency Indicators",
     "bdr_note": "Website managed by Dealer.com (Cox Automotive)"},
    {"pattern": "dealerinspire.com", "name": "Dealer Inspire", "category": "Agency Indicators",
     "bdr_note": "Website managed by Dealer Inspire (Cars.com)"},
    {"pattern": "dealeron.com", "name": "DealerOn", "category": "Agency Indicators",
     "bdr_note": "Website managed by DealerOn"},
    {"pattern": "nakedlime.com", "name": "Naked Lime", "category": "Agency Indicators",
     "bdr_note": "Marketing managed by Naked Lime (Reynolds & Reynolds)"},
    {"pattern": "digital-air-strike", "name": "Digital Air Strike", "category": "Agency Indicators",
     "bdr_note": "Social/reputation managed by Digital Air Strike"},
    {"pattern": "wpromote.com", "name": "Wpromote", "category": "Agency Indicators",
     "bdr_note": "Marketing managed by Wpromote agency"},
    {"pattern": "webfx.com", "name": "WebFX", "category": "Agency Indicators",
     "bdr_note": "Marketing managed by WebFX agency"},
]


# Priority order for display: most BDR-relevant first
CATEGORY_ORDER = [
    "Ad Platforms",
    "Agency Indicators",
    "Analytics",
    "Tag Management",
    "Marketing / CRM",
    "Chat / Support",
    "CMS / E-commerce",
    "Other Tech",
]


def scan_website_tech(
    url: str,
    progress_cb=None,
) -> dict:
    """Scan a website for technologies. Returns categorized results.

    Uses webtech for header fingerprinting + Playwright subprocess for
    JS-loaded tags.

    Returns:
        {
            "url": str,
            "techs": [{"name", "category", "version", "bdr_note"}],
            "by_category": {"Ad Platforms": [...], ...},
            "error": str | None,
        }
    """
    _status(progress_cb, f"Scanning technologies for {url}...")

    if not url.startswith("http"):
        url = f"https://{url}"

    all_techs = {}  # name -> {name, category, version, bdr_note}

    # Phase 1: webtech header scan (fast, no browser)
    _status(progress_cb, "Running header fingerprint scan...")
    try:
        from webtech import WebTech
        wt = WebTech(options={"json": True})
        report = wt.start_from_url(url)
        for item in report.get("tech", []):
            name = item.get("name", "Unknown")
            if name not in all_techs:
                all_techs[name] = {
                    "name": name,
                    "category": _categorize_webtech(name),
                    "version": item.get("version"),
                    "bdr_note": None,
                    "source": "webtech",
                }
    except Exception as e:
        logger.warning("webtech scan failed: %s", e)

    # Phase 2: Playwright JS tag scan (subprocess for Streamlit compat)
    _status(progress_cb, "Scanning for ad tags and marketing pixels...")
    js_techs = _scan_js_tags_subprocess(url, progress_cb)
    for tech in js_techs:
        name = tech["name"]
        if name not in all_techs:
            all_techs[name] = tech
        else:
            # Merge: JS scan may have better category/note
            if tech.get("bdr_note") and not all_techs[name].get("bdr_note"):
                all_techs[name]["bdr_note"] = tech["bdr_note"]
            if tech.get("category") != "Other Tech":
                all_techs[name]["category"] = tech["category"]

    # Build categorized output
    techs = sorted(all_techs.values(), key=lambda t: (
        CATEGORY_ORDER.index(t["category"]) if t["category"] in CATEGORY_ORDER else 99,
        t["name"],
    ))

    by_category = {}
    for t in techs:
        cat = t["category"]
        by_category.setdefault(cat, []).append(t)

    return {
        "url": url,
        "techs": techs,
        "by_category": by_category,
        "error": None,
    }


def _scan_js_tags_subprocess(url: str, progress_cb) -> list:
    """Run Playwright in a subprocess to detect JS-loaded tags."""
    result_file = Path(tempfile.mktemp(suffix=".json", prefix="tech_scan_"))

    script = (
        f"import sys, json\n"
        f"from pathlib import Path\n"
        f"sys.path.insert(0, {str(Path(__file__).resolve().parent.parent)!r})\n"
        f"from core.tech_scanner import _playwright_tag_scan\n"
        f"r = _playwright_tag_scan({url!r})\n"
        f"Path({str(result_file)!r}).write_text(json.dumps(r, default=str), encoding='utf-8')\n"
    )

    try:
        proc = subprocess.Popen(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        elapsed = 0
        while proc.poll() is None and elapsed < 45:
            time.sleep(2)
            elapsed += 2
            if progress_cb and elapsed % 6 == 0:
                _status(progress_cb, f"Scanning JS tags ({elapsed}s)...")

        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
            return []

        if not result_file.exists():
            stderr = proc.stderr.read().decode("utf-8", errors="replace")[:300]
            logger.warning("Tech scan subprocess failed: %s", stderr)
            return []

        return json.loads(result_file.read_text(encoding="utf-8"))

    except Exception as e:
        logger.warning("Tech scan subprocess error: %s", e)
        return []
    finally:
        result_file.unlink(missing_ok=True)


def _playwright_tag_scan(url: str) -> list:
    """Actual Playwright scan — runs in subprocess.

    Loads the page, waits for JS execution, then checks:
    1. All script src URLs
    2. Network requests made during page load
    3. Inline script content
    """
    from playwright.sync_api import sync_playwright

    found = {}  # name -> tech dict
    network_urls = []

    with sync_playwright() as p:
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
        )
        page = ctx.new_page()

        # Capture network requests
        def on_request(request):
            network_urls.append(request.url)

        page.on("request", on_request)

        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
        except Exception:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=15000)
            except Exception:
                browser.close()
                return []

        time.sleep(3)

        # Collect all script src URLs
        script_srcs = page.evaluate('''() => {
            return Array.from(document.querySelectorAll('script[src]'))
                .map(s => s.src);
        }''')

        # Collect inline script content (first 500 chars each, up to 50 scripts)
        inline_scripts = page.evaluate('''() => {
            return Array.from(document.querySelectorAll('script:not([src])'))
                .slice(0, 50)
                .map(s => s.textContent.substring(0, 500));
        }''')

        # Also check meta tags and link tags
        meta_content = page.evaluate('''() => {
            const metas = Array.from(document.querySelectorAll('meta[name], meta[property], meta[content]'));
            return metas.map(m => (m.name || '') + ' ' + (m.content || '')).join(' ');
        }''')

        browser.close()

    # Match all collected URLs against signatures
    all_urls = script_srcs + network_urls
    all_text = " ".join(all_urls) + " " + " ".join(inline_scripts) + " " + meta_content

    for sig in AD_TECH_SIGNATURES:
        pattern = sig["pattern"].lower()
        if pattern in all_text.lower():
            name = sig["name"]
            if name not in found:
                found[name] = {
                    "name": name,
                    "category": sig["category"],
                    "version": None,
                    "bdr_note": sig["bdr_note"],
                    "source": "playwright",
                }

    # Check for GTM container IDs
    for script in inline_scripts:
        import re
        gtm_ids = re.findall(r'GTM-[A-Z0-9]+', script)
        if gtm_ids and "Google Tag Manager" in found:
            found["Google Tag Manager"]["version"] = ", ".join(set(gtm_ids))
        ga_ids = re.findall(r'G-[A-Z0-9]+', script)
        if ga_ids and "Google Analytics 4 (GA4)" in found:
            found["Google Analytics 4 (GA4)"]["version"] = ", ".join(set(ga_ids))
        fb_ids = re.findall(r"fbq\('init',\s*'(\d+)'", script)
        if fb_ids and "Facebook Pixel" in found:
            found["Facebook Pixel"]["version"] = ", ".join(set(fb_ids))

    # Check for Tealium account
    for u in all_urls:
        if "tiqcdn.com" in u:
            import re
            m = re.search(r'/utag/([^/]+/[^/]+)/', u)
            if m and "Tealium" in found:
                found["Tealium"]["version"] = m.group(1)

    return list(found.values())


def _categorize_webtech(name: str) -> str:
    """Categorize a webtech-detected technology for BDR relevance."""
    name_lower = name.lower()

    cms = ["wordpress", "drupal", "joomla", "wix", "squarespace", "webflow",
           "shopify", "magento", "bigcommerce", "woocommerce", "prestashop",
           "ghost", "contentful", "strapi", "sanity"]
    if any(c in name_lower for c in cms):
        return "CMS / E-commerce"

    analytics = ["google analytics", "adobe analytics", "matomo", "plausible",
                 "mixpanel", "amplitude", "heap"]
    if any(a in name_lower for a in analytics):
        return "Analytics"

    servers = ["nginx", "apache", "iis", "asp.net", "php", "node.js",
               "cloudflare", "amazon", "akamai", "fastly", "varnish",
               "hsts", "http/2", "http/3"]
    if any(s in name_lower for s in servers):
        return "Other Tech"

    fonts_media = ["font", "typekit", "youtube", "vimeo", "bootstrap",
                   "jquery", "react", "angular", "vue"]
    if any(f in name_lower for f in fonts_media):
        return "Other Tech"

    return "Other Tech"


def _status(cb, msg: str):
    if cb:
        cb(msg)
    logger.info(msg)
