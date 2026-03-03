"""Ad Presence Report Tool — Streamlit web app.

Accepts a website URL and produces a report showing what ads the brand
is running across Google and Facebook, the ad creatives, and whether
ads are managed by an agency or in-house.
"""

import json
import time
from datetime import datetime

import streamlit as st

from core.domain_extractor import extract
from core.google_scraper import scrape_google_ads
from core.facebook_scraper import scrape_facebook_ads
from core.screenshot_manager import download_ad_images
from core.agency_detector import detect_agency, analyze_all_ads
from core.tech_scanner import scan_website_tech

# ---------------------------------------------------------------------------
# Helper functions (defined before use)
# ---------------------------------------------------------------------------


def _render_agency_flag(platform: str, flag: dict):
    """Render an agency detection result."""
    if flag["is_agency"]:
        css_class = "agency-warn"
        icon = "⚠️"
        label = f"Possible agency detected ({flag['confidence']} confidence)"
    else:
        css_class = "agency-ok"
        icon = "✅"
        label = "Appears to be in-house"

    st.markdown(f"""
    <div class="{css_class}">
        <strong>{icon} {platform}: {label}</strong><br>
        {flag['reason']}
    </div>
    """, unsafe_allow_html=True)


def _serialize_result(result: dict) -> dict:
    """Make result JSON-serializable by removing bytes fields."""
    if not result:
        return result
    out = dict(result)
    cleaned_ads = []
    for ad in out.get("ads", []):
        ad_copy = {k: v for k, v in ad.items() if not isinstance(v, bytes)}
        ad_copy.pop("thumbnail_bytes", None)
        ad_copy.pop("local_image_path", None)
        cleaned_ads.append(ad_copy)
    out["ads"] = cleaned_ads
    return out


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Ad Presence Report",
    page_icon="📊",
    layout="wide",
)

st.markdown("""
<style>
    .metric-card {
        background: #f8f9fa;
        border-radius: 8px;
        padding: 16px;
        text-align: center;
        border: 1px solid #e9ecef;
    }
    .metric-value { font-size: 2rem; font-weight: 700; color: #1a73e8; }
    .metric-label { font-size: 0.85rem; color: #5f6368; margin-top: 4px; }
    .agency-warn { background: #fff3cd; border-left: 4px solid #ffc107; padding: 12px; border-radius: 4px; margin: 8px 0; }
    .agency-ok { background: #d4edda; border-left: 4px solid #28a745; padding: 12px; border-radius: 4px; margin: 8px 0; }
    .error-box { background: #f8d7da; border-left: 4px solid #dc3545; padding: 12px; border-radius: 4px; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Sidebar — inputs
# ---------------------------------------------------------------------------

st.sidebar.title("Ad Presence Report")
st.sidebar.markdown("Enter a website URL to discover active ads across Google and Facebook.")

url_input = st.sidebar.text_input(
    "Website URL or domain",
    placeholder="e.g. nike.com",
)

col_g, col_f = st.sidebar.columns(2)
max_google = col_g.slider("Max Google ads", 5, 200, 50, step=5)
max_facebook = col_f.slider("Max Facebook ads", 5, 100, 30, step=5)

run_google = st.sidebar.checkbox("Search Google Ads Transparency", value=True)
run_facebook = st.sidebar.checkbox("Search Facebook Ad Library", value=True)
run_techscan = st.sidebar.checkbox("Scan Website Tech Stack", value=True)

run_btn = st.sidebar.button("Generate Report", type="primary", use_container_width=True)

# ---------------------------------------------------------------------------
# Main area — landing page
# ---------------------------------------------------------------------------

if not run_btn or not url_input:
    st.title("Ad Presence Report")
    st.markdown(
        "Enter a website URL in the sidebar and click **Generate Report** "
        "to discover active ads across Google and Facebook."
    )
    st.markdown("""
    **What this tool does:**
    - Searches the **Google Ads Transparency Center** for ad creatives by domain
    - Searches the **Facebook Ad Library** for active ads by brand name
    - Detects whether ads are managed by an **agency** or run **in-house**
    - Downloads ad screenshots and generates a visual report
    """)
    st.stop()

# ---------------------------------------------------------------------------
# Run the report
# ---------------------------------------------------------------------------

info = extract(url_input)
domain = info["domain"]
brand = info["brand_name"]

st.title(f"Ad Presence Report: {brand}")
st.caption(f"Domain: `{domain}` | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

progress = st.progress(0)
status = st.empty()

# Collect results
google_result = None
facebook_result = None
tech_result = None
google_agency = None
facebook_agency = None

total_steps = int(run_google) + int(run_facebook) + int(run_techscan) + 1
step = 0


def update_status(msg):
    status.text(msg)


# --- Google ---
if run_google:
    step += 1
    progress.progress(step / total_steps, "Searching Google Ads Transparency...")
    google_result = scrape_google_ads(domain, max_ads=max_google, progress_cb=update_status)

    # Download images for Google ads
    if google_result["ads"]:
        update_status("Downloading Google ad images...")
        download_ad_images(google_result["ads"], domain, progress_cb=update_status)

        # Agency detection for Google
        analyze_all_ads(google_result["ads"], brand, source="google")

        # Overall agency detection from advertiser name
        google_agency = detect_agency(
            advertiser_name=google_result.get("advertiser_name"),
            brand_name=brand,
        )

# --- Facebook ---
if run_facebook:
    step += 1
    progress.progress(step / total_steps, "Searching Facebook Ad Library...")
    # Pass Google advertiser name as a search hint (if available)
    google_adv_name = google_result.get("advertiser_name") if google_result else None
    try:
        facebook_result = scrape_facebook_ads(
            brand_name=brand,
            domain=domain,
            max_ads=max_facebook,
            progress_cb=update_status,
            google_advertiser_name=google_adv_name,
        )
    except Exception as e:
        import traceback
        facebook_result = {
            "ads": [],
            "total_found": 0,
            "source": "playwright",
            "error": f"Exception: {e}\n{traceback.format_exc()}",
        }

    if facebook_result["ads"]:
        # Agency detection for Facebook ads
        analyze_all_ads(facebook_result["ads"], brand, source="facebook")

        # Overall agency detection from first ad's page/payer
        first_ad = facebook_result["ads"][0]
        facebook_agency = detect_agency(
            advertiser_name=first_ad.get("page_name"),
            brand_name=brand,
            paid_for_by=first_ad.get("paid_for_by"),
        )

# --- Tech Scan ---
if run_techscan:
    step += 1
    progress.progress(step / total_steps, "Scanning website tech stack...")
    try:
        tech_result = scan_website_tech(
            url=f"https://{domain}",
            progress_cb=update_status,
        )
    except Exception as e:
        import traceback
        tech_result = {
            "techs": [],
            "by_category": {},
            "error": f"Exception: {e}\n{traceback.format_exc()}",
        }

progress.progress(1.0, "Report complete.")
status.empty()

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Summary")

m1, m2, m3, m4, m5 = st.columns(5)

google_count = len(google_result["ads"]) if google_result else 0
facebook_count = len(facebook_result["ads"]) if facebook_result else 0
total_ads = google_count + facebook_count
tech_count = len(tech_result["techs"]) if tech_result else 0

# Count agency flags
agency_flags = 0
for src in [google_result, facebook_result]:
    if src:
        for ad in src.get("ads", []):
            if ad.get("agency_flag", {}).get("is_agency"):
                agency_flags += 1

with m1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{total_ads}</div>
        <div class="metric-label">Total Ads Found</div>
    </div>
    """, unsafe_allow_html=True)

with m2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{google_count}</div>
        <div class="metric-label">Google Ads</div>
    </div>
    """, unsafe_allow_html=True)

with m3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{facebook_count}</div>
        <div class="metric-label">Facebook Ads</div>
    </div>
    """, unsafe_allow_html=True)

with m4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{tech_count}</div>
        <div class="metric-label">Technologies</div>
    </div>
    """, unsafe_allow_html=True)

with m5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{agency_flags}</div>
        <div class="metric-label">Agency Flags</div>
    </div>
    """, unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Agency detection summary
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Agency Detection")

if google_agency:
    _render_agency_flag("Google Ads", google_agency)

if facebook_agency:
    _render_agency_flag("Facebook Ads", facebook_agency)

if not google_agency and not facebook_agency:
    st.info("No advertiser information available for agency analysis.")

# ---------------------------------------------------------------------------
# Tabbed ad views
# ---------------------------------------------------------------------------

st.markdown("---")

tab_google, tab_facebook, tab_tech = st.tabs(["Google Ads", "Facebook Ads", "Tech Stack"])

# --- Google Ads tab ---
with tab_google:
    if not run_google:
        st.info("Google Ads search was not enabled.")
    elif google_result and google_result.get("error"):
        st.markdown(
            f'<div class="error-box">Google: {google_result["error"]}</div>',
            unsafe_allow_html=True,
        )
    elif google_result and google_result["ads"]:
        st.markdown(
            f"**{google_count} ads found** "
            f"(of ~{google_result.get('total_found', google_count)} total) "
            f"via {google_result['source'].upper()}"
        )
        if google_result.get("advertiser_name"):
            st.markdown(f"**Advertiser:** {google_result['advertiser_name']}")
        if google_result.get("advertiser_id"):
            st.markdown(f"**Advertiser ID:** `{google_result['advertiser_id']}`")

        for i, ad in enumerate(google_result["ads"]):
            with st.expander(
                f"Ad {i+1}: {ad.get('title') or ad.get('creative_id', 'Unknown')} "
                f"[{ad.get('format', '?')}]"
            ):
                col_img, col_info = st.columns([1, 2])
                with col_img:
                    thumb = ad.get("thumbnail_bytes")
                    if thumb:
                        st.image(thumb, use_column_width=True)
                    elif ad.get("image_url"):
                        st.image(ad["image_url"], use_column_width=True)
                    else:
                        st.caption("No image available")

                with col_info:
                    if ad.get("title"):
                        st.markdown(f"**Title:** {ad['title']}")
                    if ad.get("body"):
                        st.markdown(f"**Body:** {ad['body']}")
                    if ad.get("format"):
                        st.markdown(f"**Format:** {ad['format']}")
                    if ad.get("last_shown"):
                        st.markdown(f"**Last shown:** {ad['last_shown']}")
                    if ad.get("advertiser_name"):
                        st.markdown(f"**Advertiser:** {ad['advertiser_name']}")

                    flag = ad.get("agency_flag")
                    if flag and flag.get("is_agency"):
                        st.warning(
                            f"Agency flag ({flag['confidence']}): {flag['reason']}"
                        )
    else:
        st.info("No Google ads found for this domain.")

# --- Facebook Ads tab ---
with tab_facebook:
    if not run_facebook:
        st.info("Facebook Ads search was not enabled.")
    elif facebook_result and facebook_result.get("error"):
        st.markdown(
            f'<div class="error-box">Facebook: {facebook_result["error"]}</div>',
            unsafe_allow_html=True,
        )
    elif facebook_result and facebook_result["ads"]:
        st.markdown(f"**{facebook_count} ads found**")

        for i, ad in enumerate(facebook_result["ads"]):
            label = ad.get("page_name") or f"Ad {i+1}"
            with st.expander(f"Ad {i+1}: {label}"):
                # --- Ad info header ---
                if ad.get("page_name"):
                    st.markdown(f"**Page:** {ad['page_name']}")
                if ad.get("paid_for_by"):
                    st.markdown(f"**Paid for by:** {ad['paid_for_by']}")
                if ad.get("body"):
                    st.markdown(f"**Body:** {ad['body']}")
                if ad.get("start_date"):
                    st.markdown(f"**Started:** {ad['start_date']}")
                if ad.get("platforms"):
                    st.markdown(f"**Platforms:** {ad['platforms']}")
                if ad.get("library_id"):
                    st.markdown(f"**Library ID:** `{ad['library_id']}`")

                flag = ad.get("agency_flag")
                if flag and flag.get("is_agency"):
                    st.warning(
                        f"Agency flag ({flag['confidence']}): {flag['reason']}"
                    )

                # --- Full card screenshot ---
                screenshot = ad.get("screenshot_bytes")
                if screenshot:
                    st.markdown("**Ad Preview (full card):**")
                    st.image(screenshot, use_column_width=True)

                # --- Creative images ---
                creatives = ad.get("creative_images", [])
                if creatives:
                    st.markdown(f"**Ad Creatives ({len(creatives)} images):**")
                    # Show up to 4 images per row
                    for row_start in range(0, len(creatives), 4):
                        row_imgs = creatives[row_start:row_start + 4]
                        cols = st.columns(len(row_imgs))
                        for col, img_url in zip(cols, row_imgs):
                            with col:
                                try:
                                    st.image(img_url, use_column_width=True)
                                except Exception:
                                    st.caption("Image failed to load")
                elif not screenshot:
                    # Fallback: single image_url
                    if ad.get("image_url"):
                        st.image(ad["image_url"], use_column_width=True)
                    else:
                        st.caption("No images available")

                # --- Carousel slides ---
                slides = ad.get("carousel_cards", [])
                if slides:
                    st.markdown(f"**Carousel Slides ({len(slides)}):**")
                    for j, slide in enumerate(slides, 1):
                        headline = slide.get("headline", "")
                        desc = slide.get("description", "")
                        cta = slide.get("cta", "")
                        link = slide.get("link_url", "")
                        parts = [f"**{j}.** {headline}"]
                        if desc:
                            parts.append(f" — {desc}")
                        if cta:
                            parts.append(f" [{cta}]")
                        st.markdown("".join(parts))
    else:
        st.info("No Facebook ads found for this brand.")

# --- Tech Stack tab ---
with tab_tech:
    if not run_techscan:
        st.info("Tech stack scan was not enabled.")
    elif tech_result and tech_result.get("error"):
        st.markdown(
            f'<div class="error-box">Tech scan: {tech_result["error"]}</div>',
            unsafe_allow_html=True,
        )
    elif tech_result and tech_result.get("techs"):
        st.markdown(f"**{tech_count} technologies detected** on `{domain}`")

        # Display by category in BDR-priority order
        category_icons = {
            "Ad Platforms": "&#128200;",       # chart
            "Agency Indicators": "&#9888;",    # warning
            "Analytics": "&#128202;",          # bar chart
            "Tag Management": "&#127991;",     # label
            "Marketing / CRM": "&#128231;",    # email
            "Chat / Support": "&#128172;",     # speech
            "CMS / E-commerce": "&#128187;",   # computer
            "Other Tech": "&#9881;",           # gear
        }
        category_colors = {
            "Ad Platforms": "#1a73e8",
            "Agency Indicators": "#ffc107",
            "Analytics": "#34a853",
            "Tag Management": "#673ab7",
            "Marketing / CRM": "#e91e63",
            "Chat / Support": "#00bcd4",
            "CMS / E-commerce": "#ff5722",
            "Other Tech": "#607d8b",
        }

        for cat in [
            "Ad Platforms", "Agency Indicators", "Analytics",
            "Tag Management", "Marketing / CRM", "Chat / Support",
            "CMS / E-commerce", "Other Tech",
        ]:
            techs_in_cat = tech_result["by_category"].get(cat, [])
            if not techs_in_cat:
                continue

            color = category_colors.get(cat, "#607d8b")
            icon = category_icons.get(cat, "&#9881;")
            st.markdown(
                f'<div style="border-left: 4px solid {color}; padding: 4px 12px; '
                f'margin: 16px 0 8px 0; font-weight: 700; font-size: 1.05rem;">'
                f'{icon} {cat} ({len(techs_in_cat)})</div>',
                unsafe_allow_html=True,
            )

            for tech in techs_in_cat:
                version_str = f" `v{tech['version']}`" if tech.get("version") else ""
                note = tech.get("bdr_note", "")
                note_str = f" — *{note}*" if note else ""
                st.markdown(f"- **{tech['name']}**{version_str}{note_str}")
    else:
        st.info("No technologies detected.")

# ---------------------------------------------------------------------------
# JSON download
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("Export")

report_data = {
    "domain": domain,
    "brand_name": brand,
    "generated_at": datetime.now().isoformat(),
    "google": _serialize_result(google_result) if google_result else None,
    "facebook": _serialize_result(facebook_result) if facebook_result else None,
    "tech_stack": tech_result if tech_result else None,
    "agency_detection": {
        "google": google_agency,
        "facebook": facebook_agency,
    },
}

st.download_button(
    label="Download Full Report (JSON)",
    data=json.dumps(report_data, indent=2, default=str),
    file_name=f"ad_report_{domain}_{datetime.now().strftime('%Y%m%d')}.json",
    mime="application/json",
    use_container_width=True,
)
