"""Download ad images and generate thumbnails for the report."""

import io
import logging
import os
import time
from pathlib import Path
from typing import Optional

import requests
from PIL import Image

logger = logging.getLogger(__name__)

THUMB_WIDTH = 300
SCREENSHOTS_DIR = Path(__file__).resolve().parent.parent / "screenshots"


def ensure_output_dir(domain: str) -> Path:
    """Create and return the screenshot directory for a domain."""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = SCREENSHOTS_DIR / f"{domain}_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def download_image(url: str, out_dir: Path, filename: str) -> Optional[Path]:
    """Download an image from a URL and save it to out_dir.

    Returns the saved file path, or None on failure.
    """
    if not url:
        return None
    try:
        resp = requests.get(url, timeout=15, stream=True, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0"
        })
        resp.raise_for_status()

        # Determine extension from content type
        ct = resp.headers.get("Content-Type", "")
        ext = ".jpg"
        if "png" in ct:
            ext = ".png"
        elif "webp" in ct:
            ext = ".webp"
        elif "gif" in ct:
            ext = ".gif"

        # Sanitize filename
        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in filename)
        filepath = out_dir / f"{safe_name}{ext}"

        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)

        return filepath

    except Exception as e:
        logger.debug("Failed to download %s: %s", url, e)
        return None


def make_thumbnail(image_path: Path, width: int = THUMB_WIDTH) -> Optional[bytes]:
    """Generate a thumbnail (as PNG bytes) from an image file.

    Returns PNG bytes suitable for display in Streamlit, or None on failure.
    """
    try:
        with Image.open(image_path) as img:
            # Calculate height to maintain aspect ratio
            ratio = width / img.width
            height = int(img.height * ratio)
            img = img.resize((width, height), Image.LANCZOS)

            buf = io.BytesIO()
            img.save(buf, format="PNG")
            return buf.getvalue()

    except Exception as e:
        logger.debug("Thumbnail failed for %s: %s", image_path, e)
        return None


def image_to_thumbnail_bytes(image_bytes: bytes, width: int = THUMB_WIDTH) -> Optional[bytes]:
    """Generate a thumbnail from raw image bytes.

    Returns PNG bytes or None.
    """
    try:
        img = Image.open(io.BytesIO(image_bytes))
        ratio = width / img.width
        height = int(img.height * ratio)
        img = img.resize((width, height), Image.LANCZOS)

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    except Exception as e:
        logger.debug("Thumbnail from bytes failed: %s", e)
        return None


def download_ad_images(ads: list, domain: str, progress_cb=None) -> dict:
    """Download images for a list of ad dicts and return a mapping of
    creative_id -> {"path": Path, "thumbnail": bytes}.

    Modifies ad dicts in-place to add "local_image_path" and "thumbnail_bytes".
    """
    if not ads:
        return {}

    out_dir = ensure_output_dir(domain)
    results = {}

    for i, ad in enumerate(ads):
        cid = ad.get("creative_id", f"ad_{i}")
        url = ad.get("image_url")

        if not url:
            continue

        if progress_cb:
            progress_cb(f"Downloading image {i + 1}/{len(ads)}...")

        filepath = download_image(url, out_dir, cid)
        if filepath:
            thumb = make_thumbnail(filepath)
            ad["local_image_path"] = str(filepath)
            ad["thumbnail_bytes"] = thumb
            results[cid] = {"path": filepath, "thumbnail": thumb}

    return results
