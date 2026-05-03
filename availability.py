import json
import requests
from bs4 import BeautifulSoup

# cache + counter
_avail_cache = {}
_avail_html_checks = 0

def infer_availability_from_html(handle, variant_id, site_root, headers, log=lambda *_: None, timeout=15):
    """
    Fallback: fetch the product page with the specific variant selected and
    read availability from JSON-LD. Returns True/False/None.
    """
    global _avail_html_checks
    key = (handle, variant_id)
    if key in _avail_cache:
        return _avail_cache[key]

    _avail_html_checks += 1
    try:
        url = f"{site_root}/products/{handle}?variant={variant_id}"
        log(f"HTML availability check #{_avail_html_checks}: {url}")
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(tag.string or "")
            except Exception:
                continue
            items = data if isinstance(data, list) else [data]
            for obj in items:
                if not isinstance(obj, dict) or obj.get("@type") != "Product":
                    continue
                offers = obj.get("offers")
                offer_list = offers if isinstance(offers, list) else ([offers] if isinstance(offers, dict) else [])
                for offer in offer_list:
                    if not isinstance(offer, dict):
                        continue
                    url_field = (offer.get("url") or "")
                    avail = (offer.get("availability") or "").lower()
                    if str(variant_id) in url_field:
                        res = True if "instock" in avail else False if "outofstock" in avail else None
                        _avail_cache[key] = res
                        return res

        add_btn = soup.select_one("button[name='add'], button[type='submit'][name='add']")
        if add_btn:
            txt = (add_btn.get_text(strip=True) or "").lower()
            if "sold out" in txt or "unavailable" in txt or "notify" in txt:
                _avail_cache[key] = False
                return False

        _avail_cache[key] = None
        return None
    except Exception:
        _avail_cache[key] = None
        return None

def get_avail_html_checks():
    return _avail_html_checks

def reset_avail_state():
    """Reset cache and counter between crawler runs."""
    global _avail_cache, _avail_html_checks
    _avail_cache = {}
    _avail_html_checks = 0