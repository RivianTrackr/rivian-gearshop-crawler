"""
Social media posting primitives for X/Twitter, Bluesky, and Threads.

Each ``post_to_*`` function takes a per-platform config dict plus a message and
RAISES ``RuntimeError`` on failure so callers can route the call through the
shared notification retry queue (which retries any callable that raises). On
success it returns a short identifier string (post id / URI) for logging.

These are intentionally text-only ("text + link" posts). Product images are not
uploaded — the platforms generate their own link-preview cards from the URL.

Auth models:
- Bluesky : AT Protocol app password -> session JWT -> createRecord
- X       : OAuth 1.0a user context (consumer key/secret + access token/secret)
- Threads : Meta Graph long-lived token, two-step container -> publish
"""

import logging

import requests

logger = logging.getLogger("crawler.social")

# Posting character ceiling. X is the tightest at 280; we clamp every message to
# this so the same text is safe to fan out to all three platforms unchanged.
MAX_POST_CHARS = 280


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def clamp_message(text: str, link: str = "", limit: int = MAX_POST_CHARS) -> str:
    """Trim ``text`` so the whole post fits within ``limit`` characters.

    If a ``link`` is present it is kept intact (links are the payload) and the
    body before it is shortened with an ellipsis. Without a link the whole
    string is truncated.
    """
    if len(text) <= limit:
        return text

    if link and link in text:
        prefix = text[: text.rindex(link)].rstrip()
        # room for the link, a separating space and a trailing ellipsis char
        budget = limit - len(link) - 2
        if budget < 1:
            # Link alone already blows the budget; nothing sensible to do.
            return text
        trimmed = prefix[: budget - 1].rstrip() + "…"
        return f"{trimmed} {link}"

    return text[: limit - 1].rstrip() + "…"


def _utf8_span(text: str, substring: str):
    """Return (byte_start, byte_end) of ``substring`` within ``text`` as UTF-8.

    Bluesky richtext facets index by UTF-8 byte offset, not character offset.
    Returns ``None`` if the substring is absent.
    """
    idx = text.find(substring)
    if idx < 0:
        return None
    byte_start = len(text[:idx].encode("utf-8"))
    byte_end = byte_start + len(substring.encode("utf-8"))
    return byte_start, byte_end


# --------------------------------------------------------------------------- #
# Bluesky (AT Protocol)
# --------------------------------------------------------------------------- #

def post_to_bluesky(cfg: dict, text: str, link: str = "", timeout: int = 20) -> str:
    """Post ``text`` to Bluesky. Adds a clickable richtext facet for ``link``.

    cfg keys: ``handle`` (e.g. ``riviantrackr.bsky.social``), ``app_password``,
    optional ``service`` (defaults to the public PDS at bsky.social).
    """
    handle = (cfg.get("handle") or "").strip()
    app_password = (cfg.get("app_password") or "").strip()
    service = (cfg.get("service") or "https://bsky.social").rstrip("/")

    if not handle or not app_password:
        raise RuntimeError("Bluesky handle and app password are required")

    # 1) Create a session to obtain an access JWT.
    sess = requests.post(
        f"{service}/xrpc/com.atproto.server.createSession",
        json={"identifier": handle, "password": app_password},
        timeout=timeout,
    )
    if sess.status_code >= 300:
        raise RuntimeError(f"Bluesky auth failed: {sess.status_code} {sess.text[:200]}")
    sess_data = sess.json()
    jwt = sess_data.get("accessJwt")
    did = sess_data.get("did")
    if not jwt or not did:
        raise RuntimeError("Bluesky auth response missing accessJwt/did")

    # 2) Build the post record, with a link facet if we have one.
    record = {
        "$type": "app.bsky.feed.post",
        "text": text,
        "createdAt": _bsky_now(),
    }
    if link:
        span = _utf8_span(text, link)
        if span:
            record["facets"] = [{
                "index": {"byteStart": span[0], "byteEnd": span[1]},
                "features": [{
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": link,
                }],
            }]

    resp = requests.post(
        f"{service}/xrpc/com.atproto.repo.createRecord",
        headers={"Authorization": f"Bearer {jwt}"},
        json={"repo": did, "collection": "app.bsky.feed.post", "record": record},
        timeout=timeout,
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"Bluesky post failed: {resp.status_code} {resp.text[:200]}")
    uri = resp.json().get("uri", "")
    logger.info("Posted to Bluesky: %s", uri)
    return uri


def _bsky_now() -> str:
    # Imported lazily-style to keep the module's import side-effect-free of time.
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# --------------------------------------------------------------------------- #
# X / Twitter (API v2, OAuth 1.0a user context)
# --------------------------------------------------------------------------- #

def post_to_x(cfg: dict, text: str, link: str = "", timeout: int = 20) -> str:
    """Post ``text`` to X via POST /2/tweets using OAuth 1.0a user context.

    cfg keys: ``api_key``, ``api_secret``, ``access_token``, ``access_secret``.
    ``link`` is accepted for a uniform poster signature but is left inline in the
    text; X auto-wraps URLs as t.co and renders a preview card.
    """
    api_key = (cfg.get("api_key") or "").strip()
    api_secret = (cfg.get("api_secret") or "").strip()
    access_token = (cfg.get("access_token") or "").strip()
    access_secret = (cfg.get("access_secret") or "").strip()

    if not all([api_key, api_secret, access_token, access_secret]):
        raise RuntimeError("X requires api_key, api_secret, access_token and access_secret")

    try:
        from requests_oauthlib import OAuth1
    except ImportError as e:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "requests-oauthlib is required for X posting (pip install requests-oauthlib)"
        ) from e

    auth = OAuth1(api_key, api_secret, access_token, access_secret)
    resp = requests.post(
        "https://api.twitter.com/2/tweets",
        auth=auth,
        json={"text": text},
        timeout=timeout,
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"X post failed: {resp.status_code} {resp.text[:200]}")
    tweet_id = (resp.json().get("data") or {}).get("id", "")
    logger.info("Posted to X: %s", tweet_id)
    return tweet_id


# --------------------------------------------------------------------------- #
# Threads (Meta Graph API)
# --------------------------------------------------------------------------- #

def post_to_threads(cfg: dict, text: str, link: str = "", timeout: int = 20) -> str:
    """Post ``text`` to Threads via the two-step container -> publish flow.

    cfg keys: ``user_id`` (Threads user id), ``access_token`` (long-lived).
    The ``link`` is kept inline; Threads auto-links it and builds a preview.
    """
    user_id = (cfg.get("user_id") or "").strip()
    access_token = (cfg.get("access_token") or "").strip()
    base = (cfg.get("api_base") or "https://graph.threads.net/v1.0").rstrip("/")

    if not user_id or not access_token:
        raise RuntimeError("Threads requires user_id and access_token")

    # 1) Create a media container for a TEXT post.
    create = requests.post(
        f"{base}/{user_id}/threads",
        data={"media_type": "TEXT", "text": text, "access_token": access_token},
        timeout=timeout,
    )
    if create.status_code >= 300:
        raise RuntimeError(f"Threads container failed: {create.status_code} {create.text[:200]}")
    creation_id = create.json().get("id")
    if not creation_id:
        raise RuntimeError("Threads container response missing id")

    # 2) Publish the container.
    publish = requests.post(
        f"{base}/{user_id}/threads_publish",
        data={"creation_id": creation_id, "access_token": access_token},
        timeout=timeout,
    )
    if publish.status_code >= 300:
        raise RuntimeError(f"Threads publish failed: {publish.status_code} {publish.text[:200]}")
    post_id = publish.json().get("id", "")
    logger.info("Posted to Threads: %s", post_id)
    return post_id


# Dispatch table for callers that want to post to a platform by name.
POSTERS = {
    "bluesky": post_to_bluesky,
    "x": post_to_x,
    "threads": post_to_threads,
}
