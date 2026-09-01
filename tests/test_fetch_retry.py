"""Tests for HTTP retry on the browser fetch path.

Context: in production `_browser_page` is set, so every Shopify fetch routes
through `fetch_via_browser`. That path had no retry at all, so a single 429 on
/products/<handle>.json raised, the caller logged a warning and `continue`d,
and the product was skipped for the whole run — that hour's price and
availability change silently never recorded. `_retry_session` only ever served
the no-browser fallback, and its status_forcelist did not include 429 either.
"""

import time

import pytest
import requests

import crawler


class FakeResponse:
    def __init__(self, status, body="{}", headers=None):
        self.status = status
        self._body = body
        self.headers = headers or {}

    def text(self):
        return self._body


class FakePage:
    """Returns each queued response in turn; raises if over-consumed."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def goto(self, url, **kwargs):
        self.calls.append(url)
        if not self._responses:
            raise AssertionError("goto called more times than responses queued")
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture(autouse=True)
def _no_sleep_and_clean_state(monkeypatch):
    """Never actually sleep, and isolate the per-run retry counters."""
    monkeypatch.setattr(time, "sleep", lambda _s: None)
    crawler._reset_retry_state()
    yield
    crawler._reset_retry_state()


# ---------------------- retry behaviour ----------------------

def test_429_is_retried_and_succeeds():
    page = FakePage([FakeResponse(429), FakeResponse(429), FakeResponse(200, '{"ok":1}')])
    status, body = crawler.fetch_via_browser(page, "https://x/products/a.json")
    assert (status, body) == (200, '{"ok":1}')
    assert len(page.calls) == 3
    assert crawler._retry_stats["retries"] == 2


@pytest.mark.parametrize("status", [500, 502, 503, 504, 520, 522])
def test_5xx_is_retried(status):
    page = FakePage([FakeResponse(status), FakeResponse(200, "ok")])
    assert crawler.fetch_via_browser(page, "https://x/a")[0] == 200
    assert len(page.calls) == 2


def test_gives_up_after_max_attempts_and_returns_last_response():
    """Callers keep the (status, body) contract — exhaustion must not raise."""
    page = FakePage([FakeResponse(429) for _ in range(4)])
    status, _ = crawler.fetch_via_browser(page, "https://x/a", max_attempts=4)
    assert status == 429
    assert len(page.calls) == 4
    assert crawler._retry_stats["gave_up"] == 1


# ---------------------- what must NOT be retried ----------------------

def test_404_is_not_retried():
    """confirm_product_removed() and check_variant_api_available() both treat
    404 as a real answer; retrying it would slow every run and could turn a
    genuine removal into an indeterminate result."""
    page = FakePage([FakeResponse(404)])
    assert crawler.fetch_via_browser(page, "https://x/a")[0] == 404
    assert len(page.calls) == 1


@pytest.mark.parametrize("status", [200, 301, 400, 403, 404, 410])
def test_non_retryable_statuses_make_exactly_one_request(status):
    page = FakePage([FakeResponse(status)])
    assert crawler.fetch_via_browser(page, "https://x/a")[0] == status
    assert len(page.calls) == 1


# ---------------------- Retry-After ----------------------

def test_retry_after_seconds_is_honoured(monkeypatch):
    seen = []
    monkeypatch.setattr(crawler.time, "sleep", lambda s: seen.append(s))
    page = FakePage([FakeResponse(429, headers={"retry-after": "7"}), FakeResponse(200)])
    crawler.fetch_via_browser(page, "https://x/a")
    assert seen == [7.0]


def test_retry_after_is_capped(monkeypatch):
    """A hostile or absurd Retry-After must not stall the whole run."""
    seen = []
    monkeypatch.setattr(crawler.time, "sleep", lambda s: seen.append(s))
    page = FakePage([FakeResponse(429, headers={"retry-after": "99999"}), FakeResponse(200)])
    crawler.fetch_via_browser(page, "https://x/a")
    assert seen == [crawler.FETCH_RETRY_AFTER_CAP]


@pytest.mark.parametrize("value,expected", [
    ("5", 5.0),
    ("0", 0.0),
    ("  12  ", 12.0),
    ("not-a-date", None),
    ("", None),
    (None, None),
])
def test_parse_retry_after(value, expected):
    assert crawler._parse_retry_after(value) == expected


def test_parse_retry_after_http_date():
    from email.utils import format_datetime
    from datetime import datetime, timezone, timedelta
    future = datetime.now(timezone.utc) + timedelta(seconds=30)
    parsed = crawler._parse_retry_after(format_datetime(future))
    assert parsed is not None and 25 <= parsed <= 31


def test_malformed_retry_after_falls_back_to_backoff(monkeypatch):
    seen = []
    monkeypatch.setattr(crawler.time, "sleep", lambda s: seen.append(s))
    page = FakePage([FakeResponse(429, headers={"retry-after": "garbage"}), FakeResponse(200)])
    crawler.fetch_via_browser(page, "https://x/a")
    assert len(seen) == 1 and seen[0] > 0


# ---------------------- budget ----------------------

def test_retry_budget_caps_total_sleep(monkeypatch):
    """A sustained rate-limit must not push the run past the unit's
    TimeoutStartSec and get it killed mid-crawl."""
    monkeypatch.setattr(crawler, "FETCH_RETRY_BUDGET_SECONDS", 5.0)
    crawler._reset_retry_state()
    slept = []
    monkeypatch.setattr(crawler.time, "sleep", lambda s: slept.append(s))

    for _ in range(20):
        page = FakePage([FakeResponse(429, headers={"retry-after": "4"}), FakeResponse(200)])
        crawler.fetch_via_browser(page, "https://x/a")

    assert sum(slept) <= 5.0
    assert crawler._retry_stats["budget_exhausted"] > 0


def test_budget_exhaustion_returns_response_rather_than_hanging(monkeypatch):
    monkeypatch.setattr(crawler, "FETCH_RETRY_BUDGET_SECONDS", 0.0)
    crawler._reset_retry_state()
    page = FakePage([FakeResponse(429)])
    status, _ = crawler.fetch_via_browser(page, "https://x/a")
    assert status == 429
    assert len(page.calls) == 1


# ---------------------- navigation errors ----------------------

def test_navigation_error_is_retried():
    page = FakePage([RuntimeError("nav timeout"), FakeResponse(200, "ok")])
    assert crawler.fetch_via_browser(page, "https://x/a")[0] == 200
    assert len(page.calls) == 2


def test_navigation_error_reraises_after_final_attempt():
    page = FakePage([RuntimeError("nav timeout")] * 3)
    with pytest.raises(RuntimeError, match="nav timeout"):
        crawler.fetch_via_browser(page, "https://x/a", max_attempts=3)


def test_none_response_is_retried_then_raises():
    page = FakePage([None, None])
    # goto returning None raises HTTPError internally; both attempts fail.
    with pytest.raises(Exception):
        crawler.fetch_via_browser(page, "https://x/a", max_attempts=2)


# ---------------------- requests fallback session ----------------------

def test_fallback_session_retries_429():
    """The no-browser path previously omitted 429 from status_forcelist."""
    session = crawler._requests_session_with_retries()
    retry = session.get_adapter("https://x/").max_retries
    assert 429 in retry.status_forcelist
    assert retry.respect_retry_after_header is True


# ---------------------- integration with fetch_product_json ----------------------

def test_fetch_product_json_survives_a_transient_429(monkeypatch):
    """End to end: the product that used to be skipped now gets recorded."""
    page = FakePage([
        FakeResponse(429),
        FakeResponse(200, '{"product": {"id": 1, "title": "Jacket", "variants": []}}'),
    ])
    monkeypatch.setattr(crawler, "_browser_page", page)
    product = crawler.fetch_product_json("jacket")
    assert product["title"] == "Jacket"
    assert len(page.calls) == 2


def test_fetch_product_json_still_raises_when_limit_persists(monkeypatch):
    page = FakePage([FakeResponse(429) for _ in range(4)])
    monkeypatch.setattr(crawler, "_browser_page", page)
    with pytest.raises(requests.HTTPError):
        crawler.fetch_product_json("jacket")
