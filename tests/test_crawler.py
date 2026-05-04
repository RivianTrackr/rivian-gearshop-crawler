"""Tests for crawler.py — core crawl logic, helpers, and change detection."""

import sqlite3
from unittest.mock import patch, MagicMock

import pytest

# We need to patch env vars before importing crawler
import os
os.environ.setdefault("DB_PATH", ":memory:")
os.environ.setdefault("BREVO_API_KEY", "")
os.environ.setdefault("DISCORD_WEBHOOK_URL", "")

import crawler


class TestCents:
    """Test the cents() price normalization function."""

    def test_none(self):
        assert crawler.cents(None) is None

    def test_integer_string(self):
        assert crawler.cents("12500") == 12500

    def test_integer(self):
        assert crawler.cents(12500) == 12500

    def test_float_string_dollars(self):
        # "125.00" treated as float → * 100 = 12500
        assert crawler.cents("125.00") == 12500

    def test_float_value(self):
        # cents() tries int() first; int(125.0) == 125, which is already cents
        assert crawler.cents(125.0) == 125

    def test_invalid(self):
        assert crawler.cents("not-a-number") is None

    def test_zero(self):
        assert crawler.cents(0) == 0

    def test_zero_string(self):
        assert crawler.cents("0") == 0


class TestHandleFromProductUrl:
    """Test URL → product handle extraction."""

    def test_full_url(self):
        assert crawler.handle_from_product_url(
            "https://gearshop.rivian.com/products/adventure-jacket"
        ) == "adventure-jacket"

    def test_url_with_query(self):
        # urlparse strips query from path, so handle won't include query params
        assert crawler.handle_from_product_url(
            "https://gearshop.rivian.com/products/adventure-jacket?variant=123"
        ) == "adventure-jacket"

    def test_url_with_trailing_slash(self):
        assert crawler.handle_from_product_url(
            "https://gearshop.rivian.com/products/adventure-jacket/"
        ) == "adventure-jacket"

    def test_no_products_path(self):
        assert crawler.handle_from_product_url("https://gearshop.rivian.com/collections/all") is None

    def test_relative_path(self):
        assert crawler.handle_from_product_url("/products/my-product") == "my-product"


class TestRenderMoney:
    """Test money formatting."""

    def test_none(self):
        assert crawler.render_money(None) == "\u2014"

    def test_whole_dollars(self):
        assert crawler.render_money(10000) == "$100.00"

    def test_with_cents(self):
        assert crawler.render_money(12599) == "$125.99"

    def test_zero(self):
        assert crawler.render_money(0) == "$0.00"

    def test_large(self):
        assert crawler.render_money(123456789) == "$1,234,567.89"


class TestNowUtcIso:
    def test_returns_iso_string(self):
        result = crawler.now_utc_iso()
        assert "T" in result
        assert "+" in result or "Z" in result


class TestDatabaseHelpers:
    """Test SQLite helper functions."""

    def test_has_any_snapshot_empty(self, crawler_db):
        assert crawler.has_any_snapshot(crawler_db) is False

    def test_has_any_snapshot_with_data(self, crawler_db):
        crawler_db.execute(
            "INSERT INTO products (product_id, handle) VALUES (1, 'test')"
        )
        crawler_db.execute(
            "INSERT INTO variants (variant_id, product_id) VALUES (1, 1)"
        )
        crawler_db.execute(
            "INSERT INTO snapshots (crawled_at, product_id, variant_id, price_cents, available) VALUES ('2024-01-01', 1, 1, 100, 1)"
        )
        crawler_db.commit()
        assert crawler.has_any_snapshot(crawler_db) is True

    def test_last_product_count_empty(self, crawler_db):
        assert crawler.last_product_count(crawler_db) is None

    def test_last_product_count_with_data(self, crawler_db):
        crawler_db.execute(
            "INSERT INTO crawl_stats (run_at, product_count) VALUES ('2024-01-01', 42)"
        )
        crawler_db.commit()
        assert crawler.last_product_count(crawler_db) == 42

    def test_latest_snapshot_for_variant_none(self, crawler_db):
        assert crawler.latest_snapshot_for_variant(crawler_db, 999) is None

    def test_latest_snapshot_for_variant(self, crawler_db):
        crawler_db.execute(
            "INSERT INTO products (product_id, handle) VALUES (1, 'test')"
        )
        crawler_db.execute(
            "INSERT INTO variants (variant_id, product_id) VALUES (1, 1)"
        )
        crawler_db.execute(
            "INSERT INTO snapshots (crawled_at, product_id, variant_id, price_cents, available) VALUES ('2024-01-01', 1, 1, 100, 1)"
        )
        crawler_db.execute(
            "INSERT INTO snapshots (crawled_at, product_id, variant_id, price_cents, available) VALUES ('2024-01-02', 1, 1, 200, 0)"
        )
        crawler_db.commit()
        snap = crawler.latest_snapshot_for_variant(crawler_db, 1)
        assert snap["price_cents"] == 200
        assert snap["available"] == 0

    def test_recent_snapshots_for_variant_none(self, crawler_db):
        assert crawler.recent_snapshots_for_variant(crawler_db, 999) == []

    def test_recent_snapshots_for_variant_returns_two_newest(self, crawler_db):
        crawler_db.execute(
            "INSERT INTO products (product_id, handle) VALUES (1, 'test')"
        )
        crawler_db.execute(
            "INSERT INTO variants (variant_id, product_id) VALUES (1, 1)"
        )
        for crawled_at, avail in [
            ("2024-01-01", 1),
            ("2024-01-02", 0),
            ("2024-01-03", 1),
        ]:
            crawler_db.execute(
                "INSERT INTO snapshots (crawled_at, product_id, variant_id, price_cents, available) VALUES (?, 1, 1, 100, ?)",
                (crawled_at, avail),
            )
        crawler_db.commit()
        rows = crawler.recent_snapshots_for_variant(crawler_db, 1, limit=2)
        assert len(rows) == 2
        assert rows[0]["available"] == 1  # newest
        assert rows[1]["available"] == 0  # second-newest


def _walk(seq, debounce_runs=3):
    """Helper: walk a chronological sequence (oldest → newest) through the
    rule and return a list of (index, emitted_change) tuples for runs that
    fired an email. The crawler-side caller passes recent_avails newest-first,
    so for index i we slice seq[i-1::-1] to get [prev, prev2, prev3, ...].
    """
    emits = []
    for i in range(len(seq)):
        recent = list(reversed(seq[:i]))  # newest → oldest, prior to i
        change = crawler.availability_change_to_report(seq[i], recent, debounce_runs=debounce_runs)
        if change:
            emits.append((i, change))
    return emits


class TestAvailabilityChangeToReport:
    """The debounce rule: emit only when the new value has held for
    `debounce_runs` runs AND the prior value also held for `debounce_runs`
    runs AND the two windows differ. Default is 3 runs on each side.
    """

    def test_returns_none_when_history_too_short(self):
        # Need 2*debounce_runs - 1 = 5 prior values for default debounce_runs=3.
        assert crawler.availability_change_to_report(1, []) is None
        assert crawler.availability_change_to_report(1, [1]) is None
        assert crawler.availability_change_to_report(0, [0, 1, 1, 1]) is None  # only 4 prior

    def test_returns_none_when_history_has_none(self):
        # Padding with None (young variant with gaps) yields no emit.
        assert crawler.availability_change_to_report(0, [0, 1, 1, 1, None]) is None

    def test_stable_returns_none(self):
        assert crawler.availability_change_to_report(1, [1, 1, 1, 1, 1]) is None
        assert crawler.availability_change_to_report(0, [0, 0, 0, 0, 0]) is None

    def test_confirmed_yes_to_no_transition(self):
        # Chronological: 1,1,1,0,0,0. Args (newest first): current=0, prev[0..4] = 0,0,1,1,1.
        assert (
            crawler.availability_change_to_report(0, [0, 0, 1, 1, 1])
            == "Availability Yes → No"
        )

    def test_confirmed_no_to_yes_transition(self):
        # Chronological: 0,0,0,1,1,1. Args: current=1, prev[0..4] = 1,1,0,0,0.
        assert (
            crawler.availability_change_to_report(1, [1, 1, 0, 0, 0])
            == "Availability No → Yes"
        )

    def test_single_run_blip_does_not_emit(self):
        # Chronological: 0,0,0,1,0,0. The "1" is a single-run blip on a stable 0.
        # Args: current=0, prev = [0, 1, 0, 0, 0].
        assert crawler.availability_change_to_report(0, [0, 1, 0, 0, 0]) is None

    def test_two_run_blip_does_not_emit(self):
        # The exact pattern from production AP000166-GRA-005: stable 0, then 1,1
        # blip, then back to 0. Old 2-run rule fired here (the bug from #58);
        # 3-run rule must absorb.
        # Chronological: 0,0,0,1,1,0,0. Args: current=0, prev = [0, 1, 1, 0, 0, 0]
        # — but we only need 5 prior; the function ignores the rest.
        assert crawler.availability_change_to_report(0, [0, 1, 1, 0, 0]) is None

    def test_two_run_blip_inverse_does_not_emit(self):
        # Stable 1, then 0,0 blip, back to 1.
        # Chronological: 1,1,1,0,0,1,1. Args: current=1, prev = [1, 0, 0, 1, 1].
        assert crawler.availability_change_to_report(1, [1, 0, 0, 1, 1]) is None

    def test_change_just_observed_waits_for_confirmation(self):
        # Chronological: 1,1,1,1,1,0. Args: current=0, prev = [1, 1, 1, 1, 1].
        # Need 3 runs of 0 before emitting; we only have 1.
        assert crawler.availability_change_to_report(0, [1, 1, 1, 1, 1]) is None

    def test_change_two_runs_in_waits_for_third(self):
        # Chronological: 1,1,1,1,0,0. Args: current=0, prev = [0, 1, 1, 1, 1].
        # Old #58 rule fired here; 3-run rule waits for one more confirmation.
        assert crawler.availability_change_to_report(0, [0, 1, 1, 1, 1]) is None

    def test_oscillating_data_does_not_emit(self):
        # Pure flap. Args: current=1, prev = [0, 1, 0, 1, 0].
        assert crawler.availability_change_to_report(1, [0, 1, 0, 1, 0]) is None

    def test_production_pattern_gra_005_recent_blip_no_false_email(self):
        """The 19:05 UTC May 4 email reported AP000166-GRA-005 as Yes → No.
        The variant had been stable at 0 for ~24 hours, then a 2-run blip
        to 1, then back to 0 — the bug from PR #58.

        Chronological for the recent window (oldest → newest):
        """
        # 8 runs at 0, then 1,1 blip, then 2 runs at 0. With the 3-run rule
        # the blip can't satisfy "old held 3 runs", so no emit on either side.
        seq = [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0]
        assert _walk(seq) == []

    def test_full_gra_005_history_only_emits_on_real_transition(self):
        """Walking the full 30-snapshot GRA-005 history asserts the rule
        emits exactly once — at the genuine 1,1,1→0,0,0 transition mid-May 3.
        All single-run and 2-run blips (4 of them across the window) are
        absorbed."""
        seq = [
            1, 1, 1, 1, 0, 1, 0, 0, 0, 1,
            0, 0, 1, 1, 1, 0, 0, 0, 1, 1,
            0, 0, 0, 0, 0, 0, 1, 1, 0, 0,
        ]
        emits = _walk(seq)
        assert emits == [(17, "Availability Yes → No")], f"Got {emits}"

    def test_production_pattern_blk_006_no_false_emails(self):
        """AP000711-BLK-006: stable 0 with single-run 1 blips. Zero emits."""
        seq = [0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0]
        assert _walk(seq) == []

    def test_clean_transition_emits_once(self):
        """A clean 1→0 transition with old value held ≥3 runs and new held ≥3 runs.

        Chronological: 1,1,1,1,1,1,0,0,0 — emit at index 8 (the third 0).
        """
        seq = [1, 1, 1, 1, 1, 1, 0, 0, 0]
        assert _walk(seq) == [(8, "Availability Yes → No")]

    def test_two_run_window_setting_emits_two_run_blip(self):
        """Sanity check: with debounce_runs=2 (the buggy #58 behavior), the
        2-run blip pattern DOES fire. This pins the difference and prevents
        a future regression that silently widens or narrows the window."""
        # Args: current=0, prev = [0, 1, 1, 0, 0]. Need 3 prior for runs=2.
        assert (
            crawler.availability_change_to_report(0, [0, 1, 1, 0, 0], debounce_runs=2)
            == "Availability Yes → No"
        )


class TestHeartbeat:
    def test_heartbeat_disabled(self, crawler_db):
        with patch.object(crawler, 'HEARTBEAT_UTC_HOUR', -1):
            assert crawler.should_send_heartbeat(crawler_db) is False

    def test_heartbeat_sent_today_false(self, crawler_db):
        assert crawler.heartbeat_sent_today(crawler_db) is False

    def test_mark_heartbeat_sent(self, crawler_db):
        crawler.mark_heartbeat_sent(crawler_db)
        assert crawler.heartbeat_sent_today(crawler_db) is True


class TestBuildEmailFixed:
    """Test email HTML generation."""

    def test_initial_email(self):
        html = crawler.build_email_fixed(
            is_initial=True, diffs=[], new_products=[], removed_products=[],
            initial_rows=["<tr><td>Test</td></tr>"]
        )
        assert "Initial Catalog" in html
        assert "first run" in html.lower()

    def test_changes_email_with_new(self):
        new_products = [{"url": "https://example.com/p1", "title": "New Product", "handle": "new-product", "vendor": "Rivian"}]
        html = crawler.build_email_fixed(
            is_initial=False, diffs=[], new_products=new_products, removed_products=[]
        )
        assert "New products" in html
        assert "New Product" in html

    def test_changes_email_with_removed(self):
        removed = [{"title": "Old Product", "handle": "old-product"}]
        html = crawler.build_email_fixed(
            is_initial=False, diffs=[], new_products=[], removed_products=removed
        )
        assert "Removed products" in html
        assert "Old Product" in html

    def test_no_changes_email(self):
        html = crawler.build_email_fixed(
            is_initial=False, diffs=[], new_products=[], removed_products=[]
        )
        assert "No changes detected" in html

    def test_variant_changes_email(self):
        diffs = [{
            "url": "https://example.com/p1",
            "variant_url": "https://example.com/p1?variant=1",
            "product_title": "Test Product",
            "variant_title": "Small",
            "sku": "TP-SM",
            "new_price": 12500,
            "new_compare_at": None,
            "new_available": 1,
            "change_desc": "Price $100.00 → $125.00",
        }]
        html = crawler.build_email_fixed(
            is_initial=False, diffs=diffs, new_products=[], removed_products=[]
        )
        assert "Test Product" in html
        assert "Price" in html


class TestDiscordHelpers:
    def test_discord_hex_color_valid(self):
        assert crawler._discord_hex_color("#FBA919") == 0xFBA919

    def test_discord_hex_color_no_hash(self):
        assert crawler._discord_hex_color("FBA919") == 0xFBA919

    def test_discord_hex_color_invalid(self):
        assert crawler._discord_hex_color("not-a-color") == 0xFBA919

    def test_discord_mention_disabled(self):
        with patch.dict(crawler.DISCORD_CONFIG, {"mention_on_new": False}):
            assert crawler._discord_mention("new") == ""

    def test_discord_mention_with_role(self):
        with patch.dict(crawler.DISCORD_CONFIG, {
            "mention_on_new": True,
            "mention_role_id": "12345",
            "mention_user_id": "",
        }):
            assert crawler._discord_mention("new") == "<@&12345>"


class TestConfirmProductRemoved:
    @patch("crawler.requests")
    def test_404_means_removed(self, mock_requests):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_requests.get.return_value = mock_resp
        assert crawler.confirm_product_removed("test-handle") is True

    @patch("crawler.requests")
    def test_200_means_exists(self, mock_requests):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.ok = True
        mock_requests.get.return_value = mock_resp
        assert crawler.confirm_product_removed("test-handle") is False

    @patch("crawler.requests")
    def test_exception_returns_none(self, mock_requests):
        mock_requests.get.side_effect = Exception("network error")
        assert crawler.confirm_product_removed("test-handle") is None
