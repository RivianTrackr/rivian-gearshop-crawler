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


class TestAvailabilityChangeToReport:
    """The debounce rule: emit only when the new value held for 2 runs AND
    the old value held for at least 2 runs AND there was a flip between them.
    Snapshot order in args is: current, prev, prev2, prev3 (newest → older).
    """

    def test_returns_none_when_history_too_short(self):
        # New variant: no prior snapshots at all.
        assert crawler.availability_change_to_report(1, None, None, None) is None
        # Only one prior snapshot.
        assert crawler.availability_change_to_report(1, 1, None, None) is None
        # Only two prior snapshots.
        assert crawler.availability_change_to_report(0, 0, 1, None) is None

    def test_stable_returns_none(self):
        # Last 4 runs all 1: nothing changed.
        assert crawler.availability_change_to_report(1, 1, 1, 1) is None
        # Last 4 runs all 0: nothing changed.
        assert crawler.availability_change_to_report(0, 0, 0, 0) is None

    def test_confirmed_yes_to_no_transition(self):
        # Pattern: 1, 1, 0, 0 (oldest → newest). Old held 2 runs, new held 2 runs.
        # Args (newest first): current=0, prev=0, prev2=1, prev3=1.
        assert (
            crawler.availability_change_to_report(0, 0, 1, 1)
            == "Availability Yes → No"
        )

    def test_confirmed_no_to_yes_transition(self):
        # Pattern: 0, 0, 1, 1 (oldest → newest).
        assert (
            crawler.availability_change_to_report(1, 1, 0, 0)
            == "Availability No → Yes"
        )

    def test_single_run_blip_returning_to_old_value_does_not_emit(self):
        # Real bug observed in production. Stable at 0, blip to 1, return to 0.
        # Pattern: 0, 1, 0, 0 (oldest → newest). Args: current=0, prev=0, prev2=1, prev3=0.
        # Old rule fired ("Yes → No"); new rule must not.
        assert crawler.availability_change_to_report(0, 0, 1, 0) is None

    def test_single_run_blip_returning_to_old_value_does_not_emit_inverse(self):
        # Stable at 1, blip to 0, return to 1. Pattern: 1, 0, 1, 1.
        # Args: current=1, prev=1, prev2=0, prev3=1.
        assert crawler.availability_change_to_report(1, 1, 0, 1) is None

    def test_change_just_observed_waits_for_confirmation(self):
        # Pattern: 1, 1, 1, 0 (oldest → newest). Change just observed; not confirmed yet.
        # Args: current=0, prev=1, prev2=1, prev3=1.
        assert crawler.availability_change_to_report(0, 1, 1, 1) is None

    def test_oscillating_data_does_not_emit(self):
        # Pattern: 0, 1, 0, 1. Pure flap, no stable old value.
        # Args: current=1, prev=0, prev2=1, prev3=0.
        assert crawler.availability_change_to_report(1, 0, 1, 0) is None

    def test_production_pattern_blk_006_no_false_emails(self):
        """Walk the real AP000711-BLK-006 sequence and assert zero emits.

        Chronological: 0,0,1,0,1,0,0,0,0,1,0,0,1,0,0,0,0,1,0,0
        At each step from index 3 onward, current = seq[i], prev = seq[i-1],
        prev2 = seq[i-2], prev3 = seq[i-3]. The variant has been stable at 0
        with single-run blips to 1 — there is no confirmed transition.
        """
        seq = [0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0]
        emits = []
        for i in range(3, len(seq)):
            change = crawler.availability_change_to_report(
                seq[i], seq[i - 1], seq[i - 2], seq[i - 3]
            )
            if change:
                emits.append((i, change))
        assert emits == [], f"Expected no emits, got {emits}"

    def test_production_pattern_genuine_transition_does_emit(self):
        """Walk a sequence with a clean transition and assert exactly one emit.

        Chronological: 1,1,1,1,1,0,0,0,0 — stable in stock, then stable out.
        Should emit "Yes → No" exactly once at index 6 (the second 0).
        """
        seq = [1, 1, 1, 1, 1, 0, 0, 0, 0]
        emits = []
        for i in range(3, len(seq)):
            change = crawler.availability_change_to_report(
                seq[i], seq[i - 1], seq[i - 2], seq[i - 3]
            )
            if change:
                emits.append((i, change))
        assert emits == [(6, "Availability Yes → No")], f"Got {emits}"


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
