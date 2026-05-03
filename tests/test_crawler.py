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
