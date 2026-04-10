"""Tests for support_crawler.py — content helpers, diff generation, and DB operations."""

import sqlite3

import pytest

from support_migrations import run_migrations
from support_crawler import (
    normalize_text,
    compute_content_hash,
    slug_from_url,
    category_from_referrer,
    generate_text_diff,
    generate_html_diff,
    build_changes_email,
)


@pytest.fixture
def support_db(tmp_path):
    """Create a temporary support database with full schema."""
    db_path = str(tmp_path / "support_test.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    yield conn
    conn.close()


class TestNormalizeText:
    def test_basic(self):
        assert normalize_text("  hello   world  ") == "hello world"

    def test_newlines(self):
        assert normalize_text("hello\n\n  world\t\tfoo") == "hello world foo"

    def test_empty(self):
        assert normalize_text("") == ""

    def test_already_normalized(self):
        assert normalize_text("hello world") == "hello world"


class TestComputeContentHash:
    def test_deterministic(self):
        h1 = compute_content_hash("hello world")
        h2 = compute_content_hash("hello world")
        assert h1 == h2

    def test_whitespace_invariant(self):
        h1 = compute_content_hash("hello   world")
        h2 = compute_content_hash("hello world")
        assert h1 == h2

    def test_different_content(self):
        h1 = compute_content_hash("hello")
        h2 = compute_content_hash("world")
        assert h1 != h2

    def test_returns_hex_string(self):
        h = compute_content_hash("test")
        assert len(h) == 64  # SHA-256 hex
        assert all(c in "0123456789abcdef" for c in h)


class TestSlugFromUrl:
    def test_standard_url(self):
        assert slug_from_url("https://rivian.com/support/article/my-article") == "my-article"

    def test_trailing_slash(self):
        assert slug_from_url("https://rivian.com/support/article/my-article/") == "my-article"

    def test_query_params(self):
        assert slug_from_url("https://rivian.com/support/article/my-article?ref=foo") == "my-article"

    def test_hash_fragment(self):
        assert slug_from_url("https://rivian.com/support/article/my-article#section") == "my-article"

    def test_no_article_path(self):
        result = slug_from_url("https://rivian.com/support/charging")
        assert result == "charging"


class TestCategoryFromReferrer:
    def test_category_url(self):
        assert category_from_referrer("https://rivian.com/support/charging") == "charging"

    def test_nested_category(self):
        assert category_from_referrer("https://rivian.com/support/vehicles") == "vehicles"

    def test_empty(self):
        assert category_from_referrer("") == ""

    def test_trailing_slash(self):
        assert category_from_referrer("https://rivian.com/support/charging/") == "charging"


class TestGenerateTextDiff:
    def test_identical(self):
        assert generate_text_diff("hello\nworld", "hello\nworld") == ""

    def test_addition(self):
        diff = generate_text_diff("hello", "hello\nworld")
        assert "+world" in diff

    def test_removal(self):
        diff = generate_text_diff("hello\nworld", "hello")
        assert "-world" in diff

    def test_modification(self):
        diff = generate_text_diff("hello\nworld", "hello\nearth")
        assert "-world" in diff
        assert "+earth" in diff


class TestGenerateHtmlDiff:
    def test_identical(self):
        result = generate_html_diff("same", "same")
        assert "No visible text differences" in result

    def test_addition_green(self):
        result = generate_html_diff("old line", "old line\nnew line")
        assert "background:#d4edda" in result
        assert "new line" in result

    def test_removal_red(self):
        result = generate_html_diff("old line\nremoved", "old line")
        assert "background:#f8d7da" in result
        assert "removed" in result

    def test_html_escaping(self):
        result = generate_html_diff("old", "<script>alert(1)</script>")
        assert "<script>" not in result
        assert "&lt;script&gt;" in result


class TestBuildChangesEmail:
    def test_initial_scan(self):
        changes = {"new": [
            {"url": "https://rivian.com/support/article/test", "title": "Test", "category": "charging"},
        ], "removed": [], "title_changed": [], "body_changed": [], "url_changed": []}
        html = build_changes_email(changes, is_initial=True, article_count=1)
        assert "Initial Scan" in html
        assert "Test" in html
        assert "charging" in html

    def test_new_articles(self):
        changes = {"new": [
            {"url": "https://rivian.com/support/article/new", "title": "New Article", "category": ""},
        ], "removed": [], "title_changed": [], "body_changed": [], "url_changed": []}
        html = build_changes_email(changes)
        assert "New Articles" in html
        assert "New Article" in html

    def test_removed_articles(self):
        changes = {"new": [], "removed": [
            {"title": "Old Article", "slug": "old-article"},
        ], "title_changed": [], "body_changed": [], "url_changed": []}
        html = build_changes_email(changes)
        assert "Removed Articles" in html
        assert "Old Article" in html

    def test_body_changes(self):
        changes = {"new": [], "removed": [], "title_changed": [], "url_changed": [],
                    "body_changed": [{
                        "slug": "test",
                        "url": "https://rivian.com/support/article/test",
                        "title": "Test Article",
                        "diff_html": '<div style="background:#d4edda">+ new content</div>',
                    }]}
        html = build_changes_email(changes)
        assert "Content Changes" in html
        assert "new content" in html

    def test_title_changes(self):
        changes = {"new": [], "removed": [], "body_changed": [], "url_changed": [],
                    "title_changed": [{
                        "slug": "test",
                        "url": "https://rivian.com/support/article/test",
                        "old_title": "Old Title",
                        "new_title": "New Title",
                    }]}
        html = build_changes_email(changes)
        assert "Title Changes" in html
        assert "Old Title" in html
        assert "New Title" in html

    def test_html_escaping_in_email(self):
        changes = {"new": [
            {"url": "https://example.com", "title": "<b>XSS</b>", "category": ""},
        ], "removed": [], "title_changed": [], "body_changed": [], "url_changed": []}
        html = build_changes_email(changes)
        assert "<b>XSS</b>" not in html
        assert "&lt;b&gt;XSS&lt;/b&gt;" in html


class TestDatabaseOperations:
    def test_insert_article(self, support_db):
        support_db.execute(
            """INSERT INTO support_articles
               (slug, url, title, body_text, body_hash, category,
                first_seen_at, last_seen_at, updated_at, removed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
            ("test-article", "https://rivian.com/support/article/test-article",
             "Test Article", "Body text here", "abc123", "charging",
             "2026-01-01T00:00:00", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        support_db.commit()

        row = support_db.execute(
            "SELECT * FROM support_articles WHERE slug = ?", ("test-article",)
        ).fetchone()
        assert row is not None
        assert row["title"] == "Test Article"
        assert row["category"] == "charging"

    def test_insert_snapshot(self, support_db):
        support_db.execute(
            """INSERT INTO support_articles
               (slug, url, title, body_text, body_hash, category,
                first_seen_at, last_seen_at, updated_at, removed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
            ("test", "https://rivian.com/support/article/test",
             "Test", "Body", "hash1", "",
             "2026-01-01T00:00:00", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        support_db.commit()
        article_id = support_db.execute("SELECT id FROM support_articles WHERE slug='test'").fetchone()["id"]

        support_db.execute(
            """INSERT INTO article_snapshots
               (article_id, crawled_at, title, body_text, body_hash, url)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (article_id, "2026-01-01T00:00:00", "Test", "Body", "hash1",
             "https://rivian.com/support/article/test"),
        )
        support_db.commit()

        snaps = support_db.execute(
            "SELECT * FROM article_snapshots WHERE article_id = ?", (article_id,)
        ).fetchall()
        assert len(snaps) == 1

    def test_crawl_markers(self, support_db):
        support_db.execute(
            """INSERT INTO support_articles
               (slug, url, title, body_text, body_hash, category,
                first_seen_at, last_seen_at, updated_at, removed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
            ("test", "https://rivian.com/support/article/test",
             "Test", "Body", "hash1", "",
             "2026-01-01T00:00:00", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        support_db.commit()
        article_id = support_db.execute("SELECT id FROM support_articles WHERE slug='test'").fetchone()["id"]

        support_db.execute(
            "INSERT INTO support_crawl_markers (crawled_at, article_id) VALUES (?, ?)",
            ("2026-01-01T00:00:00", article_id),
        )
        support_db.commit()

        row = support_db.execute(
            "SELECT * FROM support_crawl_markers WHERE article_id = ?", (article_id,)
        ).fetchone()
        assert row is not None

    def test_crawl_stats(self, support_db):
        support_db.execute(
            "INSERT INTO support_crawl_stats (run_at, article_count) VALUES (?, ?)",
            ("2026-01-01T00:00:00", 42),
        )
        support_db.commit()

        row = support_db.execute(
            "SELECT article_count FROM support_crawl_stats ORDER BY run_at DESC LIMIT 1"
        ).fetchone()
        assert row["article_count"] == 42

    def test_crawl_runs(self, support_db):
        support_db.execute(
            """INSERT INTO support_crawl_runs
               (started_at, finished_at, status, article_count, new_articles,
                removed_articles, title_changes, body_changes, url_changes,
                duration_seconds)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("2026-01-01T00:00:00", "2026-01-01T00:01:00", "success",
             100, 5, 2, 1, 3, 0, 60.0),
        )
        support_db.commit()

        row = support_db.execute(
            "SELECT * FROM support_crawl_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row["status"] == "success"
        assert row["article_count"] == 100
        assert row["new_articles"] == 5
