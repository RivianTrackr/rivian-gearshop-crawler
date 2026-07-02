"""Tests for social.py posting primitives and crawler.send_social orchestration."""

import logging
import sqlite3
import tempfile
import os
from unittest.mock import MagicMock, patch

import pytest

import social


class TestClampMessage:
    def test_short_message_unchanged(self):
        assert social.clamp_message("hello", link="") == "hello"

    def test_long_message_keeps_link_intact(self):
        link = "https://gearshop.rivian.com/products/foo"
        text = ("New: " + "X" * 400 + " " + link)
        out = social.clamp_message(text, link=link)
        assert len(out) <= social.MAX_POST_CHARS
        assert out.endswith(link)
        assert "…" in out

    def test_long_message_without_link_truncated(self):
        out = social.clamp_message("Y" * 400)
        assert len(out) <= social.MAX_POST_CHARS
        assert out.endswith("…")


class TestUtf8Span:
    def test_ascii_offsets(self):
        t = "New: https://x.com/p"
        assert social._utf8_span(t, "https://x.com/p") == (5, 20)

    def test_multibyte_prefix_offsets(self):
        # The leading emoji is 4 UTF-8 bytes, so the link byte-start must reflect that.
        t = "🆕 New https://x.com/p"
        start, end = social._utf8_span(t, "https://x.com/p")
        assert start == len(t[: t.find("https")].encode("utf-8"))
        assert end == start + len("https://x.com/p".encode("utf-8"))

    def test_missing_substring(self):
        assert social._utf8_span("no link here", "https://x.com") is None


class TestPosters:
    def test_bluesky_requires_credentials(self):
        with pytest.raises(RuntimeError, match="handle and app password"):
            social.post_to_bluesky({}, "hi")

    def test_x_requires_all_keys(self):
        with pytest.raises(RuntimeError, match="api_key"):
            social.post_to_x({"api_key": "a"}, "hi")

    def test_threads_requires_credentials(self):
        with pytest.raises(RuntimeError, match="user_id"):
            social.post_to_threads({}, "hi")

    def test_bluesky_happy_path(self):
        sess = MagicMock(status_code=200)
        sess.json.return_value = {"accessJwt": "jwt", "did": "did:plc:x"}
        create = MagicMock(status_code=200)
        create.json.return_value = {"uri": "at://did/post/1"}
        with patch("social.requests.post", side_effect=[sess, create]) as p:
            uri = social.post_to_bluesky(
                {"handle": "h.bsky.social", "app_password": "pw"},
                "🆕 New https://gearshop.rivian.com/p",
                link="https://gearshop.rivian.com/p",
            )
        assert uri == "at://did/post/1"
        # Second call carries a richtext link facet.
        record = p.call_args_list[1].kwargs["json"]["record"]
        assert record["facets"][0]["features"][0]["uri"] == "https://gearshop.rivian.com/p"

    def test_threads_two_step_publish(self):
        create = MagicMock(status_code=200)
        create.json.return_value = {"id": "container1"}
        publish = MagicMock(status_code=200)
        publish.json.return_value = {"id": "post1"}
        with patch("social.requests.post", side_effect=[create, publish]) as p:
            pid = social.post_to_threads(
                {"user_id": "123", "access_token": "tok"}, "hello", link="")
        assert pid == "post1"
        assert p.call_args_list[0].args[0].endswith("/123/threads")
        assert p.call_args_list[1].args[0].endswith("/123/threads_publish")


class TestSendSocial:
    @pytest.fixture
    def crawler_db(self):
        import crawler
        tmp = tempfile.mktemp(suffix=".db")
        old_path = crawler.DB_PATH
        crawler.DB_PATH = tmp
        crawler.init_db()
        # Reset config to a known state.
        crawler.SOCIAL_CONFIG["bluesky"].update(enabled=False)
        crawler.SOCIAL_CONFIG["x"].update(enabled=False)
        crawler.SOCIAL_CONFIG["threads"].update(enabled=False)
        crawler.SOCIAL_CONFIG["max_posts_per_run"] = 5
        crawler.SOCIAL_CONFIG["post_new"] = True
        crawler.SOCIAL_CONFIG["post_removed"] = True
        yield crawler
        crawler.DB_PATH = old_path
        if os.path.exists(tmp):
            os.unlink(tmp)

    def test_noop_when_no_platforms(self, crawler_db):
        # Nothing enabled -> POSTERS never invoked.
        with patch.dict(social.POSTERS, {"x": MagicMock()}):
            crawler_db.send_social(new_products=[{"product_id": 1, "title": "T", "url": "u"}])
            social.POSTERS["x"].assert_not_called()

    def test_posts_and_dedupes(self, crawler_db):
        crawler_db.SOCIAL_CONFIG["x"].update(
            enabled=True, api_key="a", api_secret="b",
            access_token="c", access_secret="d")
        poster = MagicMock(return_value="id1")
        new = [{"product_id": 1, "title": "Charger", "url": "https://g/p"}]
        with patch.dict(social.POSTERS, {"x": poster}):
            crawler_db.send_social(new_products=new)
            assert poster.call_count == 1
            # Re-run: dedup suppresses the repeat.
            crawler_db.send_social(new_products=new)
            assert poster.call_count == 1

    def test_overflow_summary(self, crawler_db):
        crawler_db.SOCIAL_CONFIG["x"].update(
            enabled=True, api_key="a", api_secret="b",
            access_token="c", access_secret="d")
        crawler_db.SOCIAL_CONFIG["max_posts_per_run"] = 1
        poster = MagicMock(return_value="id1")
        new = [
            {"product_id": 1, "title": "A", "url": "https://g/a"},
            {"product_id": 2, "title": "B", "url": "https://g/b"},
            {"product_id": 3, "title": "C", "url": "https://g/c"},
        ]
        with patch.dict(social.POSTERS, {"x": poster}):
            crawler_db.send_social(new_products=new)
        # 1 individual + 1 summary = 2 posts.
        assert poster.call_count == 2
        assert "📰 Plus" in poster.call_args_list[1].args[1]

    def test_failure_enqueues_retry(self, crawler_db):
        crawler_db.SOCIAL_CONFIG["bluesky"].update(
            enabled=True, handle="h", app_password="p")
        poster = MagicMock(side_effect=RuntimeError("boom"))
        crawler_db.retry_queue._queue.clear()
        with patch.dict(social.POSTERS, {"bluesky": poster}):
            crawler_db.send_social(new_products=[{"product_id": 9, "title": "T", "url": "u"}])
        assert crawler_db.retry_queue.pending_count == 1
        crawler_db.retry_queue._queue.clear()

    def test_logs_attempt_summary(self, crawler_db, caplog):
        crawler_db.SOCIAL_CONFIG["x"].update(
            enabled=True, api_key="a", api_secret="b",
            access_token="c", access_secret="d")
        with patch.dict(social.POSTERS, {"x": MagicMock(return_value="id1")}):
            with caplog.at_level(logging.INFO, logger="crawler"):
                crawler_db.send_social(new_products=[{"product_id": 1, "title": "T", "url": "u"}])
        msgs = " ".join(r.getMessage() for r in caplog.records)
        assert "attempted" in msgs


class TestSchemaHardening:
    def test_schema_creates_social_posts_without_migrations(self):
        """Executing the hardcoded SCHEMA alone (no migrations) must create
        social_posts, so a DB with out-of-sync migration versions can't strand
        send_social() on a missing table."""
        import crawler
        con = sqlite3.connect(":memory:")
        con.executescript(crawler.SCHEMA)
        cols = [r[1] for r in con.execute("PRAGMA table_info(social_posts)")]
        assert cols == ["product_id", "change_type", "platform", "posted_at", "post_ref"]
