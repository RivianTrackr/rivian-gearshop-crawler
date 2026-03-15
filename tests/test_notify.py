"""Tests for notify.py — retry queue and error alert channel."""

import time
from unittest.mock import patch, MagicMock, call

import pytest

from notify import RetryQueue, send_error_alert


class TestRetryQueue:
    def test_enqueue_and_count(self):
        rq = RetryQueue(max_retries=2, delays=[0, 0])
        assert rq.pending_count == 0

        rq.enqueue("test", lambda: None)
        assert rq.pending_count == 1

    def test_flush_success_on_first_retry(self):
        rq = RetryQueue(max_retries=3, delays=[0, 0, 0])
        func = MagicMock()
        rq.enqueue("test", func, args=("arg1",))

        failed = rq.flush()

        assert len(failed) == 0
        func.assert_called_once_with("arg1")
        assert rq.pending_count == 0

    def test_flush_success_on_second_retry(self):
        rq = RetryQueue(max_retries=3, delays=[0, 0, 0])
        func = MagicMock(side_effect=[Exception("fail"), None])
        rq.enqueue("test", func)

        failed = rq.flush()

        assert len(failed) == 0
        assert func.call_count == 2

    def test_flush_permanent_failure(self):
        rq = RetryQueue(max_retries=2, delays=[0, 0])
        func = MagicMock(side_effect=Exception("always fails"))
        rq.enqueue("test", func)

        failed = rq.flush()

        assert len(failed) == 1
        assert failed[0]["label"] == "test"
        assert func.call_count == 2

    def test_flush_with_kwargs(self):
        rq = RetryQueue(max_retries=1, delays=[0])
        func = MagicMock()
        rq.enqueue("test", func, args=("a",), kwargs={"key": "val"})

        rq.flush()

        func.assert_called_once_with("a", key="val")

    def test_flush_empties_queue(self):
        rq = RetryQueue(max_retries=1, delays=[0])
        rq.enqueue("test", MagicMock())

        rq.flush()
        assert rq.pending_count == 0

    def test_multiple_items(self):
        rq = RetryQueue(max_retries=1, delays=[0])
        f1 = MagicMock()
        f2 = MagicMock(side_effect=Exception("fail"))

        rq.enqueue("success", f1)
        rq.enqueue("failure", f2)

        failed = rq.flush()

        assert len(failed) == 1
        assert failed[0]["label"] == "failure"
        f1.assert_called_once()


class TestSendErrorAlert:
    @patch("notify.requests.post")
    def test_discord_error_alert(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        send_error_alert(
            "Test Error",
            "Something went wrong",
            details="traceback here",
            discord_webhook_url="https://discord.com/api/webhooks/test",
            discord_config={"username": "TestBot"},
        )

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert payload["username"] == "TestBot"
        assert payload["embeds"][0]["title"] == "Crawler Error: Test Error"
        assert payload["embeds"][0]["color"] == 0xDC2626

    @patch("notify.requests.post")
    def test_email_error_alert(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        send_error_alert(
            "Test Error",
            "Something went wrong",
            brevo_api_key="test-key",
            email_from="Test <test@example.com>",
            email_to=["admin@example.com"],
        )

        # Should be called for email
        assert mock_post.called

    @patch("notify.requests.post")
    def test_no_channels_configured(self, mock_post):
        """Should not crash when no notification channels are configured."""
        send_error_alert(
            "Test Error",
            "Something went wrong",
        )
        # No discord or email configured, should not post
        mock_post.assert_not_called()

    @patch("notify.requests.post")
    def test_discord_with_mentions(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        send_error_alert(
            "Test Error",
            "Error message",
            discord_webhook_url="https://discord.com/api/webhooks/test",
            discord_config={
                "username": "TestBot",
                "mention_role_id": "12345",
                "mention_user_id": "67890",
            },
        )

        payload = mock_post.call_args.kwargs.get("json") or mock_post.call_args[1].get("json")
        assert "<@&12345>" in payload["content"]
        assert "<@67890>" in payload["content"]

    @patch("notify.requests.post")
    def test_discord_thread_support(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        send_error_alert(
            "Test Error",
            "Error message",
            discord_webhook_url="https://discord.com/api/webhooks/test",
            discord_config={"thread_id": "99999"},
        )

        url_called = mock_post.call_args.args[0] if mock_post.call_args.args else mock_post.call_args.kwargs.get("url", "")
        # The URL is passed as first positional arg to requests.post
        assert "thread_id=99999" in str(mock_post.call_args)
