"""Tests for offers_crawler debounce logic."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import offers_crawler


def _walk(seq, debounce_runs=3):
    """Walk a chronological sequence (oldest -> newest) through the debounce
    rule and return (index, old_hash) tuples for runs that would emit.
    For index i, we slice seq[i-1::-1] to get [prev, prev2, ...] newest-first.
    """
    emits = []
    for i in range(len(seq)):
        recent = list(reversed(seq[:i]))
        result = offers_crawler.body_change_confirmed(
            seq[i], recent, debounce_runs=debounce_runs
        )
        if result is not None:
            emits.append((i, result))
    return emits


class TestBodyChangeConfirmed:
    """3-run debounce: emit only when the new hash has held for 3 runs AND
    the prior hash also held for 3 runs AND the two differ. Absorbs blips
    shorter than 3 runs on either side of a stable value.
    """

    def test_returns_none_when_history_too_short(self):
        # Need 2*debounce_runs - 1 = 5 prior values for default debounce_runs=3.
        assert offers_crawler.body_change_confirmed("b", []) is None
        assert offers_crawler.body_change_confirmed("b", ["a"]) is None
        assert (
            offers_crawler.body_change_confirmed("b", ["b", "a", "a", "a"]) is None
        )  # only 4 prior

    def test_returns_none_when_history_has_none(self):
        # Padding with None (pre-migration rows or fresh offer) yields no emit.
        assert (
            offers_crawler.body_change_confirmed("b", ["b", "a", "a", "a", None])
            is None
        )

    def test_stable_returns_none(self):
        assert (
            offers_crawler.body_change_confirmed("a", ["a", "a", "a", "a", "a"])
            is None
        )

    def test_confirmed_transition_returns_old_hash(self):
        # Chronological: a,a,a,b,b,b. Args: current=b, recent=[b,b,a,a,a].
        assert (
            offers_crawler.body_change_confirmed("b", ["b", "b", "a", "a", "a"])
            == "a"
        )

    def test_single_run_blip_does_not_emit(self):
        # Chronological: a,a,a,b,a,a (one-run b blip).
        # Args: current=a, recent=[a, b, a, a, a].
        assert (
            offers_crawler.body_change_confirmed("a", ["a", "b", "a", "a", "a"])
            is None
        )

    def test_two_run_blip_does_not_emit(self):
        # Chronological: a,a,a,b,b,a. Two-run blip absorbed.
        # Args: current=a, recent=[b, b, a, a, a].
        assert (
            offers_crawler.body_change_confirmed("a", ["b", "b", "a", "a", "a"])
            is None
        )

    def test_change_just_observed_waits_for_confirmation(self):
        # Chronological: a,a,a,a,a,b. Only 1 run of b — wait for 2 more.
        # Args: current=b, recent=[a, a, a, a, a].
        assert (
            offers_crawler.body_change_confirmed("b", ["a", "a", "a", "a", "a"])
            is None
        )

    def test_change_two_runs_in_waits_for_third(self):
        # Chronological: a,a,a,a,b,b. Args: current=b, recent=[b, a, a, a, a].
        assert (
            offers_crawler.body_change_confirmed("b", ["b", "a", "a", "a", "a"])
            is None
        )

    def test_oscillating_data_does_not_emit(self):
        # Pure 1-run flap. Args: current=b, recent=[a, b, a, b, a].
        assert (
            offers_crawler.body_change_confirmed("b", ["a", "b", "a", "b", "a"])
            is None
        )

    def test_observed_flap_from_screenshots_no_emit(self):
        """Walk the pattern from the 8:32/9:32 emails: stable A, then a 1-run
        flap to B, back to A, repeatedly. Zero emits expected.
        """
        # A = body without "Valid until", B = body with "Valid until"
        seq = ["a", "a", "a", "a", "a", "b", "a", "b", "a", "b", "a"]
        assert _walk(seq) == []

    def test_clean_transition_emits_once(self):
        """A clean a -> b transition where both sides hold >= 3 runs."""
        seq = ["a", "a", "a", "a", "a", "a", "b", "b", "b"]
        emits = _walk(seq)
        assert emits == [(8, "a")], f"Got {emits}"

    def test_flap_followed_by_stable_old_is_absorbed(self):
        """User scenario: flap A,B,A,B then back to A stable. Zero emits."""
        seq = ["a", "a", "a", "a", "a", "b", "a", "b", "a", "a", "a", "a"]
        assert _walk(seq) == []

    def test_does_not_emit_when_new_equals_old(self):
        """Edge case: hash matches both windows -> not a transition."""
        assert (
            offers_crawler.body_change_confirmed("a", ["a", "a", "a", "a", "a"])
            is None
        )

    def test_debounce_runs_2_is_more_permissive(self):
        """Regression-pin: under debounce_runs=2 (old behavior), a single-run
        blip would emit. This test pins the difference so we can verify the
        3-run rule is what's active by default.
        """
        # current=a, recent=[b, a, a]. Under debounce=2, new=[a,b]? no wait,
        # debounce=2 needs 2*2-1=3 prior. new_window=[a, b], old_window=[a]?
        # Actually with debounce_runs=2: needed=3, new_window=[current,recent[0]]
        # = [a, b], not stable. So this particular sequence doesn't trip it.
        # Use the classic blip case from PR #58.
        # Chronological: a,a,a,b,a. current=a, recent=[b,a,a,a] (4 prior, but
        # only 3 are needed). new_window=[a, b], old_window=[a]. Not stable on new.
        # So debounce=2 also absorbs this. The point of the test is to ensure
        # the function honors the debounce_runs parameter.
        assert (
            offers_crawler.body_change_confirmed(
                "b", ["b", "a", "a"], debounce_runs=2
            )
            == "a"
        )
        # Same args under debounce_runs=3 require 5 prior — too short.
        assert (
            offers_crawler.body_change_confirmed(
                "b", ["b", "a", "a"], debounce_runs=3
            )
            is None
        )


class TestRecentBodyHashesForOffer:
    """The DB helper that backs the debounce."""

    def _setup_db(self, tmp_path):
        import sqlite3

        from offers_migrations import run_migrations

        db_path = str(tmp_path / "offers_test.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        run_migrations(conn)
        return conn

    def test_empty_history(self, tmp_path):
        conn = self._setup_db(tmp_path)
        try:
            assert offers_crawler.recent_body_hashes_for_offer(conn, 1) == []
        finally:
            conn.close()

    def test_returns_newest_first(self, tmp_path):
        conn = self._setup_db(tmp_path)
        try:
            for ts, h in [
                ("2026-05-21T08:00:00", "hash_a"),
                ("2026-05-21T09:00:00", "hash_b"),
                ("2026-05-21T10:00:00", "hash_a"),
            ]:
                conn.execute(
                    "INSERT INTO offers_crawl_markers (crawled_at, offer_id, body_hash) VALUES (?,?,?)",
                    (ts, 42, h),
                )
            conn.commit()
            hashes = offers_crawler.recent_body_hashes_for_offer(conn, 42, limit=3)
            assert hashes == ["hash_a", "hash_b", "hash_a"]
        finally:
            conn.close()

    def test_respects_limit(self, tmp_path):
        conn = self._setup_db(tmp_path)
        try:
            for i in range(10):
                conn.execute(
                    "INSERT INTO offers_crawl_markers (crawled_at, offer_id, body_hash) VALUES (?,?,?)",
                    (f"2026-05-21T{i:02d}:00:00", 7, f"hash_{i}"),
                )
            conn.commit()
            hashes = offers_crawler.recent_body_hashes_for_offer(conn, 7, limit=3)
            assert len(hashes) == 3
            assert hashes == ["hash_9", "hash_8", "hash_7"]
        finally:
            conn.close()

    def test_null_body_hash_propagates_as_none(self, tmp_path):
        """Pre-migration rows have NULL body_hash. The helper returns None
        for those, which trips the `any(h is None ...)` guard in
        body_change_confirmed and yields no emit until history rebuilds.
        """
        conn = self._setup_db(tmp_path)
        try:
            conn.execute(
                "INSERT INTO offers_crawl_markers (crawled_at, offer_id, body_hash) VALUES (?,?,?)",
                ("2026-05-21T08:00:00", 9, None),
            )
            conn.commit()
            hashes = offers_crawler.recent_body_hashes_for_offer(conn, 9, limit=1)
            assert hashes == [None]
            assert offers_crawler.body_change_confirmed("b", hashes * 5) is None
        finally:
            conn.close()
