from __future__ import annotations

import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.adapters.git_state_branch import (
    GitStateBranchError,
    GitStateBranchStore,
)
from investing_monitor.adapters.sqlite_repository import SCHEMA_VERSION, SQLiteMonitorRepository
from investing_monitor.ports.runtime import TaskCheckpoint
from investing_monitor.runtime.tick import TickPlanner, TickRunner, TickTask


NOW = datetime(2026, 9, 2, 14, 31, tzinfo=timezone.utc)


def checkpoint(key: str, completed_at: datetime) -> TaskCheckpoint:
    return TaskCheckpoint(
        checkpoint_key=key,
        task_name=key.split(":", 1)[0],
        last_success_at=completed_at,
        last_attempt_at=completed_at,
    )


class TickPlannerTest(unittest.TestCase):
    def test_active_session_plans_market_and_sources(self):
        plan = TickPlanner().plan(NOW, {}, last_completed_run_at=None)

        names = [task.name for task in plan.tasks]
        self.assertEqual(
            names,
            [TickTask.MARKET, TickTask.NEWS, TickTask.SEC, TickTask.DELIVERY],
        )

    def test_recent_successes_are_not_scheduled_again(self):
        checkpoints = {
            "market": checkpoint("market", NOW - timedelta(minutes=3)),
            "news": checkpoint("news", NOW - timedelta(minutes=4)),
            "sec": checkpoint("sec", NOW - timedelta(minutes=9)),
        }

        plan = TickPlanner().plan(NOW, checkpoints, last_completed_run_at=NOW)

        self.assertEqual([task.name for task in plan.tasks], [TickTask.DELIVERY])

    def test_gap_and_delayed_close_are_recovered(self):
        now = datetime(2026, 9, 2, 20, 20, tzinfo=timezone.utc)
        checkpoints = {
            "market": checkpoint("market", now - timedelta(minutes=2)),
            "news": checkpoint("news", now - timedelta(minutes=2)),
            "sec": checkpoint("sec", now - timedelta(minutes=2)),
        }

        plan = TickPlanner().plan(
            now,
            checkpoints,
            last_completed_run_at=now - timedelta(minutes=35),
        )

        names = [task.name for task in plan.tasks]
        self.assertIn(TickTask.RECOVERY, names)
        self.assertIn(TickTask.CLOSE, names)
        self.assertEqual(plan.gap_seconds, 35 * 60)


class TickRunnerTest(unittest.TestCase):
    def test_task_failure_does_not_block_later_tasks(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")

            def fail_market(_task):
                raise RuntimeError("quote unavailable")

            runner = TickRunner(
                repository,
                {
                    TickTask.MARKET: fail_market,
                    TickTask.NEWS: lambda _task: {"candidates": 0},
                    TickTask.SEC: lambda _task: {"filings": 0},
                    TickTask.DELIVERY: lambda _task: {"delivered": 0},
                },
                clock=lambda: NOW,
            )
            report = runner.run("run-1", scheduled_at=NOW - timedelta(minutes=2))

            self.assertEqual(report.status, "partial")
            self.assertIn("market", report.failed)
            self.assertIn("news", report.succeeded)
            self.assertIn("sec", report.succeeded)
            checkpoints = repository.task_checkpoints()
            self.assertEqual(checkpoints["market"].last_error, "quote unavailable")
            self.assertEqual(checkpoints["news"].metadata, {"candidates": 0})
            self.assertEqual(repository.recent_runs()[0].status, "partial")

    def test_missing_handler_is_reported_as_partial_not_success(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            runner = TickRunner(repository, {}, clock=lambda: NOW)

            report = runner.run("run-without-handlers", scheduled_at=NOW)

            self.assertEqual(report.status, "partial")
            self.assertIn("market", report.skipped)
            self.assertEqual(repository.recent_runs()[0].status, "partial")


class SQLiteRuntimeTest(unittest.TestCase):
    def test_runtime_schema_and_checkpoints_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.db"
            repository = SQLiteMonitorRepository(path)
            repository.start_run(
                "run-1",
                scheduled_at=NOW - timedelta(minutes=2),
                started_at=NOW,
                gap_seconds=900,
            )
            repository.mark_task_succeeded("news", "news", NOW, {"items": 2})
            repository.finish_run(
                "run-1",
                completed_at=NOW,
                status="success",
                summary={"ok": True},
            )

            restarted = SQLiteMonitorRepository(path)
            with closing(sqlite3.connect(path)) as connection, connection:
                version = connection.execute("PRAGMA user_version").fetchone()[0]

            self.assertEqual(version, SCHEMA_VERSION)
            self.assertEqual(restarted.task_checkpoints()["news"].metadata, {"items": 2})
            self.assertEqual(restarted.recent_runs()[0].gap_seconds, 900)


class GitStateBranchStoreTest(unittest.TestCase):
    def test_twenty_checkpoints_keep_main_unchanged_and_restore_latest_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = root / "remote.git"
            working = root / "working"
            fresh = root / "fresh"
            _git(root, "init", "--bare", str(remote))
            _git(root, "init", "-b", "main", str(working))
            _git(working, "config", "user.name", "Test User")
            _git(working, "config", "user.email", "test@example.com")
            (working / "README.md").write_text("code\n", encoding="utf-8")
            _git(working, "add", "README.md")
            _git(working, "commit", "-m", "initial")
            _git(working, "remote", "add", "origin", str(remote))
            _git(working, "push", "-u", "origin", "main")
            main_before = _git(working, "rev-parse", "main").stdout.strip()

            database = working / "monitor.db"
            repository = SQLiteMonitorRepository(database)
            store = GitStateBranchStore(working)
            checkpoints = []
            for index in range(20):
                repository.mark_task_succeeded(
                    "news",
                    "news",
                    NOW + timedelta(minutes=index),
                    {"items": index + 1},
                )
                result = store.checkpoint(
                    database,
                    run_id=f"run-{index + 1}",
                    checkpointed_at=NOW + timedelta(minutes=index),
                )
                checkpoints.append(result)

            self.assertEqual(checkpoints[0].previous_commit_sha, "")
            self.assertEqual(
                checkpoints[-1].previous_commit_sha,
                checkpoints[-2].commit_sha,
            )
            self.assertEqual(_git(working, "rev-parse", "main").stdout.strip(), main_before)

            _git(root, "clone", "--branch", "main", str(remote), str(fresh))
            restored = fresh / ".runtime" / "monitor.db"
            restored.parent.mkdir(parents=True)
            Path(f"{restored}-wal").write_bytes(b"stale wal")
            Path(f"{restored}-shm").write_bytes(b"stale shm")
            result = GitStateBranchStore(fresh).restore(restored)
            self.assertFalse(Path(f"{restored}-wal").exists())
            self.assertFalse(Path(f"{restored}-shm").exists())
            restored_repository = SQLiteMonitorRepository(restored)

            self.assertTrue(result.restored)
            self.assertEqual(result.commit_sha, checkpoints[-1].commit_sha)
            self.assertIn("news", restored_repository.task_checkpoints())
            self.assertEqual(
                restored_repository.task_checkpoints()["news"].metadata,
                {"items": 20},
            )
            _git(fresh, "fetch", "origin", "runtime-state")
            commit_count = _git(fresh, "rev-list", "--count", "FETCH_HEAD").stdout.strip()
            self.assertEqual(commit_count, "1")

    def test_stale_runner_cannot_overwrite_newer_runtime_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = root / "remote.git"
            seed = root / "seed"
            runner_a = root / "runner-a"
            runner_b = root / "runner-b"
            _git(root, "init", "--bare", str(remote))
            _git(root, "init", "-b", "main", str(seed))
            _git(seed, "config", "user.name", "Test User")
            _git(seed, "config", "user.email", "test@example.com")
            (seed / "README.md").write_text("code\n", encoding="utf-8")
            _git(seed, "add", "README.md")
            _git(seed, "commit", "-m", "initial")
            _git(seed, "remote", "add", "origin", str(remote))
            _git(seed, "push", "-u", "origin", "main")

            seed_database = seed / "monitor.db"
            SQLiteMonitorRepository(seed_database).mark_task_succeeded(
                "news", "news", NOW, {"items": 1}
            )
            GitStateBranchStore(seed).checkpoint(
                seed_database,
                run_id="seed",
                checkpointed_at=NOW,
            )
            _git(root, "clone", "--branch", "main", str(remote), str(runner_a))
            _git(root, "clone", "--branch", "main", str(remote), str(runner_b))

            database_a = runner_a / ".runtime" / "monitor.db"
            database_b = runner_b / ".runtime" / "monitor.db"
            store_a = GitStateBranchStore(runner_a)
            store_b = GitStateBranchStore(runner_b)
            store_a.restore(database_a)
            store_b.restore(database_b)
            SQLiteMonitorRepository(database_a).mark_task_succeeded(
                "sec", "sec", NOW, {"runner": "a"}
            )
            SQLiteMonitorRepository(database_b).mark_task_succeeded(
                "sec", "sec", NOW, {"runner": "b"}
            )

            store_a.checkpoint(
                database_a,
                run_id="runner-a",
                checkpointed_at=NOW + timedelta(minutes=5),
            )
            with self.assertRaises(GitStateBranchError):
                store_b.checkpoint(
                    database_b,
                    run_id="runner-b",
                    checkpointed_at=NOW + timedelta(minutes=6),
                )


class ShadowWorkflowTest(unittest.TestCase):
    def test_shadow_workflow_has_ci_and_does_not_send_slack(self):
        workflow = (ROOT / ".github" / "workflows" / "monitor_v2_shadow.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("push:", workflow)
        self.assertIn("schedule:", workflow)
        self.assertIn("2-57/5 4-19 * * 1-5", workflow)
        self.assertIn("America/New_York", workflow)
        self.assertNotIn("hood_monitor.py", workflow)
        self.assertIn("persist_state", workflow)
        self.assertIn("unittest discover", workflow)
        self.assertIn("market-tick", workflow)
        self.assertIn("shadow-tick", workflow)


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )


if __name__ == "__main__":
    unittest.main()
