from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import tempfile
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from investing_monitor.adapters.sqlite_repository import SCHEMA_VERSION


@dataclass(frozen=True)
class RestoreResult:
    restored: bool
    commit_sha: str = ""
    database_sha256: str = ""


@dataclass(frozen=True)
class CheckpointResult:
    commit_sha: str
    previous_commit_sha: str
    database_sha256: str


class GitStateBranchError(RuntimeError):
    pass


class GitStateBranchStore:
    def __init__(
        self,
        repository: str | Path,
        *,
        remote: str = "origin",
        branch: str = "runtime-state",
    ) -> None:
        self.repository = Path(repository).resolve()
        self.remote = remote
        self.branch = branch

    def restore(self, target_database: str | Path) -> RestoreResult:
        target = Path(target_database)
        target.parent.mkdir(parents=True, exist_ok=True)
        remote_sha = self._remote_sha()
        if not remote_sha:
            self._write_base_sha(target, "")
            return RestoreResult(restored=False)

        tracking_ref = f"refs/remotes/{self.remote}/{self.branch}"
        self._git(
            [
                "fetch",
                "--quiet",
                self.remote,
                f"+refs/heads/{self.branch}:{tracking_ref}",
            ]
        )
        fetched_sha = self._git(["rev-parse", tracking_ref]).stdout.strip()
        database = self._git_bytes(["show", f"{tracking_ref}:monitor.db"])
        manifest_raw = self._git_bytes(["show", f"{tracking_ref}:state_manifest.json"])
        try:
            manifest = json.loads(manifest_raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise GitStateBranchError("runtime-state manifest is invalid") from exc

        digest = hashlib.sha256(database).hexdigest()
        if manifest.get("database_sha256") != digest:
            raise GitStateBranchError("runtime-state database checksum mismatch")
        schema_version = manifest.get("schema_version")
        if not isinstance(schema_version, int) or schema_version > SCHEMA_VERSION:
            raise GitStateBranchError("runtime-state schema is not supported by this release")

        temporary = target.with_suffix(target.suffix + ".restore")
        temporary.write_bytes(database)
        try:
            self._verify_database(temporary)
            self._remove_sidecars(target)
            temporary.replace(target)
            self._write_base_sha(target, fetched_sha)
        finally:
            temporary.unlink(missing_ok=True)
        return RestoreResult(
            restored=True,
            commit_sha=fetched_sha,
            database_sha256=digest,
        )

    def checkpoint(
        self,
        database_path: str | Path,
        *,
        run_id: str,
        checkpointed_at: datetime | None = None,
    ) -> CheckpointResult:
        database = Path(database_path)
        if not database.is_file():
            raise GitStateBranchError(f"state database not found: {database}")

        self._flush_wal(database)
        self._verify_database(database)
        database_sha256 = hashlib.sha256(database.read_bytes()).hexdigest()
        checkpointed_at = checkpointed_at or datetime.now(timezone.utc)
        if checkpointed_at.tzinfo is None:
            raise ValueError("checkpointed_at must be timezone-aware")
        checkpointed_at = checkpointed_at.astimezone(timezone.utc)
        previous_sha = self._expected_base_sha(database)

        manifest = {
            "schema_version": SCHEMA_VERSION,
            "database_sha256": database_sha256,
            "checkpointed_at": checkpointed_at.isoformat(),
            "run_id": run_id,
        }

        with tempfile.TemporaryDirectory(prefix="investing-state-") as directory:
            index_path = Path(directory) / "index"
            manifest_path = Path(directory) / "state_manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=True, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            index_env = {"GIT_INDEX_FILE": str(index_path)}
            self._git(["read-tree", "--empty"], extra_env=index_env)
            database_blob = self._git(
                ["hash-object", "-w", "--", str(database)]
            ).stdout.strip()
            manifest_blob = self._git(
                ["hash-object", "-w", "--", str(manifest_path)]
            ).stdout.strip()
            self._git(
                [
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    f"100644,{database_blob},monitor.db",
                ],
                extra_env=index_env,
            )
            self._git(
                [
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    f"100644,{manifest_blob},state_manifest.json",
                ],
                extra_env=index_env,
            )
            tree_sha = self._git(["write-tree"], extra_env=index_env).stdout.strip()
            identity_env = {
                "GIT_AUTHOR_NAME": "github-actions[bot]",
                "GIT_AUTHOR_EMAIL": "41898282+github-actions[bot]@users.noreply.github.com",
                "GIT_COMMITTER_NAME": "github-actions[bot]",
                "GIT_COMMITTER_EMAIL": "41898282+github-actions[bot]@users.noreply.github.com",
                "GIT_AUTHOR_DATE": checkpointed_at.isoformat(),
                "GIT_COMMITTER_DATE": checkpointed_at.isoformat(),
            }
            commit_sha = self._git(
                ["commit-tree", tree_sha],
                extra_env=identity_env,
                input_text=f"runtime state checkpoint {run_id}\n",
            ).stdout.strip()

        lease = f"--force-with-lease=refs/heads/{self.branch}:{previous_sha}"
        self._git(
            [
                "push",
                "--quiet",
                lease,
                self.remote,
                f"{commit_sha}:refs/heads/{self.branch}",
            ]
        )
        self._write_base_sha(database, commit_sha)
        return CheckpointResult(
            commit_sha=commit_sha,
            previous_commit_sha=previous_sha,
            database_sha256=database_sha256,
        )

    def _remote_sha(self) -> str:
        result = self._git(
            ["ls-remote", "--heads", self.remote, f"refs/heads/{self.branch}"]
        )
        line = result.stdout.strip()
        return line.split()[0] if line else ""

    def _expected_base_sha(self, database: Path) -> str:
        marker = self._base_marker(database)
        if marker.is_file():
            return marker.read_text(encoding="ascii").strip()
        remote_sha = self._remote_sha()
        if remote_sha:
            raise GitStateBranchError(
                "runtime-state already exists; restore it before checkpointing"
            )
        return ""

    def _write_base_sha(self, database: Path, commit_sha: str) -> None:
        marker = self._base_marker(database)
        marker.parent.mkdir(parents=True, exist_ok=True)
        temporary = marker.with_suffix(marker.suffix + ".tmp")
        temporary.write_text(f"{commit_sha}\n", encoding="ascii")
        temporary.replace(marker)

    @staticmethod
    def _base_marker(database: Path) -> Path:
        return Path(f"{database}.state-base")

    def _git(
        self,
        args: list[str],
        *,
        extra_env: dict[str, str] | None = None,
        input_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(extra_env or {})
        try:
            return subprocess.run(
                ["git", *args],
                cwd=self.repository,
                env=env,
                input=input_text,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or str(exc)).strip()
            raise GitStateBranchError(f"git {' '.join(args)} failed: {detail}") from exc

    def _git_bytes(self, args: list[str]) -> bytes:
        try:
            result = subprocess.run(
                ["git", *args],
                cwd=self.repository,
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or b"").decode("utf-8", errors="replace").strip()
            raise GitStateBranchError(f"git {' '.join(args)} failed: {detail}") from exc
        return result.stdout

    @staticmethod
    def _flush_wal(database: Path) -> None:
        with closing(sqlite3.connect(database)) as connection, connection:
            result = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if result and result[0] != 0:
            raise GitStateBranchError("state database WAL is busy")

    @staticmethod
    def _verify_database(database: Path) -> None:
        try:
            with closing(
                sqlite3.connect(f"file:{database}?mode=ro", uri=True)
            ) as connection, connection:
                result = connection.execute("PRAGMA quick_check").fetchone()
        except sqlite3.DatabaseError as exc:
            raise GitStateBranchError("runtime-state database is not valid SQLite") from exc
        if not result or result[0] != "ok":
            raise GitStateBranchError("runtime-state database quick_check failed")

    @staticmethod
    def _remove_sidecars(database: Path) -> None:
        Path(f"{database}-wal").unlink(missing_ok=True)
        Path(f"{database}-shm").unlink(missing_ok=True)
