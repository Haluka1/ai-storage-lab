from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import MetadataMismatch
from .metadata import BlockKey, BlockLocation, KVMetadata, TierName


LOCATION_FIELDS = (
    "locality",
    "transport",
    "cloud",
    "region",
    "zone",
    "cluster_id",
    "node_id",
    "estimated_load_p95_ms",
    "estimated_transfer_p95_ms",
    "egress_cost_class",
    "ttl_seconds",
    "confidence",
)


class MetadataStore:
    def __init__(self, db_path: str | Path):
        # The owner lock must be derived from the canonical SQLite identity.
        # Otherwise two spellings of the same file (for example a symlink
        # alias) can acquire different adjacent lock files and reset each
        # other's in-flight epoch.
        self.db_path = Path(db_path).expanduser().resolve(strict=False)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._mutation_lock = threading.RLock()
        self._closed = False
        self._owner_epoch = uuid.uuid4().hex
        self._owner_lock_path = self.db_path.with_name(f"{self.db_path.name}.owner.lock")
        self._owner_lock = self._owner_lock_path.open("a+b")
        try:
            fcntl.flock(self._owner_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._owner_lock.close()
            raise RuntimeError(
                f"metadata database already has a live owner: {self.db_path}"
            ) from exc
        try:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._init_schema()
            self._begin_owner_epoch()
        except Exception:
            try:
                self._conn.close()
            except (AttributeError, sqlite3.Error):
                pass
            fcntl.flock(self._owner_lock.fileno(), fcntl.LOCK_UN)
            self._owner_lock.close()
            raise

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS blocks (
                  block_hash TEXT NOT NULL,
                  namespace TEXT NOT NULL,
                  tier TEXT NOT NULL,
                  uri TEXT NOT NULL,
                  bytes INTEGER NOT NULL,
                  checksum TEXT NOT NULL,
                  metadata_json TEXT NOT NULL,
                  location_json TEXT NOT NULL DEFAULT '{}',
                  created_at REAL NOT NULL,
                  last_access REAL NOT NULL,
                  reuse_count INTEGER NOT NULL DEFAULT 0,
                  in_flight INTEGER NOT NULL DEFAULT 0,
                  in_flight_epoch TEXT NOT NULL DEFAULT '',
                  state TEXT NOT NULL DEFAULT 'active',
                  PRIMARY KEY (block_hash, namespace, tier)
                )
                """
            )
            columns = {
                str(row[1])
                for row in self._conn.execute("PRAGMA table_info(blocks)").fetchall()
            }
            if "location_json" not in columns:
                self._conn.execute(
                    "ALTER TABLE blocks ADD COLUMN location_json TEXT NOT NULL DEFAULT '{}'"
                )
            if "in_flight_epoch" not in columns:
                self._conn.execute(
                    "ALTER TABLE blocks ADD COLUMN in_flight_epoch TEXT NOT NULL DEFAULT ''"
                )
            if "state" not in columns:
                self._conn.execute(
                    "ALTER TABLE blocks ADD COLUMN state TEXT NOT NULL DEFAULT 'active'"
                )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata_runtime (
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                  owner_epoch TEXT NOT NULL,
                  owner_pid INTEGER NOT NULL,
                  started_at REAL NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_blocks_last_access ON blocks(tier, last_access)"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_blocks_namespace ON blocks(namespace)"
            )

    def _begin_owner_epoch(self) -> None:
        """Recover counters left by a crashed instance and publish this owner epoch.

        MetadataStore intentionally supports one live process per SQLite database.
        The adjacent flock enforces that contract.  Because the kernel releases the
        lock when a process exits, every counter visible here belongs to a previous,
        dead owner and can be reset without racing a live reader.
        """
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE blocks SET in_flight=0, in_flight_epoch=''
                WHERE in_flight<>0 OR in_flight_epoch<>''
                """
            )
            self._conn.execute(
                """
                INSERT INTO metadata_runtime(singleton, owner_epoch, owner_pid, started_at)
                VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                  owner_epoch=excluded.owner_epoch,
                  owner_pid=excluded.owner_pid,
                  started_at=excluded.started_at
                """,
                (self._owner_epoch, os.getpid(), time.time()),
            )

    @contextmanager
    def mutation(self) -> Iterator[None]:
        """Serialize physical tier mutations that share this metadata owner.

        The repository intentionally supports one process with multiple threads,
        but callers may construct more than one Tier object over the same store.
        Holding this lock across payload mutation and metadata commit prevents
        per-instance locks from admitting evict/store interleavings for one key.
        """
        with self._mutation_lock:
            if self._closed:
                raise RuntimeError("metadata store is closed")
            yield

    def upsert(self, location: BlockLocation, metadata: KVMetadata) -> None:
        _validate_upsert(location, metadata)
        with self._mutation_lock, self._lock, self._conn:
            state = self._conn.execute(
                """
                SELECT state, metadata_json FROM blocks
                WHERE block_hash=? AND namespace=? AND tier=?
                """,
                (
                    location.key.block_hash,
                    location.key.namespace(),
                    location.tier.value,
                ),
            ).fetchone()
            if state is not None and str(state[0]) == "deleting":
                raise RuntimeError(
                    "cannot upsert a block while its delete tombstone is pending"
                )
            if state is not None:
                try:
                    persisted = KVMetadata.from_dict(json.loads(str(state[1])))
                    persisted.validate(require_checksum=True)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise MetadataMismatch(
                        "persisted metadata is structurally invalid"
                    ) from exc
                if persisted.payload_descriptor() != metadata.payload_descriptor():
                    raise MetadataMismatch(
                        "active BlockKey metadata descriptor cannot change"
                    )
            self._conn.execute(
                """
                INSERT INTO blocks(
                  block_hash, namespace, tier, uri, bytes, checksum,
                  metadata_json, location_json, created_at, last_access,
                  reuse_count, in_flight, in_flight_epoch, state
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((
                  SELECT in_flight FROM blocks
                  WHERE block_hash=? AND namespace=? AND tier=? AND in_flight_epoch=?
                ), 0), ?, 'active')
                ON CONFLICT(block_hash, namespace, tier) DO UPDATE SET
                  uri=excluded.uri,
                  bytes=excluded.bytes,
                  checksum=excluded.checksum,
                  metadata_json=excluded.metadata_json,
                  location_json=excluded.location_json,
                  created_at=MIN(blocks.created_at, excluded.created_at),
                  last_access=MAX(blocks.last_access, excluded.last_access),
                  reuse_count=MAX(blocks.reuse_count, excluded.reuse_count),
                  in_flight=excluded.in_flight,
                  in_flight_epoch=excluded.in_flight_epoch,
                  state='active'
                """,
                (
                    location.key.block_hash,
                    location.key.namespace(),
                    location.tier.value,
                    location.uri,
                    location.bytes,
                    location.checksum,
                    json.dumps(metadata.to_dict(), sort_keys=True),
                    json.dumps(_location_attributes(location), sort_keys=True),
                    location.created_at,
                    location.last_access,
                    metadata.reuse_count,
                    location.key.block_hash,
                    location.key.namespace(),
                    location.tier.value,
                    self._owner_epoch,
                    self._owner_epoch,
                ),
            )

    def lookup(self, key: BlockKey, tier: TierName | None = None) -> list[BlockLocation]:
        query = (
            "SELECT tier, uri, bytes, checksum, created_at, last_access, location_json "
            "FROM blocks WHERE block_hash=? AND namespace=? AND state='active'"
        )
        params: tuple[object, ...] = (key.block_hash, key.namespace())
        if tier is not None:
            query += " AND tier=?"
            params += (tier.value,)
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [_location_from_row(key, row) for row in rows]

    def lookup_and_acquire(self, key: BlockKey, tier: TierName) -> BlockLocation | None:
        # Physical mutations (notably segment compaction) select candidates and
        # replace their backing locations while holding mutation().  Join that
        # boundary briefly so a reader cannot acquire a stale location between
        # the compaction snapshot and its metadata commit.  The lock is an RLock
        # because compaction also acquires its own candidate while already inside
        # mutation().  Keep the global lock order mutation -> SQLite lock, which
        # is also the order used by close().
        with self.mutation(), self._lock, self._conn:
            row = self._conn.execute(
                """
                UPDATE blocks
                SET in_flight=CASE
                      WHEN in_flight_epoch=? THEN in_flight+1 ELSE 1
                    END,
                    in_flight_epoch=?
                WHERE block_hash=? AND namespace=? AND tier=? AND state='active'
                RETURNING tier, uri, bytes, checksum, created_at, last_access, location_json
                """,
                (
                    self._owner_epoch,
                    self._owner_epoch,
                    key.block_hash,
                    key.namespace(),
                    tier.value,
                ),
            ).fetchone()
        return None if row is None else _location_from_row(key, row)

    def get_metadata(self, key: BlockKey, tier: TierName) -> KVMetadata | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT metadata_json, last_access, reuse_count
                FROM blocks
                WHERE block_hash=? AND namespace=? AND tier=? AND state='active'
                """,
                (key.block_hash, key.namespace(), tier.value),
            ).fetchone()
        if row is None:
            return None
        try:
            metadata = KVMetadata.from_dict(json.loads(row[0]))
            metadata.last_access = float(row[1])
            metadata.reuse_count = int(row[2])
            metadata.validate(require_checksum=True)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise MetadataMismatch("persisted metadata is structurally invalid") from exc
        return metadata

    def touch(self, key: BlockKey, tier: TierName) -> None:
        now = time.time()
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE blocks SET last_access=?, reuse_count=reuse_count+1
                WHERE block_hash=? AND namespace=? AND tier=? AND state='active'
                """,
                (now, key.block_hash, key.namespace(), tier.value),
            )

    def delete(self, key: BlockKey, tier: TierName) -> bool:
        with self._mutation_lock, self._lock, self._conn:
            cursor = self._conn.execute(
                """
                DELETE FROM blocks
                WHERE block_hash=? AND namespace=? AND tier=?
                  AND state='active'
                  AND (in_flight=0 OR in_flight_epoch<>?)
                """,
                (key.block_hash, key.namespace(), tier.value, self._owner_epoch),
            )
            return cursor.rowcount > 0

    def acquire(self, key: BlockKey, tier: TierName) -> bool:
        with self.mutation(), self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE blocks
                SET in_flight=CASE
                      WHEN in_flight_epoch=? THEN in_flight+1 ELSE 1
                    END,
                    in_flight_epoch=?
                WHERE block_hash=? AND namespace=? AND tier=? AND state='active'
                """,
                (
                    self._owner_epoch,
                    self._owner_epoch,
                    key.block_hash,
                    key.namespace(),
                    tier.value,
                ),
            )
            return cursor.rowcount > 0

    def release(self, key: BlockKey, tier: TierName) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE blocks SET in_flight=MAX(in_flight-1, 0)
                WHERE block_hash=? AND namespace=? AND tier=? AND in_flight_epoch=?
                """,
                (key.block_hash, key.namespace(), tier.value, self._owner_epoch),
            )

    def begin_delete(self, key: BlockKey, tier: TierName) -> BlockLocation | None:
        """Make a block logically invisible before deleting its physical payload.

        A retained ``deleting`` row is a durable tombstone.  Retrying this method
        returns the same physical location, so a caller can finish an interrupted
        or previously failed deletion without allowing lazy rehydration meanwhile.
        """
        with self._mutation_lock, self._lock, self._conn:
            row = self._conn.execute(
                """
                SELECT state, in_flight, in_flight_epoch,
                       tier, uri, bytes, checksum, created_at, last_access, location_json
                FROM blocks
                WHERE block_hash=? AND namespace=? AND tier=?
                """,
                (key.block_hash, key.namespace(), tier.value),
            ).fetchone()
            if row is None:
                return None
            state, in_flight, in_flight_epoch = str(row[0]), int(row[1]), str(row[2])
            if state == "active":
                if in_flight_epoch == self._owner_epoch and in_flight > 0:
                    return None
                cursor = self._conn.execute(
                    """
                    UPDATE blocks SET state='deleting', in_flight=0, in_flight_epoch=''
                    WHERE block_hash=? AND namespace=? AND tier=? AND state='active'
                    """,
                    (key.block_hash, key.namespace(), tier.value),
                )
                if cursor.rowcount == 0:
                    return None
            elif state != "deleting":
                raise RuntimeError(f"unknown metadata state: {state}")
            return _location_from_row(key, row[3:])

    def finish_delete(self, key: BlockKey, tier: TierName) -> bool:
        with self._mutation_lock, self._lock, self._conn:
            cursor = self._conn.execute(
                """
                DELETE FROM blocks
                WHERE block_hash=? AND namespace=? AND tier=? AND state='deleting'
                """,
                (key.block_hash, key.namespace(), tier.value),
            )
            return cursor.rowcount > 0

    def is_deleting(self, key: BlockKey, tier: TierName) -> bool:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1 FROM blocks
                WHERE block_hash=? AND namespace=? AND tier=? AND state='deleting'
                """,
                (key.block_hash, key.namespace(), tier.value),
            ).fetchone()
        return row is not None

    def lru_candidates(self, tier: TierName, limit: int) -> list[BlockLocation]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT metadata_json, tier, uri, bytes, checksum, created_at,
                       last_access, location_json
                FROM blocks
                WHERE tier=? AND state='active'
                  AND (in_flight=0 OR in_flight_epoch<>?)
                ORDER BY last_access ASC LIMIT ?
                """,
                (tier.value, self._owner_epoch, limit),
            ).fetchall()
        return [
            _location_from_row(KVMetadata.from_dict(json.loads(row[0])).key, row[1:])
            for row in rows
        ]

    def tier_entries(
        self, tier: TierName, include_in_flight: bool = False
    ) -> list[tuple[BlockLocation, KVMetadata, int]]:
        where = "tier=? AND state='active'"
        params: tuple[object, ...] = (tier.value,)
        if not include_in_flight:
            where += " AND (in_flight=0 OR in_flight_epoch<>?)"
            params += (self._owner_epoch,)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT metadata_json, tier, uri, bytes, checksum, created_at,
                       last_access, reuse_count, in_flight, location_json
                FROM blocks
                WHERE {where}
                ORDER BY namespace, block_hash
                """,
                params,
            ).fetchall()
        out: list[tuple[BlockLocation, KVMetadata, int]] = []
        for row in rows:
            metadata = KVMetadata.from_dict(json.loads(row[0]))
            metadata.last_access = float(row[6])
            metadata.reuse_count = int(row[7])
            location_row = (*row[1:7], row[9])
            out.append((_location_from_row(metadata.key, location_row), metadata, int(row[8])))
        return out

    def bytes_used(self, tier: TierName) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(bytes), 0) FROM blocks WHERE tier=? AND state='active'",
                (tier.value,),
            ).fetchone()
        return int(row[0] if row is not None else 0)

    def close(self) -> None:
        with self._mutation_lock, self._lock:
            if self._closed:
                return
            self._conn.close()
            fcntl.flock(self._owner_lock.fileno(), fcntl.LOCK_UN)
            self._owner_lock.close()
            self._closed = True

    def __enter__(self) -> MetadataStore:
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


def _location_attributes(location: BlockLocation) -> dict[str, Any]:
    return {field: getattr(location, field) for field in LOCATION_FIELDS}


def _validate_upsert(location: BlockLocation, metadata: KVMetadata) -> None:
    try:
        metadata.validate(require_checksum=True)
    except ValueError as exc:
        raise MetadataMismatch("invalid metadata") from exc
    if location.key != metadata.key:
        raise MetadataMismatch("location key does not match metadata key")
    if not isinstance(location.tier, TierName):
        raise MetadataMismatch("location tier must be a TierName")
    if not location.uri:
        raise MetadataMismatch("location URI must not be empty")
    if location.bytes < 0 or metadata.bytes < 0 or location.bytes != metadata.bytes:
        raise MetadataMismatch("location bytes do not match metadata bytes")
    if location.checksum != metadata.checksum:
        raise MetadataMismatch("location checksum does not match metadata checksum")


def _location_from_row(key: BlockKey, row: tuple[Any, ...]) -> BlockLocation:
    try:
        attributes = json.loads(row[6] or "{}")
        if not isinstance(attributes, dict):
            raise ValueError("location attributes must be an object")
        filtered = {
            field: attributes[field]
            for field in LOCATION_FIELDS
            if field in attributes
        }
        location = BlockLocation(
            key=key,
            tier=TierName(row[0]),
            uri=str(row[1]),
            bytes=int(row[2]),
            checksum=str(row[3]),
            created_at=float(row[4]),
            last_access=float(row[5]),
            **filtered,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise MetadataMismatch("persisted location is structurally invalid") from exc
    if not location.uri or location.bytes < 0:
        raise MetadataMismatch("persisted location has invalid core fields")
    return location
