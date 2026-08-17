from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from kvstore.metadata import BlockKey, BlockLocation, KVMetadata, TierName
from kvstore.metadata_store import MetadataStore


class MetadataStoreTest(unittest.TestCase):
    def test_adversarial_namespaces_do_not_overwrite_each_other(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            block_hash = "a" * 64
            key_a = BlockKey("a/model=b", "c", "r", "tok", block_hash)
            key_b = BlockKey("a", "b/model=c", "r", "tok", block_hash)
            for key, uri in [(key_a, "memory://a"), (key_b, "memory://b")]:
                store.upsert(
                    BlockLocation(
                        key, TierName.MEMORY, uri, 4, "abcd", time.time(), time.time()
                    ),
                    KVMetadata(key, "bf16", 1, 1, 1, 1, 4, checksum="abcd"),
                )
            self.assertEqual(store.lookup(key_a, TierName.MEMORY)[0].uri, "memory://a")
            self.assertEqual(store.lookup(key_b, TierName.MEMORY)[0].uri, "memory://b")

    def test_lookup_and_inflight_delete_guard(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            key = BlockKey("t", "m", "r", "tok", "a" * 64)
            meta = KVMetadata(key, "bf16", 1, 1, 1, 1, 4, checksum="abcd")
            loc = BlockLocation(key, TierName.MEMORY, "memory://x", 4, "abcd", time.time(), time.time())
            store.upsert(loc, meta)
            self.assertEqual(len(store.lookup(key)), 1)
            store.acquire(key, TierName.MEMORY)
            self.assertFalse(store.delete(key, TierName.MEMORY))
            store.release(key, TierName.MEMORY)
            self.assertTrue(store.delete(key, TierName.MEMORY))

    def test_touch_updates_returned_metadata_and_location_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            key = BlockKey("t", "m", "r", "tok", "b" * 64)
            meta = KVMetadata(key, "bf16", 1, 1, 1, 1, 4, reuse_count=2)
            loc = BlockLocation(
                key,
                TierName.NVME,
                "file:///tmp/x",
                4,
                "abcd",
                time.time(),
                time.time(),
                locality="same_zone",
                transport="nvmeof_tcp",
                cloud="alibaba",
                region="cn-hangzhou",
                zone="cn-hangzhou-i",
                cluster_id="ack",
                node_id="node-a",
                estimated_load_p95_ms=1.25,
                egress_cost_class="same_zone",
                confidence=0.9,
            )
            store.upsert(loc, meta)
            store.touch(key, TierName.NVME)
            loaded_meta = store.get_metadata(key, TierName.NVME)
            loaded_loc = store.lookup(key, TierName.NVME)[0]
            self.assertEqual(loaded_meta.reuse_count, 3)
            self.assertEqual(loaded_loc.transport, "nvmeof_tcp")
            self.assertEqual(loaded_loc.zone, "cn-hangzhou-i")
            self.assertEqual(loaded_loc.estimated_load_p95_ms, 1.25)
            self.assertEqual(loaded_loc.confidence, 0.9)

    def test_lookup_and_acquire_atomically_guards_delete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            key = BlockKey("t", "m", "r", "tok", "c" * 64)
            meta = KVMetadata(key, "bf16", 1, 1, 1, 1, 4)
            loc = BlockLocation(
                key, TierName.MEMORY, "memory://c", 4, "abcd", time.time(), time.time()
            )
            store.upsert(loc, meta)
            self.assertIsNotNone(store.lookup_and_acquire(key, TierName.MEMORY))
            self.assertFalse(store.delete(key, TierName.MEMORY))
            store.release(key, TierName.MEMORY)
            self.assertTrue(store.delete(key, TierName.MEMORY))

    def test_second_live_owner_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "meta.sqlite3"
            store = MetadataStore(db_path)
            try:
                with self.assertRaisesRegex(RuntimeError, "already has a live owner"):
                    MetadataStore(db_path)
            finally:
                store.close()

    def test_symlink_alias_cannot_bypass_owner_identity(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "meta.sqlite3"
            alias_path = Path(td) / "alias.sqlite3"
            key = BlockKey("t", "m", "r", "tok", "f" * 64)
            meta = KVMetadata(key, "bf16", 1, 1, 1, 1, 4)
            loc = BlockLocation(
                key, TierName.NVME, "file:///tmp/f", 4, "abcd", time.time(), time.time()
            )
            store = MetadataStore(db_path)
            store.upsert(loc, meta)
            self.assertTrue(store.acquire(key, TierName.NVME))
            alias_path.symlink_to(db_path)
            try:
                with self.assertRaisesRegex(RuntimeError, "already has a live owner"):
                    MetadataStore(alias_path)
                entries = store.tier_entries(TierName.NVME, include_in_flight=True)
                self.assertEqual(entries[0][2], 1)
            finally:
                store.release(key, TierName.NVME)
                store.close()

    def test_reopen_clears_legacy_blank_epoch_counter(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "meta.sqlite3"
            key = BlockKey("t", "m", "r", "tok", "1" * 64)
            meta = KVMetadata(key, "bf16", 1, 1, 1, 1, 4)
            loc = BlockLocation(
                key, TierName.NVME, "file:///tmp/legacy", 4, "abcd", time.time(), time.time()
            )
            store = MetadataStore(db_path)
            store.upsert(loc, meta)
            store.close()
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "UPDATE blocks SET in_flight=7, in_flight_epoch='' WHERE block_hash=?",
                    (key.block_hash,),
                )
                conn.commit()

            reopened = MetadataStore(db_path)
            try:
                entries = reopened.tier_entries(TierName.NVME, include_in_flight=True)
                self.assertEqual(entries[0][2], 0)
                with sqlite3.connect(db_path) as conn:
                    persisted = conn.execute(
                        "SELECT in_flight, in_flight_epoch FROM blocks WHERE block_hash=?",
                        (key.block_hash,),
                    ).fetchone()
                self.assertEqual(persisted, (0, ""))
            finally:
                reopened.close()

    def test_crashed_owner_inflight_is_recovered_on_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "meta.sqlite3"
            key = BlockKey("t", "m", "r", "tok", "d" * 64)
            meta = KVMetadata(key, "bf16", 1, 1, 1, 1, 4)
            loc = BlockLocation(
                key, TierName.NVME, "file:///tmp/d", 4, "abcd", time.time(), time.time()
            )
            store = MetadataStore(db_path)
            store.upsert(loc, meta)
            store.close()

            project_dir = Path(__file__).resolve().parents[1]
            script = """
import os
import sys
from kvstore.metadata import BlockKey, TierName
from kvstore.metadata_store import MetadataStore

store = MetadataStore(sys.argv[1])
key = BlockKey("t", "m", "r", "tok", "d" * 64)
assert store.acquire(key, TierName.NVME)
os._exit(0)
"""
            env = dict(os.environ)
            env["PYTHONPATH"] = str(project_dir)
            subprocess.check_call([sys.executable, "-c", script, str(db_path)], env=env)

            with sqlite3.connect(db_path) as conn:
                persisted = conn.execute(
                    "SELECT in_flight FROM blocks WHERE block_hash=?", (key.block_hash,)
                ).fetchone()
            self.assertEqual(persisted, (1,))

            reopened = MetadataStore(db_path)
            try:
                entries = reopened.tier_entries(TierName.NVME, include_in_flight=True)
                self.assertEqual(entries[0][2], 0)
                self.assertTrue(reopened.delete(key, TierName.NVME))
            finally:
                reopened.close()

    def test_delete_tombstone_survives_reopen_until_finished(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "meta.sqlite3"
            key = BlockKey("t", "m", "r", "tok", "e" * 64)
            meta = KVMetadata(key, "bf16", 1, 1, 1, 1, 4)
            loc = BlockLocation(
                key, TierName.S3, "s3://bucket/e", 4, "abcd", time.time(), time.time()
            )
            store = MetadataStore(db_path)
            store.upsert(loc, meta)
            self.assertEqual(store.begin_delete(key, TierName.S3), loc)
            self.assertEqual(store.lookup(key, TierName.S3), [])
            self.assertTrue(store.is_deleting(key, TierName.S3))
            store.close()

            reopened = MetadataStore(db_path)
            try:
                self.assertEqual(reopened.lookup(key, TierName.S3), [])
                self.assertTrue(reopened.is_deleting(key, TierName.S3))
                self.assertIsNotNone(reopened.begin_delete(key, TierName.S3))
                self.assertTrue(reopened.finish_delete(key, TierName.S3))
                self.assertFalse(reopened.is_deleting(key, TierName.S3))
            finally:
                reopened.close()

    def test_upsert_cannot_silently_reactivate_delete_tombstone(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            key = BlockKey("t", "m", "r", "tok", "2" * 64)
            meta = KVMetadata(key, "bf16", 1, 1, 1, 1, 4)
            loc = BlockLocation(
                key, TierName.S3, "s3://bucket/object", 4, "abcd", time.time(), time.time()
            )
            store.upsert(loc, meta)
            self.assertIsNotNone(store.begin_delete(key, TierName.S3))

            with self.assertRaisesRegex(RuntimeError, "delete tombstone is pending"):
                store.upsert(loc, meta)

            self.assertTrue(store.is_deleting(key, TierName.S3))
            self.assertEqual(store.lookup(key, TierName.S3), [])
            store.close()


if __name__ == "__main__":
    unittest.main()
