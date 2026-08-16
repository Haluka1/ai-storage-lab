from __future__ import annotations

import json
import struct
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

from kvstore.errors import BlockNotFound, ChecksumMismatch, MetadataMismatch
from kvstore.metadata import BlockKey, KVMetadata
from kvstore.metadata_store import MetadataStore
from kvstore.nvme_tier import HEADER_LEN_STRUCT, MAGIC, NVMeTier


class NVMeTierTest(unittest.TestCase):
    def test_store_load_atomic_and_evict(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            tier = NVMeTier(Path(td) / "nvme", 1024, store)
            key = _key("a")
            result = tier.store(key, b"payload", _metadata(key, 7))
            self.assertEqual(result.bytes, 7)
            self.assertEqual(tier.load(key).data, b"payload")
            self.assertEqual(list((Path(td) / "nvme").rglob("*.tmp")), [])
            self.assertTrue(tier.evict(key))
            self.assertIsNone(tier.lookup(key))

    def test_concurrent_stores_respect_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            tier = NVMeTier(Path(td) / "nvme", 8, store)
            keys = [_key("a"), _key("b")]
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(
                    pool.map(
                        lambda key: tier.store(key, b"12345678", _metadata(key, 8)),
                        keys,
                    )
                )
            self.assertEqual(len(results), 2)
            self.assertLessEqual(store.bytes_used(tier.name), 8)
            self.assertEqual(sum(tier.contains(key) for key in keys), 1)

    def test_metadata_mismatch_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            tier = NVMeTier(Path(td) / "nvme", 1024, store)
            key = _key("a")
            tier.store(key, b"payload", _metadata(key, 7))
            path = tier.layout.block_path(key)
            header, payload = _read_raw(path)
            header["metadata"]["key"]["tenant_id"] = "other"
            _write_raw(path, header, payload)
            with self.assertRaises(MetadataMismatch):
                tier.load(key)

    def test_checksum_mismatch_evicts_bad_block(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            tier = NVMeTier(Path(td) / "nvme", 1024, store)
            key = _key("a")
            tier.store(key, b"payload", _metadata(key, 7))
            path = tier.layout.block_path(key)
            header, _payload = _read_raw(path)
            _write_raw(path, header, b"corrupt")
            with self.assertRaises(ChecksumMismatch):
                tier.load(key)
            self.assertIsNone(tier.lookup(key))

    def test_segment_layout_appends_blocks_to_shared_segment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            tier = NVMeTier(Path(td) / "nvme", 4096, store, layout_mode="segment", segment_bytes=8192)
            key1 = _key_hash("aa" + "1" * 62)
            key2 = _key_hash("aa" + "2" * 62)
            tier.store(key1, b"payload-1", _metadata(key1, 9))
            tier.store(key2, b"payload-2", _metadata(key2, 9))

            loc1 = tier.lookup(key1)
            loc2 = tier.lookup(key2)
            self.assertIsNotNone(loc1)
            self.assertIsNotNone(loc2)
            self.assertTrue(loc1.uri.startswith("segment://"))
            self.assertTrue(loc2.uri.startswith("segment://"))
            ref1 = tier._segment_ref_from_uri(loc1.uri)
            ref2 = tier._segment_ref_from_uri(loc2.uri)
            self.assertEqual(ref1.path, ref2.path)
            self.assertLess(ref1.offset, ref2.offset)
            self.assertEqual(len(list((Path(td) / "nvme").rglob("*.kvseg"))), 1)
            self.assertEqual(list((Path(td) / "nvme").rglob("*.kv")), [])
            self.assertEqual(tier.load(key1).data, b"payload-1")
            self.assertEqual(tier.load(key2).data, b"payload-2")
            stats = tier.stats()
            self.assertEqual(stats["layout_mode"], "segment")
            self.assertGreaterEqual(stats["physical_bytes"], stats["used_bytes"])

    def test_segment_layout_rolls_when_segment_is_full(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(Path(td) / "meta.sqlite3")
            tier = NVMeTier(Path(td) / "nvme", 4096, store, layout_mode="segment", segment_bytes=1)
            key1 = _key_hash("bb" + "1" * 62)
            key2 = _key_hash("bb" + "2" * 62)
            tier.store(key1, b"payload-1", _metadata(key1, 9))
            tier.store(key2, b"payload-2", _metadata(key2, 9))
            self.assertEqual(len(list((Path(td) / "nvme").rglob("*.kvseg"))), 2)
            self.assertEqual(tier.load(key1).data, b"payload-1")
            self.assertEqual(tier.load(key2).data, b"payload-2")

    def test_segment_evict_is_metadata_authoritative(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "meta.sqlite3"
            root = Path(td) / "nvme"
            store = MetadataStore(db_path)
            tier = NVMeTier(root, 4096, store, layout_mode="segment")
            key = _key_hash("cc" + "1" * 62)
            tier.store(key, b"payload", _metadata(key, 7))
            self.assertTrue(tier.evict(key))
            self.assertEqual(len(list(root.rglob("*.kvseg"))), 1)
            self.assertIsNone(tier.lookup(key))
            with self.assertRaises(BlockNotFound):
                tier.load(key)

            store.close()
            reopened = NVMeTier(root, 4096, MetadataStore(db_path), layout_mode="segment")
            self.assertIsNone(reopened.lookup(key))
            with self.assertRaises(BlockNotFound):
                reopened.load(key)

    def test_content_addressed_delete_failure_retains_tombstone_for_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "meta.sqlite3"
            root = Path(td) / "nvme"
            store = MetadataStore(db_path)
            tier = NVMeTier(root, 4096, store)
            key = _key_hash("ca" + "1" * 62)
            tier.store(key, b"payload", _metadata(key, 7))
            path = tier.layout.block_path(key)

            with mock.patch.object(Path, "unlink", side_effect=PermissionError("denied")):
                with self.assertRaises(PermissionError):
                    tier.evict(key)

            self.assertTrue(path.exists())
            self.assertIsNone(tier.lookup(key))
            self.assertTrue(store.is_deleting(key, tier.name))
            self.assertEqual(store.bytes_used(tier.name), 0)
            store.close()

            reopened_store = MetadataStore(db_path)
            reopened = NVMeTier(root, 4096, reopened_store)
            self.assertIsNone(reopened.lookup(key))
            self.assertTrue(reopened.evict(key))
            self.assertFalse(path.exists())
            self.assertFalse(reopened_store.is_deleting(key, reopened.name))

    def test_content_addressed_delete_recovers_after_physical_delete_crash_point(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "meta.sqlite3"
            root = Path(td) / "nvme"
            store = MetadataStore(db_path)
            tier = NVMeTier(root, 4096, store)
            key = _key_hash("cb" + "1" * 62)
            tier.store(key, b"payload", _metadata(key, 7))
            location = store.begin_delete(key, tier.name)
            self.assertIsNotNone(location)
            tier.layout.block_path(key).unlink()
            store.close()  # Simulate a crash before finish_delete().

            reopened_store = MetadataStore(db_path)
            reopened = NVMeTier(root, 4096, reopened_store)
            self.assertIsNone(reopened.lookup(key))
            self.assertTrue(reopened.evict(key))
            self.assertFalse(reopened_store.is_deleting(key, reopened.name))

    def test_two_tier_instances_serialize_evict_then_store(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = MetadataStore(root / "meta.sqlite3")
            first = NVMeTier(root / "nvme", 4096, store)
            second = NVMeTier(root / "nvme", 4096, store)
            key = _key_hash("cd" + "1" * 62)
            first.store(key, b"old", _metadata(key, 3))
            unlink_started = threading.Event()
            allow_unlink = threading.Event()
            store_started = threading.Event()
            original_unlink = Path.unlink

            def delayed_unlink(path: Path, *args, **kwargs):
                unlink_started.set()
                if not allow_unlink.wait(2):
                    raise TimeoutError("test did not release unlink")
                return original_unlink(path, *args, **kwargs)

            def store_new_payload():
                store_started.set()
                return second.store(key, b"new", _metadata(key, 3))

            with mock.patch.object(Path, "unlink", new=delayed_unlink):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    evict_future = pool.submit(first.evict, key)
                    self.assertTrue(unlink_started.wait(1))
                    store_future = pool.submit(store_new_payload)
                    self.assertTrue(store_started.wait(1))
                    self.assertFalse(store_future.done())
                    allow_unlink.set()
                    self.assertTrue(evict_future.result(timeout=2))
                    store_future.result(timeout=2)

            self.assertEqual(second.load(key).data, b"new")
            self.assertFalse(store.is_deleting(key, second.name))

    def test_evict_requires_tombstone_finish_commit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = MetadataStore(root / "meta.sqlite3")
            tier = NVMeTier(root / "nvme", 4096, store)
            key = _key_hash("ce" + "1" * 62)
            tier.store(key, b"payload", _metadata(key, 7))

            with mock.patch.object(store, "finish_delete", return_value=False):
                with self.assertRaisesRegex(RuntimeError, "tombstone disappeared"):
                    tier.evict(key)

    def test_segment_compaction_reclaims_evicted_records(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "nvme"
            store = MetadataStore(Path(td) / "meta.sqlite3")
            tier = NVMeTier(root, 4096, store, layout_mode="segment", segment_bytes=8192)
            key1 = _key_hash("dd" + "1" * 62)
            key2 = _key_hash("dd" + "2" * 62)
            tier.store(key1, b"payload-1", _metadata(key1, 9))
            tier.store(key2, b"payload-2", _metadata(key2, 9))
            physical_before = tier.stats()["physical_bytes"]
            self.assertTrue(tier.evict(key1))

            result = tier.compact_segments()

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["moved_records"], 1)
            self.assertGreaterEqual(result["removed_segment_files"], 1)
            self.assertGreater(result["bytes_reclaimed"], 0)
            self.assertLess(tier.stats()["physical_bytes"], physical_before)
            self.assertEqual(tier.load(key2).data, b"payload-2")
            with self.assertRaises(BlockNotFound):
                tier.load(key1)
            self.assertEqual(len(list(root.rglob("*.kvseg"))), 1)

    def test_segment_compaction_skips_inflight_records(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "nvme"
            store = MetadataStore(Path(td) / "meta.sqlite3")
            tier = NVMeTier(root, 4096, store, layout_mode="segment", segment_bytes=8192)
            key1 = _key_hash("ee" + "1" * 62)
            key2 = _key_hash("ee" + "2" * 62)
            tier.store(key1, b"payload-1", _metadata(key1, 9))
            tier.store(key2, b"payload-2", _metadata(key2, 9))

            store.acquire(key1, tier.name)
            try:
                result = tier.compact_segments()
            finally:
                store.release(key1, tier.name)

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["inflight_segment_records"], 1)
            self.assertEqual(result["moved_records"], 1)
            self.assertEqual(tier.load(key1).data, b"payload-1")
            self.assertEqual(tier.load(key2).data, b"payload-2")
            self.assertGreaterEqual(len(list(root.rglob("*.kvseg"))), 2)

    def test_segment_load_acquisition_waits_for_compaction_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "nvme"
            store = MetadataStore(Path(td) / "meta.sqlite3")
            tier = NVMeTier(
                root, 4096, store, layout_mode="segment", segment_bytes=8192
            )
            key = _key_hash("ef" + "1" * 62)
            tier.store(key, b"payload", _metadata(key, 7))
            original_uri = tier.lookup(key).uri

            snapshot_and_reader = threading.Barrier(2)
            allow_compaction = threading.Event()
            acquisition_started = threading.Event()
            acquisition_finished = threading.Event()
            original_tier_entries = store.tier_entries
            original_lookup_and_acquire = store.lookup_and_acquire
            snapshot_paused = False

            def pause_after_candidate_snapshot(
                tier_name, include_in_flight: bool = False
            ):
                nonlocal snapshot_paused
                entries = original_tier_entries(tier_name, include_in_flight)
                if not include_in_flight and not snapshot_paused:
                    snapshot_paused = True
                    snapshot_and_reader.wait(timeout=2)
                    if not allow_compaction.wait(2):
                        raise TimeoutError("test did not release compaction")
                return entries

            def observe_lookup_and_acquire(block_key, tier_name):
                snapshot_and_reader.wait(timeout=2)
                acquisition_started.set()
                try:
                    return original_lookup_and_acquire(block_key, tier_name)
                finally:
                    acquisition_finished.set()

            with (
                mock.patch.object(
                    store, "tier_entries", side_effect=pause_after_candidate_snapshot
                ),
                mock.patch.object(
                    store,
                    "lookup_and_acquire",
                    side_effect=observe_lookup_and_acquire,
                ),
                ThreadPoolExecutor(max_workers=2) as pool,
            ):
                compact_future = pool.submit(tier.compact_segments)
                load_future = pool.submit(tier.load, key)
                self.assertTrue(acquisition_started.wait(1))
                try:
                    self.assertFalse(
                        acquisition_finished.wait(0.1),
                        "reader acquired a stale segment while compaction held mutation",
                    )
                finally:
                    allow_compaction.set()

                compact_result = compact_future.result(timeout=2)
                load_result = load_future.result(timeout=2)

            self.assertEqual(compact_result["moved_records"], 1)
            self.assertEqual(load_result.data, b"payload")
            self.assertNotEqual(tier.lookup(key).uri, original_uri)

    def test_segment_compaction_dry_run_does_not_rewrite(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "nvme"
            store = MetadataStore(Path(td) / "meta.sqlite3")
            tier = NVMeTier(root, 4096, store, layout_mode="segment", segment_bytes=8192)
            key = _key_hash("ff" + "1" * 62)
            tier.store(key, b"payload", _metadata(key, 7))
            uri_before = tier.lookup(key).uri
            physical_before = tier.stats()["physical_bytes"]

            result = tier.compact_segments(dry_run=True)

            self.assertTrue(result["dry_run"])
            self.assertEqual(result["moved_records"], 0)
            self.assertEqual(tier.lookup(key).uri, uri_before)
            self.assertEqual(tier.stats()["physical_bytes"], physical_before)


def _key(seed: str) -> BlockKey:
    return BlockKey("t", "m", "r", "tok", seed * 64)


def _key_hash(block_hash: str) -> BlockKey:
    return BlockKey("t", "m", "r", "tok", block_hash)


def _metadata(key: BlockKey, bytes_: int) -> KVMetadata:
    return KVMetadata(key, "bf16", 1, 1, 1, 16, bytes_)


def _read_raw(path: Path) -> tuple[dict, bytes]:
    data = path.read_bytes()
    self_magic = data[: len(MAGIC)]
    if self_magic != MAGIC:
        raise AssertionError("bad magic")
    header_len = HEADER_LEN_STRUCT.unpack(data[len(MAGIC) : len(MAGIC) + HEADER_LEN_STRUCT.size])[0]
    offset = len(MAGIC) + HEADER_LEN_STRUCT.size
    header = json.loads(data[offset : offset + header_len].decode("utf-8"))
    return header, data[offset + header_len :]


def _write_raw(path: Path, header: dict, payload: bytes) -> None:
    encoded = json.dumps(header, sort_keys=True, separators=(",", ":")).encode("utf-8")
    path.write_bytes(MAGIC + struct.pack(">I", len(encoded)) + encoded + payload)


if __name__ == "__main__":
    unittest.main()
