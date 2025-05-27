"""
A simple two-level ISAM (Indexed Sequential Access Method) implementation using on-disk pages.

This module provides:
- Page: an on-disk page holding sorted keys and file offsets with overflow chaining.
- ISAMIndex: a two-level ISAM index supporting build, search, range_search, add, and remove operations.

Pages and index metadata are serialized with pickle for persistence.
"""

import pickle
from bisect import bisect_right
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Protocol, Self


class SupportsRichComparison(Protocol):
    """Keys must support all four rich comparisons."""

    def __lt__(self, other: Self, /) -> bool: ...  # noqa: D105
    def __le__(self, other: Self, /) -> bool: ...  # noqa: D105
    def __gt__(self, other: Self, /) -> bool: ...  # noqa: D105
    def __ge__(self, other: Self, /) -> bool: ...  # noqa: D105

@dataclass
class Page[K : SupportsRichComparison]:
    """
    Represents a fixed-capacity page storing sorted keys and their corresponding file offsets.

    Attributes:
        keys (list[K]): Sorted list of keys in the page.
        offsets (list[int]): List of file offsets corresponding to each key.
        overflow_ptr (int | None): File offset of the next overflow page, or None.
        capacity (int): Maximum number of entries in this page.

    """

    keys: list[K] = field(default_factory=list)
    offsets: list[int] = field(default_factory=list)
    overflow_ptr: int | None = None
    capacity: int = 128

    def is_full(self) -> bool:
        """
        Check if the page has reached its capacity.

        Returns:
            bool: True if the number of keys >= capacity, False otherwise.

        """
        return len(self.keys) >= self.capacity

    def insert(self, key: K, offset: int) -> None:
        """
        Insert a key and its file offset into the page in sorted order.

        Args:
            key (K): The key to insert; must support rich comparison.
            offset (int): The file offset associated with the key.

        """
        idx = bisect_right(self.keys, key)
        self.keys.insert(idx, key)
        self.offsets.insert(idx, offset)

    def delete(self, key: K) -> bool:
        """
        Remove a key and its offset from the page if present.

        Args:
            key (K): The key to remove.

        Returns:
            bool: True if the key was found and removed, False otherwise.

        """
        try:
            idx = self.keys.index(key)
            del self.keys[idx]
            del self.offsets[idx]
        except ValueError:
            return False
        else:
            return True

    def __getstate__(self):
        # Remove the typing-generic info so pickle never sees it
        state = self.__dict__.copy()
        state.pop("__orig_class__", None)
        return state

    def __setstate__(self, state: dict[str, object]):
        # Restore everything else
        self.__dict__.update(state)


class ISAMIndex[K : SupportsRichComparison]:
    """
    Two-level on-disk ISAM index with overflow chaining for inserts beyond page capacity.

    Attributes:
        index_path (Path): File path for the index metadata (split_keys and leaf_offsets).
        data_path (Path): File path for serialized Page objects.
        leaf_capacity (int): Capacity for each leaf page.
        split_keys (list[K]): Split keys demarcating leaf page ranges.
        leaf_offsets (list[int]): File offsets to each leaf page.

    """

    def __init__(
        self,
        index_path: Path,
        data_path: Path,
        leaf_capacity: int = 128,
    ) -> None:
        """
        Initialize an ISAMIndex, loading existing metadata or creating new files.

        Args:
            index_path (Path): Path to the index metadata file.
            data_path (Path): Path to the page data file.
            leaf_capacity (int, optional): Maximum entries per leaf page. Defaults to 128.

        """
        self.index_path = index_path
        self.data_path = data_path
        self.leaf_capacity = leaf_capacity
        self.split_keys: list[K] = []
        self.leaf_offsets: list[int] = []
        self._lock = Lock()

        if self.index_path.exists():
            self._load_index()
        else:
            self.split_keys = []
            self.leaf_offsets = []
            self._write_index()
            self.data_path.parent.mkdir(parents=True, exist_ok=True)
            self.data_path.write_bytes(b"")

    def _load_index(self) -> None:
        """Load split_keys and leaf_offsets from the index metadata file."""
        with Path.open(self.index_path, "rb") as f:
            data = pickle.load(f)  # noqa: S301
            self.split_keys = data["split_keys"]
            self.leaf_offsets = data["leaf_offsets"]

    def _write_index(self) -> None:
        """Atomically write split_keys and leaf_offsets to the index metadata file."""
        tmp = self.index_path.with_suffix(".tmp")
        with Path.open(tmp, "wb") as f:
            pickle.dump({
                "split_keys": self.split_keys,
                "leaf_offsets": self.leaf_offsets,
            }, f)
        Path.replace(tmp, self.index_path)

    def _read_page(self, ptr: int) -> Page[K]:
        """
        Deserialize and return a Page object from the data file at the given offset.

        Args:
            ptr (int): Byte offset in data_path.

        Returns:
            Page[K]: The deserialized page.

        """
        with Path.open(self.data_path, "rb") as f:
            f.seek(ptr)
            return pickle.load(f)  # noqa: S301

    def _write_page(self, page: Page[K]) -> int:
        """
        Serialize a Page object by appending it to the data file.

        Args:
            page (Page[K]): The page to write.

        Returns:
            int: Byte offset where the page was written.

        """
        data = pickle.dumps(page)
        with Path.open(self.data_path, "ab") as f:
            ptr = f.tell()
            f.write(data)
        return ptr

    def build(self, initial_data: list[tuple[K, int]]) -> None:
        """
        Build the two-level index from sorted (key, offset) pairs.

        Args:
            initial_data (list[tuple[K, int]]): Sorted list of key-offset pairs.

        """
        self.split_keys.clear()
        self.leaf_offsets.clear()

        for i in range(0, len(initial_data), self.leaf_capacity):
            chunk = initial_data[i : i + self.leaf_capacity]
            page = Page[K](capacity=self.leaf_capacity)
            page.keys = [k for k, _ in chunk]
            page.offsets = [ofs for _, ofs in chunk]
            ptr = self._write_page(page)
            self.leaf_offsets.append(ptr)
            self.split_keys.append(page.keys[0])

        if self.split_keys:
            self.split_keys = self.split_keys[1:]
        self._write_index()

    def _find_leaf_index(self, key: K) -> int:
        """
        Determine the leaf page index for a given key using split_keys.

        Args:
            key (K): The key to locate.

        Returns:
            int: Index in leaf_offsets and split_keys.

        """
        return bisect_right(self.split_keys, key)

    def _find_leaf(self, key: K) -> tuple[Page[K], int]:
        """
        Retrieve the Page object and its index for a specific key.

        Args:
            key (K): Key to find.

        Returns:
            tuple[Page[K], int]: (Page, leaf index)

        """
        idx = self._find_leaf_index(key)
        ptr = self.leaf_offsets[idx]
        return self._read_page(ptr), idx

    def search(self, key: K) -> int | None:
        """
        Look up a single key, scanning the primary page and overflow chain.

        Args:
            key (K): Key to search.

        Returns:
            int | None: File offset if found, else None.

        """
        # if we've never built any leaf pages, there is nothing to find
        if not self.leaf_offsets:
            print("ISAMIndex: no leaf pages built yet")
            return None
        page, _ = self._find_leaf(key)


        for k, ofs in zip(page.keys, page.offsets, strict=False):
            if k == key:
                return ofs
        ptr = page.overflow_ptr
        while ptr is not None:
            page = self._read_page(ptr)
            for k, ofs in zip(page.keys, page.offsets, strict=False):
                if k == key:
                    return ofs
            ptr = page.overflow_ptr
        return None

    def range_search(self, lo: K, hi: K) -> list[tuple[K, int]]:
        """
        Retrieve all key-offset pairs in the inclusive range [lo, hi].

        Args:
            lo (K): Lower bound of range.
            hi (K): Upper bound of range.

        Returns:
            list[tuple[K, int]]: List of (key, offset) in range order.

        """
        # empty index ⇒ no hits
        if not self.leaf_offsets:
            return []
        results: list[tuple[K, int]] = []

        start = bisect_right(self.split_keys, lo)
        for leaf_ptr in self.leaf_offsets[start:]:
            page = self._read_page(leaf_ptr)
            for k, ofs in zip(page.keys, page.offsets, strict=False):
                if lo <= k <= hi:
                    results.append((k, ofs))
            ptr = page.overflow_ptr
            while ptr is not None:
                page = self._read_page(ptr)
                for k, ofs in zip(page.keys, page.offsets, strict=False):
                    if lo <= k <= hi:
                        results.append((k, ofs))
                ptr = page.overflow_ptr
            if page.keys and page.keys[-1] > hi:
                break
        return results

    def add(self, key: K, ofs: int) -> None:
        """
        Insert a key-offset pair, using overflow chaining if the leaf page is full.

        Args:
            key (K): The key to insert.
            ofs (int): File offset of the record.

        """
        with self._lock:
            page, leaf_idx = self._find_leaf(key)
            if not page.is_full():
                page.insert(key, ofs)
                new_ptr = self._write_page(page)
                self.leaf_offsets[leaf_idx] = new_ptr
            else:
                if page.overflow_ptr is None:
                    ov = Page[K](capacity=self.leaf_capacity)
                    page.overflow_ptr = self._write_page(ov)
                    self.leaf_offsets[leaf_idx] = self._write_page(page)
                ov_page = self._read_page(page.overflow_ptr)
                ov_page.insert(key, ofs)
                new_ov_ptr = self._write_page(ov_page)
                page.overflow_ptr = new_ov_ptr
                self.leaf_offsets[leaf_idx] = self._write_page(page)
            self._write_index()

    def remove(self, key: K) -> bool:
        """
        Remove a key-offset pair from the index, updating overflow links if needed.

        Args:
            key (K): The key to remove.

        Returns:
            bool: True if removal succeeded, False otherwise.

        """
        with self._lock:
            page, leaf_idx = self._find_leaf(key)
            if page.delete(key):
                self.leaf_offsets[leaf_idx] = self._write_page(page)
                self._write_index()
                return True

            prev_ptr: int | None = None
            ptr = page.overflow_ptr
            while ptr is not None:
                ov = self._read_page(ptr)
                if ov.delete(key):
                    new_ptr = self._write_page(ov)
                    if prev_ptr is None:
                        page.overflow_ptr = new_ptr
                        self.leaf_offsets[leaf_idx] = self._write_page(page)
                    else:
                        prev = self._read_page(prev_ptr)
                        prev.overflow_ptr = new_ptr
                        self._write_page(prev)
                    self._write_index()
                    return True
                prev_ptr, ptr = ptr, ov.overflow_ptr

        return False
