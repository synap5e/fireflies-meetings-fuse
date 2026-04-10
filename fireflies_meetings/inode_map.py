"""Bidirectional path <-> inode mapping for the FUSE filesystem.

Tracks per-inode lookup counts so the kernel's `forget` call can tell
us when to evict entries. Each `get_or_create` (via `lookup` or
`readdir`) bumps the count; `forget(inode, nlookup)` decrements.
When the count drops to zero the entry is removed.
"""

from __future__ import annotations

import pyfuse3


class InodeMap:
    """Manages bidirectional mapping between paths and inodes.

    Inode 1 is reserved for the root.
    """

    def __init__(self) -> None:
        self._path_to_inode: dict[str, int] = {"/": pyfuse3.ROOT_INODE}
        self._inode_to_path: dict[int, str] = {pyfuse3.ROOT_INODE: "/"}
        self._lookup_count: dict[int, int] = {pyfuse3.ROOT_INODE: 1}
        self._next_inode = pyfuse3.ROOT_INODE + 1

    def get_or_create(self, path: str) -> int:
        """Get existing inode for path, or create a new one.

        Always increments the lookup count — each call corresponds to a
        kernel reference that will later be balanced by a `forget`.
        """
        if path in self._path_to_inode:
            inode = self._path_to_inode[path]
            self._lookup_count[inode] = self._lookup_count.get(inode, 0) + 1
            return inode
        inode = self._next_inode
        self._next_inode += 1
        self._path_to_inode[path] = inode
        self._inode_to_path[inode] = path
        self._lookup_count[inode] = 1
        return inode

    def get_inode(self, path: str) -> int | None:
        """Get inode for path, or None if not mapped."""
        return self._path_to_inode.get(path)

    def get_path(self, inode: int) -> str | None:
        """Get path for inode, or None if not mapped."""
        return self._inode_to_path.get(inode)

    def forget(self, inode: int, nlookup: int) -> None:
        """Decrement the lookup count by `nlookup`. Remove the entry when it reaches zero."""
        if inode == pyfuse3.ROOT_INODE:
            return  # never evict root
        count = self._lookup_count.get(inode, 0)
        new_count = count - nlookup
        if new_count <= 0:
            self._lookup_count.pop(inode, None)
            path = self._inode_to_path.pop(inode, None)
            if path is not None:
                self._path_to_inode.pop(path, None)
        else:
            self._lookup_count[inode] = new_count

    @property
    def count(self) -> int:
        """Number of mapped inodes."""
        return len(self._inode_to_path)

    def clear(self) -> None:
        """Reset all mappings except root."""
        self._path_to_inode = {"/": pyfuse3.ROOT_INODE}
        self._inode_to_path = {pyfuse3.ROOT_INODE: "/"}
        self._lookup_count = {pyfuse3.ROOT_INODE: 1}
        self._next_inode = pyfuse3.ROOT_INODE + 1
