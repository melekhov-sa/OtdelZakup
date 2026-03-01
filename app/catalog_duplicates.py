"""Automatic duplicate/analog detection for internal catalog items.

Grouping rules (strict — only "exact" matches):

Duplicates:
  composite key = (type_norm, size_key, canonical_name_key)
  ALL three must be non-empty; both standards either absent or identical.

Analogs:
  index key = (type_norm, size_key, standard_norm)
  ALL three must be non-empty; standards linked via standard_equivalents table.

Items without a recognized size_key are excluded from all groups.
"""
from __future__ import annotations

import re
from collections import defaultdict

# ── Text normalization ────────────────────────────────────────────────────────

_YO_TABLE = str.maketrans("ёЁ", "еЕ")
# Cyrillic х/Х and Unicode × → Latin x
_CYR_X_TABLE = str.maketrans("хХ\u00d7*", "xxxx")
# Unit stopwords that add no meaning to the name
_STOP_RE = re.compile(r"(?<![а-яa-z])(?:мм|шт\.?)(?![а-яa-z])", re.UNICODE | re.IGNORECASE)
# Keep only Cyrillic letters, Latin letters, digits, spaces, dots
_CLEAN_RE = re.compile(r"[^а-яa-z0-9\s.]", re.UNICODE)


def canonical_name_key(name: str) -> str:
    """Normalize item name for duplicate detection.

    Steps: lowercase → ё→е → Cyrillic-х/×→Latin-x → decimal comma→dot
           → strip unit stopwords (мм, шт) → remove garbage chars
           → collapse whitespace.
    """
    if not name:
        return ""
    s = name.lower()
    s = s.translate(_YO_TABLE)
    s = s.translate(_CYR_X_TABLE)
    s = re.sub(r"(\d),(\d)", r"\1.\2", s)   # decimal comma → dot
    s = _STOP_RE.sub(" ", s)
    s = _CLEAN_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ── Union-Find ────────────────────────────────────────────────────────────────

class _DSU:
    """Disjoint-Set Union with path compression."""

    def __init__(self):
        self._parent: dict[int, int] = {}

    def _ensure(self, x: int) -> None:
        if x not in self._parent:
            self._parent[x] = x

    def find(self, x: int) -> int:
        self._ensure(x)
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]
            x = self._parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[rb] = ra


# ── Size key ─────────────────────────────────────────────────────────────────

def _item_size_key(item) -> str:
    """Return a canonical size key string for comparison.

    Tries item.size first; falls back to combining item.diameter + item.length.
    Returns "" (treated as "no size") when nothing can be parsed.
    """
    size_text = (item.size or "").strip()

    if not size_text:
        # Fall back to diameter + length fields
        d = (item.diameter or "").strip()
        ln = (item.length or "").strip()
        if d and ln:
            size_text = f"{d}x{ln}"
        elif d:
            size_text = d

    if not size_text:
        return ""

    try:
        from app.matching.normalizer import normalize_size, parse_size_tokens
        toks = sorted(parse_size_tokens(normalize_size(size_text)))
        return "x".join(f"{t:g}" for t in toks) if toks else ""
    except Exception:
        return ""


# ── Parent selection ──────────────────────────────────────────────────────────

def _select_parent(items: list) -> tuple:
    """Return (parent, children) sorted by priority rules.

    Priority (lower sort key = higher priority):
    1. Lowest folder_priority (non-None beats None)
    2. folder_path contains "основн"  (preferred → sort key 0, else 1)
    3. Shortest stored name
    4. uid_1c alphabetically
    5. id ascending (deterministic tie-break)
    """
    _LARGE = 999_999

    def _key(it):
        fp = it.folder_priority if it.folder_priority is not None else _LARGE
        not_osnov = 0 if "основн" in (it.folder_path or "").lower() else 1
        name_len = len(it.name or "")
        uid = it.uid_1c or ""
        return (fp, not_osnov, name_len, uid, it.id)

    sorted_items = sorted(items, key=_key)
    return sorted_items[0], sorted_items[1:]


# ── Child reason lookup ───────────────────────────────────────────────────────

def _child_reason(edges: list[dict], child_id: int) -> tuple[str, str]:
    """Return (reason, detail) for the given child_id from the edges list.

    Prefers 'duplicate' over 'analog' when both exist.
    """
    best: dict | None = None
    for edge in edges:
        if edge["a_id"] == child_id or edge["b_id"] == child_id:
            if best is None or (edge["reason"] == "duplicate" and best["reason"] == "analog"):
                best = edge
    if best is None:
        return ("", "")
    return (best["reason"], best["detail"])


# ── Main analysis engine ──────────────────────────────────────────────────────

def compute_duplicate_groups(
    include_duplicates: bool = True,
    include_analogs: bool = True,
    session=None,
) -> list[dict]:
    """Compute duplicate/analog groups for all active internal catalog items.

    Strict rules — both type_norm and size_key must be non-empty for an item
    to participate in any group.

    Returns a list of group dicts (only groups with ≥ 2 members):
    {
        "parent":     InternalItem,
        "children":   [InternalItem, ...],
        "child_info": [{"child": InternalItem, "reason": str, "detail": str}, ...],
        "edges":      [{"a_id": int, "b_id": int, "reason": str, "detail": str}, ...],
        "size":       int,
    }
    """
    from app.models import InternalItem, StandardEquivalent

    close_session = False
    if session is None:
        from app.database import get_db_session
        session = get_db_session()
        close_session = True

    try:
        items = (
            session.query(InternalItem)
            .filter_by(is_active=True)
            .order_by(InternalItem.id)
            .all()
        )
        if not items:
            return []

        item_by_id = {it.id: it for it in items}
        dsu = _DSU()
        edges: list[dict] = []

        # Seed all item IDs into DSU
        for it in items:
            dsu.find(it.id)

        # ── A. Duplicate detection ────────────────────────────────────────
        # Group by (type_norm, size_key, canonical_name_key).
        # Items without type_norm or size_key are silently excluded.
        if include_duplicates:
            by_dup_key: dict[tuple, list[int]] = defaultdict(list)
            for it in items:
                type_norm = (it.item_type or "").strip().lower()
                sz        = _item_size_key(it)
                can       = canonical_name_key(it.name or "")
                if type_norm and sz and can:
                    by_dup_key[(type_norm, sz, can)].append(it.id)

            for (type_norm, sz, can), ids in by_dup_key.items():
                if len(ids) < 2:
                    continue
                detail = f"{type_norm} | {sz} | {can}"
                for i in range(len(ids)):
                    for j in range(i + 1, len(ids)):
                        a = item_by_id[ids[i]]
                        b = item_by_id[ids[j]]
                        a_std = (a.standard_key or "").strip()
                        b_std = (b.standard_key or "").strip()
                        # Both standards filled and different → analog, not duplicate
                        if a_std and b_std and a_std != b_std:
                            continue
                        dsu.union(a.id, b.id)
                        edges.append({
                            "a_id": a.id,
                            "b_id": b.id,
                            "reason": "duplicate",
                            "detail": detail,
                        })

        # ── B. Analog detection ───────────────────────────────────────────
        # Index: (type_norm, size_key, std_norm) → [item_ids].
        # Items without any of the three fields are excluded.
        if include_analogs:
            std_equivs = (
                session.query(StandardEquivalent)
                .filter_by(is_active=True)
                .all()
            )
            # Build bidirectional adjacency: std_key → set of equivalent keys
            equiv_adj: dict[str, set[str]] = defaultdict(set)
            for se in std_equivs:
                if se.src_canonical and se.dst_canonical:
                    equiv_adj[se.src_canonical].add(se.dst_canonical)
                    equiv_adj[se.dst_canonical].add(se.src_canonical)

            if equiv_adj:
                items_by_std_key: dict[tuple, list[int]] = defaultdict(list)
                for it in items:
                    type_norm = (it.item_type or "").strip().lower()
                    sz        = _item_size_key(it)
                    std       = (it.standard_key or "").strip()
                    if type_norm and sz and std:
                        items_by_std_key[(type_norm, sz, std)].append(it.id)

                visited_analog_pairs: set[frozenset] = set()
                for it in items:
                    type_norm = (it.item_type or "").strip().lower()
                    sz        = _item_size_key(it)
                    std       = (it.standard_key or "").strip()
                    if not (type_norm and sz and std):
                        continue

                    for std2 in equiv_adj.get(std, set()):
                        candidates = items_by_std_key.get((type_norm, sz, std2), [])
                        for cid in candidates:
                            if cid == it.id:
                                continue
                            pair = frozenset({it.id, cid})
                            if pair in visited_analog_pairs:
                                continue
                            visited_analog_pairs.add(pair)
                            detail = f"{std} ↔ {std2} | {type_norm} | {sz}"
                            dsu.union(it.id, cid)
                            edges.append({
                                "a_id": it.id,
                                "b_id": cid,
                                "reason": "analog",
                                "detail": detail,
                            })

        # ── Build connected components ────────────────────────────────────
        components: dict[int, list[int]] = defaultdict(list)
        for it in items:
            components[dsu.find(it.id)].append(it.id)

        edge_by_root: dict[int, list[dict]] = defaultdict(list)
        for edge in edges:
            root = dsu.find(edge["a_id"])
            edge_by_root[root].append(edge)

        # ── Assemble result ───────────────────────────────────────────────
        result = []
        for root, member_ids in components.items():
            if len(member_ids) < 2:
                continue
            group_items = [item_by_id[mid] for mid in member_ids]
            parent, children = _select_parent(group_items)
            grp_edges = edge_by_root[root]

            child_info = [
                {
                    "child": child,
                    "reason": _child_reason(grp_edges, child.id)[0],
                    "detail": _child_reason(grp_edges, child.id)[1],
                }
                for child in children
            ]

            result.append({
                "parent": parent,
                "children": children,
                "child_info": child_info,
                "edges": grp_edges,
                "size": len(member_ids),
            })

        result.sort(key=lambda g: (-g["size"], (g["parent"].name or "").lower()))
        return result

    finally:
        if close_session:
            session.close()
