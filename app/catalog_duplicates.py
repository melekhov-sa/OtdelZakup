"""Automatic duplicate/analog detection for internal catalog items.

Computes connected components (via Union-Find) of InternalItem records
that are either:

- Duplicates: same canonical_name_key (normalized full item name)
- Analogs:    equivalent standards (via standard_equivalents table)
              with matching item_type (when both non-empty)
              and matching size (when both non-empty)
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

    Steps: lowercase → ё→е → Cyrillic-х/×/×→Latin-x → decimal comma→dot
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


# ── Size key helper ───────────────────────────────────────────────────────────

def _item_size_key(item) -> str:
    """Return sorted numeric size tokens as "12x60" for an InternalItem."""
    if not (item.size or "").strip():
        return ""
    try:
        from app.matching.normalizer import normalize_size, parse_size_tokens
        toks = sorted(parse_size_tokens(normalize_size(item.size)))
        return "x".join(f"{t:g}" for t in toks) if toks else ""
    except Exception:
        return ""


# ── Parent selection ──────────────────────────────────────────────────────────

def _select_parent(items: list) -> tuple:
    """Return (parent, children) sorted by priority rules.

    Priority (lower sort key = higher priority):
    1. Lowest folder_priority (non-None beats None)
    2. folder_path does NOT contain "основн"  (i.e. "основн" → preferred)
    3. Shortest name
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

        # ── A. Duplicate detection (same canonical_name_key) ─────────────
        if include_duplicates:
            by_name_key: dict[str, list[int]] = defaultdict(list)
            for it in items:
                key = canonical_name_key(it.name or "")
                if key:
                    by_name_key[key].append(it.id)

            for key, ids in by_name_key.items():
                if len(ids) < 2:
                    continue
                for i in range(len(ids)):
                    for j in range(i + 1, len(ids)):
                        a = item_by_id[ids[i]]
                        b = item_by_id[ids[j]]

                        # Type guard: if both have types, they must match
                        a_type = (a.item_type or "").strip().lower()
                        b_type = (b.item_type or "").strip().lower()
                        if a_type and b_type and a_type != b_type:
                            continue

                        # Size guard: if both have sizes, they must match
                        a_sz = _item_size_key(a)
                        b_sz = _item_size_key(b)
                        if a_sz and b_sz and a_sz != b_sz:
                            continue

                        dsu.union(a.id, b.id)
                        edges.append({
                            "a_id": a.id,
                            "b_id": b.id,
                            "reason": "duplicate",
                            "detail": key,
                        })

        # ── B. Analog detection (equivalent standards) ───────────────────
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
                # Index items by their standard_key
                by_std: dict[str, list[int]] = defaultdict(list)
                for it in items:
                    sk = (it.standard_key or "").strip()
                    if sk:
                        by_std[sk].append(it.id)

                visited_pairs: set[frozenset] = set()
                for src_std, dst_stds in equiv_adj.items():
                    src_ids = by_std.get(src_std, [])
                    if not src_ids:
                        continue
                    for dst_std in dst_stds:
                        pair_key = frozenset({src_std, dst_std})
                        if pair_key in visited_pairs:
                            continue
                        visited_pairs.add(pair_key)
                        dst_ids = by_std.get(dst_std, [])
                        if not dst_ids:
                            continue
                        detail = f"{src_std} ↔ {dst_std}"
                        for aid in src_ids:
                            for bid in dst_ids:
                                a = item_by_id[aid]
                                b = item_by_id[bid]

                                # Type guard
                                a_type = (a.item_type or "").strip().lower()
                                b_type = (b.item_type or "").strip().lower()
                                if a_type and b_type and a_type != b_type:
                                    continue

                                # Size guard
                                a_sz = _item_size_key(a)
                                b_sz = _item_size_key(b)
                                if a_sz and b_sz and a_sz != b_sz:
                                    continue

                                dsu.union(a.id, b.id)
                                edges.append({
                                    "a_id": a.id,
                                    "b_id": b.id,
                                    "reason": "analog",
                                    "detail": detail,
                                })

        # ── Build connected components ────────────────────────────────────
        components: dict[int, list[int]] = defaultdict(list)
        for it in items:
            components[dsu.find(it.id)].append(it.id)

        # Index edges by component root (using pre-union DSU state is fine
        # because find() is idempotent after all unions are done)
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

        # Sort by size descending, then by parent name for determinism
        result.sort(key=lambda g: (-g["size"], (g["parent"].name or "").lower()))
        return result

    finally:
        if close_session:
            session.close()
