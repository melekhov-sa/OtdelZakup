"""Full-replace sync of internal catalog from 1C JSON export.

Expected payload structure:
    {
        "folders": [
            {
                "folder_uid":  "GUID",
                "folder_name": "Крепёж основной",
                "parent_uid":  null,
                "folder_path": "Номенклатура/Крепёж основной"
            }, ...
        ],
        "items": [
            {
                "uid_1c":      "GUID",
                "uid_1c_char": "GUID or null",
                "name":        "Болт М8х50 ГОСТ 7798-70",
                "char_name":   "Цинк · Цинк 5.6",   // appended to name for matching
                "folder_uid":  "GUID",
                "folder_name": "Крепёж основной",    // optional, for display
                "folder_path": "Номенклатура/...",   // optional, for display
                "is_active":   true
            }, ...
        ]
    }

Sync logic (full replace of 1C-sourced items):
  1. Upsert all folders by folder_uid; preserve user-set priorities.
  2. Upsert items by (uid_1c, uid_1c_char): parse name+char_name, extract fields.
  3. Items with uid_1c that are NOT in payload → is_active = False (soft-delete).
  4. Rebuild MinHash index.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Folder priority bonus map: priority → extra score points in matcher
PRIORITY_BONUS: dict[int, int] = {1: 8, 2: 4, 3: 2, 4: 1}


def _resolve_priority(folder_uid: str | None, folder_map: dict) -> int | None:
    """Walk up the folder hierarchy to find the nearest assigned priority."""
    uid = folder_uid
    visited: set[str] = set()
    while uid and uid not in visited:
        visited.add(uid)
        folder = folder_map.get(uid)
        if folder is None:
            break
        if folder.priority is not None:
            return folder.priority
        uid = folder.parent_uid
    return None


def sync_from_1c(data: dict[str, Any], session, progress_cb=None) -> dict[str, int]:
    """Perform full-replace sync from 1C JSON payload.

    Returns counts: folders_synced, created, updated, deactivated.
    Raises ValueError on structural errors in the payload.
    """
    from app.models import InternalItem, NomenclatureFolder
    from app.item_parser import parse_internal_item_name
    from app.standard_normalizer import standard_key_from_text
    from app.matching.canonicalize import compute_canonical_key

    if not isinstance(data, dict):
        raise ValueError("Ожидается JSON-объект с ключами 'folders' и 'items'")

    raw_folders: list = data.get("folders") or []
    raw_items: list   = data.get("items") or []

    # ── 1. Upsert folders ─────────────────────────────────────────────────
    folder_map: dict[str, NomenclatureFolder] = {}
    now = datetime.now(timezone.utc)

    for f in raw_folders:
        uid = (f.get("folder_uid") or "").strip()
        if not uid:
            continue
        existing = session.get(NomenclatureFolder, uid)
        if existing:
            existing.folder_name = (f.get("folder_name") or "").strip() or existing.folder_name
            existing.parent_uid  = f.get("parent_uid") or None
            existing.folder_path = (f.get("folder_path") or "").strip() or existing.folder_path
            existing.updated_at  = now
            # Preserve user-set priority (do NOT overwrite)
            folder_map[uid] = existing
        else:
            obj = NomenclatureFolder(
                folder_uid  = uid,
                folder_name = (f.get("folder_name") or "").strip(),
                parent_uid  = f.get("parent_uid") or None,
                folder_path = (f.get("folder_path") or "").strip(),
                priority    = None,
                updated_at  = now,
            )
            session.add(obj)
            session.flush()
            folder_map[uid] = obj

    # ── 2. Upsert items ───────────────────────────────────────────────────
    incoming_pairs: set[tuple[str, str]] = set()   # (uid_1c, uid_1c_char or "")
    created = updated = 0
    total_items = len(raw_items)

    for idx, r in enumerate(raw_items):
        if progress_cb:
            progress_cb(idx, total_items)

        uid_1c = (r.get("uid_1c") or "").strip()
        if not uid_1c:
            continue

        uid_char   = (r.get("uid_1c_char") or "").strip() or None
        pair_key   = (uid_1c, uid_char or "")
        incoming_pairs.add(pair_key)

        # Build full match text: name + char_name
        name_base  = (r.get("name") or "").strip()
        char_name  = (r.get("char_name") or "").strip()
        match_text = f"{name_base} {char_name}".strip() if char_name else name_base

        if not match_text:
            continue

        # Parse extracted fields from match_text
        p        = parse_internal_item_name(match_text)
        std_text = p.get("standard_text") or None
        std_key  = standard_key_from_text(std_text) if std_text else None

        # Folder linkage + priority
        f_uid     = (r.get("folder_uid") or "").strip() or None
        f_name    = (r.get("folder_name") or "").strip() or None
        f_path    = (r.get("folder_path") or "").strip() or None
        priority  = _resolve_priority(f_uid, folder_map)
        is_active = bool(r.get("is_active", True))

        # Find existing by (uid_1c, uid_1c_char)
        q = session.query(InternalItem).filter(InternalItem.uid_1c == uid_1c)
        if uid_char:
            q = q.filter(InternalItem.uid_1c_char == uid_char)
        else:
            q = q.filter(InternalItem.uid_1c_char.is_(None))
        existing = q.first()

        if existing:
            existing.name             = match_text
            existing.name_full        = match_text
            existing.item_type        = p.get("item_type") or None
            existing.size             = p.get("size") or None
            existing.diameter         = p.get("diameter") or None
            existing.length           = p.get("length") or None
            existing.standard_text    = std_text
            existing.standard_key     = std_key
            existing.strength_class   = p.get("strength_class") or None
            existing.material_coating = p.get("material_coating") or None
            existing.parse_status     = p.get("parse_status")
            existing.parse_reason     = p.get("parse_reason") or None
            existing.folder_uid       = f_uid
            existing.folder_name      = f_name
            existing.folder_path      = f_path
            existing.folder_priority  = priority
            existing.is_active        = is_active
            existing.canonical_key    = compute_canonical_key(existing)
            updated += 1
        else:
            obj = InternalItem(
                name             = match_text,
                name_full        = match_text,
                uid_1c           = uid_1c,
                uid_1c_char      = uid_char,
                item_type        = p.get("item_type") or None,
                size             = p.get("size") or None,
                diameter         = p.get("diameter") or None,
                length           = p.get("length") or None,
                standard_text    = std_text,
                standard_key     = std_key,
                strength_class   = p.get("strength_class") or None,
                material_coating = p.get("material_coating") or None,
                parse_status     = p.get("parse_status"),
                parse_reason     = p.get("parse_reason") or None,
                folder_uid       = f_uid,
                folder_name      = f_name,
                folder_path      = f_path,
                folder_priority  = priority,
                is_active        = is_active,
            )
            session.add(obj)
            session.flush()
            obj.canonical_key = compute_canonical_key(obj)
            created += 1

    # ── 3. Soft-delete 1C items missing from payload ──────────────────────
    deactivated = 0
    synced = session.query(InternalItem).filter(InternalItem.uid_1c.isnot(None)).all()
    for item in synced:
        if (item.uid_1c, item.uid_1c_char or "") not in incoming_pairs:
            if item.is_active:
                item.is_active = False
                deactivated += 1

    session.commit()

    # ── 4. Rebuild MinHash index ───────────────────────────────────────────
    try:
        from app.matching.minhash_index import is_index_ready, rebuild_index
        if is_index_ready():
            active_items = session.query(InternalItem).filter_by(is_active=True).all()
            rebuild_index(active_items)
    except Exception:
        logger.exception("MinHash rebuild failed after 1C sync (non-fatal)")

    return {
        "folders_synced": len(folder_map),
        "created":        created,
        "updated":        updated,
        "deactivated":    deactivated,
    }


def update_folder_priorities(priorities: dict[str, int | None], session) -> int:
    """Bulk-update folder priorities from a {folder_uid: priority} map.

    After updating folders, re-resolves folder_priority for all InternalItems
    whose folder is in the updated set (including child folders).

    Returns number of folders updated.
    """
    from app.models import InternalItem, NomenclatureFolder

    updated = 0
    now = datetime.now(timezone.utc)

    # Load all folders into memory for hierarchy traversal
    all_folders = session.query(NomenclatureFolder).all()
    folder_map = {f.folder_uid: f for f in all_folders}

    # Apply priority changes
    for uid, prio in priorities.items():
        folder = folder_map.get(uid)
        if folder is None:
            continue
        folder.priority   = prio
        folder.updated_at = now
        updated += 1

    session.flush()

    # Re-resolve folder_priority for all items that have a folder_uid
    items_with_folder = session.query(InternalItem).filter(
        InternalItem.folder_uid.isnot(None)
    ).all()
    for item in items_with_folder:
        new_prio = _resolve_priority(item.folder_uid, folder_map)
        if item.folder_priority != new_prio:
            item.folder_priority = new_prio

    session.commit()
    return updated
