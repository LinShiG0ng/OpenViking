# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Sync hooks that intercept OpenViking storage operations and mirror them
to the cloud database via CloudSyncManager.

These hooks are designed to be non-intrusive: if the sync manager is not
initialized, all hooks are no-ops.
"""

import json
from typing import Any, Dict, List, Optional

from openviking_cli.utils import get_logger

logger = get_logger(__name__)

# Global sync manager reference
_sync_manager = None


def set_sync_manager(manager) -> None:
    """Set the global sync manager."""
    global _sync_manager
    _sync_manager = manager


def get_sync_manager():
    """Get the global sync manager."""
    return _sync_manager


async def on_context_indexed(context_data: Dict[str, Any]) -> None:
    """Hook: called when a context is indexed to VikingDB."""
    if not _sync_manager:
        return
    try:
        # Remove vector data (too large for cloud DB)
        sync_data = {k: v for k, v in context_data.items()
                     if k not in ("vector", "sparse_vector")}
        await _sync_manager.sync_context(sync_data)
    except Exception as e:
        logger.debug(f"Cloud sync (context indexed) failed: {e}")


async def on_context_deleted(uri: str) -> None:
    """Hook: called when a context is deleted."""
    if not _sync_manager:
        return
    try:
        await _sync_manager.sync_context_delete(uri)
    except Exception as e:
        logger.debug(f"Cloud sync (context deleted) failed: {e}")


async def on_skill_processed(skill_data: Dict[str, Any]) -> None:
    """Hook: called when a skill is processed and stored."""
    if not _sync_manager:
        return
    try:
        await _sync_manager.sync_skill(skill_data)
    except Exception as e:
        logger.debug(f"Cloud sync (skill processed) failed: {e}")


async def on_session_updated(
    session_id: str,
    user_data: Optional[Dict[str, Any]] = None,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    """Hook: called when a session is created or updated."""
    if not _sync_manager:
        return
    try:
        session_data = {
            "session_id": session_id,
            "user_account": (user_data or {}).get("account", ""),
            "user_name": (user_data or {}).get("user", ""),
            "user_agent": (user_data or {}).get("agent", ""),
            "total_turns": (stats or {}).get("total_turns", 0),
            "total_messages": (stats or {}).get("total_messages", 0),
            "compression_count": (stats or {}).get("compression_count", 0),
        }
        await _sync_manager.sync_session(session_data)
    except Exception as e:
        logger.debug(f"Cloud sync (session updated) failed: {e}")


async def on_message_added(
    session_id: str,
    message_id: str,
    role: str,
    content: str,
    parts: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """Hook: called when a message is added to a session."""
    if not _sync_manager:
        return
    try:
        message_data = {
            "id": message_id,
            "session_id": session_id,
            "role": role,
            "content": content,
            "parts": parts or [],
        }
        await _sync_manager.sync_message(message_data)
    except Exception as e:
        logger.debug(f"Cloud sync (message added) failed: {e}")


async def on_file_written(uri: str, content: str,
                          content_type: str = "text") -> None:
    """Hook: called when a file is written to VikingFS."""
    if not _sync_manager:
        return
    try:
        await _sync_manager.sync_file(uri, content, content_type)
    except Exception as e:
        logger.debug(f"Cloud sync (file written) failed: {e}")


async def on_file_deleted(uri: str) -> None:
    """Hook: called when a file is deleted from VikingFS."""
    if not _sync_manager:
        return
    try:
        await _sync_manager.sync_file_delete(uri)
    except Exception as e:
        logger.debug(f"Cloud sync (file deleted) failed: {e}")
