# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
同步钩子：拦截 OpenViking 存储操作，将数据通过 CloudSyncManager 镜像到云端数据库。

这些钩子设计为非侵入式：如果同步管理器未初始化，所有钩子均为空操作（no-op）。
"""

import json
from typing import Any, Dict, List, Optional

from openviking_cli.utils import get_logger

logger = get_logger(__name__)

# 全局同步管理器引用
_sync_manager = None


def set_sync_manager(manager) -> None:
    """设置全局同步管理器。"""
    global _sync_manager
    _sync_manager = manager


def get_sync_manager():
    """获取全局同步管理器。"""
    return _sync_manager


async def on_context_indexed(context_data: Dict[str, Any]) -> None:
    """钩子：当上下文被索引到 VikingDB 时触发。"""
    if not _sync_manager:
        return
    try:
        # 移除向量数据（体积过大，不适合存入云端数据库）
        sync_data = {k: v for k, v in context_data.items()
                     if k not in ("vector", "sparse_vector")}
        await _sync_manager.sync_context(sync_data)
    except Exception as e:
        logger.debug(f"云端同步（上下文索引）失败: {e}")


async def on_context_deleted(uri: str) -> None:
    """钩子：当上下文被删除时触发。"""
    if not _sync_manager:
        return
    try:
        await _sync_manager.sync_context_delete(uri)
    except Exception as e:
        logger.debug(f"云端同步（上下文删除）失败: {e}")


async def on_skill_processed(skill_data: Dict[str, Any]) -> None:
    """钩子：当技能处理完成并存储时触发。"""
    if not _sync_manager:
        return
    try:
        await _sync_manager.sync_skill(skill_data)
    except Exception as e:
        logger.debug(f"云端同步（技能处理）失败: {e}")


async def on_session_updated(
    session_id: str,
    user_data: Optional[Dict[str, Any]] = None,
    stats: Optional[Dict[str, Any]] = None,
) -> None:
    """钩子：当会话创建或更新时触发。"""
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
        logger.debug(f"云端同步（会话更新）失败: {e}")


async def on_message_added(
    session_id: str,
    message_id: str,
    role: str,
    content: str,
    parts: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """钩子：当消息添加到会话时触发。"""
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
        logger.debug(f"云端同步（消息添加）失败: {e}")


async def on_file_written(uri: str, content: str,
                          content_type: str = "text") -> None:
    """钩子：当文件写入 VikingFS 时触发。"""
    if not _sync_manager:
        return
    try:
        await _sync_manager.sync_file(uri, content, content_type)
    except Exception as e:
        logger.debug(f"云端同步（文件写入）失败: {e}")


async def on_file_deleted(uri: str) -> None:
    """钩子：当文件从 VikingFS 删除时触发。"""
    if not _sync_manager:
        return
    try:
        await _sync_manager.sync_file_delete(uri)
    except Exception as e:
        logger.debug(f"云端同步（文件删除）失败: {e}")
