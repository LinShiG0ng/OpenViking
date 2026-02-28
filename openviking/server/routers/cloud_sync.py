# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""OpenViking 云端同步 API 端点。

提供对云端同步数据库的只读访问，用于管理和监控。
"""

from typing import Optional

from fastapi import APIRouter, Query

from openviking.server.models import Response
from openviking.sync.sync_hooks import get_sync_manager

router = APIRouter(prefix="/api/v1/cloud", tags=["cloud-sync"])


def _get_cloud_db():
    """从同步管理器获取云端数据库实例。"""
    manager = get_sync_manager()
    if not manager:
        return None
    return manager._cloud_db


@router.get("/stats")
async def get_sync_stats():
    """获取同步的总体统计信息。"""
    manager = get_sync_manager()
    if not manager:
        return Response(
            status="error",
            result={"message": "云端同步未启用"},
        )

    cloud_db = _get_cloud_db()
    db_stats = await cloud_db.get_sync_stats()
    manager_stats = manager.stats

    return Response(
        status="ok",
        result={
            "sync_manager": manager_stats,
            "database": db_stats,
        },
    )


@router.get("/contexts")
async def get_synced_contexts(
    context_type: Optional[str] = Query(
        None, description="按类型筛选: resource, memory, skill"
    ),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """获取云端数据库中已同步的上下文。"""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "云端同步未启用"})

    contexts = await cloud_db.get_contexts(
        context_type=context_type, limit=limit, offset=offset
    )
    return Response(status="ok", result={"items": contexts, "count": len(contexts)})


@router.get("/contexts/detail")
async def get_synced_context_detail(
    uri: str = Query(..., description="上下文 URI"),
):
    """根据 URI 获取指定的已同步上下文。"""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "云端同步未启用"})

    context = await cloud_db.get_context_by_uri(uri)
    if not context:
        return Response(status="error",
                        result={"message": f"未找到上下文: {uri}"})
    return Response(status="ok", result=context)


@router.get("/skills")
async def get_synced_skills(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """获取云端数据库中已同步的技能。"""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "云端同步未启用"})

    skills = await cloud_db.get_skills(limit=limit, offset=offset)
    return Response(status="ok", result={"items": skills, "count": len(skills)})


@router.get("/skills/detail")
async def get_synced_skill_detail(
    name: str = Query(..., description="技能名称"),
):
    """根据名称获取指定的已同步技能。"""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "云端同步未启用"})

    skill = await cloud_db.get_skill_by_name(name)
    if not skill:
        return Response(status="error",
                        result={"message": f"未找到技能: {name}"})
    return Response(status="ok", result=skill)


@router.get("/sessions")
async def get_synced_sessions(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """获取云端数据库中已同步的会话。"""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "云端同步未启用"})

    sessions = await cloud_db.get_sessions(limit=limit, offset=offset)
    return Response(status="ok", result={"items": sessions, "count": len(sessions)})


@router.get("/sessions/{session_id}/messages")
async def get_synced_messages(
    session_id: str,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """获取指定会话的已同步消息。"""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "云端同步未启用"})

    messages = await cloud_db.get_messages(
        session_id=session_id, limit=limit, offset=offset
    )
    return Response(
        status="ok", result={"items": messages, "count": len(messages)}
    )


@router.get("/files")
async def get_synced_files(
    content_type: Optional[str] = Query(None, description="按内容类型筛选"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """获取云端数据库中已同步的文件。"""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "云端同步未启用"})

    files = await cloud_db.get_files(
        content_type=content_type, limit=limit, offset=offset
    )
    return Response(status="ok", result={"items": files, "count": len(files)})


@router.get("/files/detail")
async def get_synced_file_detail(
    uri: str = Query(..., description="文件 URI"),
):
    """根据 URI 获取指定的已同步文件内容。"""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "云端同步未启用"})

    file_data = await cloud_db.get_file(uri)
    if not file_data:
        return Response(status="error",
                        result={"message": f"未找到文件: {uri}"})
    return Response(status="ok", result=file_data)


@router.get("/logs")
async def get_sync_logs(
    status: Optional[str] = Query(None, description="按状态筛选"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """获取近期同步日志。"""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "云端同步未启用"})

    logs = await cloud_db.get_sync_logs(
        limit=limit, offset=offset, status=status
    )
    return Response(status="ok", result={"items": logs, "count": len(logs)})
