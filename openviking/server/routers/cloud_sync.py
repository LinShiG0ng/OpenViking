# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Cloud sync API endpoints for OpenViking.

Provides read-only access to the cloud-synced database for admin/monitoring purposes.
"""

from typing import Optional

from fastapi import APIRouter, Query

from openviking.server.models import Response
from openviking.sync.sync_hooks import get_sync_manager

router = APIRouter(prefix="/api/v1/cloud", tags=["cloud-sync"])


def _get_cloud_db():
    """Get the cloud database instance from the sync manager."""
    manager = get_sync_manager()
    if not manager:
        return None
    return manager._cloud_db


@router.get("/stats")
async def get_sync_stats():
    """Get overall sync statistics."""
    manager = get_sync_manager()
    if not manager:
        return Response(
            status="error",
            result={"message": "Cloud sync not enabled"},
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
        None, description="Filter by type: resource, memory, skill"
    ),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get synced contexts from the cloud database."""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "Cloud sync not enabled"})

    contexts = await cloud_db.get_contexts(
        context_type=context_type, limit=limit, offset=offset
    )
    return Response(status="ok", result={"items": contexts, "count": len(contexts)})


@router.get("/contexts/detail")
async def get_synced_context_detail(
    uri: str = Query(..., description="Context URI"),
):
    """Get a specific synced context by URI."""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "Cloud sync not enabled"})

    context = await cloud_db.get_context_by_uri(uri)
    if not context:
        return Response(status="error",
                        result={"message": f"Context not found: {uri}"})
    return Response(status="ok", result=context)


@router.get("/skills")
async def get_synced_skills(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get synced skills from the cloud database."""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "Cloud sync not enabled"})

    skills = await cloud_db.get_skills(limit=limit, offset=offset)
    return Response(status="ok", result={"items": skills, "count": len(skills)})


@router.get("/skills/detail")
async def get_synced_skill_detail(
    name: str = Query(..., description="Skill name"),
):
    """Get a specific synced skill by name."""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "Cloud sync not enabled"})

    skill = await cloud_db.get_skill_by_name(name)
    if not skill:
        return Response(status="error",
                        result={"message": f"Skill not found: {name}"})
    return Response(status="ok", result=skill)


@router.get("/sessions")
async def get_synced_sessions(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get synced sessions from the cloud database."""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "Cloud sync not enabled"})

    sessions = await cloud_db.get_sessions(limit=limit, offset=offset)
    return Response(status="ok", result={"items": sessions, "count": len(sessions)})


@router.get("/sessions/{session_id}/messages")
async def get_synced_messages(
    session_id: str,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get synced messages for a session."""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "Cloud sync not enabled"})

    messages = await cloud_db.get_messages(
        session_id=session_id, limit=limit, offset=offset
    )
    return Response(
        status="ok", result={"items": messages, "count": len(messages)}
    )


@router.get("/files")
async def get_synced_files(
    content_type: Optional[str] = Query(None, description="Filter by content type"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get synced files from the cloud database."""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "Cloud sync not enabled"})

    files = await cloud_db.get_files(
        content_type=content_type, limit=limit, offset=offset
    )
    return Response(status="ok", result={"items": files, "count": len(files)})


@router.get("/files/detail")
async def get_synced_file_detail(
    uri: str = Query(..., description="File URI"),
):
    """Get a specific synced file content by URI."""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "Cloud sync not enabled"})

    file_data = await cloud_db.get_file(uri)
    if not file_data:
        return Response(status="error",
                        result={"message": f"File not found: {uri}"})
    return Response(status="ok", result=file_data)


@router.get("/logs")
async def get_sync_logs(
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get recent sync log entries."""
    cloud_db = _get_cloud_db()
    if not cloud_db:
        return Response(status="error",
                        result={"message": "Cloud sync not enabled"})

    logs = await cloud_db.get_sync_logs(
        limit=limit, offset=offset, status=status
    )
    return Response(status="ok", result={"items": logs, "count": len(logs)})
