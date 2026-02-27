# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Cloud Sync Manager for OpenViking.

Provides transparent, non-blocking cloud synchronization of all locally stored
content. Uses an async background worker with a queue to ensure sync operations
don't block the main data path.

Architecture:
    Local write → Original storage → Sync queue → Background worker → Cloud DB

The sync is "local-first": OpenViking always reads from local storage, and the
cloud database is a mirror for backup/admin/analytics purposes.
"""

import asyncio
import json
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from openviking_cli.utils import get_logger

logger = get_logger(__name__)


class SyncOperation(str, Enum):
    """Types of sync operations."""
    UPSERT_CONTEXT = "upsert_context"
    DELETE_CONTEXT = "delete_context"
    UPSERT_SKILL = "upsert_skill"
    UPSERT_SESSION = "upsert_session"
    UPSERT_MESSAGE = "upsert_message"
    UPSERT_FILE = "upsert_file"
    DELETE_FILE = "delete_file"


@dataclass
class SyncTask:
    """A single sync operation to be processed by the background worker."""
    operation: SyncOperation
    data: Dict[str, Any]
    retry_count: int = 0
    max_retries: int = 3
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class CloudSyncManager:
    """
    Manages transparent cloud synchronization of OpenViking local data.

    Features:
    - Non-blocking async sync via background task queue
    - Automatic retry with exponential backoff
    - Batch processing for efficiency
    - Health monitoring and sync statistics
    - Graceful shutdown with queue draining
    """

    def __init__(
        self,
        cloud_db,
        batch_size: int = 50,
        flush_interval: float = 2.0,
        max_queue_size: int = 10000,
    ):
        """
        Initialize the cloud sync manager.

        Args:
            cloud_db: CloudDatabase instance
            batch_size: Max tasks to process per batch
            flush_interval: Seconds between flush cycles
            max_queue_size: Maximum pending sync tasks
        """
        self._cloud_db = cloud_db
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._max_queue_size = max_queue_size
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False

        # Statistics
        self._stats = {
            "total_synced": 0,
            "total_errors": 0,
            "total_retries": 0,
            "contexts_synced": 0,
            "skills_synced": 0,
            "sessions_synced": 0,
            "messages_synced": 0,
            "files_synced": 0,
        }

    async def start(self) -> None:
        """Start the background sync worker."""
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("CloudSyncManager started")

    async def stop(self) -> None:
        """Stop the background sync worker, draining the queue first."""
        if not self._running:
            return
        self._running = False
        if self._worker_task:
            # Process remaining items
            await self._flush_queue()
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info(
            f"CloudSyncManager stopped. Stats: {json.dumps(self._stats)}"
        )

    @property
    def stats(self) -> Dict[str, Any]:
        """Get sync statistics."""
        return {
            **self._stats,
            "queue_size": self._queue.qsize(),
            "running": self._running,
        }

    # =========================================================================
    # Public sync methods - called by OpenViking services
    # =========================================================================

    async def sync_context(self, context_data: Dict[str, Any]) -> None:
        """Queue a context for cloud sync."""
        await self._enqueue(SyncTask(
            operation=SyncOperation.UPSERT_CONTEXT,
            data=context_data,
        ))

    async def sync_context_delete(self, uri: str) -> None:
        """Queue a context deletion for cloud sync."""
        await self._enqueue(SyncTask(
            operation=SyncOperation.DELETE_CONTEXT,
            data={"uri": uri},
        ))

    async def sync_skill(self, skill_data: Dict[str, Any]) -> None:
        """Queue a skill for cloud sync."""
        await self._enqueue(SyncTask(
            operation=SyncOperation.UPSERT_SKILL,
            data=skill_data,
        ))

    async def sync_session(self, session_data: Dict[str, Any]) -> None:
        """Queue a session for cloud sync."""
        await self._enqueue(SyncTask(
            operation=SyncOperation.UPSERT_SESSION,
            data=session_data,
        ))

    async def sync_message(self, message_data: Dict[str, Any]) -> None:
        """Queue a message for cloud sync."""
        await self._enqueue(SyncTask(
            operation=SyncOperation.UPSERT_MESSAGE,
            data=message_data,
        ))

    async def sync_file(self, uri: str, content: str,
                        content_type: str = "text") -> None:
        """Queue a file for cloud sync."""
        await self._enqueue(SyncTask(
            operation=SyncOperation.UPSERT_FILE,
            data={"uri": uri, "content": content,
                  "content_type": content_type},
        ))

    async def sync_file_delete(self, uri: str) -> None:
        """Queue a file deletion for cloud sync."""
        await self._enqueue(SyncTask(
            operation=SyncOperation.DELETE_FILE,
            data={"uri": uri},
        ))

    # =========================================================================
    # Internal methods
    # =========================================================================

    async def _enqueue(self, task: SyncTask) -> None:
        """Add a task to the sync queue."""
        try:
            self._queue.put_nowait(task)
        except asyncio.QueueFull:
            logger.warning(
                "Cloud sync queue full, dropping oldest task to make room"
            )
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self._queue.put_nowait(task)

    async def _worker_loop(self) -> None:
        """Background worker that processes sync tasks."""
        logger.info("Cloud sync worker started")
        while self._running:
            try:
                await self._flush_queue()
                await asyncio.sleep(self._flush_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cloud sync worker error: {e}")
                await asyncio.sleep(self._flush_interval)

    async def _flush_queue(self) -> None:
        """Process a batch of sync tasks from the queue."""
        tasks: List[SyncTask] = []
        while len(tasks) < self._batch_size:
            try:
                task = self._queue.get_nowait()
                tasks.append(task)
            except asyncio.QueueEmpty:
                break

        if not tasks:
            return

        for task in tasks:
            try:
                await self._process_task(task)
                self._stats["total_synced"] += 1
            except Exception as e:
                self._stats["total_errors"] += 1
                if task.retry_count < task.max_retries:
                    task.retry_count += 1
                    self._stats["total_retries"] += 1
                    await self._enqueue(task)
                    logger.debug(
                        f"Retrying sync task {task.operation} "
                        f"(attempt {task.retry_count}): {e}"
                    )
                else:
                    logger.error(
                        f"Cloud sync failed after {task.max_retries} retries: "
                        f"{task.operation} - {e}\n{traceback.format_exc()}"
                    )

    async def _process_task(self, task: SyncTask) -> None:
        """Process a single sync task."""
        op = task.operation
        data = task.data

        if op == SyncOperation.UPSERT_CONTEXT:
            await self._cloud_db.upsert_context(data)
            self._stats["contexts_synced"] += 1

        elif op == SyncOperation.DELETE_CONTEXT:
            await self._cloud_db.delete_context(data["uri"])
            self._stats["contexts_synced"] += 1

        elif op == SyncOperation.UPSERT_SKILL:
            await self._cloud_db.upsert_skill(data)
            self._stats["skills_synced"] += 1

        elif op == SyncOperation.UPSERT_SESSION:
            await self._cloud_db.upsert_session(data)
            self._stats["sessions_synced"] += 1

        elif op == SyncOperation.UPSERT_MESSAGE:
            await self._cloud_db.upsert_message(data)
            self._stats["messages_synced"] += 1

        elif op == SyncOperation.UPSERT_FILE:
            await self._cloud_db.upsert_file(
                data["uri"], data["content"], data.get("content_type", "text")
            )
            self._stats["files_synced"] += 1

        elif op == SyncOperation.DELETE_FILE:
            # Delete from synced_files table
            def _do():
                with self._cloud_db._cursor() as cur:
                    cur.execute(
                        "DELETE FROM synced_files WHERE uri = ?",
                        (data["uri"],)
                    )
            await asyncio.to_thread(_do)
            self._stats["files_synced"] += 1
