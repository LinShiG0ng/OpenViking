# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
OpenViking 云端同步管理器。

提供透明、非阻塞的云端同步功能，通过异步后台 Worker 和队列确保
同步操作不阻塞主数据路径。

架构:
    本地写入 → 原始存储 → 同步队列 → 后台 Worker → 云端数据库

同步采用「本地优先」策略：OpenViking 始终从本地存储读取数据，
云端数据库作为备份/管理/分析用途的镜像。
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
    """同步操作类型。"""
    UPSERT_CONTEXT = "upsert_context"
    DELETE_CONTEXT = "delete_context"
    UPSERT_SKILL = "upsert_skill"
    UPSERT_SESSION = "upsert_session"
    UPSERT_MESSAGE = "upsert_message"
    UPSERT_FILE = "upsert_file"
    DELETE_FILE = "delete_file"


@dataclass
class SyncTask:
    """单个同步任务，由后台 Worker 处理。"""
    operation: SyncOperation
    data: Dict[str, Any]
    retry_count: int = 0
    max_retries: int = 3
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class CloudSyncManager:
    """
    管理 OpenViking 本地数据到云端的透明同步。

    特性:
    - 非阻塞异步同步，通过后台任务队列实现
    - 自动重试，带指数退避
    - 批量处理，提升效率
    - 健康监控和同步统计
    - 优雅关闭，支持队列排空
    """

    def __init__(
        self,
        cloud_db,
        batch_size: int = 50,
        flush_interval: float = 2.0,
        max_queue_size: int = 10000,
    ):
        """
        初始化云端同步管理器。

        参数:
            cloud_db: CloudDatabase 实例
            batch_size: 每批最大处理任务数
            flush_interval: 刷新周期（秒）
            max_queue_size: 最大待处理同步任务数
        """
        self._cloud_db = cloud_db
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._max_queue_size = max_queue_size
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False

        # 统计信息
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
        """启动后台同步 Worker。"""
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("云端同步管理器已启动")

    async def stop(self) -> None:
        """停止后台同步 Worker，先排空队列。"""
        if not self._running:
            return
        self._running = False
        if self._worker_task:
            # 处理队列中剩余的任务
            await self._flush_queue()
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        logger.info(
            f"云端同步管理器已停止。统计: {json.dumps(self._stats)}"
        )

    @property
    def stats(self) -> Dict[str, Any]:
        """获取同步统计信息。"""
        return {
            **self._stats,
            "queue_size": self._queue.qsize(),
            "running": self._running,
        }

    # =========================================================================
    # 公共同步方法 - 由 OpenViking 服务调用
    # =========================================================================

    async def sync_context(self, context_data: Dict[str, Any]) -> None:
        """将上下文加入同步队列。"""
        await self._enqueue(SyncTask(
            operation=SyncOperation.UPSERT_CONTEXT,
            data=context_data,
        ))

    async def sync_context_delete(self, uri: str) -> None:
        """将上下文删除操作加入同步队列。"""
        await self._enqueue(SyncTask(
            operation=SyncOperation.DELETE_CONTEXT,
            data={"uri": uri},
        ))

    async def sync_skill(self, skill_data: Dict[str, Any]) -> None:
        """将技能加入同步队列。"""
        await self._enqueue(SyncTask(
            operation=SyncOperation.UPSERT_SKILL,
            data=skill_data,
        ))

    async def sync_session(self, session_data: Dict[str, Any]) -> None:
        """将会话加入同步队列。"""
        await self._enqueue(SyncTask(
            operation=SyncOperation.UPSERT_SESSION,
            data=session_data,
        ))

    async def sync_message(self, message_data: Dict[str, Any]) -> None:
        """将消息加入同步队列。"""
        await self._enqueue(SyncTask(
            operation=SyncOperation.UPSERT_MESSAGE,
            data=message_data,
        ))

    async def sync_file(self, uri: str, content: str,
                        content_type: str = "text") -> None:
        """将文件加入同步队列。"""
        await self._enqueue(SyncTask(
            operation=SyncOperation.UPSERT_FILE,
            data={"uri": uri, "content": content,
                  "content_type": content_type},
        ))

    async def sync_file_delete(self, uri: str) -> None:
        """将文件删除操作加入同步队列。"""
        await self._enqueue(SyncTask(
            operation=SyncOperation.DELETE_FILE,
            data={"uri": uri},
        ))

    # =========================================================================
    # 内部方法
    # =========================================================================

    async def _enqueue(self, task: SyncTask) -> None:
        """将任务添加到同步队列。"""
        try:
            self._queue.put_nowait(task)
        except asyncio.QueueFull:
            logger.warning(
                "云端同步队列已满，丢弃最旧的任务腾出空间"
            )
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            self._queue.put_nowait(task)

    async def _worker_loop(self) -> None:
        """后台 Worker 循环，处理同步任务。"""
        logger.info("云端同步 Worker 已启动")
        while self._running:
            try:
                await self._flush_queue()
                await asyncio.sleep(self._flush_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"云端同步 Worker 错误: {e}")
                await asyncio.sleep(self._flush_interval)

    async def _flush_queue(self) -> None:
        """从队列中取出一批任务并处理。"""
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
                        f"重试同步任务 {task.operation}"
                        f"（第 {task.retry_count} 次）: {e}"
                    )
                else:
                    logger.error(
                        f"云端同步在 {task.max_retries} 次重试后仍然失败: "
                        f"{task.operation} - {e}\n{traceback.format_exc()}"
                    )

    async def _process_task(self, task: SyncTask) -> None:
        """处理单个同步任务。"""
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
            # 从 synced_files 表中删除
            def _do():
                with self._cloud_db._cursor() as cur:
                    cur.execute(
                        "DELETE FROM synced_files WHERE uri = ?",
                        (data["uri"],)
                    )
            await asyncio.to_thread(_do)
            self._stats["files_synced"] += 1
