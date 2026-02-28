# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
OpenViking 云端数据库抽象层。

默认使用 SQLite 作为后端（便于演示和单机部署）。
如需用于生产环境，可替换为 PostgreSQL/MySQL，只需实现相同接口即可。
"""

import asyncio
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from openviking_cli.utils import get_logger

logger = get_logger(__name__)


class CloudDatabase:
    """
    云端数据库，用于存储同步的 OpenViking 数据。

    使用 SQLite WAL 模式支持并发读写，所有操作线程安全。
    """

    def __init__(self, db_path: str = "openviking_cloud.db"):
        self._db_path = db_path
        self._local = threading.local()
        self._lock = threading.Lock()
        self._initialized = False

    def _get_conn(self) -> sqlite3.Connection:
        """获取线程本地的数据库连接。"""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self._db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            self._local.conn = conn
        return self._local.conn

    @contextmanager
    def _cursor(self):
        """获取数据库游标，自动提交/回滚。"""
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def initialize(self) -> None:
        """创建所有必需的数据表。"""
        if self._initialized:
            return

        with self._cursor() as cur:
            # 同步的上下文（技能/记忆/资源）
            cur.execute("""
                CREATE TABLE IF NOT EXISTS synced_contexts (
                    id TEXT PRIMARY KEY,
                    uri TEXT NOT NULL,
                    parent_uri TEXT,
                    is_leaf INTEGER DEFAULT 0,
                    abstract TEXT DEFAULT '',
                    context_type TEXT DEFAULT 'resource',
                    category TEXT DEFAULT '',
                    created_at TEXT,
                    updated_at TEXT,
                    active_count INTEGER DEFAULT 0,
                    meta TEXT DEFAULT '{}',
                    session_id TEXT,
                    synced_at TEXT NOT NULL,
                    sync_source TEXT DEFAULT 'local'
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_ctx_uri ON synced_contexts(uri)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_ctx_type ON synced_contexts(context_type)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_ctx_synced ON synced_contexts(synced_at)"
            )

            # 同步的技能
            cur.execute("""
                CREATE TABLE IF NOT EXISTS synced_skills (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    content TEXT DEFAULT '',
                    uri TEXT NOT NULL,
                    tags TEXT DEFAULT '[]',
                    allowed_tools TEXT DEFAULT '[]',
                    source_path TEXT DEFAULT '',
                    overview TEXT DEFAULT '',
                    created_at TEXT,
                    synced_at TEXT NOT NULL
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_skill_name ON synced_skills(name)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_skill_uri ON synced_skills(uri)"
            )

            # 同步的会话
            cur.execute("""
                CREATE TABLE IF NOT EXISTS synced_sessions (
                    session_id TEXT PRIMARY KEY,
                    user_account TEXT DEFAULT '',
                    user_name TEXT DEFAULT '',
                    user_agent TEXT DEFAULT '',
                    created_at TEXT,
                    updated_at TEXT,
                    total_turns INTEGER DEFAULT 0,
                    total_messages INTEGER DEFAULT 0,
                    compression_count INTEGER DEFAULT 0,
                    synced_at TEXT NOT NULL
                )
            """)

            # 同步的消息
            cur.execute("""
                CREATE TABLE IF NOT EXISTS synced_messages (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT DEFAULT '',
                    parts TEXT DEFAULT '[]',
                    created_at TEXT,
                    synced_at TEXT NOT NULL,
                    FOREIGN KEY (session_id) REFERENCES synced_sessions(session_id)
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_msg_session ON synced_messages(session_id)"
            )

            # 同步的文件内容（压缩上下文、概述、摘要等）
            cur.execute("""
                CREATE TABLE IF NOT EXISTS synced_files (
                    uri TEXT PRIMARY KEY,
                    content TEXT DEFAULT '',
                    content_type TEXT DEFAULT 'text',
                    size_bytes INTEGER DEFAULT 0,
                    synced_at TEXT NOT NULL
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_file_type ON synced_files(content_type)"
            )

            # 同步日志，用于追踪同步操作
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sync_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    uri TEXT DEFAULT '',
                    status TEXT DEFAULT 'success',
                    error_message TEXT DEFAULT '',
                    created_at TEXT NOT NULL
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_log_time ON sync_log(created_at)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_log_status ON sync_log(status)"
            )

        self._initialized = True
        logger.info(f"云端数据库已初始化，路径: {self._db_path}")

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    # =========================================================================
    # 上下文操作
    # =========================================================================

    async def upsert_context(self, context_data: Dict[str, Any]) -> str:
        """插入或更新一条上下文记录。"""
        def _do():
            with self._cursor() as cur:
                now = self._now()
                ctx_id = context_data.get("id", "")
                cur.execute("""
                    INSERT INTO synced_contexts
                        (id, uri, parent_uri, is_leaf, abstract, context_type,
                         category, created_at, updated_at, active_count, meta,
                         session_id, synced_at, sync_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        uri=excluded.uri, parent_uri=excluded.parent_uri,
                        is_leaf=excluded.is_leaf, abstract=excluded.abstract,
                        context_type=excluded.context_type, category=excluded.category,
                        updated_at=excluded.updated_at, active_count=excluded.active_count,
                        meta=excluded.meta, session_id=excluded.session_id,
                        synced_at=excluded.synced_at
                """, (
                    ctx_id,
                    context_data.get("uri", ""),
                    context_data.get("parent_uri", ""),
                    1 if context_data.get("is_leaf") else 0,
                    context_data.get("abstract", ""),
                    context_data.get("context_type", "resource"),
                    context_data.get("category", ""),
                    context_data.get("created_at", now),
                    context_data.get("updated_at", now),
                    context_data.get("active_count", 0),
                    json.dumps(context_data.get("meta", {}), ensure_ascii=False),
                    context_data.get("session_id", ""),
                    now,
                    "local",
                ))
                self._log_sync(cur, "upsert", "context", ctx_id,
                               context_data.get("uri", ""))
                return ctx_id
        return await asyncio.to_thread(_do)

    async def get_contexts(
        self,
        context_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """获取已同步的上下文列表，支持按类型筛选。"""
        def _do():
            with self._cursor() as cur:
                query = "SELECT * FROM synced_contexts"
                params: list = []
                if context_type:
                    query += " WHERE context_type = ?"
                    params.append(context_type)
                query += " ORDER BY synced_at DESC LIMIT ? OFFSET ?"
                params.extend([limit, offset])
                cur.execute(query, params)
                return [self._row_to_dict(row) for row in cur.fetchall()]
        return await asyncio.to_thread(_do)

    async def get_context_by_uri(self, uri: str) -> Optional[Dict[str, Any]]:
        """根据 URI 获取指定上下文。"""
        def _do():
            with self._cursor() as cur:
                cur.execute(
                    "SELECT * FROM synced_contexts WHERE uri = ?", (uri,)
                )
                row = cur.fetchone()
                return self._row_to_dict(row) if row else None
        return await asyncio.to_thread(_do)

    async def delete_context(self, uri: str) -> bool:
        """根据 URI 删除上下文。"""
        def _do():
            with self._cursor() as cur:
                cur.execute(
                    "DELETE FROM synced_contexts WHERE uri = ?", (uri,)
                )
                deleted = cur.rowcount > 0
                if deleted:
                    self._log_sync(cur, "delete", "context", uri, uri)
                return deleted
        return await asyncio.to_thread(_do)

    # =========================================================================
    # 技能操作
    # =========================================================================

    async def upsert_skill(self, skill_data: Dict[str, Any]) -> str:
        """插入或更新一条技能记录。"""
        def _do():
            with self._cursor() as cur:
                now = self._now()
                skill_id = skill_data.get("id", skill_data.get("name", ""))
                cur.execute("""
                    INSERT INTO synced_skills
                        (id, name, description, content, uri, tags,
                         allowed_tools, source_path, overview, created_at, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        name=excluded.name, description=excluded.description,
                        content=excluded.content, uri=excluded.uri,
                        tags=excluded.tags, allowed_tools=excluded.allowed_tools,
                        source_path=excluded.source_path, overview=excluded.overview,
                        synced_at=excluded.synced_at
                """, (
                    skill_id,
                    skill_data.get("name", ""),
                    skill_data.get("description", ""),
                    skill_data.get("content", ""),
                    skill_data.get("uri", ""),
                    json.dumps(skill_data.get("tags", []), ensure_ascii=False),
                    json.dumps(skill_data.get("allowed_tools", []), ensure_ascii=False),
                    skill_data.get("source_path", ""),
                    skill_data.get("overview", ""),
                    skill_data.get("created_at", now),
                    now,
                ))
                self._log_sync(cur, "upsert", "skill", skill_id,
                               skill_data.get("uri", ""))
                return skill_id
        return await asyncio.to_thread(_do)

    async def get_skills(
        self, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """获取所有已同步的技能。"""
        def _do():
            with self._cursor() as cur:
                cur.execute(
                    "SELECT * FROM synced_skills ORDER BY synced_at DESC LIMIT ? OFFSET ?",
                    (limit, offset),
                )
                return [self._row_to_dict(row) for row in cur.fetchall()]
        return await asyncio.to_thread(_do)

    async def get_skill_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """根据名称获取指定技能。"""
        def _do():
            with self._cursor() as cur:
                cur.execute(
                    "SELECT * FROM synced_skills WHERE name = ?", (name,)
                )
                row = cur.fetchone()
                return self._row_to_dict(row) if row else None
        return await asyncio.to_thread(_do)

    # =========================================================================
    # 会话操作
    # =========================================================================

    async def upsert_session(self, session_data: Dict[str, Any]) -> str:
        """插入或更新一条会话记录。"""
        def _do():
            with self._cursor() as cur:
                now = self._now()
                sid = session_data.get("session_id", "")
                cur.execute("""
                    INSERT INTO synced_sessions
                        (session_id, user_account, user_name, user_agent,
                         created_at, updated_at, total_turns, total_messages,
                         compression_count, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id) DO UPDATE SET
                        updated_at=excluded.updated_at,
                        total_turns=excluded.total_turns,
                        total_messages=excluded.total_messages,
                        compression_count=excluded.compression_count,
                        synced_at=excluded.synced_at
                """, (
                    sid,
                    session_data.get("user_account", ""),
                    session_data.get("user_name", ""),
                    session_data.get("user_agent", ""),
                    session_data.get("created_at", now),
                    session_data.get("updated_at", now),
                    session_data.get("total_turns", 0),
                    session_data.get("total_messages", 0),
                    session_data.get("compression_count", 0),
                    now,
                ))
                self._log_sync(cur, "upsert", "session", sid)
                return sid
        return await asyncio.to_thread(_do)

    async def get_sessions(
        self, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """获取所有已同步的会话。"""
        def _do():
            with self._cursor() as cur:
                cur.execute(
                    "SELECT * FROM synced_sessions ORDER BY updated_at DESC LIMIT ? OFFSET ?",
                    (limit, offset),
                )
                return [self._row_to_dict(row) for row in cur.fetchall()]
        return await asyncio.to_thread(_do)

    # =========================================================================
    # 消息操作
    # =========================================================================

    async def upsert_message(self, message_data: Dict[str, Any]) -> str:
        """插入或更新一条消息记录。"""
        def _do():
            with self._cursor() as cur:
                now = self._now()
                msg_id = message_data.get("id", "")
                cur.execute("""
                    INSERT INTO synced_messages
                        (id, session_id, role, content, parts, created_at, synced_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        content=excluded.content, parts=excluded.parts,
                        synced_at=excluded.synced_at
                """, (
                    msg_id,
                    message_data.get("session_id", ""),
                    message_data.get("role", ""),
                    message_data.get("content", ""),
                    json.dumps(message_data.get("parts", []), ensure_ascii=False),
                    message_data.get("created_at", now),
                    now,
                ))
                return msg_id
        return await asyncio.to_thread(_do)

    async def get_messages(
        self, session_id: str, limit: int = 200, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """获取指定会话的消息列表。"""
        def _do():
            with self._cursor() as cur:
                cur.execute(
                    "SELECT * FROM synced_messages WHERE session_id = ? "
                    "ORDER BY created_at ASC LIMIT ? OFFSET ?",
                    (session_id, limit, offset),
                )
                return [self._row_to_dict(row) for row in cur.fetchall()]
        return await asyncio.to_thread(_do)

    # =========================================================================
    # 文件内容操作
    # =========================================================================

    async def upsert_file(self, uri: str, content: str,
                          content_type: str = "text") -> str:
        """插入或更新一条文件内容记录。"""
        def _do():
            with self._cursor() as cur:
                now = self._now()
                cur.execute("""
                    INSERT INTO synced_files (uri, content, content_type, size_bytes, synced_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(uri) DO UPDATE SET
                        content=excluded.content, content_type=excluded.content_type,
                        size_bytes=excluded.size_bytes, synced_at=excluded.synced_at
                """, (
                    uri,
                    content,
                    content_type,
                    len(content.encode("utf-8")) if content else 0,
                    now,
                ))
                self._log_sync(cur, "upsert", "file", uri, uri)
                return uri
        return await asyncio.to_thread(_do)

    async def get_file(self, uri: str) -> Optional[Dict[str, Any]]:
        """根据 URI 获取文件内容。"""
        def _do():
            with self._cursor() as cur:
                cur.execute(
                    "SELECT * FROM synced_files WHERE uri = ?", (uri,)
                )
                row = cur.fetchone()
                return self._row_to_dict(row) if row else None
        return await asyncio.to_thread(_do)

    async def get_files(
        self,
        content_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """获取已同步的文件列表。"""
        def _do():
            with self._cursor() as cur:
                query = "SELECT uri, content_type, size_bytes, synced_at FROM synced_files"
                params: list = []
                if content_type:
                    query += " WHERE content_type = ?"
                    params.append(content_type)
                query += " ORDER BY synced_at DESC LIMIT ? OFFSET ?"
                params.extend([limit, offset])
                cur.execute(query, params)
                return [self._row_to_dict(row) for row in cur.fetchall()]
        return await asyncio.to_thread(_do)

    # =========================================================================
    # 同步日志与统计
    # =========================================================================

    async def get_sync_logs(
        self,
        limit: int = 100,
        offset: int = 0,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """获取近期同步日志。"""
        def _do():
            with self._cursor() as cur:
                query = "SELECT * FROM sync_log"
                params: list = []
                if status:
                    query += " WHERE status = ?"
                    params.append(status)
                query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
                params.extend([limit, offset])
                cur.execute(query, params)
                return [self._row_to_dict(row) for row in cur.fetchall()]
        return await asyncio.to_thread(_do)

    async def get_sync_stats(self) -> Dict[str, Any]:
        """获取同步统计信息。"""
        def _do():
            with self._cursor() as cur:
                stats: Dict[str, Any] = {}
                for table in ["synced_contexts", "synced_skills",
                              "synced_sessions", "synced_messages",
                              "synced_files"]:
                    cur.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                    stats[table] = cur.fetchone()["cnt"]

                # 上下文类型分布
                cur.execute("""
                    SELECT context_type, COUNT(*) as cnt
                    FROM synced_contexts GROUP BY context_type
                """)
                stats["context_type_breakdown"] = {
                    row["context_type"]: row["cnt"] for row in cur.fetchall()
                }

                # 同步状态统计
                cur.execute("""
                    SELECT status, COUNT(*) as cnt
                    FROM sync_log GROUP BY status
                """)
                stats["sync_status"] = {
                    row["status"]: row["cnt"] for row in cur.fetchall()
                }

                # 最后同步时间
                cur.execute(
                    "SELECT MAX(created_at) as last_sync FROM sync_log"
                )
                row = cur.fetchone()
                stats["last_sync_at"] = row["last_sync"] if row else None

                return stats
        return await asyncio.to_thread(_do)

    # =========================================================================
    # 辅助方法
    # =========================================================================

    def _log_sync(self, cur, operation: str, entity_type: str,
                  entity_id: str, uri: str = "",
                  status: str = "success", error: str = "") -> None:
        """写入一条同步日志。"""
        cur.execute("""
            INSERT INTO sync_log (operation, entity_type, entity_id, uri,
                                  status, error_message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (operation, entity_type, entity_id, uri, status, error,
              self._now()))

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        """将 sqlite3.Row 转换为字典。"""
        return dict(row)

    def close(self) -> None:
        """关闭数据库连接。"""
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
