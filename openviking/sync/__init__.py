# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Cloud sync module for OpenViking.

Provides transparent cloud synchronization of all locally stored content
including contexts, skills, sessions, compressed content, and memories.
"""

from openviking.sync.cloud_db import CloudDatabase
from openviking.sync.sync_hooks import get_sync_manager, set_sync_manager
from openviking.sync.sync_manager import CloudSyncManager

__all__ = [
    "CloudDatabase",
    "CloudSyncManager",
    "get_sync_manager",
    "set_sync_manager",
]
