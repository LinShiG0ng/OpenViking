# OpenViking 云端同步架构 - 安装、部署与使用指南

## 目录

- [架构概述](#架构概述)
- [环境要求](#环境要求)
- [安装步骤](#安装步骤)
- [配置说明](#配置说明)
- [启动服务](#启动服务)
- [前端页面使用](#前端页面使用)
- [后台管理页面](#后台管理页面)
- [API 接口文档](#api-接口文档)
- [同步机制详解](#同步机制详解)
- [故障排查](#故障排查)

---

## 架构概述

云端同步架构采用 **Local-First（本地优先）** 设计，OpenViking 的所有读取操作仍然从本地存储加载，同时在后台将所有数据透明同步到云端数据库。

```
用户操作 → 本地存储 (AGFS/VikingDB) → 同步队列 → 后台Worker → 云端数据库 (SQLite)
                 ↑                                              ↓
            原始读取路径                                   Admin Dashboard 查看
```

### 同步范围

| 数据类型 | 说明 | 同步时机 |
|---------|------|---------|
| Context（上下文） | 资源、记忆、技能的向量索引记录 | 向量化完成时 |
| Skill（技能） | 技能定义、描述、内容 | 技能处理完成时 |
| Session（会话） | 会话元数据、统计信息 | 消息添加时 |
| Message（消息） | 对话消息（用户/助手） | 每条消息添加时 |
| File（文件） | abstract、overview 等文本文件 | 文件写入/删除时 |

### 核心设计原则

1. **非阻塞同步**：所有同步操作通过异步队列处理，不阻塞主数据路径
2. **优雅降级**：云端同步失败不影响 OpenViking 原有功能
3. **自动重试**：失败任务自动重试最多 3 次，带指数退避
4. **批量处理**：每批最多处理 50 个同步任务，2 秒刷新间隔

---

## 环境要求

- Python >= 3.10
- OpenViking 已正确安装（参考项目主 README）
- 已配置 `~/.openviking/ov.conf` 配置文件
- （可选）LLM API Key（用于 Chat 功能，支持 OpenAI / Volcengine / 其他 litellm 兼容模型）

---

## 安装步骤

### 1. 安装 OpenViking

```bash
# 克隆仓库
git clone https://github.com/volcengine/openviking.git
cd openviking

# 安装依赖
pip install -e .
```

### 2. 验证安装

```bash
# 检查 openviking-server 命令是否可用
openviking-server --help
```

### 3. 配置文件

确保 `~/.openviking/ov.conf` 存在且配置正确。最小配置示例：

```json
{
  "storage": {
    "workspace": "/tmp/openviking-data",
    "agfs": {
      "backend": "local",
      "timeout": 10
    },
    "vectordb": {
      "backend": "local",
      "name": "openviking"
    }
  },
  "embedding": {
    "dense": {
      "provider": "openai",
      "model": "text-embedding-3-small",
      "dimension": 1536,
      "api_key": "your-api-key"
    }
  },
  "vlm": {
    "provider": "openai",
    "model": "gpt-4o-mini",
    "api_key": "your-api-key"
  },
  "server": {
    "host": "0.0.0.0",
    "port": 1933,
    "cors_origins": ["*"]
  }
}
```

---

## 配置说明

### 云端同步相关环境变量

| 环境变量 | 说明 | 默认值 | 示例 |
|---------|------|--------|------|
| `OPENVIKING_CLOUD_SYNC` | 启用云端同步 | 未启用 | `true` |
| `OPENVIKING_CLOUD_DB_PATH` | 云端数据库文件路径 | `openviking_cloud.db` | `/data/cloud.db` |

### Chat 功能相关

Chat 功能使用 `ov.conf` 中配置的 VLM 模型，也可以在请求时通过 `model` 参数指定其他 litellm 兼容模型。

支持的模型格式（通过 litellm）：
- `gpt-4o-mini`、`gpt-4o` — OpenAI 模型
- `claude-sonnet-4-20250514` — Anthropic 模型
- `deepseek/deepseek-chat` — DeepSeek 模型
- 其他 litellm 支持的模型格式

---

## 启动服务

### 方式一：启用云端同步

```bash
# 设置环境变量并启动
export OPENVIKING_CLOUD_SYNC=true
export OPENVIKING_CLOUD_DB_PATH=/path/to/openviking_cloud.db

openviking-server
```

或者一行命令：

```bash
OPENVIKING_CLOUD_SYNC=true OPENVIKING_CLOUD_DB_PATH=./cloud.db openviking-server
```

### 方式二：不启用云端同步（仅使用 Chat 和原有功能）

```bash
openviking-server
```

### 方式三：指定配置文件和端口

```bash
OPENVIKING_CLOUD_SYNC=true openviking-server --config /path/to/ov.conf --port 8080
```

### 方式四：Docker 部署

```bash
docker build -t openviking .

docker run -d \
  -p 1933:1933 \
  -e OPENVIKING_CLOUD_SYNC=true \
  -e OPENVIKING_CLOUD_DB_PATH=/data/cloud.db \
  -v /path/to/ov.conf:/root/.openviking/ov.conf \
  -v /path/to/data:/data \
  openviking
```

### 启动成功日志

启动成功后，你会看到类似以下日志：

```
OpenVikingService initialized
Cloud sync initialized (db: openviking_cloud.db)
OpenViking HTTP Server is running on 0.0.0.0:1933
```

---

## 前端页面使用

### Chat 对话页面

**访问地址**: `http://localhost:1933/static/chat.html`

#### 功能说明

1. **新建对话**：点击左上角 "New Conversation" 按钮
2. **发送消息**：在底部输入框输入内容，按 Enter 或点击 "Send"
3. **RAG 上下文增强**：勾选 "Use Context (RAG)" 后，系统会自动搜索相关上下文注入到 LLM 提示中
4. **指定模型**：在 "Model" 输入框中填入模型名称（如 `gpt-4o`）
5. **历史会话**：左侧 "Sessions" 标签页列出所有历史会话，点击可加载
6. **查看技能**：左侧 "Skills" 标签页列出所有已创建的技能

#### 创建技能

1. 点击左下角绿色 "Create Skill" 按钮
2. 填写技能信息：
   - **Skill Name**: 技能名称（英文，如 `code_review`）
   - **Description**: 技能描述
   - **Tags**: 标签（逗号分隔）
   - **Content**: 技能的详细指令内容
3. 点击 "Create Skill" 提交

#### 操作示例

```
1. 打开 http://localhost:1933/static/chat.html
2. 点击 "New Conversation"
3. 输入: "帮我写一个 Python 快速排序算法"
4. 等待 AI 回复
5. 继续对话: "请加上详细注释"
```

---

## 后台管理页面

**访问地址**: `http://localhost:1933/static/admin.html`

#### 概览（Overview）

展示云端同步的整体统计信息：
- 已同步的 Context 数量（按类型分布：resource/memory/skill）
- 已同步的 Skill 数量
- 已同步的 Session 数量
- 已同步的 Message 数量
- 已同步的 File 数量
- 同步管理器运行状态和队列大小

#### Contexts 页面

- 浏览所有已同步的上下文记录
- 按类型筛选：All / Resources / Memories / Skills
- 点击行查看详细信息（URI、类型、摘要、元数据等）

#### Skills 页面

- 查看所有已同步的技能
- 显示技能名称、描述、URI、标签、同步时间
- 点击 "Detail" 查看完整技能内容和 Overview

#### Sessions 页面

- 查看所有已同步的会话
- 显示会话 ID、用户、轮次、消息数、压缩次数
- 点击 "Messages" 查看该会话的所有消息内容

#### Files 页面

- 查看所有已同步的文件（abstract、overview 等）
- 显示文件 URI、类型、大小
- 点击 "View" 查看完整文件内容

#### Sync Logs 页面

- 查看最近的同步日志
- 显示时间、操作类型、实体类型、状态
- 可用于排查同步问题

---

## API 接口文档

### Chat API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/chat/completions` | 发送消息并获取 AI 回复 |
| POST | `/api/v1/chat/skills` | 创建新技能 |
| GET | `/api/v1/chat/skills` | 列出所有技能 |
| GET | `/api/v1/chat/sessions` | 列出所有会话 |
| GET | `/api/v1/chat/sessions/{id}/messages` | 获取会话消息 |

#### POST /api/v1/chat/completions

```json
{
  "message": "你好，请帮我写一段代码",
  "session_id": null,
  "model": "gpt-4o-mini",
  "use_context": true,
  "max_context_items": 5,
  "stream": false
}
```

响应：

```json
{
  "status": "ok",
  "result": {
    "session_id": "uuid-xxx",
    "message": "AI 的回复内容...",
    "context_used": 3,
    "model": "gpt-4o-mini"
  }
}
```

#### POST /api/v1/chat/skills

```json
{
  "name": "code_review",
  "description": "代码审查技能",
  "content": "请审查以下代码，关注安全性、性能和可读性...",
  "tags": ["code", "review"],
  "allowed_tools": []
}
```

### Cloud Sync API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/cloud/stats` | 获取同步统计信息 |
| GET | `/api/v1/cloud/contexts` | 查看已同步的上下文 |
| GET | `/api/v1/cloud/contexts/detail?uri=xxx` | 查看上下文详情 |
| GET | `/api/v1/cloud/skills` | 查看已同步的技能 |
| GET | `/api/v1/cloud/skills/detail?name=xxx` | 查看技能详情 |
| GET | `/api/v1/cloud/sessions` | 查看已同步的会话 |
| GET | `/api/v1/cloud/sessions/{id}/messages` | 查看会话消息 |
| GET | `/api/v1/cloud/files` | 查看已同步的文件 |
| GET | `/api/v1/cloud/files/detail?uri=xxx` | 查看文件内容 |
| GET | `/api/v1/cloud/logs` | 查看同步日志 |

所有列表接口支持 `limit` 和 `offset` 分页参数。

#### GET /api/v1/cloud/stats 响应示例

```json
{
  "status": "ok",
  "result": {
    "sync_manager": {
      "total_synced": 156,
      "total_errors": 0,
      "contexts_synced": 42,
      "skills_synced": 5,
      "sessions_synced": 8,
      "messages_synced": 89,
      "files_synced": 12,
      "queue_size": 0,
      "running": true
    },
    "database": {
      "synced_contexts": 42,
      "synced_skills": 5,
      "synced_sessions": 8,
      "synced_messages": 89,
      "synced_files": 12,
      "context_type_breakdown": {
        "resource": 30,
        "memory": 7,
        "skill": 5
      },
      "last_sync_at": "2026-02-27T10:30:00+00:00"
    }
  }
}
```

---

## 同步机制详解

### 数据流

```
┌─────────────────────────────────────────────────────────────┐
│                    OpenViking 服务                            │
│                                                              │
│  ┌──────────┐    ┌───────────┐    ┌─────────────────────┐   │
│  │ VikingFS │───→│ Sync Hook │───→│ CloudSyncManager    │   │
│  │ write()  │    │ (非阻塞)  │    │                     │   │
│  └──────────┘    └───────────┘    │  ┌───────────────┐  │   │
│                                    │  │  Async Queue  │  │   │
│  ┌──────────┐    ┌───────────┐    │  │  (max: 10000) │  │   │
│  │ Session  │───→│ Sync Hook │───→│  └───────┬───────┘  │   │
│  │ message  │    │ (非阻塞)  │    │          │          │   │
│  └──────────┘    └───────────┘    │  ┌───────▼───────┐  │   │
│                                    │  │ Background    │  │   │
│  ┌──────────┐    ┌───────────┐    │  │ Worker        │  │   │
│  │ Skill    │───→│ Sync Hook │───→│  │ (2s interval) │  │   │
│  │ process  │    │ (非阻塞)  │    │  └───────┬───────┘  │   │
│  └──────────┘    └───────────┘    │          │          │   │
│                                    └──────────┼──────────┘   │
│  ┌──────────┐    ┌───────────┐               │              │
│  │ VikingDB │───→│ Sync Hook │───────────────┘              │
│  │ indexed  │    │ (非阻塞)  │                               │
│  └──────────┘    └───────────┘                               │
└──────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                        ┌───────────────────┐
                        │  Cloud Database   │
                        │  (SQLite WAL)     │
                        │                   │
                        │  synced_contexts  │
                        │  synced_skills    │
                        │  synced_sessions  │
                        │  synced_messages  │
                        │  synced_files     │
                        │  sync_log         │
                        └───────────────────┘
```

### 同步钩子触发点

| 触发位置 | 文件 | 触发时机 |
|---------|------|---------|
| `VikingFS.write_file()` | `storage/viking_fs.py` | 任何文件写入时 |
| `VikingFS.write_context()` | `storage/viking_fs.py` | 上下文写入（abstract/overview）时 |
| `VikingFS.rm()` | `storage/viking_fs.py` | 文件/目录删除时 |
| `Session.add_message()` | `session/session.py` | 消息添加时（含会话更新） |
| `Session._write_archive()` | `session/session.py` | 会话归档时 |
| `SkillProcessor.process_skill()` | `utils/skill_processor.py` | 技能处理完成时 |
| `TextEmbeddingHandler.on_dequeue()` | `storage/collection_schemas.py` | 向量化写入 VikingDB 时 |

### 重试策略

- 最大重试次数：3 次
- 队列满时：丢弃最旧的任务腾出空间
- 重试任务重新入队，与新任务一起按批次处理

### 云端数据库表结构

| 表名 | 主键 | 说明 |
|------|------|------|
| `synced_contexts` | id | 上下文记录（资源/记忆/技能） |
| `synced_skills` | id | 技能定义及内容 |
| `synced_sessions` | session_id | 会话元数据 |
| `synced_messages` | id | 会话消息 |
| `synced_files` | uri | 文件内容（abstract/overview 等） |
| `sync_log` | id (自增) | 同步操作日志 |

---

## 故障排查

### 1. 云端同步未启动

**现象**: Admin 页面显示 "Cloud sync not enabled"

**排查**:
```bash
# 确认环境变量已设置
echo $OPENVIKING_CLOUD_SYNC
# 应输出: true

# 检查日志中是否有初始化信息
# 正常: "Cloud sync initialized (db: xxx)"
# 异常: "Failed to initialize cloud sync: xxx"
```

### 2. 同步数据为空

**排查**:
```bash
# 检查数据库文件是否存在
ls -la openviking_cloud.db

# 使用 sqlite3 直接查看
sqlite3 openviking_cloud.db "SELECT COUNT(*) FROM sync_log;"
```

### 3. Chat 功能报错

**现象**: "LLM call failed"

**排查**:
- 确认 `ov.conf` 中的 VLM 配置正确
- 确认 API Key 有效
- 确认网络可访问 LLM API
- 检查请求中的 `model` 参数是否为 litellm 支持的格式

### 4. 静态页面 404

**排查**:
```bash
# 确认静态文件存在
ls openviking/server/static/
# 应包含: chat.html admin.html
```

### 5. 同步队列积压

**现象**: Admin Overview 中 queue_size 持续增长

**排查**:
- 检查云端数据库是否可写
- 检查磁盘空间
- 可适当增大 `batch_size` 或减小 `flush_interval`

---

## 新增文件清单

```
openviking/
├── sync/                              # 云端同步模块
│   ├── __init__.py                    # 模块导出
│   ├── cloud_db.py                    # 云端数据库 (SQLite)
│   ├── sync_manager.py                # 同步管理器 (异步队列 + 后台Worker)
│   └── sync_hooks.py                  # 同步钩子 (非侵入式)
│
├── server/
│   ├── routers/
│   │   ├── chat.py                    # Chat API (LLM 对话 + 技能创建)
│   │   └── cloud_sync.py             # Cloud Sync API (数据查看)
│   └── static/
│       ├── chat.html                  # 前端对话页面
│       └── admin.html                 # 后台管理页面
```

## 修改文件清单

| 文件 | 修改内容 |
|------|---------|
| `service/core.py` | 添加 CloudSyncManager 生命周期管理 |
| `server/app.py` | 注册新路由、挂载静态文件 |
| `server/routers/__init__.py` | 导出新路由 |
| `session/session.py` | 添加消息和会话同步钩子 |
| `utils/skill_processor.py` | 添加技能同步钩子 |
| `storage/viking_fs.py` | 添加文件写入/删除同步钩子 |
| `storage/collection_schemas.py` | 添加上下文索引同步钩子 |
