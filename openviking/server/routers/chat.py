# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
OpenViking 对话 API 端点。

提供集成以下功能的交互式对话端点：
- LLM 大模型对话（通过 litellm/OpenAI）
- OpenViking 会话消息持久化
- OpenViking 上下文搜索（RAG 增强）
- 云端同步（数据镜像）
"""

import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from openviking.message.part import TextPart
from openviking.server.dependencies import get_service
from openviking.server.models import Response
from openviking.sync.sync_hooks import (
    on_file_written,
    on_message_added,
    on_session_updated,
    on_skill_processed,
)
from openviking_cli.utils import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/chat", tags=["chat"])


class ChatRequest(BaseModel):
    """对话请求模型。"""
    message: str
    session_id: Optional[str] = None
    model: Optional[str] = None
    use_context: bool = True
    max_context_items: int = 5
    stream: bool = False


class CreateSkillRequest(BaseModel):
    """通过对话界面创建技能的请求。"""
    name: str
    description: str
    content: str
    tags: List[str] = []
    allowed_tools: List[str] = []


class ChatMessage(BaseModel):
    """对话消息模型。"""
    role: str
    content: str


@router.post("/completions")
async def chat_completions(request: ChatRequest):
    """
    与 LLM 对话，可选使用 OpenViking 上下文进行 RAG 增强。

    该端点的处理流程：
    1. 创建/加载 OpenViking 会话
    2. 如果启用了 use_context，搜索相关上下文
    3. 将增强后的提示发送给 LLM
    4. 存储用户和助手的消息
    5. 将所有数据同步到云端
    """
    service = get_service()

    # 获取或创建会话
    session = service.sessions.session(request.session_id)
    await session.load()

    # 存储用户消息
    user_msg = session.add_message("user", [TextPart(text=request.message)])

    # 同步用户消息到云端
    await on_message_added(
        session_id=session.session_id,
        message_id=user_msg.id,
        role="user",
        content=request.message,
    )

    # 搜索相关上下文
    context_parts = []
    if request.use_context:
        try:
            search_result = await service.search.search(
                query=request.message,
                limit=request.max_context_items,
            )
            if search_result:
                for item in search_result:
                    uri = item.get("uri", "")
                    abstract = item.get("abstract", "")
                    if abstract:
                        context_parts.append(
                            f"[上下文: {uri}]\n{abstract}"
                        )
        except Exception as e:
            logger.debug(f"上下文搜索失败: {e}")

    # 构建 LLM 消息
    messages = _build_llm_messages(
        session_messages=session.messages,
        context_parts=context_parts,
        current_message=request.message,
    )

    # 调用 LLM
    try:
        from openviking_cli.utils.config import get_openviking_config

        config = get_openviking_config()
        model_name = request.model or _get_default_model(config)
        model_name = _resolve_model_for_litellm(model_name, config)
        llm_kwargs = _get_llm_kwargs(config)

        if request.stream:
            return StreamingResponse(
                _stream_llm_response(
                    model_name, messages, session, service, **llm_kwargs
                ),
                media_type="text/event-stream",
            )

        response_text = await _call_llm(model_name, messages, **llm_kwargs)

        # 存储助手消息
        assistant_msg = session.add_message(
            "assistant", [TextPart(text=response_text)]
        )

        # 同步助手消息到云端
        await on_message_added(
            session_id=session.session_id,
            message_id=assistant_msg.id,
            role="assistant",
            content=response_text,
        )

        # 同步会话更新
        await on_session_updated(
            session_id=session.session_id,
            user_data=session.user.to_dict() if session.user else None,
            stats={
                "total_turns": session.stats.total_turns,
                "total_messages": len(session.messages),
                "compression_count": session.compression.compression_index,
            },
        )

        return Response(
            status="ok",
            result={
                "session_id": session.session_id,
                "message": response_text,
                "context_used": len(context_parts),
                "model": model_name,
            },
        )
    except Exception as e:
        logger.error(f"LLM 调用失败: {e}")
        return Response(
            status="error",
            result={"message": f"LLM 调用失败: {str(e)}"},
        )


@router.post("/skills")
async def create_skill(request: CreateSkillRequest):
    """通过对话界面创建新技能。"""
    service = get_service()

    skill_data = {
        "name": request.name,
        "description": request.description,
        "content": request.content,
        "tags": request.tags,
        "allowed_tools": request.allowed_tools,
    }

    try:
        result = await service.resources.add_skill(data=skill_data, wait=True, timeout=30)

        # 同步技能到云端
        await on_skill_processed({
            **skill_data,
            "uri": result.get("uri", f"viking://agent/skills/{request.name}"),
        })

        return Response(status="ok", result=result)
    except Exception as e:
        logger.error(f"技能创建失败: {e}")
        return Response(
            status="error",
            result={"message": f"技能创建失败: {str(e)}"},
        )


@router.get("/skills")
async def list_skills():
    """列出所有可用技能。"""
    service = get_service()
    try:
        viking_fs = service.viking_fs
        if not viking_fs:
            return Response(status="ok", result={"items": []})

        entries = await viking_fs.ls("viking://agent/skills")
        skills = []
        for entry in entries:
            name = entry.get("name", "")
            if name in (".", ".."):
                continue
            uri = f"viking://agent/skills/{name}"
            try:
                abstract = await viking_fs.abstract(uri)
            except Exception:
                abstract = ""
            skills.append({
                "name": name,
                "uri": uri,
                "abstract": abstract,
            })
        return Response(status="ok", result={"items": skills})
    except Exception as e:
        logger.debug(f"列出技能失败: {e}")
        return Response(status="ok", result={"items": []})


@router.get("/sessions")
async def list_chat_sessions():
    """列出所有对话会话。"""
    service = get_service()
    sessions = await service.sessions.sessions()
    return Response(status="ok", result={"items": sessions})


@router.get("/sessions/{session_id}/messages")
async def get_session_messages(session_id: str):
    """获取指定会话的消息列表。"""
    service = get_service()
    session = service.sessions.session(session_id)
    await session.load()

    messages = []
    for msg in session.messages:
        messages.append({
            "id": msg.id,
            "role": msg.role,
            "content": msg.content,
            "created_at": msg.created_at.isoformat() if msg.created_at else "",
        })

    return Response(status="ok", result={"messages": messages})


# =========================================================================
# 内部辅助方法
# =========================================================================

def _get_default_model(config) -> str:
    """从配置中获取默认的 LLM 模型名称。"""
    if config.vlm and hasattr(config.vlm, "model") and config.vlm.model:
        return config.vlm.model
    return "gpt-4o-mini"


def _resolve_model_for_litellm(model: str, config) -> str:
    """为模型名称添加 litellm 提供商前缀（如果尚未添加）。

    litellm 需要 'openai/'、'dashscope/' 等前缀来识别使用哪种协议。
    当设置了自定义 api_base（例如 DashScope 兼容模式）时，
    会自动添加 'openai/' 前缀以使用 OpenAI 兼容协议。
    """
    # 已包含提供商前缀 — 保持不变
    if "/" in model:
        return model

    vlm = config.vlm if config else None
    if not vlm:
        return model

    provider = getattr(vlm, "provider", None) or ""
    api_base = getattr(vlm, "api_base", None) or ""

    # 已知的 litellm 原生提供商及其前缀映射
    _LITELLM_PREFIXES = {
        "dashscope": "openai",
        "volcengine": "volcengine",
        "deepseek": "deepseek",
        "moonshot": "openai",
    }

    if provider in _LITELLM_PREFIXES:
        return f"{_LITELLM_PREFIXES[provider]}/{model}"

    # 有自定义 api_base 但无法识别的提供商 → 视为 OpenAI 兼容
    if api_base:
        return f"openai/{model}"

    return model


def _get_llm_kwargs(config) -> Dict[str, Any]:
    """从 VLM 配置中提取 api_base 和 api_key，用于 litellm 调用。"""
    kwargs: Dict[str, Any] = {}
    if config.vlm:
        if getattr(config.vlm, "api_base", None):
            kwargs["api_base"] = config.vlm.api_base
        if getattr(config.vlm, "api_key", None):
            kwargs["api_key"] = config.vlm.api_key
    return kwargs


def _build_llm_messages(
    session_messages,
    context_parts: List[str],
    current_message: str,
) -> List[Dict[str, str]]:
    """构建发送给 LLM 的消息列表。"""
    messages = []

    # 带上下文的系统消息
    system_parts = ["你是一个由 OpenViking 驱动的智能 AI 助手。"]
    if context_parts:
        system_parts.append(
            "\n来自知识库的相关上下文:\n"
            + "\n---\n".join(context_parts)
        )
    messages.append({"role": "system", "content": "\n".join(system_parts)})

    # 近期历史（最后 20 条消息，排除刚添加的当前消息）
    history = session_messages[:-1] if session_messages else []
    for msg in history[-20:]:
        messages.append({"role": msg.role, "content": msg.content})

    # 当前用户消息
    messages.append({"role": "user", "content": current_message})

    return messages


async def _call_llm(
    model: str,
    messages: List[Dict[str, str]],
    **extra_kwargs,
) -> str:
    """调用 LLM 并返回响应文本。"""
    import litellm

    response = await litellm.acompletion(
        model=model,
        messages=messages,
        temperature=0.7,
        max_tokens=4096,
        **extra_kwargs,
    )
    return response.choices[0].message.content


async def _stream_llm_response(model, messages, session, service, **extra_kwargs):
    """以 SSE 事件流方式返回 LLM 响应。"""
    import litellm

    full_response = ""
    try:
        response = await litellm.acompletion(
            model=model,
            messages=messages,
            temperature=0.7,
            max_tokens=4096,
            stream=True,
            **extra_kwargs,
        )

        async for chunk in response:
            delta = chunk.choices[0].delta
            if delta and delta.content:
                full_response += delta.content
                yield f"data: {json.dumps({'content': delta.content})}\n\n"

        # 存储完整的助手消息
        assistant_msg = session.add_message(
            "assistant", [TextPart(text=full_response)]
        )
        await on_message_added(
            session_id=session.session_id,
            message_id=assistant_msg.id,
            role="assistant",
            content=full_response,
        )

        yield f"data: {json.dumps({'done': True, 'session_id': session.session_id})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'error': str(e)})}\n\n"
