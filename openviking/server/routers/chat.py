# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Chat API endpoints for OpenViking.

Provides an interactive chat endpoint that integrates with:
- LLM (via litellm/OpenAI) for conversation
- OpenViking sessions for message persistence
- OpenViking context search for RAG
- Cloud sync for mirroring all data
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
    """Chat request model."""
    message: str
    session_id: Optional[str] = None
    model: Optional[str] = None
    use_context: bool = True
    max_context_items: int = 5
    stream: bool = False


class CreateSkillRequest(BaseModel):
    """Request to create a skill via the chat interface."""
    name: str
    description: str
    content: str
    tags: List[str] = []
    allowed_tools: List[str] = []


class ChatMessage(BaseModel):
    """Chat message model."""
    role: str
    content: str


@router.post("/completions")
async def chat_completions(request: ChatRequest):
    """
    Chat with LLM, optionally augmented with OpenViking context.

    This endpoint:
    1. Creates/loads an OpenViking session
    2. Searches for relevant context if use_context=True
    3. Sends the augmented prompt to the LLM
    4. Stores both user and assistant messages
    5. Syncs all data to cloud
    """
    service = get_service()

    # Get or create session
    session = service.sessions.session(request.session_id)
    await session.load()

    # Store user message
    user_msg = session.add_message("user", [TextPart(text=request.message)])

    # Sync user message to cloud
    await on_message_added(
        session_id=session.session_id,
        message_id=user_msg.id,
        role="user",
        content=request.message,
    )

    # Search for relevant context
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
                            f"[Context: {uri}]\n{abstract}"
                        )
        except Exception as e:
            logger.debug(f"Context search failed: {e}")

    # Build LLM messages
    messages = _build_llm_messages(
        session_messages=session.messages,
        context_parts=context_parts,
        current_message=request.message,
    )

    # Call LLM
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

        # Store assistant message
        assistant_msg = session.add_message(
            "assistant", [TextPart(text=response_text)]
        )

        # Sync assistant message to cloud
        await on_message_added(
            session_id=session.session_id,
            message_id=assistant_msg.id,
            role="assistant",
            content=response_text,
        )

        # Sync session update
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
        logger.error(f"LLM call failed: {e}")
        return Response(
            status="error",
            result={"message": f"LLM call failed: {str(e)}"},
        )


@router.post("/skills")
async def create_skill(request: CreateSkillRequest):
    """Create a new skill via the chat interface."""
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

        # Sync skill to cloud
        await on_skill_processed({
            **skill_data,
            "uri": result.get("uri", f"viking://agent/skills/{request.name}"),
        })

        return Response(status="ok", result=result)
    except Exception as e:
        logger.error(f"Skill creation failed: {e}")
        return Response(
            status="error",
            result={"message": f"Skill creation failed: {str(e)}"},
        )


@router.get("/skills")
async def list_skills():
    """List available skills."""
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
        logger.debug(f"List skills failed: {e}")
        return Response(status="ok", result={"items": []})


@router.get("/sessions")
async def list_chat_sessions():
    """List all chat sessions."""
    service = get_service()
    sessions = await service.sessions.sessions()
    return Response(status="ok", result={"items": sessions})


@router.get("/sessions/{session_id}/messages")
async def get_session_messages(session_id: str):
    """Get messages for a chat session."""
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
# Internal helpers
# =========================================================================

def _get_default_model(config) -> str:
    """Get the default LLM model name from config."""
    if config.vlm and hasattr(config.vlm, "model") and config.vlm.model:
        return config.vlm.model
    return "gpt-4o-mini"


def _resolve_model_for_litellm(model: str, config) -> str:
    """Add litellm provider prefix if the model name doesn't already have one.

    litellm requires a prefix like 'openai/', 'dashscope/' etc. to know
    which protocol to use.  When a custom api_base is set (e.g. DashScope
    compatible-mode), we prefix with 'openai/' so litellm uses the
    OpenAI-compatible protocol.
    """
    # Already has a provider prefix — leave as-is
    if "/" in model:
        return model

    vlm = config.vlm if config else None
    if not vlm:
        return model

    provider = getattr(vlm, "provider", None) or ""
    api_base = getattr(vlm, "api_base", None) or ""

    # Known litellm-native providers that need their own prefix
    _LITELLM_PREFIXES = {
        "dashscope": "openai",
        "volcengine": "volcengine",
        "deepseek": "deepseek",
        "moonshot": "openai",
    }

    if provider in _LITELLM_PREFIXES:
        return f"{_LITELLM_PREFIXES[provider]}/{model}"

    # Custom api_base but no recognised provider → treat as OpenAI-compatible
    if api_base:
        return f"openai/{model}"

    return model


def _get_llm_kwargs(config) -> Dict[str, Any]:
    """Extract api_base and api_key from VLM config for litellm calls."""
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
    """Build the message list for LLM call."""
    messages = []

    # System message with context
    system_parts = ["You are a helpful AI assistant powered by OpenViking."]
    if context_parts:
        system_parts.append(
            "\nRelevant context from the knowledge base:\n"
            + "\n---\n".join(context_parts)
        )
    messages.append({"role": "system", "content": "\n".join(system_parts)})

    # Recent history (last 20 messages, excluding the current one just added)
    history = session_messages[:-1] if session_messages else []
    for msg in history[-20:]:
        messages.append({"role": msg.role, "content": msg.content})

    # Current user message
    messages.append({"role": "user", "content": current_message})

    return messages


async def _call_llm(
    model: str,
    messages: List[Dict[str, str]],
    **extra_kwargs,
) -> str:
    """Call the LLM and return the response text."""
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
    """Stream LLM response as SSE events."""
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

        # Store complete assistant message
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
