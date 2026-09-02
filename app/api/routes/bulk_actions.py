"""Bulk action endpoints.

The four the assignment specifies, plus the ones its "UI Interaction" section
implies: filterable log retrieval, batch-level detail, cancellation, and a
server-sent-events stream for real-time progress.

Note how little is here. Every route is a thin translation between HTTP and the
service layer; nothing in this file knows what a contact is or what "update"
means, which is why adding an action requires no change to it.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Annotated, Any

from arq.connections import ArqRedis
from fastapi import APIRouter, Depends, Query, Request, Response, status
from fastapi.responses import StreamingResponse
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import arq_pool, db_session, redis_client
from app.models.enums import BulkActionStatus, EntityLogStatus
from app.schemas.bulk_action import (
    BulkActionBatchItem,
    BulkActionCreate,
    BulkActionLogItem,
    BulkActionResponse,
    BulkActionStats,
    Page,
)
from app.services import bulk_action_service as svc
from app.services.progress import PROGRESS_CHANNEL, progress_payload

router = APIRouter(prefix="/bulk-actions", tags=["bulk-actions"])

DbSession = Annotated[AsyncSession, Depends(db_session)]
Arq = Annotated[ArqRedis, Depends(arq_pool)]
RedisDep = Annotated[Redis, Depends(redis_client)]


@router.get(
    "/registry",
    summary="Supported entities, actions and their payload schemas",
    response_model=dict,
)
async def get_registry() -> dict[str, Any]:
    """Rendered from the live registries.

    Every entity x action combination listed here works, and the list grows
    automatically when a new module is dropped into `app/domain/`.
    """
    return svc.registry_snapshot()


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BulkActionResponse,
    summary="Submit a bulk action",
)
async def create_bulk_action(
    request: BulkActionCreate,
    session: DbSession,
    arq: Arq,
    redis: RedisDep,
    response: Response,
) -> BulkActionResponse:
    """Accepts the work and returns immediately.

    202, not 201: the action is queued, not performed. Poll
    `GET /bulk-actions/{id}` or subscribe to `/events` for progress.
    """
    action, created = await svc.create_bulk_action(session, arq, redis, request)
    if not created:
        # Idempotency replay: same key, same answer, no new work.
        response.status_code = status.HTTP_200_OK
    response.headers["Location"] = f"/bulk-actions/{action.id}"
    return BulkActionResponse.from_model(action)


@router.get("", response_model=Page[BulkActionResponse], summary="List bulk actions")
async def list_bulk_actions(
    session: DbSession,
    account_id: uuid.UUID | None = Query(None, description="Filter by tenant."),
    status_filter: BulkActionStatus | None = Query(
        None, alias="status", description="ongoing / completed / queued views."
    ),
    entity_type: str | None = Query(None),
    action_type: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> Page[BulkActionResponse]:
    items, total = await svc.list_bulk_actions(
        session,
        account_id=account_id,
        status=status_filter.value if status_filter else None,
        entity_type=entity_type,
        action_type=action_type,
        limit=limit,
        offset=offset,
    )
    return Page[BulkActionResponse](
        items=[BulkActionResponse.from_model(a) for a in items],
        count=total,
        has_more=offset + len(items) < total,
    )


@router.get("/{action_id}", response_model=BulkActionResponse, summary="Bulk action detail")
async def get_bulk_action(action_id: uuid.UUID, session: DbSession) -> BulkActionResponse:
    action = await svc.get_bulk_action(session, action_id)
    return BulkActionResponse.from_model(action)


@router.get(
    "/{action_id}/stats",
    response_model=BulkActionStats,
    summary="Success / failure / skipped summary",
)
async def get_stats(action_id: uuid.UUID, session: DbSession) -> BulkActionStats:
    return await svc.get_stats(session, action_id)


@router.get(
    "/{action_id}/logs",
    response_model=Page[BulkActionLogItem],
    summary="Per-entity logs, filterable",
)
async def get_logs(
    action_id: uuid.UUID,
    session: DbSession,
    log_status: EntityLogStatus | None = Query(
        None, alias="status", description="success / failed / skipped."
    ),
    reason_code: str | None = Query(
        None, description="e.g. duplicate_email, validation_failed, entity_not_found."
    ),
    entity_id: uuid.UUID | None = Query(None, description="Trace one entity."),
    cursor: int | None = Query(None, description="Last id from the previous page."),
    limit: int = Query(100, ge=1, le=1000),
) -> Page[BulkActionLogItem]:
    page = await svc.list_logs(
        session,
        action_id,
        status=log_status.value if log_status else None,
        reason_code=reason_code,
        entity_id=entity_id,
        cursor=cursor,
        limit=limit,
    )
    return Page[BulkActionLogItem](
        items=[BulkActionLogItem.model_validate(item) for item in page.items],
        count=page.count,
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )


@router.get(
    "/{action_id}/batches",
    response_model=list[BulkActionBatchItem],
    summary="Batch-level progress",
)
async def get_batches(
    action_id: uuid.UUID,
    session: DbSession,
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> list[BulkActionBatchItem]:
    batches = await svc.list_batches(session, action_id, limit=limit, offset=offset)
    return [BulkActionBatchItem.model_validate(b) for b in batches]


@router.post(
    "/{action_id}/cancel",
    response_model=BulkActionResponse,
    summary="Cancel a scheduled, queued or running action",
)
async def cancel(action_id: uuid.UUID, session: DbSession) -> BulkActionResponse:
    action = await svc.cancel_bulk_action(session, action_id)
    return BulkActionResponse.from_model(action)


@router.get(
    "/{action_id}/events",
    summary="Real-time progress (Server-Sent Events)",
    response_class=StreamingResponse,
)
async def stream_progress(
    action_id: uuid.UUID,
    request: Request,
    session: DbSession,
    redis: RedisDep,
) -> StreamingResponse:
    """Live progress over SSE.

    Workers publish a progress frame to Redis after each batch commits, and this
    endpoint relays them. SSE rather than WebSockets because the traffic is
    one-directional and SSE survives proxies and reconnects without extra code.

    The current state is sent first, so a client that connects late is never
    left staring at an empty stream, and a heartbeat keeps idle proxies from
    closing the connection.
    """
    action = await svc.get_bulk_action(session, action_id)
    initial = progress_payload(action)

    async def event_stream():
        yield f"event: progress\ndata: {json.dumps(initial)}\n\n"
        if BulkActionStatus(initial["status"]).is_terminal:
            return

        pubsub = redis.pubsub()
        await pubsub.subscribe(f"{PROGRESS_CHANNEL}:{action_id}")
        try:
            while True:
                if await request.is_disconnected():
                    break
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=15.0
                )
                if message is None:
                    yield ": heartbeat\n\n"
                    continue
                payload = json.loads(message["data"])
                yield f"event: progress\ndata: {json.dumps(payload)}\n\n"
                if BulkActionStatus(payload["status"]).is_terminal:
                    yield f"event: done\ndata: {json.dumps(payload)}\n\n"
                    break
        except asyncio.CancelledError:  # pragma: no cover - client hung up
            raise
        finally:
            await pubsub.unsubscribe()
            await pubsub.aclose()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
