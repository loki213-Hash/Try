"""Health and monitoring API router."""

from __future__ import annotations
from fastapi import APIRouter
from app.monitoring.health import health_monitor, HealthStatus

router = APIRouter(prefix="/api/health", tags=["Health & Monitoring"])


@router.get("", response_model=HealthStatus)
async def get_health_status() -> HealthStatus:
    """Returns real-time operational status, feeds, staleness, and database health."""
    return health_monitor.get_status()
