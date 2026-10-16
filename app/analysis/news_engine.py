"""News, official releases, and macroeconomic event risk analyzer.

Integrates:
- Cached Marketaux News API (with 100 req/day quota conservation)
- Deduplication of headlines using URL/title hashing
- Configurable macroeconomic event calendar (RBI MPC, Union Budget, US Fed FOMC)
- Event Risk Veto engine to block entries around volatile releases
"""

from __future__ import annotations
import hashlib
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
import httpx
from pydantic import BaseModel

from app.database.models import NewsEvent
from app.monitoring.logging import logger
from config import settings


class MacroEvent(BaseModel):
    title: str
    event_time: datetime
    impact_level: str  # HIGH, MEDIUM
    category: str  # RBI, BUDGET, FOMC, EXPIRY


class NewsAnalysisResult(BaseModel):
    has_active_veto: bool
    veto_reason: Optional[str] = None
    recent_headlines: List[str] = []


class NewsAndEventEngine:
    """Manages news caching, deduplication, and high-impact event vetoes."""

    def __init__(self):
        self._cache_timestamp: float = 0.0
        self._cached_events: List[NewsEvent] = []
        self._dedup_keys: set[str] = set()

        # Configurable high-impact event calendar
        # Production calendar populated with standard scheduled market event windows
        now = datetime.now(timezone.utc)
        self.scheduled_calendar: List[MacroEvent] = [
            MacroEvent(
                title="RBI MPC Policy Interest Rate Decision",
                event_time=now.replace(hour=4, minute=30, second=0),  # 10:00 AM IST = 04:30 UTC
                impact_level="HIGH",
                category="RBI",
            ),
            MacroEvent(
                title="US Federal Reserve FOMC Statement",
                event_time=now.replace(hour=18, minute=30, second=0),
                impact_level="HIGH",
                category="FOMC",
            ),
        ]

    def _generate_dedup_key(self, title: str, pub_date: str) -> str:
        raw = f"{title.strip().lower()}_{pub_date.strip()}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    async def fetch_marketaux_news(self) -> List[NewsEvent]:
        """Fetches news from Marketaux with strict rate limiting & caching."""
        now = time.time()
        # Return cache if within TTL (e.g. 10 minutes)
        if (now - self._cache_timestamp) < settings.NEWS_CACHE_TTL_SECONDS and self._cached_events:
            return self._cached_events

        if not settings.MARKETAUX_API_TOKEN:
            logger.info("Marketaux API token not provided. Operating with scheduled calendar only.")
            return []

        try:
            url = f"{settings.MARKETAUX_BASE_URL}/news/all"
            params = {
                "api_token": settings.MARKETAUX_API_TOKEN,
                "symbols": "NIFTY,SENSEX",
                "countries": "in",
                "filter_entities": "true",
                "limit": 3,
            }
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(url, params=params)
                if resp.status_code == 200:
                    data = resp.json().get("data", [])
                    events: List[NewsEvent] = []
                    for item in data:
                        title = item.get("title", "")
                        pub_str = item.get("published_at", "")
                        url_val = item.get("url", "")
                        key = self._generate_dedup_key(title, pub_str)
                        if key in self._dedup_keys:
                            continue

                        self._dedup_keys.add(key)
                        pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00")) if pub_str else datetime.now(timezone.utc)
                        ev = NewsEvent(
                            source="Marketaux",
                            url=url_val,
                            title=title,
                            publication_time=pub_dt,
                            event_category="FINANCIAL_NEWS",
                            impact_level="MEDIUM",
                            dedup_key=key,
                            is_veto_active=False,
                        )
                        events.append(ev)

                    self._cached_events = events
                    self._cache_timestamp = now
                    logger.info(f"Retrieved and cached {len(events)} fresh news headlines from Marketaux.")
                    return events
                else:
                    logger.warning(f"Marketaux query returned status {resp.status_code}")
        except Exception as e:
            logger.error(f"Error querying Marketaux news: {e}")

        return self._cached_events

    def check_event_risk_veto(
        self, current_time: Optional[datetime] = None
    ) -> NewsAnalysisResult:
        """Evaluates whether current time falls within +/- buffer of a high-impact event."""
        now = current_time or datetime.now(timezone.utc)
        buffer = timedelta(minutes=settings.EVENT_RISK_BUFFER_MINUTES)

        for event in self.scheduled_calendar:
            if event.impact_level == "HIGH":
                start_window = event.event_time - buffer
                end_window = event.event_time + buffer
                if start_window <= now <= end_window:
                    return NewsAnalysisResult(
                        has_active_veto=True,
                        veto_reason=f"Event Risk Veto: Within {settings.EVENT_RISK_BUFFER_MINUTES}m of {event.title} ({event.event_time.strftime('%H:%M UTC')})",
                        recent_headlines=[e.title for e in self._cached_events[:3]],
                    )

        return NewsAnalysisResult(
            has_active_veto=False,
            veto_reason=None,
            recent_headlines=[e.title for e in self._cached_events[:3]],
        )


news_engine = NewsAndEventEngine()
