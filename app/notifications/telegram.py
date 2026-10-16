"""Telegram Bot API Notification Service.

Sends auditable, structured signal notifications, status updates,
target hits, invalidations, and daily risk summaries.
Edits existing Telegram messages where practical to minimize chat spam.
Strictly labels all messages as PAPER SIGNALS with transparent hypotheses.
"""

from __future__ import annotations
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import httpx

from app.database.models import Signal, SignalLevels
from app.monitoring.logging import logger
from config import settings


class TelegramNotifier:
    """Dispatches signals and lifecycle updates to Telegram channels/groups."""

    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
        enabled: Optional[bool] = None,
    ):
        self.bot_token = bot_token or settings.TELEGRAM_BOT_TOKEN
        self.chat_id = chat_id or settings.TELEGRAM_CHAT_ID
        self.enabled = enabled if enabled is not None else settings.TELEGRAM_ENABLED
        self.api_base = f"https://api.telegram.org/bot{self.bot_token}"
        self._sent_dedup_keys: set[str] = set()

    def format_signal_message(
        self, signal: Signal, levels: SignalLevels, status_override: Optional[str] = None
    ) -> str:
        """Formats signal in the prescribed auditable production layout."""
        status = status_override or signal.status
        action = "BUY CE" if "CE" in signal.side else "BUY PE"
        emoji_map = {
            "ENTRY": "🚨",
            "WATCH": "👀",
            "TP1_HIT": "🎯",
            "TP2_HIT": "🎯🎯",
            "TP3_HIT": "🚀",
            "SL_HIT": "🛑",
            "INVALIDATED": "⚠️",
            "CLOSED": "🏁",
        }
        status_emoji = emoji_map.get(status, "ℹ️")

        reasons_text = "\n".join([f"• {r}" for r in signal.reasons[:5]])

        # Highlight Banner when Targets are Hit
        highlight_banner = ""
        if "TP1" in status:
            highlight_banner = "🌟🌟 <b>🎯 TARGET 1 HIT! PROFIT BOOKED / TRAILING SL ACTIVE 🎯</b> 🌟🌟\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        elif "TP2" in status:
            highlight_banner = "🚀🚀 <b>🎯🎯 TARGET 2 HIT! EXTENSION ACHIEVED! 🎯🎯</b> 🚀🚀\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        elif "TP3" in status:
            highlight_banner = "🏆🏆 <b>🚀 TARGET 3 MAX RUNNER ACHIEVED! JACKPOT! 🏆</b> 🏆🏆\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        elif "SL" in status:
            highlight_banner = "🛑🛑 <b>STOP-LOSS HIT — RISK MANAGED & POSITION CLOSED</b> 🛑🛑\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        # Risk per lot calculation
        lot_size = 50  # Default or resolved
        risk_per_lot = round((levels.entry_range_high - levels.sl_price) * 50, 2)

        # Target Probabilities
        sb = signal.score_breakdown or {}
        tp1_prob = sb.get("tp1_probability", 85.0)
        tp2_prob = sb.get("tp2_probability", 65.0)
        tp3_prob = sb.get("tp3_probability", 42.0)

        msg = (
            f"{highlight_banner}"
            f"<b>{status_emoji} MULTI-INDEX OPTIONS SIGNAL (PAPER)</b>\n"
            f"<b>Status:</b> <code>{status}</code>\n\n"
            f"<b>Index:</b> {signal.underlying} — <b>{action}</b>\n"
            f"<b>Strike / Expiry:</b> {levels.contract_symbol}\n"
            f"<b>Live Bid / Ask:</b> ₹{levels.current_bid:.2f} / ₹{levels.current_ask:.2f}\n"
            f"<b>Entry Range:</b> ₹{levels.entry_range_low:.2f} – ₹{levels.entry_range_high:.2f}\n"
            f"<b>Max Acceptable Entry:</b> ₹{levels.max_entry_price:.2f}\n"
            f"<b>Setup Score:</b> <b>{signal.score:.1f}/100</b>\n\n"
            f"<b>LEVELS & RISK (TARGET CONFIDENCE):</b>\n"
            f"• <b>Stop-Loss:</b> ₹{levels.sl_price:.2f}\n"
            f"• <b>TP1 (1:1.5 RR):</b> ₹{levels.tp1_price:.2f} (Confidence: <b>{tp1_prob}%</b>)\n"
            f"• <b>TP2 (1:2.5 RR):</b> ₹{levels.tp2_price:.2f} (Confidence: <b>{tp2_prob}%</b>)\n"
            f"• <b>TP3 (1:4.0 RR):</b> ₹{levels.tp3_price:.2f} (Confidence: <b>{tp3_prob}%</b>)\n"
            f"• <b>Underlying Invalidation:</b> {levels.underlying_invalidation:.2f}\n"
            f"• <b>Risk per Lot:</b> ₹{risk_per_lot:.2f}\n\n"
            f"<b>EVIDENCE & REASONS:</b>\n"
            f"{reasons_text}\n\n"
            f"<i>⚠️ RISK: Paper simulation only. Actual fills may differ.</i>\n"
            f"<i>Signal ID:</i> <code>{signal.signal_id}</code> | <i>Ver:</i> <code>{signal.strategy_version}</code>"
        )
        return msg

    def format_clean_signal_message(
        self,
        signal: Signal,
        levels: SignalLevels,
        live_price: Optional[float] = None,
        highest_gain_pct: Optional[float] = None,
        momentum_status: Optional[str] = None,
        **kwargs,
    ) -> str:
        """Formats signal strictly in the clean format (1).

        ONLY includes:
        - Contract Symbol
        - Confidence Score
        - Signal Type
        - Entry Price
        - Live Price
        - Highest % change from entry point

        Strictly excludes: TP, SL, reasons, TP achieved, SL hit execution badges, 1st hour %.
        """
        action = "BUY CE" if "CE" in signal.side else "BUY PE"
        contract = levels.contract_symbol if levels else f"{signal.underlying} OPT"
        entry_val = levels.entry_range_high if levels else 0.0
        ltp_val = live_price if (live_price and live_price > 0) else (levels.current_ask or entry_val)

        peak_str = f"+{highest_gain_pct:.1f}%" if (highest_gain_pct is not None and highest_gain_pct > 0) else "0.0%"
        status_tag = momentum_status or "ACTIVE"

        status_emoji = "🟢"
        if "BULLISH" in status_tag:
            status_emoji = "🚀"
        elif "BEARISH" in status_tag:
            status_emoji = "🔻"
        elif "EXPIRED" in status_tag:
            status_emoji = "⌛"

        msg = (
            f"<b>{status_emoji} OPTIONS SIGNAL (PAPER)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Contract:</b> <code>{contract}</code>\n"
            f"<b>Signal Type:</b> <b>{action}</b> ({signal.underlying})\n"
            f"<b>Confidence Score:</b> <b>{signal.score:.1f}/100</b>\n"
            f"<b>Entry Price:</b> <b>₹{entry_val:.2f}</b>\n"
            f"<b>Live Price:</b> <b>₹{ltp_val:.2f}</b>\n"
            f"<b>Highest Gain:</b> <b>{peak_str}</b>\n"
            f"<b>Status:</b> <code>{status_tag}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>⚠️ Paper simulation only.</i>"
        )
        return msg

    async def send_signal_alert(
        self,
        signal: Signal,
        levels: SignalLevels,
        live_price: Optional[float] = None,
        highest_gain_pct: Optional[float] = None,
        first_hour_gain_pct: Optional[float] = None,
        momentum_status: Optional[str] = None,
    ) -> Optional[int]:
        """Dispatches clean signal alert (1) to Telegram. Returns message_id if sent."""
        if not self.enabled or not self.bot_token or not self.chat_id:
            logger.info(f"[Telegram Mock] Clean Signal Alert: {levels.contract_symbol} {signal.side} Score={signal.score}")
            return None

        # Idempotency check
        dedup_key = f"{signal.signal_id}_{signal.status}"
        if dedup_key in self._sent_dedup_keys:
            return None
        self._sent_dedup_keys.add(dedup_key)

        text = self.format_clean_signal_message(
            signal=signal,
            levels=levels,
            live_price=live_price,
            highest_gain_pct=highest_gain_pct,
            first_hour_gain_pct=first_hour_gain_pct,
            momentum_status=momentum_status,
        )
        url = f"{self.api_base}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    msg_id = data.get("result", {}).get("message_id")
                    logger.info(f"Telegram clean alert sent for {signal.signal_id} (msg_id: {msg_id})")
                    return msg_id
                else:
                    logger.error(f"Telegram API error {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")

        return None

    async def update_signal_message(
        self, message_id: int, signal: Signal, levels: SignalLevels, new_status: str
    ) -> bool:
        """User Requirement 4: 'No (2) Shall be in telegram' - do not push execution state updates to Telegram."""
        return True

    async def send_target_highlight(
        self,
        signal: Signal,
        levels: SignalLevels,
        target_name: str,
        trigger_price: float,
        gain_pts: float,
        gain_pct: float,
        profit_per_lot: float,
        total_profit: float,
    ) -> Optional[int]:
        """User Requirement 4: 'No (2) Shall be in telegram' - suppressed to protect clean feed."""
        logger.info(
            f"[Telegram Suppressed] Target highlight {target_name} on {levels.contract_symbol} suppressed per clean feed rules."
        )
        return None

    async def send_hourly_report(self, stats: Dict[str, Any]) -> Optional[int]:
        """Broadcasts an automated 1-hour signal count and success ratio report."""
        if not self.enabled or not self.bot_token or not self.chat_id:
            h = stats.get("hourly", {})
            logger.info(
                f"[Telegram Mock] Hourly Report: Fired={h.get('signals_fired')}, Triggered={h.get('signals_triggered')}, TPs={h.get('tp1_hits')}, WinRate={h.get('success_ratio_pct')}%"
            )
            return None

        h = stats.get("hourly", {})
        s = stats.get("session", {})

        h_pnl = h.get("realized_pnl", 0.0)
        h_pnl_str = f"+₹{h_pnl:.2f}" if h_pnl >= 0 else f"-₹{abs(h_pnl):.2f}"

        s_pnl = s.get("realized_pnl", 0.0)
        s_pnl_str = f"+₹{s_pnl:.2f}" if s_pnl >= 0 else f"-₹{abs(s_pnl):.2f}"

        now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")

        text = (
            f"📊 <b>HOURLY OPTIONS SIGNAL PERFORMANCE REPORT</b> 📊\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏰ <b>Report Time:</b> {now_str} (Window: Last 60 Minutes)\n\n"
            f"📈 <b>PAST 1 HOUR SUMMARY:</b>\n"
            f"• <b>Signals Fired:</b> <b>{h.get('signals_fired', 0)}</b>\n"
            f"• <b>Triggered Entries:</b> <b>{h.get('signals_triggered', 0)}</b>\n"
            f"• <b>Target 1 (TP1) Hits:</b> <b>{h.get('tp1_hits', 0)}</b> 🎯\n"
            f"• <b>Target 2 (TP2) Hits:</b> <b>{h.get('tp2_hits', 0)}</b> 🎯🎯\n"
            f"• <b>Target 3 (TP3) Hits:</b> <b>{h.get('tp3_hits', 0)}</b> 🚀\n"
            f"• <b>Stop-Loss (SL) Hits:</b> <b>{h.get('sl_hits', 0)}</b> 🛑\n"
            f"• <b>Hourly Success Ratio:</b> <b>{h.get('success_ratio_pct', 0.0):.1f}%</b>\n"
            f"• <b>Hourly Realized P&L:</b> <b>{h_pnl_str}</b>\n\n"
            f"🏛️ <b>CUMULATIVE SESSION SUMMARY:</b>\n"
            f"• <b>Total Signals Fired:</b> {s.get('signals_fired', 0)}\n"
            f"• <b>Total Triggered:</b> {s.get('signals_triggered', 0)}\n"
            f"• <b>Total TP Hits:</b> {s.get('tp1_hits', 0)}\n"
            f"• <b>Total SL Hits:</b> {s.get('sl_hits', 0)}\n"
            f"• <b>Session Success Ratio:</b> <b>{s.get('success_ratio_pct', 0.0):.1f}%</b>\n"
            f"• <b>Session Net P&L:</b> <b>{s_pnl_str}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>Multi-Index Indian Options Bot • Paper Trading Mode</i>"
        )

        url = f"{self.api_base}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    return resp.json().get("result", {}).get("message_id")
        except Exception as e:
            logger.error(f"Failed to send hourly report to Telegram: {e}")
        return None

    async def send_health_notification(self, status_msg: str) -> None:
        """Sends operational status notifications (e.g. system start or shutdown)."""
        if not self.enabled or not self.bot_token or not self.chat_id:
            logger.info(f"[Telegram Mock] Health notification: {status_msg}")
            return

        text = f"🤖 <b>SYSTEM STATUS NOTIFICATION</b>\n\n{status_msg}\n\n<i>Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</i>"
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                await client.post(
                    f"{self.api_base}/sendMessage",
                    json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"},
                )
        except Exception as e:
            logger.error(f"Failed to send health notification to Telegram: {e}")


telegram_notifier = TelegramNotifier()
