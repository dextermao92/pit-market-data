"""Notifications by Discord webhook (as embed cards) and/or email (SMTP), whichever is configured.

  DISCORD_WEBHOOK_URL
  ALERT_EMAIL_TO, SMTP_USER, SMTP_PASSWORD   Gmail: SMTP_PASSWORD is an app password, not the account password
  SMTP_HOST (default smtp.gmail.com), SMTP_PORT (default 465)
Nothing configured -> the message is only logged. A failing channel never fails the caller.

A message is a title, an optional description, fields (name, value, inline) and a level that colours the card.
"""

import logging
import os
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

log = logging.getLogger(__name__)
LEVEL_COLOR = {"ok": 0x2ECC71, "warn": 0xF1C40F, "error": 0xE74C3C, "info": 0x3498DB}  # green, yellow, red, blue
LEVEL_MARK = {"ok": "✅", "warn": "⚠️", "error": "❌", "info": "ℹ️"}
DATASET_ZH = {"option_snapshots": "期权快照", "index_snapshots": "指数快照", "equity_bars": "股票 5 分钟 bar",
              "corporate_actions": "公司行为", "assets": "资产快照", "daily_bars": "日线 bar"}
TASK_ZH = {"chains": "期权快照", "indices": "指数快照", "fetch": "股票 bar", "corporate_actions": "公司行为",
           "assets": "资产快照", "daily_bars": "日线 bar", "refetch_recent_bars": "回拉最近 10 个交易日",
           "dbt_build": "dbt build", "close_summary": "收盘总结", "check_and_send": "状态检查",
           "discover_symbols": "全市场代码发现", "backfill_daily_bars": "全市场日线回补",
           "universe_summary": "全市场刷新总结"}

Field = tuple[str, str, bool]  # name, value, inline (side by side; only for short values)


def _embed(title: str, body: str, fields: list[Field], level: str) -> dict:
    # Discord limits: title 256, description 4096, 25 fields, field name 256 / value 1024.
    return {"title": title[:256], "description": body[:4000], "color": LEVEL_COLOR[level],
            "fields": [{"name": n[:256], "value": (v or "—")[:1024], "inline": i} for n, v, i in fields[:25]],
            "footer": {"text": "pit-market-data"}, "timestamp": datetime.now(timezone.utc).isoformat()}


def as_text(body: str, fields: list[Field]) -> str:
    return "\n".join([body] * bool(body) + [f"{n}\n  " + v.replace("\n", "\n  ") for n, v, _ in fields])


def send(title: str, body: str = "", fields: list[Field] = (), level: str = "info") -> list[str]:
    """Send to every configured channel; returns the channels that succeeded."""
    fields, sent, env = list(fields), [], os.environ
    if env.get("DISCORD_WEBHOOK_URL"):
        try:
            payload = {"embeds": [_embed(title, body, fields, level)]}
            httpx.post(env["DISCORD_WEBHOOK_URL"], json=payload, timeout=15).raise_for_status()
            sent.append("discord")
        except Exception:
            log.exception("discord notification failed")
    if env.get("ALERT_EMAIL_TO") and env.get("SMTP_USER") and env.get("SMTP_PASSWORD"):
        try:
            msg = EmailMessage()
            msg["Subject"], msg["From"], msg["To"] = f"[pit-market-data] {title}", env["SMTP_USER"], env["ALERT_EMAIL_TO"]
            msg.set_content(as_text(body, fields))
            with smtplib.SMTP_SSL(env.get("SMTP_HOST", "smtp.gmail.com"), int(env.get("SMTP_PORT", "465")), timeout=30) as s:
                s.login(env["SMTP_USER"], env["SMTP_PASSWORD"])
                s.send_message(msg)
            sent.append("email")
        except Exception:
            log.exception("email notification failed")
    log.warning("notification %r -> %s\n%s", title, sent or "no channel configured", as_text(body, fields))
    return sent


def alert_once(key: str, title: str, body: str = "", fields: list[Field] = (), level: str = "warn") -> bool:
    """Send at most once per New York day per key, so a problem seen every 5 minutes alerts once."""
    day = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).date()
    marker = Path(os.environ.get("DATA_DIR", "data")) / "cache" / "alerts" / str(day) / key
    if marker.exists():
        return False
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(title, encoding="utf-8")
    send(title, body, fields, level)
    return True


def _failing_marker(ti) -> Path:
    return Path(os.environ.get("DATA_DIR", "data")) / "cache" / "failing" / f"{ti.dag_id}.{ti.task_id}"


def task_failed(context) -> None:
    """Airflow on_failure_callback: fires once a task has failed for good (after its retries). One card per streak:
    while the marker exists the task is known to be failing (a 5-min task down for an hour would send 12)."""
    ti = context["ti"]
    marker = _failing_marker(ti)
    if marker.exists():
        return
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(str(context.get("run_id")), encoding="utf-8")
    send(f"❌ 任务失败：{TASK_ZH.get(ti.task_id, ti.task_id)}", f"`{ti.dag_id}.{ti.task_id}` 失败（重试也用完）；连续失败只提醒这一次，恢复时会再通知",
         [("🕒 运行", str(context.get("run_id")), True), ("🔁 尝试次数", str(ti.try_number), True),
          ("💥 错误", f"```{context.get('exception')!r}"[:1000] + "```", False)], level="error")


def task_succeeded(context) -> None:
    """Airflow on_success_callback: tell when a task that had failed works again."""
    ti = context["ti"]
    marker = _failing_marker(ti)
    if marker.exists():
        failed_run = marker.read_text(encoding="utf-8")
        marker.unlink()
        send(f"✅ 已恢复：{TASK_ZH.get(ti.task_id, ti.task_id)}", f"`{ti.dag_id}.{ti.task_id}` 重新成功",
             [("🕒 本次成功", str(context.get("run_id")), False), ("🕘 上次失败", failed_run, False)], level="ok")


CALLBACKS = {"on_failure_callback": task_failed, "on_success_callback": task_succeeded}
