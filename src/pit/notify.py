"""Notifications by email (SMTP) and/or Discord webhook, whichever is configured in the environment.

  ALERT_EMAIL_TO, SMTP_USER, SMTP_PASSWORD   Gmail: SMTP_PASSWORD is an app password, not the account password
  SMTP_HOST (default smtp.gmail.com), SMTP_PORT (default 465)
  DISCORD_WEBHOOK_URL
Nothing configured -> the message is only logged. A failing channel never fails the caller.
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
DATASET_ZH = {"option_snapshots": "期权快照", "index_snapshots": "指数快照", "equity_bars": "股票 5 分钟 bar",
              "corporate_actions": "公司行为", "assets": "资产快照", "daily_bars": "日线 bar"}
TASK_ZH = {"chains": "期权快照", "indices": "指数快照", "fetch": "股票 bar", "corporate_actions": "公司行为",
           "assets": "资产快照", "daily_bars": "日线 bar", "refetch_recent_bars": "回拉最近 10 个交易日",
           "dbt_build": "dbt build", "close_summary": "收盘总结", "check_and_send": "状态检查"}


def send(subject: str, body: str) -> list[str]:
    """Send to every configured channel; returns the channels that succeeded."""
    sent = []
    env = os.environ
    if env.get("ALERT_EMAIL_TO") and env.get("SMTP_USER") and env.get("SMTP_PASSWORD"):
        try:
            msg = EmailMessage()
            msg["Subject"], msg["From"], msg["To"] = f"[pit-market-data] {subject}", env["SMTP_USER"], env["ALERT_EMAIL_TO"]
            msg.set_content(body)
            with smtplib.SMTP_SSL(env.get("SMTP_HOST", "smtp.gmail.com"), int(env.get("SMTP_PORT", "465")), timeout=30) as s:
                s.login(env["SMTP_USER"], env["SMTP_PASSWORD"])
                s.send_message(msg)
            sent.append("email")
        except Exception:
            log.exception("email notification failed")
    if env.get("DISCORD_WEBHOOK_URL"):
        try:
            text = f"**{subject}**\n{body}"[:1990]  # Discord caps messages at 2000 chars
            httpx.post(env["DISCORD_WEBHOOK_URL"], json={"content": text}, timeout=15).raise_for_status()
            sent.append("discord")
        except Exception:
            log.exception("discord notification failed")
    log.warning("notification %r -> %s\n%s", subject, sent or "no channel configured", body)
    return sent


def alert_once(key: str, subject: str, body: str) -> bool:
    """Send at most once per New York day per key, so a problem seen every 5 minutes alerts once."""
    day = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).date()
    marker = Path(os.environ.get("DATA_DIR", "data")) / "cache" / "alerts" / str(day) / key
    if marker.exists():
        return False
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(subject)
    send(subject, body)
    return True


def _failing_marker(ti) -> Path:
    return Path(os.environ.get("DATA_DIR", "data")) / "cache" / "failing" / f"{ti.dag_id}.{ti.task_id}"


def task_failed(context) -> None:
    """Airflow on_failure_callback: fires once a task has failed for good (after its retries)."""
    ti = context["ti"]
    marker = _failing_marker(ti)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(str(context.get("run_id")))
    send(f"❌ 任务失败：{TASK_ZH.get(ti.task_id, ti.task_id)}（{ti.dag_id}.{ti.task_id}）",
         f"🕒 运行：{context.get('run_id')}\n🔁 已重试到第 {ti.try_number} 次\n💥 错误：{context.get('exception')!r}"[:1800])


def task_succeeded(context) -> None:
    """Airflow on_success_callback: tell when a task that had failed works again."""
    marker = _failing_marker(context["ti"])
    if marker.exists():
        failed_run = marker.read_text()
        marker.unlink()
        ti = context["ti"]
        send(f"✅ 已恢复：{TASK_ZH.get(ti.task_id, ti.task_id)}（{ti.dag_id}.{ti.task_id}）",
             f"🕒 本次成功：{context.get('run_id')}\n🕘 上次失败：{failed_run}")


CALLBACKS = {"on_failure_callback": task_failed, "on_success_callback": task_succeeded}
