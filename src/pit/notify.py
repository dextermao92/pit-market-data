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
            text = f"**{subject}**\n```\n{body[:1800]}\n```"  # Discord caps messages at 2000 chars
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
    send(f"FAILED {ti.dag_id}.{ti.task_id}",
         f"run: {context.get('run_id')}\ntry: {ti.try_number}\nerror: {context.get('exception')!r}"[:3000])


def task_succeeded(context) -> None:
    """Airflow on_success_callback: tell when a task that had failed works again."""
    marker = _failing_marker(context["ti"])
    if marker.exists():
        failed_run = marker.read_text()
        marker.unlink()
        send(f"RECOVERED {context['ti'].dag_id}.{context['ti'].task_id}",
             f"succeeded in {context.get('run_id')}\nlast failure: {failed_run}")


CALLBACKS = {"on_failure_callback": task_failed, "on_success_callback": task_succeeded}
