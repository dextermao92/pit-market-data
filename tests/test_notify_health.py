from datetime import date, datetime, timezone

import polars as pl

from pit import notify
from pit.health import day_summary
from pit.pipeline import RUN_SCHEMA


def test_send_without_channels_only_logs(monkeypatch):
    for k in ("DISCORD_WEBHOOK_URL", "ALERT_EMAIL_TO", "SMTP_USER", "SMTP_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    assert notify.send("hello", "body") == []


def test_send_discord_and_alert_once(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/webhook")
    posted = []

    class Ok:
        def raise_for_status(self):
            pass

    monkeypatch.setattr(notify.httpx, "post", lambda url, json, timeout: posted.append(json["content"]) or Ok())
    assert notify.alert_once("gap", "Alpaca gap", "details") is True
    assert notify.alert_once("gap", "Alpaca gap", "details") is False   # same key, same day: silent
    assert len(posted) == 1 and "Alpaca gap" in posted[0]


def run(ds, status, h, m, **kw):
    at = datetime(2026, 10, 15, h, m, tzinfo=timezone.utc)
    return {"run_id": f"{ds}-{h}{m}", "dataset": ds, "started_at": at, "finished_at": at, "status": status} | kw


def test_yesterday_summary_flags_failures_and_gaps(tmp_path):
    rows = ([run("option_snapshots", "success", 13 + (i * 5 + 30) // 60, (i * 5 + 30) % 60) for i in range(78)]
            + [run("option_snapshots", "failed", 13, 10)]
            + [run(ds, "success", 21, 30) for ds in ("index_snapshots", "corporate_actions", "assets", "daily_bars")]
            + [run("equity_bars", "success", 20, 25, missing_bars=700)])
    d = tmp_path / "pipeline_runs/dataset=all/ingest_date=2026-10-15"
    d.mkdir(parents=True)
    pl.DataFrame(rows, schema=RUN_SCHEMA).write_parquet(d / "runs.parquet")
    ok, lines = day_summary(tmp_path, date(2026, 10, 15))
    text = "\n".join(lines)
    assert not ok
    assert "期权快照：成功 78 次，失败 1 次，0 行，正规时段覆盖 78/78" in text
    assert "收盘时缺 700 根 bar" in text
