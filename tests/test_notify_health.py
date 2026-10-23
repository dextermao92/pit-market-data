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

    monkeypatch.setattr(notify.httpx, "post", lambda url, json, timeout: posted.append(json["embeds"][0]) or Ok())
    assert notify.alert_once("gap", "Alpaca gap", "details", [("rows", "700", True)]) is True
    assert notify.alert_once("gap", "Alpaca gap", "details") is False   # same key, same day: silent
    card = posted[0]
    assert len(posted) == 1 and card["title"] == "Alpaca gap" and card["color"] == notify.LEVEL_COLOR["warn"]
    assert card["fields"] == [{"name": "rows", "value": "700", "inline": True}]


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
    level, fields = day_summary(tmp_path, date(2026, 10, 15))
    by_name = {n: v for n, v, _ in fields}
    assert level == "error"
    assert by_name["❌ 期权快照"].startswith("成功 78 · 失败 1 · 0 行\n正规时段覆盖 78/78")
    assert "收盘时缺 700 根 bar" in by_name["⚠️ 股票 5 分钟 bar"]


def test_watchdog_flags_only_stale_datasets_in_their_window(tmp_path):
    from datetime import timedelta
    from pit.health import stale_datasets
    at = lambda h, m: datetime(2026, 10, 16, h, m, tzinfo=timezone.utc)  # EDT: 13:30 UTC = 09:30 ET
    rec = lambda ds, start, end: {"run_id": ds, "dataset": ds, "status": "success", "started_at": start, "finished_at": end}
    rows = [rec("option_snapshots", at(13, 15), at(13, 42)),   # long run: data landed 09:42 ET
            rec("index_snapshots", at(14, 30), at(14, 30))]    # last data 10:30 ET
    d = tmp_path / "pipeline_runs/dataset=all/ingest_date=2026-10-16"
    d.mkdir(parents=True)
    pl.DataFrame(rows, schema=RUN_SCHEMA).write_parquet(d / "runs.parquet")
    got = {ds: m for ds, m, _ in stale_datasets(tmp_path, at(14, 35))}       # 10:35 ET
    # options stale since 09:42 (53 min); indices fresh; equity never ran since its 09:50 start (45 min)
    assert got == {"option_snapshots": 53, "equity_bars": 45}
    assert stale_datasets(tmp_path, at(13, 40)) == []                          # 09:40 ET: inside the grace period
    assert stale_datasets(tmp_path, datetime(2026, 10, 17, 15, 0, tzinfo=timezone.utc)) == []   # Saturday


def test_hourly_progress_counts_due_slots_and_recovered_failures(tmp_path):
    from pit.health import progress
    opt = [run("option_snapshots", "success", 13 + (i * 5 + 30) // 60, (i * 5 + 30) % 60, rows=100) for i in range(12) if i != 3]
    rows = (opt + [run("option_snapshots", "failed", 13, 20)]            # 09:20 pre-open failure, recovered since
            + [run("index_snapshots", "failed", 14, 25)]                  # still failing
            + [run("equity_bars", "success", 14, 20, rows=50, missing_bars=2)])
    d = tmp_path / "pipeline_runs/dataset=all/ingest_date=2026-10-15"
    d.mkdir(parents=True)
    pl.DataFrame(rows, schema=RUN_SCHEMA).write_parquet(d / "runs.parquet")
    level, fields = progress(tmp_path, datetime(2026, 10, 15, 14, 30, tzinfo=timezone.utc))   # 10:30 ET
    by_name = {n: v for n, v, _ in fields}
    assert level == "error"
    # slots due 09:30-10:20 = 11; 09:45 missing -> 10/11; the old failure only makes it yellow
    assert "正规时段覆盖 10/11" in by_name["⚠️ 期权快照"] and by_name["⚠️ 期权快照"].startswith("成功 11 · 失败 1 · 1,100 行")
    assert by_name["❌ 指数快照"].startswith("成功 0 · 失败 1")
    assert "最近一次缺 2 根 bar" in by_name["⚠️ 股票 5 分钟 bar"]
