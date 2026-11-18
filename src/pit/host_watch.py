"""Watches Docker from the Windows host, where the Airflow alerts can't see: they run inside Docker, so when Docker
itself is down nothing fires. Run every 5 minutes by Task Scheduler (ops/register_host_watch.ps1), also while no one
is signed in to Windows.

Problems it reports:
  docker      the Docker engine doesn't answer: Windows restarted for an update and no one has signed in yet
              (2026-11-11: Docker Desktop only starts at sign-in), Docker Desktop not running, or the engine hung
              (2026-11-10: a WSL auto-update stopped it mid-morning)
  service_*   a compose service (airflow, ib-gateway, postgres) not running, or postgres unhealthy
  scheduler   the Airflow scheduler is not alive: no DAG runs, so no Airflow alerts either
  reboot      Windows has installed updates and is waiting to restart

A problem alerts once it is seen on CONFIRM checks in a row (a Docker restart or sign-in takes a minute or two),
then again every REMIND while it lasts inside the collection window, and a recovery card follows when it clears.
State: DATA_DIR/cache/host_watch/state.json. Discord / email settings come from the repo's .env.

  python -m pit.host_watch            check, alert, update state
  python -m pit.host_watch --dry-run  print what it sees; sends nothing, leaves state alone
"""

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
NY = ZoneInfo("America/New_York")
SERVICES = ("airflow", "ib-gateway", "postgres")
CONFIRM = 2                        # consecutive failing checks before alerting (checks are 5 min apart)
REMIND = timedelta(minutes=60)     # repeat while still failing, inside the collection window only
WINDOW = (timedelta(hours=8), timedelta(minutes=30))  # 08:00 ET .. close + 30 min on session days
NOTIFY_KEYS = ("DISCORD_WEBHOOK_URL", "ALERT_EMAIL_TO", "SMTP_USER", "SMTP_PASSWORD", "SMTP_HOST", "SMTP_PORT")
REBOOT_KEYS = (r"HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update\RebootRequired",
               r"HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing\RebootPending")
NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)  # run under pythonw: no console flashing up every 5 min

Problem = dict  # {"title", "detail", "level", "remind": bool, "recover": bool}


def run(args: list[str], timeout: float = 30) -> subprocess.CompletedProcess | None:
    """None if the command can't start or times out (a hung engine makes docker calls hang)."""
    try:
        return subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace",
                              timeout=timeout, cwd=ROOT, creationflags=NO_WINDOW)
    except (OSError, subprocess.TimeoutExpired):
        return None


def docker_exe() -> str:
    # Task Scheduler may start us with a bare PATH
    return shutil.which("docker") or r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"


def load_env(path: Path = ROOT / ".env") -> None:
    """Only the notification settings: the IBKR / Alpaca secrets in .env are none of this process's business."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() in NOTIFY_KEYS and value.strip():
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def running(image: str) -> bool:
    """Is a process with this image name running in any session (tasklist sees all sessions' names)?"""
    r = run(["tasklist", "/FI", f"IMAGENAME eq {image}", "/NH", "/FO", "CSV"], timeout=20)
    return bool(r and f'"{image.lower()}"' in r.stdout.lower())


def boot_time() -> datetime:
    import ctypes
    up = ctypes.windll.kernel32.GetTickCount64() / 1000
    return datetime.now(timezone.utc) - timedelta(seconds=up)


def when(t: datetime) -> str:
    return f"美东 {t.astimezone(NY):%m-%d %H:%M}（本地 {t.astimezone():%H:%M}）"


def docker_down(signed_in: bool, desktop: bool, booted: datetime) -> Problem:
    if not signed_in:
        detail = (f"Windows 在 {when(booted)} 启动后还没有人登录，Docker Desktop 要登录后才会启动。\n"
                  "处理：登录 Windows → 等 Docker 起来（1–2 分钟）→ 手机上确认 IB 的 2FA。")
    elif not desktop:
        detail = "Docker Desktop 没有运行（被关掉或崩溃）。\n处理：打开 Docker Desktop，或运行 `docker desktop start`，然后确认 IB 的 2FA。"
    else:
        detail = ("Docker Desktop 在运行，但引擎没有响应（2026-11-10 是 WSL 自动更新把它停了）。\n"
                  "处理：`docker desktop restart`，然后确认 IB 的 2FA。")
    return {"title": "🐳 Docker 没有运行：采集和 Airflow 报警都停了", "detail": detail, "level": "error",
            "remind": True, "recover": True}


def check_docker() -> dict[str, Problem]:
    docker = docker_exe()
    r = run([docker, "info", "--format", "{{.ServerVersion}}"], timeout=30)
    if not r or r.returncode != 0 or not r.stdout.strip():
        return {"docker": docker_down(running("explorer.exe"), running("Docker Desktop.exe"), boot_time())}
    r = run([docker, "compose", "ps", "--all", "--format", "json"], timeout=30)
    if not r or r.returncode != 0:
        return {"compose": {"title": "🐳 Docker 有响应，但查不到容器状态", "detail": f"`docker compose ps` 失败：{(r.stderr if r else '超时')[:300]}",
                            "level": "error", "remind": True, "recover": True}}
    problems = service_problems(r.stdout)
    if "service_airflow" not in problems:
        r = run([docker, "compose", "exec", "-T", "airflow", "airflow", "jobs", "check", "--job-type", "SchedulerJob"], timeout=60)
        if not r or r.returncode != 0:
            problems["scheduler"] = {"title": "🌀 Airflow 调度器没有运行：不会有新的采集，也不会有 Airflow 报警",
                                     "detail": "容器在运行，但 `airflow jobs check` 找不到活着的调度器。\n处理：`docker compose restart airflow`",
                                     "level": "error", "remind": True, "recover": True}
    return problems


def service_problems(ps_json: str) -> dict[str, Problem]:
    """From `docker compose ps --all --format json` (one JSON object per line, or one array in older versions)."""
    text = ps_json.strip()
    rows = json.loads(text) if text.startswith("[") else [json.loads(line) for line in text.splitlines() if line.strip()]
    state = {r.get("Service"): (r.get("State", ""), r.get("Health", ""), r.get("Status", "")) for r in rows}
    out = {}
    for svc in SERVICES:
        st, health, status = state.get(svc, ("missing", "", ""))
        if st != "running":
            why = "容器不存在（被删掉了）" if st == "missing" else f"状态：{st}（{status}）"
            fix = "`docker compose up -d`" + ("，然后确认 IB 的 2FA" if svc == "ib-gateway" else "")
            out[f"service_{svc}"] = {"title": f"📦 容器 {svc} 没有运行", "detail": f"{why}\n处理：{fix}",
                                     "level": "error", "remind": True, "recover": True}
        elif health and health not in ("healthy", "starting"):
            out[f"service_{svc}"] = {"title": f"📦 容器 {svc} 不健康", "detail": f"健康检查：{health}（{status}）",
                                     "level": "error", "remind": True, "recover": True}
    return out


def check_reboot() -> dict[str, Problem]:
    if not any((r := run(["reg", "query", k], timeout=10)) and r.returncode == 0 for k in REBOOT_KEYS):
        return {}
    return {"reboot": {"title": "🔄 Windows 更新已装好，等待重启",
                       "detail": "Windows 会在活跃时间之外自己重启；重启后要登录 Windows（Docker 才会启动），再在手机上确认 IB 的 2FA。\n"
                                 "建议：挑非交易时段手动重启，重启后马上登录。",
                       "level": "warn", "remind": False, "recover": False}}


def in_window(now: datetime) -> bool:
    from pit.market_hours import calendar
    cal, day = calendar(), str(now.astimezone(NY).date())
    if not cal.is_session(day):
        return False
    midnight = datetime.combine(now.astimezone(NY).date(), datetime.min.time(), NY)
    return midnight + WINDOW[0] <= now <= cal.session_close(day).to_pydatetime() + WINDOW[1]


def step(state: dict, problems: dict[str, Problem], now: datetime, window: bool) -> tuple[dict, list[tuple]]:
    """Pure: (new state, messages as (title, body, fields, level)) for this check's problems."""
    new, out = {}, []
    for key, p in problems.items():
        s = dict(state.get(key) or {"since": now.isoformat(), "count": 0, "alerted": None})
        s["count"] += 1
        s["title"], s["recover"] = p["title"], p["recover"]
        since = datetime.fromisoformat(s["since"])
        if s["alerted"] is None and s["count"] >= CONFIRM:
            out.append((p["title"], p["detail"], [("🕘 开始", when(since), True)], p["level"]))
            s["alerted"] = now.isoformat()
        elif s["alerted"] and p["remind"] and window and now - datetime.fromisoformat(s["alerted"]) >= REMIND:
            mins = int((now - since).total_seconds() // 60)
            out.append((f"{p['title']}（仍未恢复，已 {mins} 分钟）", p["detail"], [("🕘 开始", when(since), True)], p["level"]))
            s["alerted"] = now.isoformat()
        new[key] = s
    for key, s in state.items():
        if key not in problems and s.get("alerted") and s.get("recover"):
            since = datetime.fromisoformat(s["since"])
            mins = int((now - since).total_seconds() // 60)
            out.append((f"✅ 已恢复：{s['title']}", f"持续约 {mins} 分钟。如果涉及 Gateway，确认一下 2FA 已经完成。",
                        [("🕘 开始", when(since), True), ("🕒 恢复", when(now), True)], "ok"))
    return new, out


def main(argv: list[str]) -> int:
    dry = "--dry-run" in argv
    load_env()
    os.environ.setdefault("DATA_DIR", str(ROOT / "data"))
    now = datetime.now(timezone.utc)
    problems = {**check_docker(), **check_reboot()}
    path = Path(os.environ["DATA_DIR"]) / "cache" / "host_watch" / "state.json"
    state = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    new, messages = step(state, problems, now, in_window(now))
    line = f"{now:%Y-%m-%dT%H:%M:%SZ} problems={sorted(problems) or '-'} sent={[m[0] for m in messages] or '-'}"
    if dry:
        print(line)
        for k, p in problems.items():
            print(f"  {k}: {p['title']}\n    {p['detail']}")
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    log = path.with_name("host_watch.log")  # one line per check, so it's easy to see the task is running
    if log.exists() and log.stat().st_size > 1_000_000:
        log.replace(log.with_suffix(".log.1"))
    import logging  # under pythonw there is no stderr: send()'s log lines (and failures) go to the same file
    logging.basicConfig(filename=log, level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s", encoding="utf-8")
    from pit.notify import send
    for title, body, fields, level in messages:
        send(title, body, fields, level)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(new, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)
    with log.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
