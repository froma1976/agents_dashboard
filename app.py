from pathlib import Path
import os
import sqlite3
import json
import hashlib
import subprocess
from datetime import datetime, UTC
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("DB_PATH", str(BASE_DIR / "agent_activity_registry.db")))
PORTFOLIO_PATH = Path(os.getenv("PORTFOLIO_PATH", str(BASE_DIR / "portfolio_usd_sample.json")))
SIGNALS_PATH = Path(os.getenv("SIGNALS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/latest_snapshot_free.json"))
INGEST_SCRIPT = Path(os.getenv("INGEST_SCRIPT", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/scripts/source_ingest_free.py"))

app = FastAPI(title="Agent Ops Dashboard")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def fingerprint(title: str, details: str) -> str:
    return hashlib.sha256(f"{norm(title)}|{norm(details)}".encode("utf-8")).hexdigest()[:16]


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                title TEXT,
                details TEXT,
                assigned_by TEXT,
                assigned_to TEXT,
                status TEXT,
                fingerprint TEXT,
                source TEXT,
                created_at TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS token_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model TEXT,
                session_key TEXT,
                tokens_in INTEGER DEFAULT 0,
                tokens_out INTEGER DEFAULT 0,
                recorded_at TEXT,
                recorded_by TEXT
            );

            CREATE TABLE IF NOT EXISTS cron_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                cron_expr TEXT,
                active INTEGER DEFAULT 1,
                owner_user_id TEXT,
                task_ref TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            """
        )

        existing = {r[1] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()}
        for col, ddl in [
            ("details", "TEXT"),
            ("fingerprint", "TEXT"),
            ("source", "TEXT"),
            ("created_at", "TEXT"),
            ("updated_at", "TEXT"),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {ddl}")

        conn.commit()
    finally:
        conn.close()


init_db()


def q(sql: str, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def load_portfolio():
    if not PORTFOLIO_PATH.exists():
        return {
            "capital_initial_usd": 1000,
            "cash_usd": 1000,
            "positions": [],
            "rules": {"max_risk_per_trade_pct": 1.0, "max_total_exposure_pct": 70.0, "currency": "USD"},
        }
    try:
        return json.loads(PORTFOLIO_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"capital_initial_usd": 1000, "cash_usd": 1000, "positions": [], "rules": {}}


def load_signals_snapshot():
    if not SIGNALS_PATH.exists():
        return {"generated_at": None, "macro": [], "market": [], "news": []}
    try:
        return json.loads(SIGNALS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"generated_at": None, "macro": [], "market": [], "news": []}


def latest_commits(limit: int = 6):
    try:
        out = subprocess.check_output(
            ["git", "log", f"-n{limit}", "--pretty=format:%h|%ad|%s", "--date=short"],
            cwd=str(BASE_DIR),
            text=True,
            stderr=subprocess.DEVNULL,
        )
        rows = []
        for line in out.splitlines():
            parts = line.split("|", 2)
            if len(parts) == 3:
                rows.append({"hash": parts[0], "date": parts[1], "msg": parts[2]})
        return rows
    except Exception:
        return []


@app.get("/health")
def health():
    return {"ok": True, "db_path": str(DB_PATH), "exists": DB_PATH.exists()}


@app.get("/api/summary")
def api_summary():
    task_counts = q("SELECT status, COUNT(*) c FROM tasks GROUP BY status ORDER BY c DESC")
    token_by_model = q(
        "SELECT model, SUM(tokens_in) tin, SUM(tokens_out) tout, "
        "SUM(tokens_in + tokens_out) total "
        "FROM token_usage GROUP BY model ORDER BY total DESC"
    )
    recent_tasks = q(
        "SELECT task_id, status, assigned_by, assigned_to, title, details, updated_at "
        "FROM tasks ORDER BY updated_at DESC LIMIT 20"
    )
    cron_rows = q(
        "SELECT name, cron_expr, active, COALESCE(owner_user_id, '-') owner_user_id, "
        "COALESCE(task_ref, '-') task_ref, updated_at "
        "FROM cron_tasks ORDER BY name"
    )
    portfolio = load_portfolio()

    return {
        "task_counts": [dict(r) for r in task_counts],
        "token_by_model": [dict(r) for r in token_by_model],
        "recent_tasks": [dict(r) for r in recent_tasks],
        "cron_rows": [dict(r) for r in cron_rows],
        "portfolio": portfolio,
    }


@app.post("/tasks/create")
def create_task(title: str = Form(...), assigned_to: str = Form("alpha-scout"), conviction: int = Form(3)):
    conviction = max(1, min(5, conviction))
    details = f"[conviction:{conviction}] creada desde dashboard"
    fp = fingerprint(title, details)
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT task_id FROM tasks WHERE fingerprint=? AND status IN ('pending','running')",
            (fp,),
        ).fetchone()
        if not row:
            task_id = f"tsk_{hashlib.sha1((title + now_iso()).encode()).hexdigest()[:10]}"
            ts = now_iso()
            cur.execute(
                "INSERT INTO tasks(task_id,title,details,assigned_by,assigned_to,status,fingerprint,source,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (task_id, title, details, "fernando", assigned_to, "pending", fp, "dashboard", ts, ts),
            )
            conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/", status_code=303)


@app.post("/tasks/status")
def update_task_status(task_id: str = Form(...), status: str = Form(...)):
    allowed = {"pending", "running", "done", "blocked", "cancelled"}
    if status not in allowed:
        return RedirectResponse(url="/", status_code=303)

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "UPDATE tasks SET status=?, updated_at=? WHERE task_id=?",
            (status, now_iso(), task_id),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/", status_code=303)


@app.post("/signals/refresh")
def refresh_signals():
    if INGEST_SCRIPT.exists():
        try:
            subprocess.run(["py", "-3", str(INGEST_SCRIPT)], check=False, timeout=120)
        except Exception:
            pass
    return RedirectResponse(url="/", status_code=303)


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    data = api_summary()
    portfolio = data["portfolio"]
    positions = portfolio.get("positions", [])
    cash_usd = float(portfolio.get("cash_usd", 0))
    market_value = sum(float(p.get("notional_usd", 0)) for p in positions if p.get("status") == "active")
    equity = cash_usd + market_value
    signals = load_signals_snapshot()
    commits = latest_commits()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "task_counts": data["task_counts"],
            "token_by_model": data["token_by_model"],
            "recent_tasks": data["recent_tasks"],
            "cron_rows": data["cron_rows"],
            "portfolio": portfolio,
            "portfolio_positions": positions,
            "portfolio_cash_usd": cash_usd,
            "portfolio_market_value_usd": market_value,
            "portfolio_equity_usd": equity,
            "signals": signals,
            "commits": commits,
        },
    )
