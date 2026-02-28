from pathlib import Path
import os
import sqlite3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent

# DB configurable por entorno (VPS) o local por defecto
DB_PATH = Path(os.getenv("DB_PATH", str(BASE_DIR / "agent_activity_registry.db")))

app = FastAPI(title="Agent Ops Dashboard")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


# ---------------------------
# DB INIT (crea tablas si no existen)
# ---------------------------
def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT,
            status TEXT,
            assigned_by TEXT,
            assigned_to TEXT,
            title TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS token_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model TEXT,
            tokens_in INTEGER DEFAULT 0,
            tokens_out INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS cron_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            cron_expr TEXT,
            active INTEGER DEFAULT 1,
            owner_user_id TEXT,
            task_ref TEXT,
            updated_at TEXT
        );
        """)
        conn.commit()
    finally:
        conn.close()


init_db()


# ---------------------------
# QUERY HELPER
# ---------------------------
def q(sql: str, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


# ---------------------------
# ROUTES
# ---------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "db_path": str(DB_PATH),
        "exists": DB_PATH.exists()
    }


@app.get("/api/summary")
def api_summary():
    task_counts = q(
        "SELECT status, COUNT(*) c FROM tasks GROUP BY status ORDER BY c DESC"
    )

    token_by_model = q(
        "SELECT model, SUM(tokens_in) tin, SUM(tokens_out) tout, "
        "SUM(tokens_in + tokens_out) total "
        "FROM token_usage GROUP BY model ORDER BY total DESC"
    )

    recent_tasks = q(
        "SELECT task_id, status, assigned_by, assigned_to, title, updated_at "
        "FROM tasks ORDER BY updated_at DESC LIMIT 20"
    )

    cron_rows = q(
        "SELECT name, cron_expr, active, "
        "COALESCE(owner_user_id, '-') owner_user_id, "
        "COALESCE(task_ref, '-') task_ref, updated_at "
        "FROM cron_tasks ORDER BY name"
    )

    return {
        "task_counts": [dict(r) for r in task_counts],
        "token_by_model": [dict(r) for r in token_by_model],
        "recent_tasks": [dict(r) for r in recent_tasks],
        "cron_rows": [dict(r) for r in cron_rows],
    }


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    data = api_summary()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "task_counts": data["task_counts"],
            "token_by_model": data["token_by_model"],
            "recent_tasks": data["recent_tasks"],
            "cron_rows": data["cron_rows"],
        },
    )
