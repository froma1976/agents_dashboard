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
CARDS_SCRIPT = Path(os.getenv("CARDS_SCRIPT", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/scripts/generate_claw_cards_mvp.py"))
AUTOPILOT_LOG = Path(os.getenv("AUTOPILOT_LOG", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/autopilot_log.json"))
AGENTS_RUNTIME = Path(os.getenv("AGENTS_RUNTIME", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/AGENTS_RUNTIME_LOCAL.json"))
AGENTS_HEALTH = Path(os.getenv("AGENTS_HEALTH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/multiagent_health.json"))
ORDERS_PATH = Path(os.getenv("ORDERS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/orders_sim.json"))
JOURNAL_PATH = Path(os.getenv("JOURNAL_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/trades_journal.json"))

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
            ("priority", "TEXT"),
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
        return {"generated_at": None, "macro": [], "market": [], "news": [], "freshness_min": None}
    try:
        data = json.loads(SIGNALS_PATH.read_text(encoding="utf-8"))
        gen = data.get("generated_at")
        freshness = None
        if gen:
            try:
                dt = datetime.fromisoformat(gen.replace("Z", "+00:00"))
                freshness = int((datetime.now(UTC) - dt).total_seconds() // 60)
            except Exception:
                freshness = None
        data["freshness_min"] = freshness
        return data
    except Exception:
        return {"generated_at": None, "macro": [], "market": [], "news": [], "freshness_min": None}


def load_agents_runtime():
    if not AGENTS_RUNTIME.exists():
        return []
    try:
        data = json.loads(AGENTS_RUNTIME.read_text(encoding="utf-8"))
        return data.get("agents", []) if isinstance(data, dict) else []
    except Exception:
        return []


def load_orders():
    if not ORDERS_PATH.exists():
        return {"pending": [], "completed": []}
    try:
        data = json.loads(ORDERS_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"pending": [], "completed": []}
        return {"pending": data.get("pending", []), "completed": data.get("completed", [])}
    except Exception:
        return {"pending": [], "completed": []}


def load_journal():
    if not JOURNAL_PATH.exists():
        return []
    try:
        data = json.loads(JOURNAL_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def append_journal(entry: dict):
    JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    rows = load_journal()
    rows.append(entry)
    JOURNAL_PATH.write_text(json.dumps(rows[-2000:], ensure_ascii=False, indent=2), encoding="utf-8")


def load_agents_health():
    if not AGENTS_HEALTH.exists():
        return []
    try:
        data = json.loads(AGENTS_HEALTH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data.get("results", [])
        return []
    except Exception:
        return []


def load_autopilot_log(limit: int = 15):
    if not AUTOPILOT_LOG.exists():
        return []
    try:
        data = json.loads(AUTOPILOT_LOG.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data[-limit:][::-1]
        return []
    except Exception:
        return []


def save_autopilot_entry(entry: dict):
    AUTOPILOT_LOG.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    if AUTOPILOT_LOG.exists():
        try:
            rows = json.loads(AUTOPILOT_LOG.read_text(encoding="utf-8"))
            if not isinstance(rows, list):
                rows = []
        except Exception:
            rows = []
    rows.append(entry)
    AUTOPILOT_LOG.write_text(json.dumps(rows[-500:], ensure_ascii=False, indent=2), encoding="utf-8")


def upsert_order_pending(ticker: str, score: int, state: str, entry_price: float | None = None):
    ORDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    orders = load_orders()
    pending = orders.get("pending", [])
    if any(o.get("ticker") == ticker and o.get("status") == "pending" for o in pending):
        return False

    target_price = None
    stop_price = None
    if entry_price is not None and entry_price > 0:
        target_price = round(entry_price * 1.06, 4)  # +6%
        stop_price = round(entry_price * 0.97, 4)    # -3%

    pending.append({
        "id": f"ord_{hashlib.sha1((ticker + now_iso()).encode()).hexdigest()[:10]}",
        "ticker": ticker,
        "status": "pending",
        "state": state,
        "score": score,
        "entry_price": entry_price,
        "target_price": target_price,
        "stop_price": stop_price,
        "created_at": now_iso(),
    })
    orders["pending"] = pending
    ORDERS_PATH.write_text(json.dumps(orders, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def auto_close_orders_from_signals(signals: dict):
    orders = load_orders()
    pending = orders.get("pending", [])
    completed = orders.get("completed", [])

    market = signals.get("market", []) if isinstance(signals, dict) else []
    price_map = {}
    for m in market:
        if isinstance(m, dict) and m.get("ticker"):
            px = m.get("regularMarketPrice") or m.get("lastCloseSeries")
            try:
                price_map[m.get("ticker")] = float(px)
            except Exception:
                pass

    new_pending = []
    closed = 0
    for o in pending:
        ticker = o.get("ticker")
        px = price_map.get(ticker)
        target = o.get("target_price")
        stop = o.get("stop_price")
        if px is None or target is None or stop is None:
            new_pending.append(o)
            continue

        result = None
        if px >= float(target):
            result = "ganada"
        elif px <= float(stop):
            result = "perdida"

        if result:
            o["status"] = "completed"
            o["result"] = result
            o["closed_at"] = now_iso()
            o["close_price"] = px
            completed.append(o)
            r_mult = 1 if result == "ganada" else -1
            append_journal({
                "ts": now_iso(),
                "order_id": o.get("id"),
                "ticker": ticker,
                "state": o.get("state"),
                "score": o.get("score"),
                "result": result,
                "r_multiple": r_mult,
            })
            closed += 1
        else:
            new_pending.append(o)

    orders["pending"] = new_pending
    orders["completed"] = completed
    ORDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    ORDERS_PATH.write_text(json.dumps(orders, ensure_ascii=False, indent=2), encoding="utf-8")
    return closed


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
        "SELECT task_id, status, assigned_by, assigned_to, title, details, priority, updated_at "
        "FROM tasks ORDER BY updated_at DESC LIMIT 20"
    )
    token_by_actor = q(
        "SELECT COALESCE(recorded_by,'-') actor, SUM(tokens_in) tin, SUM(tokens_out) tout, SUM(tokens_in+tokens_out) total "
        "FROM token_usage GROUP BY actor ORDER BY total DESC"
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
        "token_by_actor": [dict(r) for r in token_by_actor],
        "portfolio": portfolio,
    }


@app.post("/tasks/create")
def create_task(
    title: str = Form(...),
    assigned_to: str = Form("alpha-scout"),
    conviction: int = Form(3),
    priority: str = Form("media"),
):
    conviction = max(1, min(5, conviction))
    priority = (priority or "media").lower()
    if priority not in {"alta", "media", "baja"}:
        priority = "media"
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
                "INSERT INTO tasks(task_id,title,details,assigned_by,assigned_to,status,fingerprint,source,created_at,updated_at,priority) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, title, details, "fernando", assigned_to, "pending", fp, "dashboard", ts, ts, priority),
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


@app.post("/orders/complete")
def complete_order(order_id: str = Form(...), result: str = Form("simulada")):
    orders = load_orders()
    pending = orders.get("pending", [])
    completed = orders.get("completed", [])
    moved = None
    keep = []
    for o in pending:
        if o.get("id") == order_id and moved is None:
            o["status"] = "completed"
            o["result"] = result
            o["closed_at"] = now_iso()
            moved = o
        else:
            keep.append(o)
    orders["pending"] = keep
    if moved:
        completed.append(moved)
        orders["completed"] = completed
        ORDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        ORDERS_PATH.write_text(json.dumps(orders, ensure_ascii=False, indent=2), encoding="utf-8")
        r_mult = 1 if result == "ganada" else (-1 if result == "perdida" else 0)
        append_journal({
            "ts": now_iso(),
            "order_id": moved.get("id"),
            "ticker": moved.get("ticker"),
            "state": moved.get("state"),
            "score": moved.get("score"),
            "result": result,
            "r_multiple": r_mult,
        })
    return RedirectResponse(url="/", status_code=303)


@app.post("/signals/refresh")
def refresh_signals():
    if INGEST_SCRIPT.exists():
        try:
            subprocess.run(["py", "-3", str(INGEST_SCRIPT)], check=False, timeout=120)
        except Exception:
            pass
    return RedirectResponse(url="/", status_code=303)


@app.post("/signals/autotasks")
def create_tasks_from_top(threshold: int = Form(60), assigned_to: str = Form("alpha-scout")):
    signals = load_signals_snapshot()
    top = signals.get("top_opportunities", []) if isinstance(signals, dict) else []
    conn = sqlite3.connect(DB_PATH)
    created = 0
    try:
        cur = conn.cursor()
        for o in top:
            score = int(o.get("score", 0) or 0)
            if score < threshold:
                continue
            ticker = o.get("ticker", "N/A")
            title = f"Analizar oportunidad {ticker} (score {score})"
            details = f"[conviction:4] auto desde top_opportunities score>={threshold}"
            fp = fingerprint(title, details)
            row = cur.execute(
                "SELECT task_id FROM tasks WHERE fingerprint=? AND status IN ('pending','running')",
                (fp,),
            ).fetchone()
            if row:
                continue
            task_id = f"tsk_{hashlib.sha1((title + now_iso()).encode()).hexdigest()[:10]}"
            ts = now_iso()
            cur.execute(
                "INSERT INTO tasks(task_id,title,details,assigned_by,assigned_to,status,fingerprint,source,created_at,updated_at,priority) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, title, details, "fernando", assigned_to, "pending", fp, "auto-signals", ts, ts, "alta"),
            )
            created += 1
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url=f"/?created={created}", status_code=303)


@app.post("/autopilot/run")
def autopilot_run(threshold: int = Form(60), assigned_to: str = Form("alpha-scout")):
    threshold = max(0, min(100, threshold))

    if INGEST_SCRIPT.exists():
        try:
            subprocess.run(["py", "-3", str(INGEST_SCRIPT)], check=False, timeout=180)
        except Exception:
            pass
    if CARDS_SCRIPT.exists():
        try:
            subprocess.run(["py", "-3", str(CARDS_SCRIPT)], check=False, timeout=120)
        except Exception:
            pass

    signals = load_signals_snapshot()
    top = signals.get("top_opportunities", []) if isinstance(signals, dict) else []
    created = 0
    orders_created = 0
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        for o in top:
            score = int(o.get("score", 0) or 0)
            if score < threshold:
                continue
            state = str(o.get("state", "WATCH"))
            ticker = o.get("ticker", "N/A")
            try:
                entry_price = float(o.get("regularMarketPrice") or o.get("lastCloseSeries"))
            except Exception:
                entry_price = None
            title = f"[AUTO] Ejecutar plan {ticker} (score {score})"
            details = f"[conviction:4] auto-autopilot score>={threshold} reasons={','.join(o.get('reasons', []))}"
            fp = fingerprint(title, details)
            row = cur.execute(
                "SELECT task_id FROM tasks WHERE fingerprint=? AND status IN ('pending','running')",
                (fp,),
            ).fetchone()
            if row:
                continue
            task_id = f"tsk_{hashlib.sha1((title + now_iso()).encode()).hexdigest()[:10]}"
            ts = now_iso()
            cur.execute(
                "INSERT INTO tasks(task_id,title,details,assigned_by,assigned_to,status,fingerprint,source,created_at,updated_at,priority) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, title, details, "autopilot", assigned_to, "pending", fp, "auto-signals", ts, ts, "alta"),
            )
            created += 1
            if state in {"READY", "TRIGGERED"}:
                if upsert_order_pending(ticker, score, state, entry_price):
                    orders_created += 1
        conn.commit()
    finally:
        conn.close()

    closed_orders = auto_close_orders_from_signals(signals)

    save_autopilot_entry({
        "ts": now_iso(),
        "threshold": threshold,
        "assigned_to": assigned_to,
        "created_tasks": created,
        "created_orders": orders_created,
        "closed_orders": closed_orders,
        "top_count": len(top),
    })
    return RedirectResponse(url=f"/?autopilot_created={created}", status_code=303)


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
    autopilot_log = load_autopilot_log()
    agents_runtime = load_agents_runtime()
    agents_health = load_agents_health()
    orders = load_orders()
    pending_orders = orders.get("pending", [])
    completed_orders = orders.get("completed", [])
    journal = load_journal()

    wins = sum(1 for o in completed_orders if str(o.get("result", "")).lower() == "ganada")
    losses = sum(1 for o in completed_orders if str(o.get("result", "")).lower() == "perdida")
    neutral = sum(1 for o in completed_orders if str(o.get("result", "")).lower() == "neutral")
    total_closed = len(completed_orders)
    win_rate = round((wins / total_closed) * 100, 1) if total_closed > 0 else 0.0

    # expectancy y drawdown en R-múltiplos (simulado)
    r_values = [float(j.get("r_multiple", 0)) for j in journal if isinstance(j, dict)]
    expectancy_r = round((sum(r_values) / len(r_values)), 3) if r_values else 0.0
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in r_values:
        cum += r
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)
    max_drawdown_r = round(max_dd, 3)

    freshness = signals.get("freshness_min") if isinstance(signals, dict) else None
    stale = (freshness is None) or (freshness > 20)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "task_counts": data["task_counts"],
            "token_by_model": data["token_by_model"],
            "token_by_actor": data.get("token_by_actor", []),
            "recent_tasks": data["recent_tasks"],
            "cron_rows": data["cron_rows"],
            "portfolio": portfolio,
            "portfolio_positions": positions,
            "portfolio_cash_usd": cash_usd,
            "portfolio_market_value_usd": market_value,
            "portfolio_equity_usd": equity,
            "signals": signals,
            "commits": commits,
            "signals_stale": stale,
            "autopilot_log": autopilot_log,
            "agents_runtime": agents_runtime,
            "agents_health": agents_health,
            "orders_pending": pending_orders,
            "orders_completed": completed_orders,
            "orders_kpi": {
                "pending": len(pending_orders),
                "closed": total_closed,
                "wins": wins,
                "losses": losses,
                "neutral": neutral,
                "win_rate": win_rate,
                "expectancy_r": expectancy_r,
                "max_drawdown_r": max_drawdown_r,
            },
        },
    )
