from pathlib import Path
import os
import sqlite3
import json
import hashlib
import csv
import subprocess
import urllib.request
import urllib.parse
from datetime import datetime, UTC, timedelta
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("DB_PATH", str(BASE_DIR / "agent_activity_registry.db")))
PORTFOLIO_PATH = Path(os.getenv("PORTFOLIO_PATH", str(BASE_DIR / "portfolio_usd_sample.json")))
SIGNALS_PATH = Path(os.getenv("SIGNALS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/latest_snapshot_free.json"))
INGEST_SCRIPT = Path(os.getenv("INGEST_SCRIPT", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/scripts/source_ingest_free.py"))
CARDS_SCRIPT = Path(os.getenv("CARDS_SCRIPT", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/scripts/generate_claw_cards_mvp.py"))
AUTOPILOT_LOG = Path(os.getenv("AUTOPILOT_LOG", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/autopilot_log.json"))
AGENTS_RUNTIME = Path(os.getenv("AGENTS_RUNTIME", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/AGENTS_RUNTIME_LOCAL.json"))
AGENTS_HEALTH = Path(os.getenv("AGENTS_HEALTH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/multiagent_health.json"))
SOURCES_CONFIG_PATH = Path(os.getenv("SOURCES_CONFIG_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/sources_config_free.json"))
ORDERS_PATH = Path(os.getenv("ORDERS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/orders_sim.json"))
JOURNAL_PATH = Path(os.getenv("JOURNAL_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/trades_journal.json"))
SNAPSHOT_PATH = Path(os.getenv("SNAPSHOT_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/latest_snapshot_free.json"))
BACKUP_ROOT = Path(os.getenv("BACKUP_ROOT", "C:/Users/Fernando/.openclaw/workspace/backups/state"))
CRYPTO_SIGNALS_PATH = Path(os.getenv("CRYPTO_SIGNALS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/crypto_snapshot_free.json"))
CRYPTO_ORDERS_PATH = Path(os.getenv("CRYPTO_ORDERS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/crypto_orders_sim.json"))
CRYPTO_STREAM_STATUS_PATH = Path(os.getenv("CRYPTO_STREAM_STATUS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/crypto_stream_status.json"))
LEARNING_STATUS_PATH = Path(os.getenv("LEARNING_STATUS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/learning_status.json"))
RESEARCH_AGENTS_PATH = Path(os.getenv("RESEARCH_AGENTS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/research_agents_latest.json"))
RESEARCH_QUEUE_PATH = Path(os.getenv("RESEARCH_QUEUE_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/research_experiment_queue.json"))
RESEARCH_RESULTS_PATH = Path(os.getenv("RESEARCH_RESULTS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/research_experiment_results.json"))
RESEARCH_DEPLOYMENTS_PATH = Path(os.getenv("RESEARCH_DEPLOYMENTS_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/config/research_deployments.json"))
GPT53_BUDGET_PATH = Path(os.getenv("GPT53_BUDGET_PATH", "C:/Users/Fernando/.openclaw/workspace/proyectos/analisis-mercados/data/gpt53_budget.json"))
STARTUP_LOG_PATH = Path(os.getenv("STARTUP_LOG_PATH", "C:/Users/Fernando/.openclaw/workspace/startup-stack.log"))
GPT53_MODE = os.getenv("GPT53_MODE", "normal").strip().lower()


def date_iso_to_es(iso_str: str) -> str:
    if not iso_str: return "-"
    try:
        from datetime import datetime
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y")
    except: return iso_str

app = FastAPI(title="Agent Ops Dashboard")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def fingerprint(title: str, details: str) -> str:
    return hashlib.sha256(f"{norm(title)}|{norm(details)}".encode("utf-8")).hexdigest()[:16]


def approx_tokens(text: str) -> int:
    # AproximaciÃ³n simple y estable para telemetrÃ­a local (sin SDK): ~4 chars/token
    if not text:
        return 0
    return max(1, int(len(text) / 4))


def register_token_usage(cur, model: str, actor: str, tin: int, tout: int, session_key: str = "local-autopilot"):
    cur.execute(
        "INSERT INTO token_usage(model, session_key, tokens_in, tokens_out, recorded_at, recorded_by) VALUES(?,?,?,?,?,?)",
        (model, session_key, int(max(0, tin)), int(max(0, tout)), now_iso(), actor),
    )


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
            ("start_at", "TEXT"),
            ("due_at", "TEXT"),
            ("next_check_at", "TEXT"),
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


def load_crypto_snapshot():
    if not CRYPTO_SIGNALS_PATH.exists():
        return {"generated_at": None, "assets": [], "top_opportunities": [], "freshness_min": None}
    try:
        data = json.loads(CRYPTO_SIGNALS_PATH.read_text(encoding="utf-8"))
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
        return {"generated_at": None, "assets": [], "top_opportunities": [], "freshness_min": None}


def load_learning_status():
    if not LEARNING_STATUS_PATH.exists():
        return {"semaforo": "ROJO", "reason": "Sin datos suficientes", "trades_7d": 0, "expectancy_usd": 0, "profit_factor": 0}
    try:
        d = json.loads(LEARNING_STATUS_PATH.read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else {"semaforo": "ROJO", "reason": "Formato invÃ¡lido", "trades_7d": 0}
    except Exception:
        return {"semaforo": "ROJO", "reason": "No se pudo leer learning status", "trades_7d": 0}


def _load_json_file(path: Path, default):
    if not path.exists():
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, type(default)) or isinstance(default, (dict, list)) else data
    except Exception:
        return default


def load_research_panel():
    agents = _load_json_file(RESEARCH_AGENTS_PATH, {})
    queue = _load_json_file(RESEARCH_QUEUE_PATH, {})
    results = _load_json_file(RESEARCH_RESULTS_PATH, {})
    deployments = _load_json_file(RESEARCH_DEPLOYMENTS_PATH, {})
    return {
        "agents": agents if isinstance(agents, dict) else {},
        "queue": queue if isinstance(queue, dict) else {},
        "results": results if isinstance(results, dict) else {},
        "deployments": deployments if isinstance(deployments, dict) else {},
    }


def load_crypto_stream_status():
    if not CRYPTO_STREAM_STATUS_PATH.exists():
        return {"stream_active": False, "latency_ms": None, "last_signal_sec": None}
    try:
        d = json.loads(CRYPTO_STREAM_STATUS_PATH.read_text(encoding="utf-8"))
        return d if isinstance(d, dict) else {"stream_active": False, "latency_ms": None, "last_signal_sec": None}
    except Exception:
        return {"stream_active": False, "latency_ms": None, "last_signal_sec": None}


def load_agents_runtime():
    if not AGENTS_RUNTIME.exists():
        return []
    try:
        data = json.loads(AGENTS_RUNTIME.read_text(encoding="utf-8"))
        return data.get("agents", []) if isinstance(data, dict) else []
    except Exception:
        return []


def load_sources_config():
    if not SOURCES_CONFIG_PATH.exists():
        return {}
    try:
        data = json.loads(SOURCES_CONFIG_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def build_agent_sources(agents_runtime, sources_cfg):
    macro_sources = ["FRED API", "World Bank API"]
    market_sources = ["Finnhub", "FMP", "Alpha Vantage"]
    news_sources = ["NewsAPI", "Reuters RSS", "MarketWatch RSS", "Investing RSS"]
    crypto_sources = ["CoinMarketCap", "Binance Public API"]

    rows = []
    for a in agents_runtime:
        aid = str(a.get("id", ""))
        role = str(a.get("role", ""))
        t = f"{aid} {role}".lower()

        if "macro" in t:
            sources = macro_sources
            focus = "macro/liquidez"
        elif "news" in t and "crypto" in t:
            sources = ["NewsAPI", "CoinMarketCap"]
            focus = "noticias cripto"
        elif "news" in t:
            sources = news_sources
            focus = "noticias/catalizadores"
        elif "technical" in t:
            sources = ["Snapshot mercado", "Indicadores EMA/RSI/Bollinger"]
            focus = "anÃ¡lisis tÃ©cnico"
        elif "risk" in t or "devil" in t:
            sources = ["SeÃ±ales compuestas", "Reglas de riesgo"]
            focus = "riesgo y validaciÃ³n"
        elif "crypto" in t:
            sources = crypto_sources
            focus = "scouting cripto"
        else:
            sources = market_sources + news_sources[:1]
            focus = "orquestaciÃ³n"

        where = " Â· ".join(sources)
        rows.append({"agent": aid, "focus": focus, "where": where, "sources": sources})

    return rows


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


def load_crypto_orders():
    p = CRYPTO_ORDERS_PATH
    if not p.exists():
        return {"active": [], "completed": [], "daily": {"trades": 0}}
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        return {
            "active": d.get("active", []),
            "completed": d.get("completed", []),
            "daily": d.get("daily", {"trades": 0}),
            "portfolio": d.get("portfolio", {"capital_initial_usd": 300, "cash_usd": 300, "market_value_usd": 0, "equity_usd": 300}),
        }
    except Exception:
        return {"active": [], "completed": [], "daily": {"trades": 0}, "portfolio": {"capital_initial_usd": 300, "cash_usd": 300, "market_value_usd": 0, "equity_usd": 300}}


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


def minutes_since_file(path: Path):
    try:
        if not path.exists():
            return None
        m = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        return int((datetime.now(UTC) - m).total_seconds() // 60)
    except Exception:
        return None


def tail_text(path: Path, lines: int = 120) -> str:
    try:
        if not path.exists():
            return ""
        with path.open("r", encoding="utf-8", errors="replace") as f:
            rows = f.readlines()
        return "".join(rows[-lines:])
    except Exception:
        return ""


def run_command(command: list[str], timeout: int = 8) -> dict:
    try:
        proc = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": (proc.stdout or "").strip(),
            "stderr": (proc.stderr or "").strip(),
        }
    except Exception as exc:
        return {"ok": False, "returncode": None, "stdout": "", "stderr": str(exc)}


def port_status(port: int) -> dict:
    info = {"port": port, "listening": False, "pid": None, "process": None}
    res = run_command(["netstat", "-ano"], timeout=6)
    if not res.get("ok"):
        return info

    for line in (res.get("stdout") or "").splitlines():
        if f":{port}" not in line or "LISTENING" not in line:
            continue
        parts = line.split()
        if not parts:
            continue
        try:
            pid = int(parts[-1])
        except Exception:
            pid = None
        info["listening"] = True
        info["pid"] = pid
        if pid:
            task = run_command(["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"], timeout=6)
            first = (task.get("stdout") or "").splitlines()
            if first and "No tasks are running" not in first[0] and "No hay tareas" not in first[0]:
                info["process"] = first[0]
        break
    return info


def scheduled_task_status(name: str) -> dict:
    res = run_command(["schtasks", "/Query", "/TN", name, "/FO", "LIST", "/V"], timeout=10)
    if not res.get("ok"):
        return {"name": name, "exists": False, "raw": res.get("stderr") or res.get("stdout") or "No disponible"}

    raw = res.get("stdout") or ""
    parsed = {}
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        parsed[key.strip()] = value.strip()
    return {
        "name": name,
        "exists": True,
        "state": parsed.get("Estado") or parsed.get("Status"),
        "last_result": parsed.get("Ultimo resultado") or parsed.get("Last Result"),
        "last_run": parsed.get("Ultimo tiempo de ejecucion") or parsed.get("Last Run Time"),
        "next_run": parsed.get("Hora proxima ejecucion") or parsed.get("Next Run Time"),
        "raw": raw,
    }


def sysadmin_snapshot():
    run_status = system_status()
    return {
        "generated_at": now_iso(),
        "run_status": run_status,
        "dashboard": port_status(8080),
        "gateway": port_status(18789),
        "startup_log_tail": tail_text(STARTUP_LOG_PATH, 80),
        "lstm_log_tail": tail_text(BASE_LSTM / "logs" / "history_update_and_train.log", 80),
        "snapshot_file_min": minutes_since_file(SNAPSHOT_PATH),
        "autopilot_log_min": minutes_since_file(AUTOPILOT_LOG),
        "backup_root_min": run_status.get("backup_min"),
        "scheduled_tasks": [
            scheduled_task_status(name)
            for name in [
                "OpenClaw-Autopilot-15m",
                "OpenClaw-State-Backup-10m",
                "OpenClaw-Learning-Daily",
                "OpenClaw-Crypto-Ingest-2m",
                "OpenClaw-Crypto-Scalp-1m",
                "OpenClaw-Crypto-Stream-Probe-3m",
                "OpenClaw-Crypto-Watchdog-10m",
                "LSTM Train (6h)",
            ]
        ],
    }


def terminal_snapshot():
    sysinfo = sysadmin_snapshot()
    return {
        "generated_at": now_iso(),
        "dashboard": sysinfo.get("dashboard"),
        "gateway": sysinfo.get("gateway"),
        "startup_log_tail": sysinfo.get("startup_log_tail"),
        "lstm_log_tail": sysinfo.get("lstm_log_tail"),
        "health": health(),
        "routes": sorted([r.path for r in app.routes if getattr(r, "path", None)]),
        "commands": [
            'C:\\Windows\\py.exe -3 -m uvicorn app:app --host 127.0.0.1 --port 8080',
            'openclaw status',
            'openclaw gateway restart',
            'schtasks /Query /TN "OpenClaw-Autopilot-15m" /FO LIST',
            'type "C:\\Users\\Fernando\\.openclaw\\workspace\\startup-stack.log"',
        ],
    }


def system_status():
    snap_m = minutes_since_file(SNAPSHOT_PATH)
    auto_m = minutes_since_file(AUTOPILOT_LOG)
    backup_m = None
    try:
        if BACKUP_ROOT.exists():
            latest = max(BACKUP_ROOT.iterdir(), key=lambda p: p.stat().st_mtime)
            backup_m = minutes_since_file(latest)
    except Exception:
        backup_m = None

    ok_core = (snap_m is not None and snap_m <= 20) and (auto_m is not None and auto_m <= 30)
    llm_ok = True
    try:
        hs = load_agents_health()
        llm_rows = [h for h in hs if isinstance(h, dict) and str(h.get("model", "")).startswith("ollama/")]
        if llm_rows and not all(bool(h.get("ok")) for h in llm_rows):
            llm_ok = False
    except Exception:
        llm_ok = False

    degraded = ok_core and (not llm_ok)
    if ok_core and llm_ok:
        status = "OPERATIVO"
        color = "ok"
        mode = "NORMAL"
    elif degraded:
        status = "OPERATIVO"
        color = "warn"
        mode = "DEGRADADO"
    else:
        status = "REVISAR"
        color = "no"
        mode = "DEGRADADO"

    return {
        "status": status,
        "color": color,
        "mode": mode,
        "llm_ok": llm_ok,
        "snapshot_min": snap_m,
        "autopilot_min": auto_m,
        "backup_min": backup_m,
    }


def gpt53_limits(mode: str):
    mode = (mode or "ahorro").lower()
    if mode == "pro":
        return {"mode": "pro", "max_calls": 15, "max_tokens": 450000}
    if mode == "normal":
        return {"mode": "normal", "max_calls": 8, "max_tokens": 250000}
    return {"mode": "ahorro", "max_calls": 4, "max_tokens": 120000}


def load_gpt53_budget():
    lim = gpt53_limits(GPT53_MODE)
    today = datetime.now(UTC).date().isoformat()
    data = {"date": today, "calls_used": 0, "tokens_used": 0, **lim}
    try:
        if GPT53_BUDGET_PATH.exists():
            raw = json.loads(GPT53_BUDGET_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                data.update(raw)
        if data.get("date") != today:
            data.update({"date": today, "calls_used": 0, "tokens_used": 0})
        data.update(lim)
    except Exception:
        pass
    return data


def save_gpt53_budget(data: dict):
    GPT53_BUDGET_PATH.parent.mkdir(parents=True, exist_ok=True)
    GPT53_BUDGET_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def should_use_gpt53(top: dict, budget: dict):
    if not isinstance(top, dict):
        return False, "sin_oportunidad"
    if int(budget.get("calls_used", 0)) >= int(budget.get("max_calls", 0)):
        return False, "limite_llamadas"
    if int(budget.get("tokens_used", 0)) >= int(budget.get("max_tokens", 0)):
        return False, "limite_tokens"

    confidence = int(top.get("confidence_pct") or top.get("score_final") or 0)
    state = str(top.get("state") or "WATCH")
    decision = str(top.get("decision_final") or "HOLD")

    if state in {"READY", "TRIGGERED"} and confidence >= 75:
        return True, "pre_decision_critica"
    if decision == "AVOID":
        return True, "conflicto_o_riesgo"
    return False, "no_critico"


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
        "SELECT task_id, status, assigned_by, assigned_to, title, details, priority, updated_at, start_at, due_at, next_check_at "
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
    gpt53_budget = load_gpt53_budget()

    return {
        "task_counts": [dict(r) for r in task_counts],
        "token_by_model": [dict(r) for r in token_by_model],
        "recent_tasks": [dict(r) for r in recent_tasks],
        "cron_rows": [dict(r) for r in cron_rows],
        "token_by_actor": [dict(r) for r in token_by_actor],
        "portfolio": portfolio,
        "gpt53_budget": gpt53_budget,
    }


@app.get("/api/analysis/{ticker}")
def api_analysis(ticker: str):
    tkr = (ticker or "").upper().strip()
    signals = load_signals_snapshot()
    crypto = load_crypto_snapshot()
    market = signals.get("market", []) if isinstance(signals, dict) else []
    row = next((m for m in market if isinstance(m, dict) and str(m.get("ticker", "")).upper() == tkr), None)
    top = next((m for m in (signals.get("top_opportunities", []) if isinstance(signals, dict) else []) if str(m.get("ticker", "")).upper() == tkr), None)

    # soporte cripto (ticker puede venir como BTC-USD)
    ctkr = tkr.replace("-USD", "")
    crypto_assets = crypto.get("assets", []) if isinstance(crypto, dict) else []
    crow = next((m for m in crypto_assets if isinstance(m, dict) and str(m.get("ticker", "")).upper() == ctkr), None)

    orders = load_orders()
    ord_row = next((o for o in (orders.get("pending", []) or []) if str(o.get("ticker", "")).upper() == tkr), None)

    price = None
    try:
        price = float((row or {}).get("regularMarketPrice") or (row or {}).get("lastCloseSeries"))
    except Exception:
        pass
    if price is None and crow:
        try:
            price = float(crow.get("price_usd"))
        except Exception:
            pass

    # vela simple desde Yahoo (Ãºltimas 60)
    candles = []
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(tkr)}?range=3mo&interval=1d"
        req = urllib.request.Request(url, headers={"User-Agent": "agent-ops-dashboard/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read().decode("utf-8", errors="ignore"))
        res = (((data or {}).get("chart") or {}).get("result") or [{}])[0]
        ts = res.get("timestamp") or []
        q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
        o = q.get("open") or []
        h = q.get("high") or []
        l = q.get("low") or []
        c = q.get("close") or []
        for i in range(max(0, len(ts) - 60), len(ts)):
            if i < len(o) and i < len(h) and i < len(l) and i < len(c) and None not in (o[i], h[i], l[i], c[i]):
                candles.append({"t": int(ts[i]), "o": float(o[i]), "h": float(h[i]), "l": float(l[i]), "c": float(c[i])})
    except Exception:
        pass

    base = crow or top or row or {}
    reasons = (base.get("reasons") if isinstance(base, dict) else []) or []
    contra = (base.get("argumento_en_contra") if isinstance(base, dict) else None) or "Sin objeciÃ³n crÃ­tica detectada"
    decision = (base.get("decision_final") if isinstance(base, dict) else None) or "AVOID"
    confidence = (base.get("confidence_pct") if isinstance(base, dict) else None) or (base.get("score_final") if isinstance(base, dict) else None) or (base.get("score") if isinstance(base, dict) else 0) or 0
    bubble = (base.get("bubble_level") if isinstance(base, dict) else None) or "Bajo"

    narrativa = (
        f"{ctkr if crow else tkr}: confianza {confidence}%, burbuja {bubble}. "
        f"SeÃ±ales a favor: {', '.join(reasons[:4]) if reasons else 'sin seÃ±ales fuertes'}. "
        f"Principal objeciÃ³n: {contra}. DecisiÃ³n actual: {decision}."
    )

    if crow and isinstance(crow, dict):
        sr = crow.get("senior_report") or {}
        tr = crow.get("technical_report") or {}
        se = crow.get("sentiment_report") or {}
        narrativa += (
            f" | Setup rÃ¡pido: entrada {sr.get('setup',{}).get('entry','-')}, TP1 {sr.get('setup',{}).get('tp1','-')}, SL {sr.get('setup',{}).get('sl','-')}."
            f" Sesgo tÃ©cnico: {tr.get('sesgo','-')}."
            f" Catalizador: {se.get('catalizador','-')}."
        )

    return JSONResponse({
        "ticker": tkr,
        "price": price,
        "entry_price": (ord_row or {}).get("entry_price"),
        "target_price": (ord_row or {}).get("target_price"),
        "stop_price": (ord_row or {}).get("stop_price"),
        "decision": decision,
        "confidence": confidence,
        "bubble": bubble,
        "narrativa": narrativa,
        "candles": candles,
    })


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
                "INSERT INTO tasks(task_id,title,details,assigned_by,assigned_to,status,fingerprint,source,created_at,updated_at,priority,start_at,due_at,next_check_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, title, details, "fernando", assigned_to, "pending", fp, "dashboard", ts, ts, priority, ts, None, None),
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
def complete_order(order_id: str = Form(...)):
    orders = load_orders()
    pending = orders.get("pending", [])
    completed = orders.get("completed", [])

    # Precio actual desde snapshot para calcular resultado automÃ¡tico
    signals = load_signals_snapshot()
    market = signals.get("market", []) if isinstance(signals, dict) else []
    price_map = {}
    for m in market:
        if isinstance(m, dict) and m.get("ticker"):
            try:
                price_map[m.get("ticker")] = float(m.get("regularMarketPrice") or m.get("lastCloseSeries"))
            except Exception:
                pass

    moved = None
    keep = []
    for o in pending:
        if o.get("id") == order_id and moved is None:
            ticker = o.get("ticker")
            entry = o.get("entry_price")
            close_px = price_map.get(ticker)

            result = "neutral"
            try:
                if close_px is not None and entry is not None:
                    result = "ganada" if float(close_px) >= float(entry) else "perdida"
            except Exception:
                result = "neutral"

            o["status"] = "completed"
            o["result"] = result
            o["closed_at"] = now_iso()
            if close_px is not None:
                o["close_price"] = close_px
            moved = o
        else:
            keep.append(o)

    orders["pending"] = keep
    if moved:
        completed.append(moved)
        orders["completed"] = completed
        ORDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        ORDERS_PATH.write_text(json.dumps(orders, ensure_ascii=False, indent=2), encoding="utf-8")
        res = moved.get("result")
        r_mult = 1 if res == "ganada" else (-1 if res == "perdida" else 0)
        append_journal({
            "ts": now_iso(),
            "order_id": moved.get("id"),
            "ticker": moved.get("ticker"),
            "state": moved.get("state"),
            "score": moved.get("score"),
            "result": res,
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


@app.post("/crypto/pause")
def crypto_pause():
    d = load_crypto_orders()
    daily = d.get("daily", {}) or {}
    daily["paused"] = True
    daily["pause_reason"] = "pausa manual"
    d["daily"] = daily
    CRYPTO_ORDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CRYPTO_ORDERS_PATH.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    return RedirectResponse(url="/?crypto=paused", status_code=303)


@app.post("/crypto/resume")
def crypto_resume():
    d = load_crypto_orders()
    daily = d.get("daily", {}) or {}
    daily["paused"] = False
    daily["pause_reason"] = ""
    daily["loss_streak"] = 0
    d["daily"] = daily
    CRYPTO_ORDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CRYPTO_ORDERS_PATH.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    return RedirectResponse(url="/?crypto=resumed", status_code=303)


@app.post("/kill_switch")
def kill_switch():
    d = load_crypto_orders()
    daily = d.get("daily", {}) or {}
    daily["paused"] = True
    daily["pause_reason"] = "EMERGENCIA KILL SWITCH"
    d["daily"] = daily
    CRYPTO_ORDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CRYPTO_ORDERS_PATH.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("UPDATE tasks SET status='cancelled', updated_at=? WHERE status IN ('pending', 'running')", (now_iso(),))
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(url="/?kill=activated", status_code=303)


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
    gpt53_budget = load_gpt53_budget()
    gpt53_allowed, gpt53_reason = should_use_gpt53(top[0] if top else None, gpt53_budget)
    # Reserva de presupuesto cuando el caso cumple umbral crÃ­tico
    if gpt53_allowed:
        gpt53_budget["calls_used"] = int(gpt53_budget.get("calls_used", 0)) + 1
        gpt53_budget["tokens_used"] = int(gpt53_budget.get("tokens_used", 0)) + 6000
        save_gpt53_budget(gpt53_budget)
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
                # Aunque la tarea ya exista, en modo simulador intentamos abrir orden si aplica
                if state in {"WATCH", "READY", "TRIGGERED"}:
                    if upsert_order_pending(ticker, score, state, entry_price):
                        orders_created += 1
                continue
            task_id = f"tsk_{hashlib.sha1((title + now_iso()).encode()).hexdigest()[:10]}"
            ts = now_iso()
            # prÃ³ximo ciclo aprox cada 15 minutos
            now_dt = datetime.now(UTC)
            mins = (now_dt.minute // 15 + 1) * 15
            if mins >= 60:
                next_dt = now_dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            else:
                next_dt = now_dt.replace(minute=mins, second=0, microsecond=0)
            next_check = next_dt.isoformat(timespec="seconds").replace("+00:00", "Z")
            due_at = (next_dt + timedelta(minutes=30)).isoformat(timespec="seconds").replace("+00:00", "Z")

            cur.execute(
                "INSERT INTO tasks(task_id,title,details,assigned_by,assigned_to,status,fingerprint,source,created_at,updated_at,priority,start_at,due_at,next_check_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, title, details, "autopilot", assigned_to, "pending", fp, "auto-signals", ts, ts, "alta", ts, due_at, next_check),
            )
            created += 1
            # Modo simulador dinÃ¡mico: tambiÃ©n permite WATCH para generar operativa ficticia
            if state in {"WATCH", "READY", "TRIGGERED"}:
                if upsert_order_pending(ticker, score, state, entry_price):
                    orders_created += 1

        # TelemetrÃ­a de tokens por actor (estimada) para entorno local/offline
        market_blob = " ".join(json.dumps(x, ensure_ascii=False) for x in (signals.get("market") or []))
        news_blob = " ".join(
            (it.get("title_es") or it.get("title") or "")
            for feed in (signals.get("news") or [])
            for it in (feed.get("items") or [])
        )
        social_blob = " ".join(json.dumps(x, ensure_ascii=False) for x in (signals.get("social") or []))
        top_blob = " ".join(json.dumps(x, ensure_ascii=False) for x in top)

        register_token_usage(cur, "deterministic/rules", "macro-agent", approx_tokens(market_blob) // 4, 80)
        register_token_usage(cur, "deterministic/rules", "technical-agent", approx_tokens(market_blob) // 2, 120)
        register_token_usage(cur, "deterministic/rules", "news-catalyst-agent", approx_tokens(news_blob), 110)
        register_token_usage(cur, "deterministic/rules", "risk-exec-agent", approx_tokens(social_blob), 70)
        register_token_usage(cur, "deterministic/rules", "devil-advocate-agent", approx_tokens(top_blob), 95)
        if gpt53_allowed:
            register_token_usage(cur, "ollama/qwen3:8b", "local-council-agent", 3200, 900)
        register_token_usage(cur, "deterministic/rules", assigned_to, approx_tokens(top_blob), 140 + created * 25)

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
        "gpt53_mode": gpt53_budget.get("mode"),
        "gpt53_allowed": gpt53_allowed,
        "gpt53_reason": gpt53_reason,
        "gpt53_calls_used": gpt53_budget.get("calls_used", 0),
        "gpt53_calls_max": gpt53_budget.get("max_calls", 0),
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
    crypto_signals = load_crypto_snapshot()
    crypto_stream = load_crypto_stream_status()
    learning_status = load_learning_status()
    research_panel = load_research_panel()
    crypto_orders = load_crypto_orders()
    commits = latest_commits()
    autopilot_log = load_autopilot_log()
    agents_runtime = load_agents_runtime()
    agents_health = load_agents_health()
    sources_cfg = load_sources_config()
    agent_sources = build_agent_sources(agents_runtime, sources_cfg)
    run_status = system_status()
    orders = load_orders()

    # Estado "en directo" por agente (lenguaje natural)
    agent_live = []
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        for a in agents_runtime:
            aid = a.get("id")
            row = cur.execute(
                "SELECT status,title,updated_at FROM tasks WHERE assigned_to=? ORDER BY updated_at DESC LIMIT 1",
                (aid,),
            ).fetchone()
            if row:
                st = row["status"]
                st_es = "trabajando" if st == "running" else ("en espera" if st == "pending" else ("terminado" if st == "done" else ("bloqueado" if st == "blocked" else st)))
                title = (row["title"] or "").lower()

                if "qqq" in title:
                    tarea_humana = "analizando el ETF tecnolÃ³gico principal de EE.UU."
                elif "nvda" in title:
                    tarea_humana = "analizando NVIDIA por posible oportunidad"
                elif "msft" in title:
                    tarea_humana = "analizando Microsoft por posible oportunidad"
                elif "executar plan" in title or "ejecutar plan" in title or "[auto]" in title:
                    tarea_humana = "evaluando si conviene abrir una operaciÃ³n simulada"
                else:
                    tarea_humana = "revisando seÃ±ales del mercado"

                text = f"{aid}: {st_es}; {tarea_humana}. Ãšltima actualizaciÃ³n: {row['updated_at']}"
            else:
                text = f"{aid}: en espera de nuevas seÃ±ales del mercado"
            agent_live.append({"agent": aid, "text": text})
        conn.close()
    except Exception:
        pass
    pending_orders = orders.get("pending", [])
    completed_orders = orders.get("completed", [])
    journal = load_journal()

    # Enriquecer Ã³rdenes pendientes con precio actual y variaciÃ³n % vs entrada
    unrealized_usd_est = 0.0
    try:
        market_rows = signals.get("market", []) if isinstance(signals, dict) else []
        px_map = {}
        for m in market_rows:
            if isinstance(m, dict) and m.get("ticker"):
                p = m.get("regularMarketPrice") or m.get("lastCloseSeries")
                try:
                    px_map[str(m.get("ticker"))] = float(p)
                except Exception:
                    pass

        for o in pending_orders:
            t = str(o.get("ticker") or "")
            cur_px = px_map.get(t)
            o["current_price"] = round(cur_px, 4) if cur_px is not None else None
            entry = o.get("entry_price")
            o["entry_kind"] = "forzada manual" if o.get("forced") else "automÃ¡tica"
            o["entry_status"] = "entrada abierta"
            try:
                if cur_px is not None and entry not in (None, 0, ""):
                    entry_f = float(entry)
                    o["pct_move"] = round(((cur_px - entry_f) / entry_f) * 100, 2)
                    # estimaciÃ³n simple: 1 unidad por seÃ±al
                    o["pnl_usd_est"] = round(cur_px - entry_f, 4)
                    unrealized_usd_est += (cur_px - entry_f)
                else:
                    o["pct_move"] = None
                    o["pnl_usd_est"] = None
            except Exception:
                o["pct_move"] = None
                o["pnl_usd_est"] = None
    except Exception:
        pass

    # Separar Ã³rdenes: pendientes de entrada vs activas (entrada abierta)
    pre_entry_orders = [o for o in pending_orders if o.get("entry_price") in (None, "", 0)]
    active_orders = [o for o in pending_orders if o.get("entry_price") not in (None, "", 0)]

    wins = sum(1 for o in completed_orders if str(o.get("result", "")).lower() == "ganada")
    losses = sum(1 for o in completed_orders if str(o.get("result", "")).lower() == "perdida")
    neutral = sum(1 for o in completed_orders if str(o.get("result", "")).lower() == "neutral")
    total_closed = len(completed_orders)
    win_rate = round((wins / total_closed) * 100, 1) if total_closed > 0 else 0.0

    # expectancy y drawdown en R-mÃºltiplos (simulado)
    r_values = [float(j.get("r_multiple", 0)) for j in journal if isinstance(j, dict)]
    expectancy_r = round((sum(r_values) / len(r_values)), 3) if r_values else 0.0
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    equity_curve = [0.0]
    for r in r_values:
        cum += r
        equity_curve.append(round(cum, 3))
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)
    max_drawdown_r = round(max_dd, 3)

    # SemÃ¡foro global de mercado (simple)
    market_today = {"label": "NEUTRO", "color": "warn", "reason": "seÃ±ales mixtas"}
    try:
        mr = signals.get("macro_regime", {}) if isinstance(signals, dict) else {}
        vix = mr.get("vix")
        macro_adj = mr.get("macro_adj", 0)
        if (vix is not None and float(vix) < 18) or macro_adj >= 6:
            market_today = {"label": "RISK-ON", "color": "ok", "reason": "volatilidad controlada / liquidez favorable"}
        elif (vix is not None and float(vix) > 22) or macro_adj <= -6:
            market_today = {"label": "RISK-OFF", "color": "no", "reason": "volatilidad alta / entorno defensivo"}
    except Exception:
        pass

    def api_probe(url: str, timeout: int = 4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "agent-ops-dashboard/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return (r.getcode() or 200) < 400
        except Exception:
            return False

    finnhub_k = os.getenv("FINNHUB_API_KEY", "").strip()
    fmp_k = os.getenv("FMP_API_KEY", "").strip()
    av_k = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip() or os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
    fred_k = os.getenv("FRED_API_KEY", "").strip()
    news_k = os.getenv("GOOGLE_NEWS_API_KEY", "").strip() or os.getenv("NEWSAPI_KEY", "").strip()
    cg_k = os.getenv("COINGECKO_API_KEY", "").strip()

    api_status = {
        "FINNHUB": ("OK" if (finnhub_k and api_probe(f"https://finnhub.io/api/v1/quote?symbol=AAPL&token={urllib.parse.quote(finnhub_k)}")) else ("FALTA" if not finnhub_k else "ERROR")),
        "FMP": ("OK" if (fmp_k and api_probe(f"https://financialmodelingprep.com/stable/quote?symbol=AAPL&apikey={urllib.parse.quote(fmp_k)}")) else ("FALTA" if not fmp_k else "ERROR")),
        "ALPHA_VANTAGE": ("OK" if (av_k and api_probe(f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=IBM&apikey={urllib.parse.quote(av_k)}")) else ("FALTA" if not av_k else "ERROR")),
        "FRED": ("OK" if (fred_k and api_probe(f"https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key={urllib.parse.quote(fred_k)}&file_type=json&limit=1")) else ("FALTA" if not fred_k else "ERROR")),
        "NEWSAPI": ("OK" if (news_k and api_probe(f"https://newsapi.org/v2/top-headlines?country=us&pageSize=1&apiKey={urllib.parse.quote(news_k)}")) else ("FALTA" if not news_k else "ERROR")),
        "COINGECKO": ("OK" if api_probe(f"https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd{('&x_cg_demo_api_key=' + urllib.parse.quote(cg_k)) if cg_k else ''}") else "ERROR"),
        "OPENINSIDER": "OK",
        "YAHOO_OPTIONS": "OK",
        "FINVIZ": "OK",
    }

    freshness = signals.get("freshness_min") if isinstance(signals, dict) else None
    stale = (freshness is None) or (freshness > 20)
    equity_live_est = round(equity + unrealized_usd_est, 2)

    # cartera cripto separada
    crypto_active = crypto_orders.get("active", []) or []
    crypto_completed = crypto_orders.get("completed", []) or []
    crypto_portfolio = crypto_orders.get("portfolio", {"capital_initial_usd": 300, "cash_usd": 300, "market_value_usd": 0, "equity_usd": 300})
    active_crypto_tickers = {str(o.get("ticker")) for o in crypto_active if o.get("ticker")}
    crypto_map = {str(a.get("ticker")): float(a.get("price_usd")) for a in (crypto_signals.get("assets", []) or []) if a.get("ticker") and a.get("price_usd")}
    crypto_unrealized = 0.0
    crypto_realized = 0.0

    # Calculamos PnL realizado
    for c in crypto_completed:
        try:
            crypto_realized += float(c.get("pnl_usd") or 0)
        except Exception:
            pass

    # Preparamos órdenes completadas unificadas (USANDO FECHAS ORIGINALES PARA ORDENAR)
    unified_completed_orders = []
    for o in completed_orders:
        unified_completed_orders.append({
            "market": "Cartera",
            "ticker": o.get("ticker"),
            "entry_price": o.get("entry_price"),
            "exit_price": o.get("close_price") or o.get("exit_price"),
            "result": o.get("result"),
            "pnl_usd": o.get("pnl_usd") if o.get("pnl_usd") is not None else o.get("pnl_usd_est"),
            "opened_at_raw": o.get("created_at") or o.get("opened_at"),
            "closed_at_raw": o.get("closed_at"),
        })

    for o in crypto_completed:
        unified_completed_orders.append({
            "market": "Cripto",
            "ticker": o.get("ticker"),
            "entry_price": o.get("entry_price"),
            "exit_price": o.get("close_price") or o.get("exit_price"),
            "result": o.get("result"),
            "pnl_usd": o.get("pnl_usd"),
            "opened_at_raw": o.get("opened_at"),
            "closed_at_raw": o.get("closed_at"),
        })

    # Ordenar por fecha de cierre (descendente)
    def _sort_key(row):
        return str(row.get("closed_at_raw") or row.get("opened_at_raw") or "")
    unified_completed_orders = sorted(unified_completed_orders, key=_sort_key, reverse=True)

    # Ahora formateamos las fechas para el display
    for o in unified_completed_orders:
        o["opened_at"] = date_iso_to_es(o.get("opened_at_raw"))
        o["closed_at"] = date_iso_to_es(o.get("closed_at_raw"))

    # También formateamos las listas específicas de cripto (reversas para ver las últimas arriba)
    crypto_completed_view = []
    for c in crypto_completed[::-1]:
        # Creamos una copia para no alterar el objeto original si se usa en otros sitios
        c_view = dict(c)
        c_view["opened_at"] = date_iso_to_es(c.get("opened_at"))
        c_view["closed_at"] = date_iso_to_es(c.get("closed_at"))
        crypto_completed_view.append(c_view)

    for o in crypto_active:
        try:
            o["opened_at"] = date_iso_to_es(o.get("opened_at"))
            ep = float(o.get("entry_price"))
            cp = float(crypto_map.get(o.get("ticker"), ep))
            o["current_price"] = round(cp, 6)
            o["pct_move"] = round(((cp - ep) / ep) * 100, 2)
            o["pnl_usd_est"] = round(cp - ep, 6)
            crypto_unrealized += (cp - ep)
        except Exception:
            o["pct_move"] = None
            o["pnl_usd_est"] = None

    quant_data = []
    quant_path = Path(os.getenv("PRICE_WAREHOUSE_PATH", "C:/Users/Fernando/.openclaw/workspace/memory/price_warehouse.csv"))
    try:
        if quant_path.exists():
            with open(quant_path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                quant_data = list(reader)
                quant_data.reverse()
    except Exception:
        pass

    stock_quant_data = []
    stock_quant_path = Path(os.getenv("STOCK_WAREHOUSE_PATH", "C:/Users/Fernando/.openclaw/workspace/memory/stock_price_warehouse.csv"))
    try:
        if stock_quant_path.exists():
            with open(stock_quant_path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                stock_quant_data = list(reader)
                stock_quant_data.reverse()
    except Exception:
        pass

    rag_journal = []
    journal_db = Path("C:/Users/Fernando/.openclaw/workspace/skills/trading-journal/journal_db.json")
    try:
        if journal_db.exists():
            jdata = json.loads(journal_db.read_text(encoding="utf-8"))
            if isinstance(jdata, dict) and "records" in jdata:
                for r in jdata["records"]:
                    rag_journal.append({
                        "date": r.get("timestamp_utc", ""),
                        "asset": "General/System",
                        "action": "THESIS",
                        "text": r.get("content", "")
                    })
                rag_journal.reverse()
    except Exception:
        pass

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
            "portfolio_equity_live_est": equity_live_est,
            "signals": signals,
            "crypto_signals": crypto_signals,
            "crypto_stream": crypto_stream,
            "learning_status": learning_status,
            "research_panel": research_panel,
            "crypto_orders_active": crypto_active,
            "crypto_orders_completed": crypto_completed_view,
            "crypto_daily": crypto_orders.get("daily", {}),
            "crypto_unrealized_usd_est": round(crypto_unrealized, 4),
            "crypto_realized_usd": round(crypto_realized, 4),
            "crypto_equity_reconciled": round(float(crypto_portfolio.get("capital_initial_usd", 0)) + crypto_realized + crypto_unrealized, 4),
            "crypto_portfolio": crypto_portfolio,
            "active_crypto_tickers": list(active_crypto_tickers),
            "commits": commits,
            "signals_stale": stale,
            "autopilot_log": autopilot_log,
            "agents_runtime": agents_runtime,
            "agents_health": agents_health,
            "agent_sources": agent_sources,
            "agent_live": agent_live,
            "run_status": run_status,
            "orders_pending": pre_entry_orders,
            "orders_active": active_orders,
            "orders_completed": completed_orders,
            "unified_completed_orders": unified_completed_orders[:40],
            "quant_data": quant_data[:100],
            "stock_quant_data": stock_quant_data[:100],
            "rag_journal": rag_journal[:50],
            "orders_kpi": {
                "pending": len(pre_entry_orders),
                "active": len(active_orders),
                "closed": total_closed,
                "wins": wins,
                "losses": losses,
                "neutral": neutral,
                "win_rate": win_rate,
                "expectancy_r": expectancy_r,
                "max_drawdown_r": max_drawdown_r,
                "unrealized_usd_est": round(unrealized_usd_est, 2),
            },
            "equity_curve": equity_curve,
            "market_today": market_today,
            "api_status": api_status,
            "gpt53_budget": data.get("gpt53_budget", {"mode": "ahorro", "calls_used": 0, "max_calls": 4}),
        },
    )

# ===== BEGIN_LSTM_REAL_SAFE =====
import re

BASE_LSTM = Path(r"C:\Users\Fernando\.openclaw\workspace\proyectos\analisis-mercados")
LSTM_LOG = BASE_LSTM / "logs" / "history_update_and_train.log"
LSTM_LOCK = BASE_LSTM / "logs" / "history_train.lock"
LSTM_REGISTRY = BASE_LSTM / "models" / "registry.json"
LSTM_LEARNING_STATUS = BASE_LSTM / "data" / "learning_status.json"
LSTM_WALKFORWARD = BASE_LSTM / "reports" / "walkforward_report.md"

def _tail(path: Path, n: int = 200) -> str:
    if not path.exists():
        return ""
    with path.open("r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    return "".join(lines[-n:])


def _json_or(path: Path, default):
    try:
        if not path.exists():
            return default
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, type(default)) or isinstance(default, (dict, list)) else data
    except Exception:
        return default


def _walkforward_rows() -> list[dict]:
    if not LSTM_WALKFORWARD.exists():
        return []
    rows = []
    try:
        for line in LSTM_WALKFORWARD.read_text(encoding="utf-8", errors="replace").splitlines():
            m = re.match(r"\|\s*([A-Z0-9_]+)\s*\|\s*([0-9.]+)\s*\|\s*([0-9.]+)\s*\|", line)
            if not m or m.group(1) == "Symbol":
                continue
            baseline = float(m.group(2))
            lstm = float(m.group(3))
            rows.append({
                "symbol": m.group(1),
                "baseline_acc": baseline,
                "lstm_acc": lstm,
                "delta": round(lstm - baseline, 3),
            })
    except Exception:
        return []
    return rows

@app.get("/lstm-real", response_class=HTMLResponse)
def lstm_real_page(request: Request):
    html = """
    <!doctype html><html><head><meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>LSTM Real</title>
    <style>
      :root{--bg:#08101a;--panel:#0f1724;--panel2:#101d2f;--line:#27415d;--txt:#e9f2ff;--muted:#91a7c7;--ok:#2fd08d;--warn:#ffba52;--bad:#ff6b6b}
      *{box-sizing:border-box}body{font-family:Georgia,"Segoe UI",serif;margin:0;background:radial-gradient(circle at top,#173154 0,#08101a 56%);color:var(--txt)}
      .wrap{max-width:1220px;margin:0 auto;padding:22px 16px 28px}.hero{display:flex;justify-content:space-between;gap:14px;align-items:end;margin-bottom:16px}
      h1{margin:0;font-size:30px}.muted{color:var(--muted)}.pill{display:inline-block;padding:6px 10px;border-radius:999px;border:1px solid var(--line);background:#091420;font-size:12px}
      .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:14px}.card{grid-column:span 12;background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:18px;padding:16px;box-shadow:0 18px 36px #0000002b}
      .span3{grid-column:span 3}.span4{grid-column:span 4}.span6{grid-column:span 6}.span8{grid-column:span 8}.span12{grid-column:span 12}
      .kpi{font-size:28px;font-weight:700;margin-top:4px}.ok{color:var(--ok)} .bad{color:var(--bad)} .warn{color:var(--warn)}
      button{padding:9px 12px;border-radius:10px;border:1px solid #35518a;background:#1d3b72;color:white;cursor:pointer}
      table{width:100%;border-collapse:collapse;font-size:13px}th,td{padding:8px;border-bottom:1px solid #223950;text-align:left;vertical-align:top}th{color:#c4d7f5}
      pre{background:#0b0f14;color:#d7e0ea;padding:12px;border:1px solid #2a3a5b;border-radius:14px;max-height:48vh;overflow:auto;white-space:pre-wrap}
      .badge{display:inline-block;padding:5px 9px;border-radius:999px;border:1px solid var(--line);font-size:12px;background:#0a1420}
      @media (max-width:920px){.span3,.span4,.span6,.span8{grid-column:span 12}.hero{display:block}}
    </style></head><body>
      <div class="wrap">
        <div class="hero">
          <div>
            <div class="pill">Modelo predictivo explicado para humanos</div>
            <h1>LSTM Real</h1>
            <div class="muted">Aquí ves si el modelo está sano, si mejora frente a una base simple y qué tan útil parece ahora.</div>
          </div>
          <div><button onclick="load()">Actualizar ahora</button></div>
        </div>
        <div class="grid">
          <div class="card span3"><div class="muted">Estado</div><div id="st" class="kpi">...</div><div id="stMeta" class="muted"></div></div>
          <div class="card span3"><div class="muted">Último cierre</div><div id="end" class="kpi">...</div><div id="endMeta" class="muted"></div></div>
          <div class="card span3"><div class="muted">Edge reciente</div><div id="edgeKpi" class="kpi">...</div><div id="edgeMeta" class="muted"></div></div>
          <div class="card span3"><div class="muted">Champion models</div><div id="champKpi" class="kpi">...</div><div id="champMeta" class="muted"></div></div>
          <div class="card span6"><h2>Qué está pasando</h2><div id="humanSummary" class="muted">Cargando...</div><div id="healthBadges" style="margin-top:12px"></div></div>
          <div class="card span6"><h2>Comparativa rápida</h2><table><thead><tr><th>Símbolo</th><th>Mejor MSE</th><th>LSTM vs base</th><th>Lectura</th></tr></thead><tbody id="modelRows"></tbody></table></div>
          <div class="card span12"><h2>Walk-forward entendible</h2><table><thead><tr><th>Símbolo</th><th>Base simple</th><th>LSTM</th><th>Mejora</th><th>Veredicto</th></tr></thead><tbody id="wfRows"></tbody></table></div>
          <div class="card span12"><h2>Log técnico</h2><pre id="log">Cargando log de entrenamiento...</pre></div>
        </div>
      </div>
      <script>
        function badge(text, cls){ return `<span class="badge ${cls||''}">${text}</span>` }
        async function load(){
          try {
            const r = await fetch('/api/lstm-real/status');
            const j = await r.json();
            const statusNode = document.getElementById('st');
            statusNode.textContent = j.training ? 'ENTRENANDO' : 'EN ESPERA';
            statusNode.className = 'kpi ' + (j.training ? 'warn' : 'ok');
            document.getElementById('stMeta').textContent = j.training ? 'Hay un entrenamiento corriendo ahora mismo.' : 'No hay entrenamiento abierto en este instante.';
            document.getElementById('end').textContent = j.last_end ? (j.last_end.exit === 0 ? 'OK' : 'REVISAR') : 'N/D';
            document.getElementById('end').className = 'kpi ' + (j.last_end && j.last_end.exit === 0 ? 'ok' : 'warn');
            document.getElementById('endMeta').textContent = j.last_end ? j.last_end.ended_at : 'Sin cierre detectado';
            document.getElementById('edgeKpi').textContent = (j.learning && j.learning.semaforo) || 'N/D';
            document.getElementById('edgeKpi').className = 'kpi ' + (((j.learning||{}).semaforo === 'VERDE') ? 'ok' : (((j.learning||{}).semaforo === 'AMARILLO') ? 'warn' : 'bad'));
            document.getElementById('edgeMeta').textContent = j.learning ? `Win rate ${j.learning.win_rate ?? '-'}% · Expectancy ${j.learning.expectancy_usd ?? '-'} USD` : 'Sin learning status';
            document.getElementById('champKpi').textContent = String((j.registry_rows||[]).length || 0);
            document.getElementById('champMeta').textContent = 'Modelos champion monitorizados';
            const summary = j.training
              ? 'El modelo está entrenando ahora mismo. La prioridad es dejarlo terminar y mirar si el cierre acaba limpio.'
              : ((j.learning||{}).semaforo === 'VERDE'
                ? 'La lectura actual es positiva: el edge reciente acompaña y el LSTM no parece estar estropeando la base.'
                : ((j.learning||{}).semaforo === 'AMARILLO'
                  ? 'La lectura actual es prudente: el sistema funciona, pero todavía está en una zona de validación y no de confianza total.'
                  : 'La lectura actual pide cuidado: el bloque LSTM no está en una situación bonita y conviene revisarlo antes de confiar demasiado.'));
            document.getElementById('humanSummary').textContent = summary;
            document.getElementById('healthBadges').innerHTML = [
              badge(`Trades 7d: ${(j.learning||{}).trades_7d ?? '-'}`, ''),
              badge(`PnL 7d: ${(j.learning||{}).pnl_7d_usd ?? '-'} USD`, ''),
              badge(`Max DD: ${(j.learning||{}).max_drawdown_usd ?? '-'} USD`, ''),
              badge(`Lock: ${j.training ? 'activo' : 'libre'}`, j.training ? 'warn' : 'ok')
            ].join(' ');
            const modelRows = (j.registry_rows||[]).map(row => {
              const cls = row.best_val_mse < 0.001 ? 'ok' : 'warn';
              return `<tr><td>${row.symbol}</td><td>${row.best_val_mse}</td><td>${row.delta_text}</td><td><span class="${cls}">${row.reading}</span></td></tr>`;
            }).join('');
            document.getElementById('modelRows').innerHTML = modelRows || '<tr><td colspan="4">Sin modelos registrados.</td></tr>';
            const wfRows = (j.walkforward||[]).map(row => {
              const cls = row.delta > 0 ? 'ok' : (row.delta === 0 ? 'warn' : 'bad');
              const verdict = row.delta > 0 ? 'Mejora sobre la base' : 'No mejora todavía';
              return `<tr><td>${row.symbol}</td><td>${row.baseline_acc}</td><td>${row.lstm_acc}</td><td><span class="${cls}">${row.delta > 0 ? '+' : ''}${row.delta}</span></td><td>${verdict}</td></tr>`;
            }).join('');
            document.getElementById('wfRows').innerHTML = wfRows || '<tr><td colspan="5">Sin comparativa walk-forward.</td></tr>';
            document.getElementById('log').textContent = j.log_tail || '(sin log disponible)';
          } catch(e) {
            document.getElementById('log').textContent = 'Error cargando datos: ' + e;
          }
        }
        load(); setInterval(load, 10000);
      </script>
    </body></html>
    """
    return HTMLResponse(html)

@app.get("/api/lstm-real/status")
def lstm_real_status():
    log_tail = _tail(LSTM_LOG, n=220)
    last_end = None
    if log_tail:
        for m in re.finditer(r"\[(?P<ts>[^\]]+)\]\s+END\s+exit=(?P<exit>-?\d+)", log_tail):
            last_end = {"ended_at": m.group("ts"), "exit": int(m.group("exit"))}
    registry = _json_or(LSTM_REGISTRY, {"symbols": {}})
    learning = _json_or(LSTM_LEARNING_STATUS, {})
    walkforward = _walkforward_rows()
    registry_rows = []
    for symbol, payload in (registry.get("symbols") or {}).items():
        best = payload.get("best_val_mse")
        wf = next((r for r in walkforward if r.get("symbol") == symbol), None)
        delta_text = f"{wf['lstm_acc']} vs {wf['baseline_acc']}" if wf else "Sin walk-forward"
        reading = "Fino" if isinstance(best, (int, float)) and best < 0.001 else "Aceptable"
        registry_rows.append({
            "symbol": symbol,
            "best_val_mse": best,
            "delta_text": delta_text,
            "reading": reading,
        })
    return {
        "ok": True,
        "training": LSTM_LOCK.exists(),
        "log_path": str(LSTM_LOG),
        "last_end": last_end,
        "log_tail": log_tail,
        "learning": learning,
        "walkforward": walkforward,
        "registry_rows": registry_rows,
    }
# ===== END_LSTM_REAL_SAFE =====


@app.get("/sysadmin", response_class=HTMLResponse)
def sysadmin_page(request: Request):
    html = """
    <!doctype html><html><head><meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>SysAdmin</title>
    <style>
      :root{--bg:#08111d;--panel:#0f1b2e;--panel2:#13243d;--line:#284464;--txt:#ebf2ff;--muted:#8ea6c9;--ok:#30c48d;--warn:#ffb84d;--bad:#ff6b6b}
      *{box-sizing:border-box}body{margin:0;font-family:Georgia,"Segoe UI",serif;background:radial-gradient(circle at top,#18365c 0,#08111d 55%);color:var(--txt)}
      .wrap{max-width:1240px;margin:0 auto;padding:24px 18px 30px}.hero{display:flex;justify-content:space-between;gap:16px;align-items:end;margin-bottom:18px}
      h1{margin:0;font-size:30px} .muted{color:var(--muted)} .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:14px}
      .card{grid-column:span 12;background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:18px;padding:16px;box-shadow:0 18px 40px #00000030}
      .span4{grid-column:span 4}.span6{grid-column:span 6}.span8{grid-column:span 8}.span12{grid-column:span 12}
      .pill{display:inline-flex;gap:8px;align-items:center;padding:6px 10px;border-radius:999px;border:1px solid var(--line);background:#0a1526;font-size:12px;color:#d8e6ff}
      .kpi{font-size:28px;font-weight:700;margin-top:6px}.ok{color:var(--ok)}.warn{color:var(--warn)}.bad{color:var(--bad)}
      table{width:100%;border-collapse:collapse;font-size:13px}th,td{padding:8px;border-bottom:1px solid #23405e;text-align:left;vertical-align:top}th{color:#bfd3f7}
      pre{margin:0;background:#07111e;border:1px solid #223c57;border-radius:14px;padding:12px;overflow:auto;max-height:300px;white-space:pre-wrap}
      @media (max-width:920px){.span4,.span6,.span8{grid-column:span 12}.hero{display:block}}
    </style></head><body>
      <div class="wrap">
        <div class="hero">
          <div>
            <div class="pill">Cabina operativa local</div>
            <h1>SysAdmin</h1>
            <div class="muted">Estado del stack sin tocar la home de finanzas.</div>
          </div>
          <div class="muted">Auto refresh cada 15s</div>
        </div>
        <div class="grid">
          <div class="card span4"><div class="muted">Dashboard 8080</div><div class="kpi" id="dashState">...</div><div class="muted" id="dashMeta"></div></div>
          <div class="card span4"><div class="muted">Gateway 18789</div><div class="kpi" id="gwState">...</div><div class="muted" id="gwMeta"></div></div>
          <div class="card span4"><div class="muted">Modo del sistema</div><div class="kpi" id="sysMode">...</div><div class="muted" id="sysMeta"></div></div>
          <div class="card span12"><h2>Tareas programadas</h2><table><thead><tr><th>Tarea</th><th>Estado</th><th>Ultima</th><th>Siguiente</th><th>Resultado</th></tr></thead><tbody id="tasksRows"></tbody></table></div>
          <div class="card span6"><h2>Startup log</h2><pre id="startupLog">Cargando...</pre></div>
          <div class="card span6"><h2>LSTM log</h2><pre id="lstmLog">Cargando...</pre></div>
        </div>
      </div>
      <script>
        function badgeClass(ok, warn){ return ok ? 'ok' : (warn ? 'warn' : 'bad'); }
        function yesNo(flag){ return flag ? 'ACTIVO' : 'CAIDO'; }
        async function load(){
          const r = await fetch('/api/sysadmin/status');
          const j = await r.json();
          const dash = j.dashboard || {};
          const gw = j.gateway || {};
          const rs = j.run_status || {};
          const dashNode = document.getElementById('dashState');
          dashNode.textContent = yesNo(!!dash.listening);
          dashNode.className = 'kpi ' + badgeClass(!!dash.listening, false);
          document.getElementById('dashMeta').textContent = dash.pid ? ('PID ' + dash.pid) : 'Sin listener';
          const gwNode = document.getElementById('gwState');
          gwNode.textContent = yesNo(!!gw.listening);
          gwNode.className = 'kpi ' + badgeClass(!!gw.listening, false);
          document.getElementById('gwMeta').textContent = gw.pid ? ('PID ' + gw.pid) : 'Sin listener';
          const sysNode = document.getElementById('sysMode');
          sysNode.textContent = (rs.status || 'REVISAR') + ' / ' + (rs.mode || 'DEGRADADO');
          sysNode.className = 'kpi ' + (rs.color || 'warn');
          document.getElementById('sysMeta').textContent = 'snapshot ' + (j.snapshot_file_min ?? 'n/d') + ' min · autopilot ' + (j.autopilot_log_min ?? 'n/d') + ' min';
          document.getElementById('startupLog').textContent = j.startup_log_tail || '(sin log)';
          document.getElementById('lstmLog').textContent = j.lstm_log_tail || '(sin log)';
          const rows = (j.scheduled_tasks || []).map(t => '<tr><td><strong>' + t.name + '</strong></td><td>' + (t.state || (t.exists ? 'detectada' : 'no encontrada')) + '</td><td>' + (t.last_run || '-') + '</td><td>' + (t.next_run || '-') + '</td><td>' + (t.last_result || '-') + '</td></tr>').join('');
          document.getElementById('tasksRows').innerHTML = rows || '<tr><td colspan="5">Sin tareas registradas</td></tr>';
        }
        load(); setInterval(load, 15000);
      </script>
    </body></html>
    """
    return HTMLResponse(html)


@app.get("/api/sysadmin/status")
def api_sysadmin_status():
    return JSONResponse(sysadmin_snapshot())


@app.get("/terminal", response_class=HTMLResponse)
def terminal_page(request: Request):
    html = """
    <!doctype html><html><head><meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>Terminal</title>
    <style>
      :root{--bg:#05070b;--panel:#0b0e14;--line:#233242;--txt:#d6f6df;--muted:#87c79a;--accent:#7ee787}
      *{box-sizing:border-box}body{margin:0;font-family:"Cascadia Mono","Consolas",monospace;background:#05070b;color:var(--txt)}
      .wrap{max-width:1280px;margin:0 auto;padding:20px}.top{display:flex;justify-content:space-between;gap:16px;align-items:end;margin-bottom:16px}
      h1{margin:0;font-size:28px;color:var(--accent)}.muted{color:var(--muted)}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:14px}
      .card{grid-column:span 12;background:linear-gradient(180deg,#0b0e14,#0a1119);border:1px solid var(--line);border-radius:16px;padding:14px}
      .span4{grid-column:span 4}.span6{grid-column:span 6}.span8{grid-column:span 8}.span12{grid-column:span 12}
      pre{margin:0;background:#030508;border:1px solid #1a2633;border-radius:12px;padding:12px;overflow:auto;max-height:360px;white-space:pre-wrap}
      ul{margin:0;padding-left:18px}.chip{display:inline-block;padding:6px 10px;border:1px solid #22432b;border-radius:999px;background:#0a1710;color:#a8f0b4;font-size:12px;margin-right:8px}
      @media (max-width:920px){.span4,.span6,.span8{grid-column:span 12}.top{display:block}}
    </style></head><body>
      <div class="wrap">
        <div class="top">
          <div>
            <div class="chip">Readonly ops console</div>
            <h1>Terminal</h1>
            <div class="muted">Vista de logs y comandos utiles, sin romper nada.</div>
          </div>
          <div class="muted">Auto refresh cada 10s</div>
        </div>
        <div class="grid">
          <div class="card span4"><div class="muted">Dashboard</div><pre id="dashBlock">...</pre></div>
          <div class="card span4"><div class="muted">Gateway</div><pre id="gwBlock">...</pre></div>
          <div class="card span4"><div class="muted">Health API</div><pre id="healthBlock">...</pre></div>
          <div class="card span8"><div class="muted">Startup log</div><pre id="startupLog">Cargando...</pre></div>
          <div class="card span4"><div class="muted">Comandos utiles</div><pre id="cmdBlock">Cargando...</pre></div>
          <div class="card span12"><div class="muted">LSTM training log</div><pre id="lstmLog">Cargando...</pre></div>
        </div>
      </div>
      <script>
        async function load(){
          const r = await fetch('/api/terminal/status');
          const j = await r.json();
          document.getElementById('dashBlock').textContent = JSON.stringify(j.dashboard || {}, null, 2);
          document.getElementById('gwBlock').textContent = JSON.stringify(j.gateway || {}, null, 2);
          document.getElementById('healthBlock').textContent = JSON.stringify(j.health || {}, null, 2);
          document.getElementById('startupLog').textContent = j.startup_log_tail || '(sin log)';
          document.getElementById('lstmLog').textContent = j.lstm_log_tail || '(sin log)';
          document.getElementById('cmdBlock').textContent = (j.commands || []).join('\n');
        }
        load(); setInterval(load, 10000);
      </script>
    </body></html>
    """
    return HTMLResponse(html)


@app.get("/api/terminal/status")
def api_terminal_status():
    return JSONResponse(terminal_snapshot())


# ===== BEGIN_CONTROL_PAGE =====
@app.get("/control", response_class=HTMLResponse)
def control_page():
    html_path = Path(__file__).parent / "templates" / "control.html"
    if not html_path.exists():
        return HTMLResponse("Template not found", status_code=500)
    return HTMLResponse(html_path.read_text(encoding="utf-8", errors="replace"))
# ===== END_CONTROL_PAGE =====



