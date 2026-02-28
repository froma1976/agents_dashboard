# Agent Ops Dashboard

Dashboard web (MVP) para el registro centralizado del agente.

## Incluye
- Tareas por estado
- Tokens por modelo
- Últimas tareas
- Cron registradas

## Ejecutar local
```bash
cd C:\Users\Fernando\.openclaw\workspace\agent-ops-dashboard
py -3 -m pip install -r requirements.txt
py -3 -m uvicorn app:app --reload --port 8080
```

Abrir: http://127.0.0.1:8080

## Docker
```bash
docker build -t agent-ops-dashboard .
docker run --rm -p 8080:8080 agent-ops-dashboard
```

## Nota
Usa como fuente la DB: `C:/Users/Fernando/.openclaw/workspace/agent_activity_registry.db`
