"""
FastAPI server: roda em todos os nós, porta 8000.
Endpoints:
  GET  /health           — quem sou eu, sou líder, redis ok
  GET  /queue/status     — tamanho da stream e pending count
  GET  /results/recent   — últimos N pontos 3D (para o frontend)
  GET  /geometry         — geometria ativa atual
  POST /geometry         — definir geometria ativa (torus/lorenz/mandelbulb/klein)
"""
import time
import redis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from redis.cluster import RedisCluster
from config import (
    MY_IP, MY_NAME, REDIS_PORT,
    STREAM_TASKS, STREAM_RESULTS, GROUP_NAME, NODES,
)

app = FastAPI(title=f"cluster-rpi {MY_NAME}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

START_T = time.time()

r = RedisCluster(
    host=MY_IP, 
    port=REDIS_PORT, 
    decode_responses=True,
    socket_timeout=2.0,
    socket_connect_timeout=2.0
)

LEADER_KEY = "{stream}:cluster:leader"
GEOM_KEY = "{stream}:geometry"
VALID_GEOMETRIES = {"torus", "lorenz", "mandelbulb", "klein"}


@app.get("/health")
def health():
    try:
        r.ping()
        redis_ok = "ok"
    except Exception as e:
        print(f"Erro no Ping: {e}")
        redis_ok = "down"

    try:
        leader = r.get(LEADER_KEY)
    except Exception as e:
        print(f"Erro ao buscar líder: {e}")
        leader = None

    try:
        geom = r.get(GEOM_KEY) or "torus"
    except Exception:
        geom = "torus"

    return {
        "node":      MY_NAME,
        "ip":        MY_IP,
        "is_leader": leader == MY_IP,
        "leader":    leader,
        "redis":     redis_ok,
        "uptime_s":  int(time.time() - START_T),
        "raft_peers": len(NODES) - 1,
        "status":    "ok" if redis_ok == "ok" else "degraded",
        "geometry":  geom,
    }


@app.get("/queue/status")
def queue_status():
    try:
        info = r.xinfo_stream(STREAM_TASKS)
        pending = r.xpending(STREAM_TASKS, GROUP_NAME)
        return {
            "stream_length": info["length"],
            "first_id":      info["first-entry"][0] if info.get("first-entry") else None,
            "last_id":       info["last-entry"][0]  if info.get("last-entry")  else None,
            "pending":       pending.get("pending", 0) if pending else 0,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/results/recent")
def results_recent(limit: int = 500):
    """Retorna os últimos N pontos da stream de resultados."""
    try:
        entries = r.xrevrange(STREAM_RESULTS, count=limit)
        return {
            "count": len(entries),
            "points": [
                {
                    "id":     msg_id,
                    "x":      float(f["x"]),
                    "y":      float(f["y"]),
                    "z":      float(f["z"]),
                    "worker": f["worker"],
                }
                for msg_id, f in entries
                if f.get("x") != "999"  # filtrar pontos inválidos do mandelbulb
            ],
        }
    except Exception as e:
        return {"error": str(e), "points": []}


class GeometryRequest(BaseModel):
    geometry: str


@app.get("/geometry")
def get_geometry():
    """Retorna a geometria ativa atual."""
    try:
        geom = r.get(GEOM_KEY) or "torus"
        return {"geometry": geom, "valid": list(VALID_GEOMETRIES)}
    except Exception as e:
        return {"geometry": "torus", "error": str(e)}


@app.post("/geometry")
def set_geometry(req: GeometryRequest):
    """
    Define a geometria ativa para o cluster inteiro.
    O daemon lê essa chave a cada batch e gera tarefas da geometria correta.
    Limpa a stream de resultados para não misturar pontos de geometrias diferentes.
    """
    geom = req.geometry.lower()
    if geom not in VALID_GEOMETRIES:
        return {"error": f"Geometria '{geom}' inválida. Válidas: {VALID_GEOMETRIES}"}

    try:
        # Definir nova geometria
        r.set(GEOM_KEY, geom)

        # Limpar resultados antigos para não misturar geometrias
        try:
            r.delete(STREAM_RESULTS)
        except Exception:
            pass  # Se falhar, tudo bem — os novos pontos vão sobrescrever

        return {"geometry": geom, "status": "ok", "message": f"Geometria alterada para {geom}"}
    except Exception as e:
        return {"error": str(e)}