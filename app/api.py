"""
FastAPI server: roda em todos os nós, porta 8000.
Endpoints:
  GET  /health           — quem sou eu, sou líder, redis ok
  GET  /queue/status     — tamanho da stream e pending count
  GET  /results/recent   — últimos N pontos 3D (para o frontend)
  GET  /results/stream   — pontos a partir de um last_id (acumulativo)
  GET  /geometry         — geometria ativa atual
  POST /geometry         — definir geometria ativa (torus/lorenz/mandelbulb/klein)
  GET  /mode             — retorna o modo atual (geometry/render)
  POST /mode             — seta modo
  POST /render/job       — submete um job de render (frame)
  GET  /render/progress  — progresso do frame atual
"""
import time, json
import os
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
GEOM_KEY   = "{stream}:geometry"
RENDER_KEY = "{stream}:render_mode"  # legacy — mantido por compatibilidade
MODE_KEY   = "{stream}:mode"
JOB_KEY    = "{stream}:render:job"
CURR_KEY   = "{stream}:render:current"
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

    try:
        with open("/sys/class/thermal/thermal_zone0/temp", "r") as f:
            temp = round(int(f.read().strip()) / 1000.0, 1)
    except Exception:
        temp = None

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
        "temp":      temp,
    }


@app.get("/queue/status")
def queue_status():
    try:
        info = r.xinfo_stream(STREAM_TASKS)
        pending = r.xpending(STREAM_TASKS, GROUP_NAME)
        try:
            res_info = r.xinfo_stream(STREAM_RESULTS)
            processed_total = res_info.get("entries-added", 0)
        except Exception:
            processed_total = 0

        return {
            "stream_length": info["length"],
            "first_id":      info["first-entry"][0] if info.get("first-entry") else None,
            "last_id":       info["last-entry"][0]  if info.get("last-entry")  else None,
            "pending":       pending.get("pending", 0) if pending else 0,
            "tasks_total":   info.get("entries-added", 0),
            "processed_total": processed_total
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
                    # "worker" = MY_NAME (ex: "worker-01") — campo que o frontend usa
                    # "w" = CONSUMER ID (ex: "worker-01-w0") — não bate com NODES_CONFIG
                    "worker": f.get("worker") or f.get("w", "").rsplit("-w", 1)[0],
                }
                for msg_id, f in entries
                if f.get("x") != "999"
                and f.get("mode", "geometry") == "geometry"  # filtrar tiles de render
            ],
        }
    except Exception as e:
        return {"error": str(e), "points": []}


class GeometryRequest(BaseModel):
    geometry: str
    render_mode: bool = False


@app.get("/geometry")
def get_geometry():
    """Retorna a geometria ativa atual."""
    try:
        geom = r.get(GEOM_KEY) or "torus"
        render_mode = (r.get(RENDER_KEY) == "true")
        return {"geometry": geom, "render_mode": render_mode, "valid": list(VALID_GEOMETRIES)}
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
        r.set(GEOM_KEY, geom)
        r.set(RENDER_KEY, "true" if req.render_mode else "false")

        try:
            r.xtrim(STREAM_TASKS, maxlen=0)
            r.delete(STREAM_RESULTS)
        except Exception:
            pass

        return {"geometry": geom, "render_mode": req.render_mode, "status": "ok",
                "message": f"Geometria alterada para {geom}"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/results/stream")
def results_stream(last_id: str = "0-0", limit: int = 15000):
    """Retorna pontos processados a partir de um last_id para renderização acumulativa."""
    try:
        res = r.xread({STREAM_RESULTS: last_id}, count=limit)
        if not res:
            return {"count": 0, "points": [], "last_id": last_id}

        stream_name, messages = res[0]
        points = []
        new_last_id = last_id

        for msg_id, f in messages:
            new_last_id = msg_id
            if f.get("x") != "999" and f.get("mode", "geometry") == "geometry":
                points.append({
                    "id":     msg_id,
                    "x":      float(f["x"]),
                    "y":      float(f["y"]),
                    "z":      float(f["z"]),
                    "worker": f.get("worker") or f.get("w", "").rsplit("-w", 1)[0],
                })

        return {
            "count": len(points),
            "points": points,
            "last_id": new_last_id
        }
    except Exception as e:
        return {"error": str(e), "points": [], "last_id": last_id}


# ─────────────────────────────────────────
#  B5: Endpoints de modo e render
# ─────────────────────────────────────────

class ModeRequest(BaseModel):
    mode: str


@app.get("/mode")
def get_mode():
    """Retorna o modo de operação atual do cluster."""
    try:
        modo = r.get(MODE_KEY) or "geometry"
        return {"mode": modo}
    except Exception as e:
        return {"error": str(e)}


@app.post("/mode")
def set_mode(req: ModeRequest):
    """
    Seta o modo de operação do cluster.
    Valores válidos: 'geometry' ou 'render'
    """
    if req.mode not in {"geometry", "render"}:
        return {"error": f"Modo '{req.mode}' inválido. Use 'geometry' ou 'render'"}
    try:
        r.set(MODE_KEY, req.mode)
        return {"mode": req.mode, "status": "ok"}
    except Exception as e:
        return {"error": str(e)}


class RenderJobRequest(BaseModel):
    frame_id: str
    scene: str = "mandelbulb"
    camera: dict = {"pos": [0, 1, 4], "target": [0, 0, 0], "fov": 60}
    full_w: int = 640
    full_h: int = 360
    tile_size: int = 64
    time: float = 0.0


@app.post("/render/job")
def submit_render_job(req: RenderJobRequest):
    """
    Submete um job de render para o cluster.
    O daemon (no líder) lê essa chave e enfileira as tarefas de tile.
    """
    try:
        from render.tile_generator import generate_render_tasks

        # Calcular total de tiles para retornar ao cliente
        tarefas = generate_render_tasks(
            frame_id  = req.frame_id,
            scene     = req.scene,
            camera    = req.camera,
            full_w    = req.full_w,
            full_h    = req.full_h,
            tile_size = req.tile_size,
            time_val  = req.time,
        )

        # Gravar job em {stream}:render:job — o daemon vai enfileirar as tarefas
        job_dict = {
            "frame_id":  req.frame_id,
            "scene":     req.scene,
            "camera":    req.camera,
            "full_w":    req.full_w,
            "full_h":    req.full_h,
            "tile_size": req.tile_size,
            "time":      req.time,
        }
        r.set(JOB_KEY, json.dumps(job_dict))

        return {"ok": True, "tiles_total": len(tarefas)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/render/progress")
def render_progress():
    """
    Retorna progresso do frame atual sendo renderizado.
    """
    try:
        curr_json = r.get(CURR_KEY)
        if not curr_json:
            return {"frame_id": None, "tiles_done": 0, "tiles_total": 0}

        curr = json.loads(curr_json)
        frame_id = curr.get("frame_id", "")
        tiles_total = int(curr.get("tiles_total", 0))

        # Contador atômico mantido pelos workers no write_loop
        done_key = f"{{stream}}:render:done:{frame_id}"
        tiles_done = int(r.get(done_key) or 0)

        return {
            "frame_id":    frame_id,
            "tiles_done":  tiles_done,
            "tiles_total": tiles_total,
        }
    except Exception as e:
        return {"error": str(e)}