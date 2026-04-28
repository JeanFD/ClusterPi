"""
FastAPI server: roda em todos os nós, porta 8000.
Endpoints:
  GET  /health           — quem sou eu, sou líder, redis ok
  GET  /queue/status     — soma dos tamanhos das 6 streams e pending total
  GET  /results/recent   — últimos N pontos 3D (merge de todas as shards)
  GET  /results/stream   — pontos a partir de um last_id (composto)
  GET  /geometry         — geometria ativa atual
  POST /geometry         — definir geometria ativa
  GET  /mode             — retorna o modo atual
  POST /mode             — seta modo
  POST /render/job       — submete um job de render
  POST /render/abort     — limpa render em andamento
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
    SHARDS, TASK_KEYS, RESULT_KEYS, GROUP_NAME, NODES,
    CTRL_MODE, CTRL_GEOM, CTRL_LEADER, CTRL_JOB, CTRL_CURR,
    ctrl_done_key,
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
        leader = r.get(CTRL_LEADER)
    except Exception as e:
        print(f"Erro ao buscar líder: {e}")
        leader = None

    try:
        geom = r.get(CTRL_GEOM) or "torus"
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
    """Soma agregada das 6 shards de tasks/results."""
    try:
        stream_length    = 0
        pending          = 0
        tasks_total      = 0
        processed_total  = 0

        for tkey, rkey in zip(TASK_KEYS, RESULT_KEYS):
            try:
                info = r.xinfo_stream(tkey)
                stream_length += info.get("length", 0)
                tasks_total   += info.get("entries-added", 0)
            except Exception:
                pass
            try:
                p = r.xpending(tkey, GROUP_NAME)
                pending += (p.get("pending", 0) if p else 0)
            except Exception:
                pass
            try:
                res_info = r.xinfo_stream(rkey)
                processed_total += res_info.get("entries-added", 0)
            except Exception:
                pass

        return {
            "stream_length":   stream_length,
            "pending":         pending,
            "tasks_total":     tasks_total,
            "processed_total": processed_total,
            "shards":          SHARDS,
        }
    except Exception as e:
        return {"error": str(e)}


def _extract_point(msg_id, f):
    if f.get("x") == "999" or f.get("mode", "geometry") != "geometry":
        return None
    return {
        "id":     msg_id,
        "x":      float(f["x"]),
        "y":      float(f["y"]),
        "z":      float(f["z"]),
        "worker": f.get("worker") or f.get("w", "").rsplit("-w", 1)[0],
    }


@app.get("/results/recent")
def results_recent(limit: int = 500):
    """Retorna os últimos N pontos — merge de todas as 6 shards."""
    try:
        per_shard = max(1, limit // SHARDS)
        points = []
        for rkey in RESULT_KEYS:
            try:
                entries = r.xrevrange(rkey, count=per_shard)
                for msg_id, f in entries:
                    pt = _extract_point(msg_id, f)
                    if pt:
                        points.append(pt)
            except Exception:
                continue
        points.sort(key=lambda p: p["id"], reverse=True)
        return {"count": len(points[:limit]), "points": points[:limit]}
    except Exception as e:
        return {"error": str(e), "points": []}


def _parse_composite_lastid(s: str) -> list:
    """
    last_id composto para N shards = "id0,id1,...,idN-1".
    Para compatibilidade, um único "0-0" se expande em todas as shards.
    """
    if not s or s == "0-0":
        return ["0-0"] * SHARDS
    parts = s.split(",")
    if len(parts) == 1:
        return [parts[0]] * SHARDS
    if len(parts) < SHARDS:
        parts = parts + ["0-0"] * (SHARDS - len(parts))
    return parts[:SHARDS]


@app.get("/results/stream")
def results_stream(last_id: str = "0-0", limit: int = 15000):
    """
    Retorna pontos a partir de um last_id composto (formato "id0,id1,...,id5").
    Lê de todas as 6 shards e retorna um novo last_id composto atualizado.
    """
    try:
        per_shard = max(1, limit // SHARDS)
        ids = _parse_composite_lastid(last_id)
        new_ids = list(ids)
        points = []

        for i, rkey in enumerate(RESULT_KEYS):
            try:
                res = r.xread({rkey: ids[i]}, count=per_shard)
            except Exception:
                continue
            if not res:
                continue
            _name, messages = res[0]
            for msg_id, f in messages:
                new_ids[i] = msg_id
                pt = _extract_point(msg_id, f)
                if pt:
                    points.append(pt)

        return {
            "count":   len(points),
            "points":  points,
            "last_id": ",".join(new_ids),
        }
    except Exception as e:
        return {"error": str(e), "points": [], "last_id": last_id}


class GeometryRequest(BaseModel):
    geometry: str
    render_mode: bool = False


@app.get("/geometry")
def get_geometry():
    try:
        geom = r.get(CTRL_GEOM) or "torus"
        modo = r.get(CTRL_MODE) or "geometry"
        return {"geometry": geom, "render_mode": modo == "render",
                "valid": list(VALID_GEOMETRIES)}
    except Exception as e:
        return {"geometry": "torus", "error": str(e)}


@app.post("/geometry")
def set_geometry(req: GeometryRequest):
    """
    Define a geometria ativa para o cluster inteiro. Limpa todas as 6
    streams de tasks e results para não misturar pontos entre geometrias.
    """
    geom = req.geometry.lower()
    if geom not in VALID_GEOMETRIES:
        return {"error": f"Geometria '{geom}' inválida. Válidas: {VALID_GEOMETRIES}"}

    try:
        r.set(CTRL_GEOM, geom)
        r.set(CTRL_MODE, "render" if req.render_mode else "geometry")

        for tkey, rkey in zip(TASK_KEYS, RESULT_KEYS):
            try:
                r.xtrim(tkey, maxlen=0)
                r.delete(rkey)
            except Exception:
                pass

        return {"geometry": geom, "render_mode": req.render_mode,
                "status": "ok",
                "message": f"Geometria alterada para {geom}"}
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────
#  Endpoints de modo e render
# ─────────────────────────────────────────

class ModeRequest(BaseModel):
    mode: str


@app.get("/mode")
def get_mode():
    try:
        modo = r.get(CTRL_MODE) or "geometry"
        return {"mode": modo}
    except Exception as e:
        return {"error": str(e)}


@app.post("/mode")
def set_mode(req: ModeRequest):
    if req.mode not in {"geometry", "render"}:
        return {"error": f"Modo '{req.mode}' inválido. Use 'geometry' ou 'render'"}
    try:
        r.set(CTRL_MODE, req.mode)
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
    Submete um job de render. Daemon no líder lê {ctrl}:render:job e
    distribui as tiles round-robin nas 6 shards de tasks.
    """
    try:
        from render.tile_generator import generate_render_tasks

        tarefas = generate_render_tasks(
            frame_id  = req.frame_id,
            scene     = req.scene,
            camera    = req.camera,
            full_w    = req.full_w,
            full_h    = req.full_h,
            tile_size = req.tile_size,
            time_val  = req.time,
        )

        job_dict = {
            "frame_id":  req.frame_id,
            "scene":     req.scene,
            "camera":    req.camera,
            "full_w":    req.full_w,
            "full_h":    req.full_h,
            "tile_size": req.tile_size,
            "time":      req.time,
        }
        r.set(CTRL_JOB, json.dumps(job_dict))

        return {"ok": True, "tiles_total": len(tarefas)}
    except Exception as e:
        return {"error": str(e)}


@app.post("/render/abort")
def abort_render():
    """
    Aborta render em andamento e limpa estado residual:
    - Volta para modo geometry
    - Remove job/current do Redis
    - Apaga todas as 6 task streams (remove PEL junto, libera workers)
    """
    try:
        r.set(CTRL_MODE, "geometry")
        r.delete(CTRL_JOB, CTRL_CURR)
        for tkey in TASK_KEYS:
            try:
                r.delete(tkey)
            except Exception:
                pass
        return {"status": "ok", "mode": "geometry"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/render/progress")
def render_progress():
    """Progresso do frame atual."""
    try:
        curr_json = r.get(CTRL_CURR)
        if not curr_json:
            return {"frame_id": None, "tiles_done": 0, "tiles_total": 0}

        curr = json.loads(curr_json)
        frame_id = curr.get("frame_id", "")
        tiles_total = int(curr.get("tiles_total", 0))

        tiles_done = int(r.get(ctrl_done_key(frame_id)) or 0)

        return {
            "frame_id":    frame_id,
            "tiles_done":  tiles_done,
            "tiles_total": tiles_total,
        }
    except Exception as e:
        return {"error": str(e)}
