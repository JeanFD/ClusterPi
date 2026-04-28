"""
Raft daemon: roda em cada nó, mantém estado replicado e elege um líder.
Só o líder chama gerar_tarefas(). Os outros viram workers passivos.

A geometria ativa é lida da chave Redis {ctrl}:geometry
(definida pelo frontend via API). Padrão: "torus".

Modo de operação é lido da chave {ctrl}:mode
Valores: "geometry" (padrão) ou "render"

Tasks são enfileiradas em round-robin nas 6 streams {s0}:tasks .. {s5}:tasks
— cada uma em um master diferente, espalhando a carga pelos 6 nós.
"""
import time, signal, sys, logging, json
from pysyncobj import SyncObj, SyncObjConf, replicated
from config import (
    NODES, MY_IP, MY_NAME, RAFT_PORT,
    SHARDS, TASK_KEYS, GROUP_NAME,
    CTRL_MODE, CTRL_GEOM, CTRL_LEADER, CTRL_JOB, CTRL_CURR,
    ctrl_done_key,
)

logging.basicConfig(
    level=logging.INFO,
    format="{asctime} [{levelname}] {message}",
    style='{'
)
log = logging.getLogger("daemon")

class ClusterState(SyncObj):
    def __init__(self):
        self_addr = f"{MY_IP}:{RAFT_PORT}"
        peers = [f"{ip}:{RAFT_PORT}" for ip in NODES if ip != MY_IP]

        cfg = SyncObjConf(
            dynamicMembershipChange = True,
            appendEntriesUseBatch = True,
            commandsWaitLeader = True,
            connectionTimeout = 3.5,
            raftMinTimeout = 0.4,
            raftMaxTimeout = 1.4,
        )
        super().__init__(self_addr, peers, cfg)

    def stats(self) -> dict:
        return {}

def main():
    log.info(f"[{MY_NAME}] iniciando Raft em {MY_IP}:{RAFT_PORT}")
    state = ClusterState()

    import threading
    from recovery import run as recovery_run

    th = threading.Thread(
        target=recovery_run,
        args=(state._isLeader,),
        daemon=True,
    )
    th.start()

    while state._getLeader() is None:
        log.info("aguardando eleição...")
        time.sleep(1)

    log.info(f"lider eleito: {state._getLeader()}")

    from geometry import generate_batch, DEFAULT_GEOMETRY
    from render.tile_generator import generate_render_tasks
    from redis.cluster import RedisCluster

    r = RedisCluster(host=MY_IP, port=6379, decode_responses=True,
                     socket_timeout=3.0, socket_connect_timeout=3.0)

    total_tasks = 0

    BATCH_SIZE    = 2000
    INTERVAL_S    = 0.1
    MAX_PENDENTES = 30_000   # total somado nas 6 shards

    batch_id = 0
    ultimo_frame_id = None

    limpeza_feita = False   # True apenas durante o mandato atual; reseta ao perder liderança
    era_lider     = False   # detecta transição líder→follower para resetar estado
    erros_redis = 0
    MAX_ERROS_REDIS = 300

    def xpending_total() -> int:
        total = 0
        for k in TASK_KEYS:
            try:
                info = r.xpending(k, GROUP_NAME)
                total += info.get("pending", 0) if info else 0
            except Exception:
                pass
        return total

    def clear_all_tasks():
        for k in TASK_KEYS:
            try:
                r.xtrim(k, maxlen=0)
            except Exception:
                pass

    while True:
        try:
            is_lider = state._isLeader()

            # ── Detecta perda de liderança: reseta estado do mandato ──────────
            if not is_lider:
                if era_lider:
                    limpeza_feita   = False
                    ultimo_frame_id = None
                    era_lider       = False
                    log.info(f"[{MY_NAME}] liderança perdida — estado do mandato resetado")
                time.sleep(INTERVAL_S)
                continue

            # ── É líder ───────────────────────────────────────────────────────
            r.set(CTRL_LEADER, MY_IP, ex=5)
            era_lider   = True
            erros_redis = 0

            modo = r.get(CTRL_MODE) or "geometry"

            if not limpeza_feita:
                limpeza_feita = True
                if modo != "render":
                    # Em modo geometry: limpa streams para começar fresh
                    try:
                        clear_all_tasks()
                        log.info(f"[{MY_NAME}] task streams limpas no startup como líder")
                    except Exception as e:
                        log.warning(f"[{MY_NAME}] Falha ao limpar tasks: {e}")
                else:
                    # Em modo render: mantém tiles em andamento; inicializa
                    # ultimo_frame_id a partir do Redis para não re-despachar.
                    try:
                        curr_json = r.get(CTRL_CURR)
                        if curr_json:
                            curr = json.loads(curr_json)
                            ultimo_frame_id = curr.get("frame_id")
                            log.info(f"[{MY_NAME}] render em andamento: frame "
                                     f"{ultimo_frame_id} mantido")
                    except Exception as e:
                        log.warning(f"[{MY_NAME}] Falha ao ler CTRL_CURR: {e}")

            if modo == "geometry":
                pendentes = xpending_total()
                if pendentes > MAX_PENDENTES:
                    time.sleep(0.05)
                    continue

                geom = r.get(CTRL_GEOM) or DEFAULT_GEOMETRY
                batch_id += 1
                total_tasks += BATCH_SIZE

                points = generate_batch(geom, batch_id, BATCH_SIZE)

                # Round-robin shards: cada ponto vai pra shard (batch_id+i)%SHARDS.
                # Pipeline cross-slot: redis-py roteia cada XADD para o nó dono.
                pipe = r.pipeline(transaction=False)
                shard_max = (MAX_PENDENTES * 2) // SHARDS
                for i, p in enumerate(points):
                    shard = (batch_id + i) % SHARDS
                    pipe.xadd(TASK_KEYS[shard], p,
                              maxlen=shard_max, approximate=True)
                pipe.execute()

                log.info(f"[LIDER] batch {batch_id} → {BATCH_SIZE} tarefas ({geom}) "
                         f"x{SHARDS} shards total={total_tasks}")

            elif modo == "render":
                job_json = r.get(CTRL_JOB)
                if job_json:
                    try:
                        job = json.loads(job_json)
                        frame_id = job.get("frame_id", "")

                        if frame_id != ultimo_frame_id:
                            if ultimo_frame_id:
                                try:
                                    r.delete(ctrl_done_key(ultimo_frame_id))
                                except Exception:
                                    pass
                            tarefas = generate_render_tasks(
                                frame_id   = frame_id,
                                scene      = job.get("scene", "mandelbulb"),
                                camera     = job.get("camera", {}),
                                full_w     = int(job.get("full_w", 640)),
                                full_h     = int(job.get("full_h", 360)),
                                tile_size  = int(job.get("tile_size", 64)),
                                time_val   = float(job.get("time", 0.0)),
                            )

                            # Distribuir tiles round-robin nas shards
                            pipe = r.pipeline(transaction=False)
                            shard_max = (MAX_PENDENTES * 2) // SHARDS
                            for i, t in enumerate(tarefas):
                                shard = i % SHARDS
                                pipe.xadd(TASK_KEYS[shard], t,
                                          maxlen=shard_max, approximate=True)
                            pipe.execute()

                            total_tiles = len(tarefas)
                            ultimo_frame_id = frame_id

                            r.set(CTRL_CURR, json.dumps({
                                "frame_id":    frame_id,
                                "tiles_total": total_tiles,
                                "scene":       job.get("scene", "mandelbulb"),
                            }))

                            log.info(f"[LIDER] render frame {frame_id} → "
                                     f"{total_tiles} tiles em {SHARDS} shards")
                    except Exception as e:
                        log.error(f"erro ao processar job render: {e}")

                time.sleep(0.5)
                continue

            time.sleep(INTERVAL_S)
        except Exception as e:
            erros_redis += 1
            log.error(f"loop error ({erros_redis}/{MAX_ERROS_REDIS}): {e}")
            if erros_redis >= MAX_ERROS_REDIS:
                log.critical(
                    f"[{MY_NAME}] Redis inacessível por ~15s — saindo para "
                    "reinicialização. systemd reelegerá um líder saudável."
                )
                sys.exit(2)
            time.sleep(0.1)

def shutdown(signum, frame):
    log.info("encerrando...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    main()
