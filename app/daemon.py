"""
Raft daemon: roda em cada nó, mantém estado replicado e elege um líder.
Só o líder chama gerar_tarefas(). Os outros viram workers passivos.

A geometria ativa é lida da chave Redis {stream}:geometry
(definida pelo frontend via API). Padrão: "torus".

Modo de operação é lido da chave {stream}:mode
Valores: "geometry" (padrão) ou "render"
"""
import time, signal, sys, logging, json
from pysyncobj import SyncObj, SyncObjConf, replicated
from config import NODES, MY_IP, MY_NAME, RAFT_PORT

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

    # A5: batch_id agora é contador local no líder — sem round-trip Raft por batch.
    # Economiza ~30% do tráfego Raft. O ID reseta se o líder mudar, o que
    # é aceitável: generate_batch usa o ID só como semente de índice.
    def stats(self) -> dict:
        return {}

def main():
    log.info(f"[{MY_NAME}] iniciando Raft em {MY_IP}:{RAFT_PORT}")
    state = ClusterState()

    import threading
    from recovery import run as recovery_run

    th = threading.Thread(
        target=recovery_run,
        args=(state._isLeader,),  # passa o método como callback
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
    from config import STREAM_TASKS, GROUP_NAME

    r = RedisCluster(host=MY_IP, port=6379, decode_responses=True)

    # A5: total_tasks contado localmente — evita ~30% do tráfego Raft
    total_tasks = 0

    # A2: parâmetros de produção tunados
    # BATCH_SIZE menor + backpressure evita fila de milhões de tarefas
    # O gargalo real é o throughput dos workers (~1K-5K pts/s por nó em numpy)
    BATCH_SIZE    = 2000
    INTERVAL_S    = 0.1
    # Alvo de ~30k tarefas pendentes. Com 24 workers @ ~2k pts/s cada = 48k/s
    # de consumo, 30k de headroom ≈ 600ms de buffer — saudável.
    MAX_PENDENTES = 30_000

    # Chave Redis onde o frontend define a geometria ativa
    GEOM_KEY   = "{stream}:geometry"
    MODE_KEY   = "{stream}:mode"
    JOB_KEY    = "{stream}:render:job"
    CURR_KEY   = "{stream}:render:current"

    # A5: contador local de batch — sem round-trip Raft por batch
    batch_id = 0

    # Controle de frame render: guarda o último frame_id enfileirado
    ultimo_frame_id = None

    # Detecta primeira vez que este processo vira líder (para limpar streams)
    ja_foi_lider = False
    # Contador de erros Redis consecutivos — se muitos, sai e deixa systemd reiniciar
    erros_redis = 0
    MAX_ERROS_REDIS = 300  # ~15 segundos de falhas consecutivas (300 × 0.05s)

    while True:
        try:
            if state._isLeader():
                r.set("{stream}:cluster:leader", MY_IP, ex=5)  # TTL 5s
                erros_redis = 0  # Redis OK — zera contador

                # Na primeira vez que vira líder neste processo: limpa stream de tasks.
                # Garante que entradas antigas (de antes do restart) não bloqueiam nada.
                if not ja_foi_lider:
                    ja_foi_lider = True
                    try:
                        r.xtrim(STREAM_TASKS, maxlen=0)
                        log.info(f"[{MY_NAME}] STREAM_TASKS limpa no startup como líder")
                    except Exception as e:
                        log.warning(f"[{MY_NAME}] Falha ao limpar tasks no startup: {e}")

                # B4: ler modo de operação a cada iteração
                modo = r.get(MODE_KEY) or "geometry"

                if modo == "geometry":
                    # Backpressure: usa xpending (tarefas entregues mas não ACKed)
                    # xlen inclui entradas já processadas/ACKed e cresce sem limite —
                    # usar xpending evita que o daemon congele permanentemente.
                    try:
                        pend_info = r.xpending(STREAM_TASKS, GROUP_NAME)
                        pendentes = pend_info.get("pending", 0) if pend_info else 0
                    except Exception:
                        pendentes = 0

                    if pendentes > MAX_PENDENTES:
                        time.sleep(0.05)
                        continue
                    # Modo original: gerar batch de pontos geométricos
                    geom = r.get(GEOM_KEY) or DEFAULT_GEOMETRY

                    batch_id += 1  # A5: contador local, sem round-trip Raft
                    total_tasks += BATCH_SIZE

                    points = generate_batch(geom, batch_id, BATCH_SIZE)

                    # A3: pipeline sem transaction; maxlen mantém stream limitada
                    pipe = r.pipeline(transaction=False)
                    for p in points:
                        pipe.xadd(STREAM_TASKS, p, maxlen=MAX_PENDENTES * 2, approximate=True)
                    pipe.execute()

                    log.info(f"[LIDER] batch {batch_id} → {BATCH_SIZE} tarefas ({geom}) total={total_tasks}")

                elif modo == "render":
                    # B4: modo render — enfileirar tiles de um frame
                    job_json = r.get(JOB_KEY)
                    if job_json:
                        try:
                            job = json.loads(job_json)
                            frame_id = job.get("frame_id", "")

                            # Só enfileira se for um frame novo
                            if frame_id != ultimo_frame_id:
                                # Resetar contador do frame anterior para não vazar memória
                                if ultimo_frame_id:
                                    try:
                                        r.delete(f"{{stream}}:render:done:{ultimo_frame_id}")
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

                                # Enfileirar todas as tarefas de uma vez
                                pipe = r.pipeline(transaction=False)
                                for t in tarefas:
                                    pipe.xadd(STREAM_TASKS, t, maxlen=MAX_PENDENTES * 2, approximate=True)
                                pipe.execute()

                                total_tiles = len(tarefas)
                                ultimo_frame_id = frame_id

                                # Marcar frame como em progresso
                                r.set(CURR_KEY, json.dumps({
                                    "frame_id":    frame_id,
                                    "tiles_total": total_tiles,
                                    "scene":       job.get("scene", "mandelbulb"),
                                }))

                                log.info(f"[LIDER] render frame {frame_id} → {total_tiles} tiles enfileirados")
                        except Exception as e:
                            log.error(f"erro ao processar job render: {e}")

                    # No modo render não gera continuamente — espera próximo job
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