"""
Worker: lê {stream}:tasks via consumer group, processa, escreve em {stream}:results.
Usa o nome do nó + WORKER_ID como consumer-id para suporte a multiprocessing.

Despacha automaticamente para a geometria correta baseado no campo
'geom' de cada tarefa (vem do daemon/líder), ou para o render tile worker
se mode == "render".
"""

import os, time, json, logging, signal, sys
import threading
import queue
import redis
from redis.cluster import RedisCluster
from config import (
    MY_IP, MY_NAME, REDIS_PORT,
    STREAM_TASKS, STREAM_RESULTS, GROUP_NAME,
)
from geometry import process_batch, process_point

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")

# A1: ler WORKER_ID injetado pelo systemd template (cluster-worker@N)
WORKER_ID = os.environ.get("WORKER_ID", "0")
CONSUMER  = f"{MY_NAME}-w{WORKER_ID}"

log = logging.getLogger(f"worker-{CONSUMER}")


def connect():
    r = RedisCluster(host=MY_IP, port=6379, decode_responses=True)
    # Garante o group (idempotente — erro BUSYGROUP é esperado)
    # Usa id="0" para que após restart os workers vejam tasks pendentes na fila
    # (id="$" faria os workers ignorarem tasks antigas, deixando fila acumular)
    try:
        r.xgroup_create(STREAM_TASKS, GROUP_NAME, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
    return r


def fetch_loop(r, consumer, in_queue):
    while True:
        try:
            # A2: count=200, block=1000
            resp = r.xreadgroup(GROUP_NAME, consumer, {STREAM_TASKS: ">"}, count=200, block=1000)
            if resp:
                in_queue.put(resp[0][1])
        except Exception as e:
            log.error(f"fetch_loop erro: {e}")
            time.sleep(1)


def write_loop(r, out_queue):
    while True:
        ack_ids, results, label = out_queue.get()
        try:
            # A3: pipeline sem transaction
            pipe = r.pipeline(transaction=False)
            # Contador por frame_id para /render/progress (sem O(N) no xrevrange).
            # Hash tag {stream} é OBRIGATÓRIA — se sair do slot, o pipeline quebra.
            render_done_counts: dict = {}
            for res in results:
                # A7: maxlen=10_000
                pipe.xadd(STREAM_RESULTS, res, maxlen=10_000, approximate=True)
                if res.get("mode") == "render":
                    fid = res.get("frame_id")
                    if fid:
                        render_done_counts[fid] = render_done_counts.get(fid, 0) + 1
            for fid, n in render_done_counts.items():
                pipe.incrby(f"{{stream}}:render:done:{fid}", n)
            pipe.xack(STREAM_TASKS, GROUP_NAME, *ack_ids)
            pipe.execute()
            if results:
                log.info(f"processadas {len(results)} tarefas ({label})")
        except Exception as e:
            log.error(f"Erro na gravacao: {e}")
            time.sleep(1)


def main():
    r = connect()
    log.info(f"worker {CONSUMER} pronto, lendo de {STREAM_TASKS}")

    in_queue  = queue.Queue(maxsize=16)
    out_queue = queue.Queue(maxsize=16)

    threading.Thread(target=fetch_loop, args=(r, CONSUMER, in_queue), daemon=True).start()
    threading.Thread(target=write_loop, args=(r, out_queue), daemon=True).start()

    while True:
        try:
            messages = in_queue.get()

            # B4: separar tarefas de render das de geometry
            geo_msgs    = []
            render_msgs = []
            for msg_id, fields in messages:
                if fields.get("mode") == "render":
                    render_msgs.append((msg_id, fields))
                else:
                    geo_msgs.append((msg_id, fields))

            ack_ids  = []
            ack_set  = set()   # lookup O(1)
            results  = []
            geom_label = "geometry"

            # ─── A4: processamento vetorizado de geometry ───
            if geo_msgs:
                geom_label = geo_msgs[0][1].get("geom", "torus")
                try:
                    batch_results = process_batch(geo_msgs)
                except Exception as e:
                    log.error(f"process_batch falhou, caindo no fallback: {e}")
                    # Fallback: loop Python ponto a ponto
                    batch_results = []
                    for msg_id, fields in geo_msgs:
                        pt = process_point(fields)
                        if pt.get("x") != "999":
                            batch_results.append((msg_id, pt))

                for msg_id, pt in batch_results:
                    results.append({
                        # A7: campo "w" (ID completo do consumer), "worker" com MY_NAME
                        # para compatibilidade com o frontend que agrupa por worker-XX
                        # TODO: remover campo "worker" após atualizar frontend para usar "w"
                        "w":      CONSUMER,
                        "worker": MY_NAME,
                        "x":      pt["x"],
                        "y":      pt["y"],
                        "z":      pt["z"],
                    })
                    ack_ids.append(msg_id)
                    ack_set.add(msg_id)

                # ACK de mensagens filtradas (mandelbulb escapado — sem resultado)
                for msg_id, _fields in geo_msgs:
                    if msg_id not in ack_set:
                        ack_ids.append(msg_id)
                        ack_set.add(msg_id)

            # ─── B4: processamento de render tiles ───
            if render_msgs:
                geom_label = "render"
                try:
                    from render.tile_worker import process_render_task
                    for msg_id, fields in render_msgs:
                        try:
                            res = process_render_task(fields)
                            results.append(res)
                        except Exception as e:
                            log.error(f"render tile erro: {e}")
                        ack_ids.append(msg_id)
                except ImportError as e:
                    log.error(f"render module não disponível: {e}")
                    for msg_id, _ in render_msgs:
                        ack_ids.append(msg_id)

            if ack_ids:
                out_queue.put((ack_ids, results, geom_label))

        except Exception as e:
            log.exception(f"erro inesperado: {e}")
            time.sleep(1)


def shutdown(*_):
    log.info("worker encerrado")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT,  shutdown)
    main()