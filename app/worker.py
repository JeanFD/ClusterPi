"""
Worker: lê tasks das 6 streams sharded via consumer group, processa,
escreve resultados na stream correspondente ({sN}:results para tasks
vindas de {sN}:tasks). Cada worker tem 6 fetch threads (uma por shard)
que empurram para uma in_queue compartilhada marcada com o shard.

Usa o nome do nó + WORKER_ID como consumer-id para suporte a multiprocessing.
"""

import os, time, logging, signal, sys
import threading
import queue
import redis
from redis.cluster import RedisCluster
from config import (
    MY_IP, MY_NAME, REDIS_PORT,
    SHARDS, TASK_KEYS, RESULT_KEYS, GROUP_NAME,
    ctrl_stats_key, ctrl_done_key,
)
from geometry import process_batch, process_point

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")

WORKER_ID = os.environ.get("WORKER_ID", "0")
CONSUMER  = f"{MY_NAME}-w{WORKER_ID}"

log = logging.getLogger(f"worker-{CONSUMER}")


def connect():
    r = RedisCluster(host=MY_IP, port=6379, decode_responses=True)
    # Garante grupo em cada shard (idempotente — BUSYGROUP é esperado)
    for key in TASK_KEYS:
        try:
            r.xgroup_create(key, GROUP_NAME, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                log.warning(f"xgroup_create {key}: {e}")
    return r


def fetch_loop(r, consumer, shard_idx, in_queue):
    """Uma thread por shard: XREADGROUP da shard {sN}:tasks."""
    task_key = TASK_KEYS[shard_idx]
    while True:
        try:
            resp = r.xreadgroup(GROUP_NAME, consumer, {task_key: ">"},
                                count=200, block=1000)
            if resp:
                in_queue.put((shard_idx, resp[0][1]))
        except Exception as e:
            log.error(f"fetch_loop[s{shard_idx}] erro: {e}")
            time.sleep(1)


def write_loop(r, out_queue):
    """
    Uma thread global de escrita. Recebe (shard_idx, ack_ids, results, label).
    Pipeline cross-slot: redis-py roteia XADD (shard), XACK (shard) e
    INCRBY em {ctrl} para seus respectivos nós donos.
    Faz retry com backoff em caso de falha para não perder resultados computados.
    """
    MAX_TENTATIVAS = 8
    while True:
        item = out_queue.get()
        shard_idx, ack_ids, results, label = item
        task_key   = TASK_KEYS[shard_idx]
        result_key = RESULT_KEYS[shard_idx]

        for tentativa in range(1, MAX_TENTATIVAS + 1):
            try:
                pipe = r.pipeline(transaction=False)
                render_done_counts: dict = {}
                for res in results:
                    pipe.xadd(result_key, res, maxlen=10_000, approximate=True)
                    if res.get("mode") == "render":
                        fid = res.get("frame_id")
                        if fid:
                            render_done_counts[fid] = render_done_counts.get(fid, 0) + 1
                for fid, cnt in render_done_counts.items():
                    pipe.incrby(ctrl_done_key(fid), cnt)
                if results:
                    pipe.incrby(ctrl_stats_key(MY_NAME), len(results))
                pipe.xack(task_key, GROUP_NAME, *ack_ids)
                pipe.execute()
                if results:
                    log.info(f"[s{shard_idx}] processadas {len(results)} tarefas ({label})")
                break
            except Exception as e:
                espera = min(2 ** tentativa, 30)
                if tentativa < MAX_TENTATIVAS:
                    log.error(f"write_loop[s{shard_idx}] tentativa {tentativa}/{MAX_TENTATIVAS} "
                              f"erro: {e} — retry em {espera}s")
                    time.sleep(espera)
                else:
                    log.critical(f"write_loop[s{shard_idx}] falhou {MAX_TENTATIVAS}x, "
                                 f"descartando {len(results)} resultados: {e}")


def main():
    r = connect()
    log.info(f"worker {CONSUMER} pronto, lendo de {SHARDS} shards")

    in_queue  = queue.Queue(maxsize=64)
    out_queue = queue.Queue(maxsize=64)

    for i in range(SHARDS):
        threading.Thread(target=fetch_loop,
                         args=(r, CONSUMER, i, in_queue),
                         daemon=True).start()
    threading.Thread(target=write_loop, args=(r, out_queue), daemon=True).start()

    while True:
        try:
            shard_idx, messages = in_queue.get()

            geo_msgs    = []
            render_msgs = []
            for msg_id, fields in messages:
                if fields.get("mode") == "render":
                    render_msgs.append((msg_id, fields))
                else:
                    geo_msgs.append((msg_id, fields))

            ack_ids  = []
            ack_set  = set()
            results  = []
            geom_label = "geometry"

            if geo_msgs:
                geom_label = geo_msgs[0][1].get("geom", "torus")
                try:
                    batch_results = process_batch(geo_msgs)
                except Exception as e:
                    log.error(f"process_batch falhou, caindo no fallback: {e}")
                    batch_results = []
                    for msg_id, fields in geo_msgs:
                        pt = process_point(fields)
                        if pt.get("x") != "999":
                            batch_results.append((msg_id, pt))

                for msg_id, pt in batch_results:
                    results.append({
                        "w":      CONSUMER,
                        "worker": MY_NAME,
                        "x":      pt["x"],
                        "y":      pt["y"],
                        "z":      pt["z"],
                    })
                    ack_ids.append(msg_id)
                    ack_set.add(msg_id)

                for msg_id, _fields in geo_msgs:
                    if msg_id not in ack_set:
                        ack_ids.append(msg_id)
                        ack_set.add(msg_id)

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
                out_queue.put((shard_idx, ack_ids, results, geom_label))

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
