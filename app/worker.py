"""
Worker: lê {stream}:tasks via consumer group, processa, escreve em {stream}:results.
Usa o nome do nó como consumer-id, então o recovery sabe quem morreu.

Agora despacha automaticamente para a geometria correta baseado no campo
'geom' de cada tarefa (vem do daemon/líder).
"""

import time, json, logging, signal, sys
import redis
from redis.cluster import RedisCluster
from config import (
    MY_IP, MY_NAME, REDIS_PORT,
    STREAM_TASKS, STREAM_RESULTS, GROUP_NAME,
)
from geometry import process_point

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(f"worker-{MY_NAME}")


def connect():
    # Configuração direta para o Redis-py versão 5+
    r = RedisCluster(host=MY_IP, port=6379, decode_responses=True)
    # Garante o group (idempotente — erro BUSYGROUP é esperado)
    try:
        r.xgroup_create(STREAM_TASKS, GROUP_NAME, id="$", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
    return r


def main():
    r = connect()
    log.info(f"worker {MY_NAME} pronto, lendo de {STREAM_TASKS}")

    consumer = MY_NAME  # consumer-id estável

    while True:
        try:
            # Bloqueia até 2s esperando até 16 tarefas
            resp = r.xreadgroup(
                GROUP_NAME, consumer,
                {STREAM_TASKS: ">"},
                count=16, block=2000,
            )
            if not resp:
                continue

            _, messages = resp[0]
            ack_ids = []
            results = []

            for msg_id, fields in messages:
                t0 = time.perf_counter()
                point = process_point(fields)
                dt_ms = (time.perf_counter() - t0) * 1000

                # Filtrar pontos inválidos (ex: mandelbulb fora do fractal)
                if point.get("x") == "999":
                    ack_ids.append(msg_id)
                    continue

                results.append({
                    "task_id":   msg_id,
                    "worker":    MY_NAME,
                    "x":         point["x"],
                    "y":         point["y"],
                    "z":         point["z"],
                    "compute_ms": f"{dt_ms:.2f}",
                })
                ack_ids.append(msg_id)

            # Pipeline: gravar todos os resultados + ACK em uma RTT
            if ack_ids:
                pipe = r.pipeline()
                for res in results:
                    pipe.xadd(STREAM_RESULTS, res, maxlen=50_000, approximate=True)
                pipe.xack(STREAM_TASKS, GROUP_NAME, *ack_ids)
                pipe.execute()

            if results:
                log.info(f"processadas {len(results)} tarefas ({fields.get('geom', 'torus')})")

        except redis.ConnectionError as e:
            log.error(f"redis caiu? {e} — reconectando em 2s")
            time.sleep(2)
            r = connect()
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