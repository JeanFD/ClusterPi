"""
Recovery loop: para cada shard, usa XAUTOCLAIM para reentregar mensagens
cujo consumer não fez ACK em PENDING_TIMEOUT_MS. Roda em TODOS os nós —
XAUTOCLAIM é atômico no Redis, múltiplos recoveries concorrentes são
seguros e garantem que a queda do líder não pare o reclaim.
"""
import time, logging
import redis
from redis.cluster import RedisCluster
from config import (
    MY_IP, MY_NAME, REDIS_PORT,
    TASK_KEYS, GROUP_NAME, PENDING_TIMEOUT_MS, NODES,
)

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("recovery")


def run(is_leader_fn=None, stop_fn=lambda: False):
    """
    is_leader_fn: mantido por compatibilidade, não é mais usado.
    stop_fn:     callable que retorna True para encerrar o loop.
    """
    r = RedisCluster(host=MY_IP, port=6379, decode_responses=True)
    consumer_id = f"recovery-{MY_NAME}"
    # Um cursor por shard (XAUTOCLAIM é por-key).
    cursors = {k: "0-0" for k in TASK_KEYS}

    while not stop_fn():
        try:
            for key in TASK_KEYS:
                cursor, claimed, deleted_ids = r.xautoclaim(
                    key, GROUP_NAME, consumer_id,
                    min_idle_time=PENDING_TIMEOUT_MS,
                    start_id=cursors[key], count=64,
                )
                cursors[key] = cursor

                if claimed:
                    log.warning(f"[{key}] reclaimed {len(claimed)} tarefas órfãs")
                    pipe = r.pipeline(transaction=False)
                    for msg_id, fields in claimed:
                        pipe.xadd(key, fields, maxlen=10_000, approximate=True)
                        pipe.xack(key, GROUP_NAME, msg_id)
                    pipe.execute()

                # IDs que estão no PEL mas cujas mensagens foram apagadas pelo
                # XTRIM (ex: na troca de líder). ACK explícito limpa o PEL;
                # sem isso o PEL cresce indefinidamente a cada failover.
                if deleted_ids:
                    log.info(f"[{key}] limpando {len(deleted_ids)} IDs fantasma do PEL")
                    pipe = r.pipeline(transaction=False)
                    for msg_id in deleted_ids:
                        pipe.xack(key, GROUP_NAME, msg_id)
                    pipe.execute()

            time.sleep(1)

        except Exception as e:
            log.error(f"loop error: {e}")
            time.sleep(2)


if __name__ == "__main__":
    run(is_leader_fn=lambda: True)
