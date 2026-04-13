"""
Recovery loop: usa XAUTOCLAIM para tomar mensagens cujo consumer
não fez ACK em PENDING_TIMEOUT_MS. Roda APENAS no líder Raft —
assim evitamos múltiplos recoveries simultâneos competindo.
"""
import time, logging
import redis
from redis.cluster import RedisCluster
from config import (
    MY_IP, MY_NAME, REDIS_PORT,
    STREAM_TASKS, GROUP_NAME, PENDING_TIMEOUT_MS, NODES,
)

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("recovery")


def run(is_leader_fn, stop_fn=lambda: False):
    """
    is_leader_fn: callable que retorna True se este nó é o líder Raft.
    stop_fn:     callable que retorna True para encerrar o loop.
    """
    r = RedisCluster(host=MY_IP, port=6379, decode_responses=True)
    consumer_id = f"recovery-{MY_NAME}"
    cursor = "0-0"

    while not stop_fn():
        try:
            if not is_leader_fn():
                time.sleep(2)
                cursor = "0-0"
                continue

            # XAUTOCLAIM <key> <group> <consumer> <min-idle-ms> <start>
            cursor, claimed, _ = r.xautoclaim(
                STREAM_TASKS, GROUP_NAME, consumer_id,
                min_idle_time=PENDING_TIMEOUT_MS,
                start_id=cursor, count=64,
            )

            if claimed:
                log.warning(f"reclaimed {len(claimed)} tarefas órfãs")
                # Não processamos aqui — só reentregamos.
                # Como o `consumer_id` é o recovery, deixamos pendentes
                # com idle=0 para o próximo XREADGROUP de qualquer worker.
                # Truque: re-XADD as mesmas tarefas e ACK as antigas.
                # A3: pipeline sem transaction (overhead alto no RedisCluster)
                pipe = r.pipeline(transaction=False)
                for msg_id, fields in claimed:
                    pipe.xadd(STREAM_TASKS, fields, maxlen=10_000, approximate=True)
                    pipe.xack(STREAM_TASKS, GROUP_NAME, msg_id)
                pipe.execute()

            time.sleep(2)

        except Exception as e:
            log.error(f"loop error: {e}")
            time.sleep(3)


if __name__ == "__main__":
    # Modo standalone: assume sempre líder (para testes)
    run(is_leader_fn=lambda: True)