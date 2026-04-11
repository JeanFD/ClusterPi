"""
Raft daemon: roda em cada nó, mantém estado replicado e elege um líder.
Só o líder chama gerar_tarefas(). Os outros viram workers passivos.

A geometria ativa é lida da chave Redis {stream}:geometry
(definida pelo frontend via API). Padrão: "torus".
"""
import time, signal, sys, logging
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

        self.__last_batch_id = 0
        self.__total_tasks = 0

    @replicated
    def increment_batch(self) -> int:
        self.__last_batch_id += 1
        return self.__last_batch_id
    
    @replicated
    def add_tasks(self, n:  int):
        self.__total_tasks += n

    def stats(self) -> dict:
        return {
            "last_batch_id": self.__last_batch_id,
            "total_tasks": self.__total_tasks,
        }
    
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
    from redis.cluster import RedisCluster
    from config import STREAM_TASKS

    r = RedisCluster(host=MY_IP, port=6379, decode_responses=True)

    BATCH_SIZE = 500
    INTERVAL_S = 0.5

    # Chave Redis onde o frontend define a geometria ativa
    GEOM_KEY = "{stream}:geometry"

    while True:
        try:
            if state._isLeader():
                r.set("{stream}:cluster:leader", MY_IP, ex=5)  # TTL 5s

                # Ler geometria ativa (definida pelo frontend via API)
                geom = r.get(GEOM_KEY) or DEFAULT_GEOMETRY

                batch_id = state.increment_batch(sync=True)
                points = generate_batch(geom, batch_id, BATCH_SIZE)
                
                pipe = r.pipeline()
                for p in points:
                    pipe.xadd(STREAM_TASKS, p)
                pipe.execute()
                
                state.add_tasks(BATCH_SIZE, sync=False)
                log.info(f"[LIDER] batch {batch_id} -> {BATCH_SIZE} tarefas ({geom})")
            time.sleep(INTERVAL_S)
        except Exception as e:
            log.error(f"loop error: {e}")
            time.sleep(0.1)

def shutdown(signum, frame):
    log.info("encerrando...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    main()