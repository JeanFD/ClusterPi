import os, socket

NODES = [f"192.168.1.{i}" for i in range(20, 26)]

def my_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()

MY_IP = os.environ.get("NODE_IP", my_ip())
MY_NAME = f"worker-{NODES.index(MY_IP)+1:02d}" if MY_IP in NODES else "unknown"

REDIS_PORT = 6379
RAFT_PORT = 4321
API_PORT = 8000

# ─── Sharding: 6 streams de tasks/results, cada uma em um master diferente ───
# Cada hash tag {s0}..{s5} mapeia para um slot distinto, então cada shard
# tem master/replicas em nós diferentes. A carga espalha por 6 masters em
# vez de concentrar tudo em worker-01 (como no esquema single-stream antigo).
SHARDS = 6
TASK_KEYS   = [f"{{s{i}}}:tasks"   for i in range(SHARDS)]
RESULT_KEYS = [f"{{s{i}}}:results" for i in range(SHARDS)]
GROUP_NAME  = "cluster-workers"

# ─── Control keys: escritas pouco frequentes, single slot está ok ───
CTRL_MODE      = "{ctrl}:mode"
CTRL_GEOM      = "{ctrl}:geometry"
CTRL_LEADER    = "{ctrl}:cluster:leader"
CTRL_JOB       = "{ctrl}:render:job"
CTRL_CURR      = "{ctrl}:render:current"

def ctrl_done_key(frame_id: str) -> str:
    return f"{{ctrl}}:render:done:{frame_id}"

def ctrl_stats_key(node_name: str) -> str:
    return f"{{ctrl}}:stats:proc:{node_name}"

PENDING_TIMEOUT_MS = 3_000
