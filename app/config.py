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

STREAM_TASKS = "{stream}:tasks"
STREAM_RESULTS = "{stream}:results"
GROUP_NAME = "cluster-workers"

PEDING_TIMEOUT_MS = 10_000