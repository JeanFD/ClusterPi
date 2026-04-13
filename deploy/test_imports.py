import sys, os
sys.path.insert(0, "/home/pi/cluster/app")
os.chdir("/home/pi/cluster/app")

print("testando imports do daemon...")
try:
    from render.tile_generator import generate_render_tasks
    print("render.tile_generator: OK")
except Exception as e:
    print(f"render.tile_generator: ERRO — {e}")

try:
    from geometry import generate_batch, DEFAULT_GEOMETRY
    print("geometry: OK")
except Exception as e:
    print(f"geometry: ERRO — {e}")

try:
    from redis.cluster import RedisCluster
    r = RedisCluster(host="192.168.1.21", port=6379, decode_responses=True)
    r.ping()
    print("redis: OK")
except Exception as e:
    print(f"redis: ERRO — {e}")

print("imports concluidos")
