"""
Watchdog do cluster RPi.
Uso: python3 watchdog.py
Requer: pip install requests
"""
import requests, time, os

NODES    = [f"192.168.1.{i}" for i in range(20, 26)]
INTERVAL = 2  # segundos

def clear(): os.system('cls' if os.name == 'nt' else 'clear')

def check(ip):
    try:
        h = requests.get(f"http://{ip}:8000/health", timeout=1.5).json()
        return {**h, "online": True}
    except Exception:
        return {"online": False, "node": "?", "ip": ip}

def queue(ip):
    try:
        return requests.get(f"http://{ip}:8000/queue/status", timeout=1.5).json()
    except Exception:
        return {}

print("\033[36mWatchdog ativo. Ctrl+C para sair.\033[0m")
try:
    while True:
        rows  = [check(ip) for ip in NODES]
        up    = [r for r in rows if r["online"]]
        down  = [r for r in rows if not r["online"]]
        leaders = [r for r in up if r.get("is_leader")]
        q = queue(up[0]["ip"]) if up else {}

        clear()
        print(f"┌{'─'*54}┐")
        print(f"│  Cluster RPi v2 — {time.strftime('%H:%M:%S')}   ({len(up)}/{len(NODES)} online){' '*8}│")
        print(f"├{'─'*54}┤")
        for r in rows:
            if r["online"]:
                star = "★" if r.get("is_leader") else " "
                color = "\033[32m" if r.get("status") == "ok" else "\033[33m"
                print(f"│ {color}✓\033[0m {r['ip']:<14} {star} {r['node']:<8} "
                      f"redis={r.get('redis','?'):<4} up={r.get('uptime_s',0):>5}s │")
            else:
                print(f"│ \033[31m✗\033[0m {r['ip']:<14}    OFFLINE{' '*22}│")
        print(f"├{'─'*54}┤")
        if q:
            print(f"│  fila: {q.get('stream_length',0):>6}   pendentes: {q.get('pending',0):>5}{' '*15}│")
        if leaders:
            print(f"│  líder: {leaders[0]['ip']}{' '*30}│")
        print(f"└{'─'*54}┘")

        if   len(leaders) == 0: print("\033[31m⚠ ALERTA: nenhum líder eleito!\033[0m")
        elif len(leaders) > 1:  print(f"\033[31m⚠ SPLIT-BRAIN: {len(leaders)} líderes!\033[0m")
        if down: print(f"\033[31m⚠ OFFLINE: {', '.join(d['ip'] for d in down)}\033[0m")

        time.sleep(INTERVAL)
except KeyboardInterrupt:
    print("\nencerrado.")