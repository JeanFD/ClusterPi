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
        W = 62
        def fmt_temp(t):
            if t is None: return "  -  "
            if t >= 75:   c = "\033[31m"   # vermelho
            elif t >= 65: c = "\033[33m"   # amarelo
            else:         c = "\033[32m"   # verde
            return f"{c}{t:>4.1f}°C\033[0m"

        print(f"┌{'─'*W}┐")
        print(f"│  Cluster RPi v2 — {time.strftime('%H:%M:%S')}   ({len(up)}/{len(NODES)} online){' '*(W-44)}│")
        print(f"├{'─'*W}┤")
        for r in rows:
            if r["online"]:
                star  = "★" if r.get("is_leader") else " "
                color = "\033[32m" if r.get("status") == "ok" else "\033[33m"
                temp  = fmt_temp(r.get("temp"))
                # sem códigos de cor: ✓ ip star node redis up temp
                plain_len = 4 + 14 + 3 + 9 + 9 + 10 + 9  # aproximação visual
                line = (f" {color}✓\033[0m {r['ip']:<14} {star} {r['node']:<8} "
                        f"redis={r.get('redis','?'):<4} up={r.get('uptime_s',0):>5}s "
                        f"t={temp}")
                pad = W - (1+1+1+14+1+1+1+8+1+10+1+8+1+2+7)
                print(f"│{line}{' '*max(0,pad)}│")
            else:
                print(f"│ \033[31m✗\033[0m {r['ip']:<14}    OFFLINE{' '*(W-25)}│")
        print(f"├{'─'*W}┤")
        if q:
            line = f"  fila: {q.get('stream_length',0):>6}   pendentes: {q.get('pending',0):>5}"
            print(f"│{line}{' '*(W-len(line))}│")
        if leaders:
            line = f"  líder: {leaders[0]['ip']}"
            print(f"│{line}{' '*(W-len(line))}│")
        if up:
            temps = [r.get("temp") for r in up if r.get("temp") is not None]
            if temps:
                line = f"  temp média: {sum(temps)/len(temps):.1f}°C   max: {max(temps):.1f}°C"
                print(f"│{line}{' '*(W-len(line))}│")
        print(f"└{'─'*W}┘")

        if   len(leaders) == 0: print("\033[31m⚠ ALERTA: nenhum líder eleito!\033[0m")
        elif len(leaders) > 1:  print(f"\033[31m⚠ SPLIT-BRAIN: {len(leaders)} líderes!\033[0m")
        if down: print(f"\033[31m⚠ OFFLINE: {', '.join(d['ip'] for d in down)}\033[0m")

        time.sleep(INTERVAL)
except KeyboardInterrupt:
    print("\nencerrado.")