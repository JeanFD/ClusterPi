"""
Compositor visual em tempo real.

Dois modos de uso:
  1. Standalone  (`python compositor.py ...`): roda até completar 1 frame.
  2. Biblioteca  (usado por render_movie.py): uma única janela pygame e
     um único cliente Redis persistem entre frames. A função
     `render_one_frame()` é chamada por frame e mantém a continuidade
     do `last_id` da stream entre chamadas (via objeto CompositorSession).
"""
import argparse
import base64
import io
import json
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import requests
from PIL import Image

# Cores por worker (mesmas do frontend, só em RGB 0-255)
WORKER_COLORS: Dict[str, Tuple[int, int, int]] = {
    "worker-01": (255,  85, 102),
    "worker-02": ( 85, 255, 119),
    "worker-03": ( 85, 170, 255),
    "worker-04": (255, 170,  51),
    "worker-05": (170, 102, 255),
    "worker-06": ( 51, 221, 204),
}
NODES_INFO: List[Dict] = [
    {"name": "worker-01", "ip": "192.168.1.20"},
    {"name": "worker-02", "ip": "192.168.1.21"},
    {"name": "worker-03", "ip": "192.168.1.22"},
    {"name": "worker-04", "ip": "192.168.1.23"},
    {"name": "worker-05", "ip": "192.168.1.24"},
    {"name": "worker-06", "ip": "192.168.1.25"},
]
DEFAULT_WORKER_COLOR = (160, 160, 160)
OVERLAY_ALPHA = 0.35
OVERLAY_DECAY = 1.0

# Painel de stats abaixo do canvas: cabeçalho + linha de colunas + 6 linhas + rodapé
STATS_HEADER_H    = 28
STATS_COL_HDR_H   = 18
STATS_ROW_H       = 22
STATS_FOOTER_H    = 32
STATS_PANEL_H     = (STATS_HEADER_H + STATS_COL_HDR_H
                     + STATS_ROW_H * len(NODES_INFO) + STATS_FOOTER_H)

# Posições X das colunas dentro da linha de worker (alinhamento visual).
COL_X = {
    "dot":       12,
    "name":      28,
    "leader":    105,
    "temp":      160,
    "in_flight": 235,
    "processed": 340,
    "rate":      460,
    "bar":       560,
}

# Margens para não ultrapassar a tela (taskbar, título de janela)
SCREEN_MARGIN_W = 80
SCREEN_MARGIN_H = 160

STREAM_RESULTS = "{stream}:results"
CURR_KEY       = "{stream}:render:current"


# ═══════════════════════════════════════════════
#  UTIL
# ═══════════════════════════════════════════════

def connect_redis(host, port: int):
    """
    Aceita host como string ou lista. Sempre usa todos os 6 nós como
    startup_nodes — assim a queda de um nó (inclusive o líder) não impede
    o cliente de aprender a topologia atual via outro nó vivo.
    """
    from redis.cluster import RedisCluster, ClusterNode
    hosts = host if isinstance(host, (list, tuple)) else [host]
    all_ips = [n["ip"] for n in NODES_INFO]
    for h in hosts:
        if h not in all_ips:
            all_ips.insert(0, h)
    startup = [ClusterNode(h, port) for h in all_ips]
    return RedisCluster(
        startup_nodes=startup,
        decode_responses=True,
        socket_timeout=2.0,
        socket_connect_timeout=1.5,
        cluster_error_retry_attempts=5,
    )


def decode_tile(png_b64: str) -> np.ndarray:
    raw = base64.b64decode(png_b64)
    img = Image.open(io.BytesIO(raw)).convert("RGB")
    return np.array(img, dtype=np.uint8)


def worker_base_name(w: str) -> str:
    parts = w.split("-")
    if len(parts) >= 2:
        return "-".join(parts[:2])
    return w


# ═══════════════════════════════════════════════
#  SESSÃO PERSISTENTE
# ═══════════════════════════════════════════════

@dataclass
class CompositorSession:
    """
    Estado persistente entre frames: janela pygame, cliente Redis,
    last_id da stream, thread de stats. Use `open_session()` para instanciar.
    """
    r: object
    full_w: int      # resolução nativa da render
    full_h: int
    use_display: bool
    screen: object = None
    font_hdr: object = None
    font_row: object = None
    font_sm:  object = None
    disp_w: int = 0        # tamanho exibido (possivelmente escalado)
    disp_h: int = 0
    scale:  float = 1.0
    last_id: str = "0"
    t_session_start: float = field(default_factory=time.time)

    # Stats polling
    stats: Dict[str, dict] = field(default_factory=dict)
    stats_lock: object = None
    stop_flag: object = None
    stats_thread: object = None


def _compute_display_size(full_w: int, full_h: int) -> Tuple[int, int, float]:
    """Calcula (disp_w, disp_h, scale) de forma a caber na tela."""
    try:
        import pygame
        info = pygame.display.Info()
        screen_w = info.current_w
        screen_h = info.current_h
    except Exception:
        screen_w, screen_h = 1600, 900

    max_w = max(400, screen_w - SCREEN_MARGIN_W)
    max_h = max(300, screen_h - SCREEN_MARGIN_H - STATS_PANEL_H)

    scale = min(max_w / full_w, max_h / full_h, 1.0)
    disp_w = int(round(full_w * scale))
    disp_h = int(round(full_h * scale))
    return disp_w, disp_h, scale


def _poll_stats_loop(sess: "CompositorSession") -> None:
    """
    Thread de stats: para cada nó do cluster coleta

      - /health         → online, is_leader, temp (por-nó real)
      - Redis GET       → {stream}:stats:proc:{node}  (feitos acumulados)
      - XINFO CONSUMERS → pending por-consumer (somado por nó)

    Computa rate (feitos/s) via diferença entre polls. Tudo por-nó —
    nada global repetido 6 vezes como antes.
    """
    prev_processed: Dict[str, int] = {}
    prev_t: Dict[str, float] = {}
    # EMA da taxa e do em-voo por nó — suaviza jitter de polling.
    # alpha=0.25 dá constante de tempo ~4s: esconde spikes mas ainda
    # responde a mudanças reais de carga em poucos segundos.
    ema_rate:   Dict[str, float] = {}
    ema_flight: Dict[str, float] = {}
    EMA_ALPHA = 0.25

    STREAM_TASKS = "{stream}:tasks"
    GROUP_NAME   = "cluster-workers"

    while not sess.stop_flag.is_set():
        # 1) /health em paralelo (serial é 6×0.8s = 4.8s no pior caso, serial
        #    basta aqui — timeouts curtos).
        health: Dict[str, dict] = {}
        for node in NODES_INFO:
            name = node["name"]; ip = node["ip"]
            try:
                h = requests.get(f"http://{ip}:8000/health", timeout=0.8).json()
                health[name] = {
                    "online": (h.get("status") == "ok"),
                    "leader": bool(h.get("is_leader")),
                    "temp":   h.get("temp"),
                }
            except Exception:
                health[name] = {"online": False, "leader": False, "temp": None}

        # 2) feitos por-nó (contador incrementado em worker.py)
        proc_by_node: Dict[str, int] = {n["name"]: 0 for n in NODES_INFO}
        try:
            for node in NODES_INFO:
                v = sess.r.get(f"{{stream}}:stats:proc:{node['name']}")
                proc_by_node[node["name"]] = int(v or 0)
        except Exception:
            pass

        # 3) em voo por-nó via XINFO CONSUMERS (somando workers-w0..w3)
        in_flight_by_node: Dict[str, int] = {n["name"]: 0 for n in NODES_INFO}
        try:
            consumers = sess.r.xinfo_consumers(STREAM_TASKS, GROUP_NAME)
            for c in consumers:
                cname = c.get("name", "")
                # cname = "worker-01-w0" → base = "worker-01"
                base = "-".join(cname.split("-")[:2])
                if base in in_flight_by_node:
                    in_flight_by_node[base] += int(c.get("pending", 0) or 0)
        except Exception:
            pass

        # 4) calcular rate bruto + suavizar com EMA + montar snapshot
        now = time.time()
        for node in NODES_INFO:
            name = node["name"]
            processed = proc_by_node[name]
            raw_rate = 0.0
            if name in prev_processed and name in prev_t:
                dt = now - prev_t[name]
                if dt > 0:
                    raw_rate = max(0.0, (processed - prev_processed[name]) / dt)
            prev_processed[name] = processed
            prev_t[name] = now

            # EMA: primeiro valor inicializa, depois suaviza.
            if name not in ema_rate:
                ema_rate[name] = raw_rate
            else:
                ema_rate[name] = EMA_ALPHA * raw_rate + (1 - EMA_ALPHA) * ema_rate[name]

            raw_flight = float(in_flight_by_node[name])
            if name not in ema_flight:
                ema_flight[name] = raw_flight
            else:
                ema_flight[name] = EMA_ALPHA * raw_flight + (1 - EMA_ALPHA) * ema_flight[name]

            entry = {
                **health.get(name, {"online": False, "leader": False, "temp": None}),
                "in_flight": int(round(ema_flight[name])),
                "processed": processed,
                "rate":      ema_rate[name],
            }
            with sess.stats_lock:
                sess.stats[name] = entry

        if sess.stop_flag.wait(1.0):
            break


def open_session(host: str, port: int, full_w: int, full_h: int,
                 use_display: bool = True) -> CompositorSession:
    """Cria sessão: conecta Redis, abre UMA janela pygame, inicia polling de stats."""
    r = connect_redis(host, port)
    sess = CompositorSession(r=r, full_w=full_w, full_h=full_h, use_display=use_display)
    sess.stats_lock = threading.Lock()
    sess.stop_flag  = threading.Event()

    if use_display:
        try:
            import pygame
            pygame.init()
            disp_w, disp_h, scale = _compute_display_size(full_w, full_h)
            sess.disp_w, sess.disp_h, sess.scale = disp_w, disp_h, scale
            sess.screen = pygame.display.set_mode((disp_w, disp_h + STATS_PANEL_H))
            pygame.display.set_caption(
                f"ClusterPi Render — {full_w}x{full_h} @ {int(scale*100)}%"
            )
            sess.font_hdr = pygame.font.SysFont("consolas,dejavusansmono,monospace", 16, bold=True)
            sess.font_row = pygame.font.SysFont("consolas,dejavusansmono,monospace", 14)
            sess.font_sm  = pygame.font.SysFont("consolas,dejavusansmono,monospace", 11)
        except Exception as e:
            print(f"[compositor] pygame indisponível: {e} — headless")
            sess.use_display = False

    # Iniciar polling de stats
    sess.stats_thread = threading.Thread(target=_poll_stats_loop, args=(sess,), daemon=True)
    sess.stats_thread.start()

    return sess


def close_session(sess: CompositorSession) -> None:
    if sess.stop_flag is not None:
        sess.stop_flag.set()
    if sess.stats_thread is not None:
        sess.stats_thread.join(timeout=2.0)
    if sess.use_display:
        try:
            import pygame
            pygame.quit()
        except Exception:
            pass


# ═══════════════════════════════════════════════
#  PAINEL DE STATS
# ═══════════════════════════════════════════════

def _fmt_num(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}k"
    return str(n)


def _draw_stats_panel(sess: CompositorSession, frame_id: str,
                      tiles_done: int, tiles_total: int,
                      tiles_per_sec: float, elapsed: int) -> None:
    """
    Painel de stats com colunas explicativas:

      nó | temp | em voo | feitos (total) | taxa (/s) | carga relativa

    - em voo:  tarefas entregues ao nó mas ainda não ACKeadas (trabalho corrente)
    - feitos:  total acumulado de tarefas processadas por aquele nó desde o boot
    - taxa:    derivada dos 'feitos' entre dois polls (~1s) — throughput atual
    - barra:   largura proporcional à taxa do nó dividida pela maior taxa atual,
               mostrando visualmente quem está puxando mais carga

    O cabeçalho do painel mostra o progresso do frame e a taxa agregada
    (tiles/s), além do nome do líder Raft detectado via /health.
    """
    import pygame
    screen = sess.screen
    w = sess.disp_w
    y0 = sess.disp_h

    # Fundo do painel
    pygame.draw.rect(screen, (10, 14, 22), (0, y0, w, STATS_PANEL_H))
    pygame.draw.line(screen, (40, 50, 70), (0, y0), (w, y0), 1)

    with sess.stats_lock:
        snapshot = dict(sess.stats)

    # ── Linha de cabeçalho: progresso do frame + líder ───────────────
    leader_name = next((n for n, s in snapshot.items() if s.get("leader")), "?")
    online_count = sum(1 for s in snapshot.values() if s.get("online"))
    hdr = (f"frame {frame_id}  |  {tiles_done}/{tiles_total} tiles  |  "
           f"{tiles_per_sec:.1f} tiles/s  |  {elapsed}s  |  "
           f"{online_count}/{len(NODES_INFO)} online  |  líder: {leader_name}")
    screen.blit(sess.font_hdr.render(hdr, True, (210, 225, 255)), (12, y0 + 6))
    pygame.draw.line(screen, (30, 40, 60),
                     (8, y0 + STATS_HEADER_H - 2),
                     (w - 8, y0 + STATS_HEADER_H - 2), 1)

    # ── Linha de cabeçalhos das colunas ──────────────────────────────
    col_hdr_y = y0 + STATS_HEADER_H + 2
    col_color = (130, 145, 170)
    cols = [
        ("nó",         COL_X["name"]),
        ("temp",       COL_X["temp"]),
        ("em voo",     COL_X["in_flight"]),
        ("feitos",     COL_X["processed"]),
        ("taxa",       COL_X["rate"]),
        ("carga",      COL_X["bar"]),
    ]
    for label, x in cols:
        screen.blit(sess.font_sm.render(label, True, col_color), (x, col_hdr_y))

    # ── Linhas por worker ────────────────────────────────────────────
    row_y = col_hdr_y + STATS_COL_HDR_H
    rates = [(snapshot.get(n["name"], {}).get("rate", 0.0)) for n in NODES_INFO]
    max_rate = max(rates) if rates else 0.0
    bar_max_w = max(60, w - COL_X["bar"] - 12)

    for node in NODES_INFO:
        name  = node["name"]
        color = WORKER_COLORS.get(name, DEFAULT_WORKER_COLOR)
        s = snapshot.get(name, {})

        online    = s.get("online", False)
        is_leader = s.get("leader", False)
        temp      = s.get("temp")
        in_flight = int(s.get("in_flight", 0))
        processed = int(s.get("processed", 0))
        rate      = float(s.get("rate", 0.0))

        # bolinha + nome
        dot_col = color if online else (60, 60, 60)
        pygame.draw.circle(screen, dot_col, (COL_X["dot"] + 6, row_y + STATS_ROW_H // 2), 6)
        name_col = (230, 235, 255) if online else (110, 110, 120)
        screen.blit(sess.font_row.render(name, True, name_col), (COL_X["name"], row_y + 3))

        # LÍDER badge
        if is_leader:
            bx, by = COL_X["leader"], row_y + 4
            pygame.draw.rect(screen, (251, 191, 36), (bx, by, 44, 15), border_radius=3)
            screen.blit(sess.font_sm.render("LÍDER", True, (20, 20, 20)), (bx + 6, by + 1))

        # temperatura
        if temp is not None:
            if   temp >= 75: temp_col = (248, 113, 113)
            elif temp >= 65: temp_col = (251, 191, 36)
            else:            temp_col = (52, 211, 153)
            t_txt = sess.font_row.render(f"{temp:.1f}°C", True, temp_col)
        else:
            t_txt = sess.font_row.render("--°C", True, (110, 110, 120))
        screen.blit(t_txt, (COL_X["temp"], row_y + 3))

        val_col = (200, 215, 240) if online else (100, 100, 110)
        screen.blit(sess.font_row.render(f"{_fmt_num(in_flight):>6}", True, val_col),
                    (COL_X["in_flight"], row_y + 3))
        screen.blit(sess.font_row.render(f"{_fmt_num(processed):>8}", True, val_col),
                    (COL_X["processed"], row_y + 3))
        screen.blit(sess.font_row.render(f"{rate:7.0f}/s", True, val_col),
                    (COL_X["rate"], row_y + 3))

        # barra de carga relativa — largura proporcional à taxa do nó / max
        if max_rate > 0 and rate > 0:
            bar_w = int(bar_max_w * (rate / max_rate))
            pygame.draw.rect(screen, (30, 40, 60),
                             (COL_X["bar"], row_y + 6, bar_max_w, 10), border_radius=2)
            pygame.draw.rect(screen, color,
                             (COL_X["bar"], row_y + 6, bar_w, 10), border_radius=2)
        else:
            pygame.draw.rect(screen, (30, 40, 60),
                             (COL_X["bar"], row_y + 6, bar_max_w, 10), border_radius=2)

        row_y += STATS_ROW_H

    # ── Rodapé: totais + legenda ─────────────────────────────────────
    total_proc  = sum(int(snapshot.get(n["name"], {}).get("processed", 0)) for n in NODES_INFO)
    total_rate  = sum(float(snapshot.get(n["name"], {}).get("rate", 0.0))  for n in NODES_INFO)
    total_flight= sum(int(snapshot.get(n["name"], {}).get("in_flight", 0)) for n in NODES_INFO)
    foot_y = row_y + 6
    foot = (f"TOTAL  em voo {_fmt_num(total_flight)}   "
            f"feitos {_fmt_num(total_proc)}   "
            f"taxa {total_rate:.0f}/s")
    screen.blit(sess.font_row.render(foot, True, (180, 200, 230)), (12, foot_y))
    legend = "em voo=tarefas entregues não ACKeadas | feitos=total processado | taxa=média 1s"
    screen.blit(sess.font_sm.render(legend, True, (100, 115, 140)), (12, foot_y + 16))


# ═══════════════════════════════════════════════
#  RENDER DE UM FRAME
# ═══════════════════════════════════════════════

def render_one_frame(
    sess: CompositorSession,
    frame_id: str,
    output_path: Optional[str] = None,
    tiles_total_hint: int = 0,
    timeout_s: float = 120.0,
) -> bool:
    """
    Bloqueia até completar `frame_id` (ou timeout). Retorna True se salvou.
    Reutiliza a janela e o last_id da sessão; avança last_id conforme lê
    a stream, então entre frames não relê tudo.
    """
    r = sess.r
    full_w, full_h = sess.full_w, sess.full_h
    canvas = np.zeros((full_h, full_w, 3), dtype=np.uint8)
    overlays: Dict[Tuple[int, int], Tuple[Tuple[int, int, int], float, int, int]] = {}
    tiles_done: set = set()

    # Descobrir tiles_total do frame
    tiles_total = tiles_total_hint
    if tiles_total == 0:
        try:
            curr_json = r.get(CURR_KEY)
            if curr_json:
                curr = json.loads(curr_json)
                if curr.get("frame_id") == frame_id:
                    tiles_total = int(curr.get("tiles_total", 0))
        except Exception:
            pass

    t_start = time.time()
    throughput_count = 0
    throughput_t     = time.time()
    tiles_per_sec    = 0.0
    aborted = False

    pygame = None
    if sess.use_display:
        import pygame as _pygame
        pygame = _pygame

    while True:
        if time.time() - t_start > timeout_s:
            print(f"[compositor] timeout no frame {frame_id}")
            break

        # Eventos pygame
        if pygame is not None:
            for ev in pygame.event.get():
                if ev.type == pygame.QUIT:
                    aborted = True
                    break
            if aborted:
                break

        # Ler tiles da stream, avançando o last_id da sessão.
        # block=150 mantém pygame responsivo (<200ms sem pump de eventos).
        try:
            resp = r.xread({STREAM_RESULTS: sess.last_id}, count=100, block=150)
        except Exception as e:
            print(f"[compositor] Redis erro (reconectando): {e}")
            # Recria o cliente — topologia pode ter mudado com o failover.
            try:
                sess.r = connect_redis([n["ip"] for n in NODES_INFO], 6379)
                r = sess.r
            except Exception as e2:
                print(f"[compositor] reconexão falhou: {e2}")
            time.sleep(0.3)
            continue

        if resp:
            _stream, msgs = resp[0]
            for msg_id, fields in msgs:
                sess.last_id = msg_id
                if fields.get("mode") != "render":
                    continue
                if fields.get("frame_id") != frame_id:
                    continue

                tx = int(fields["tile_x"])
                ty = int(fields["tile_y"])
                tw = int(fields["tile_w"])
                th = int(fields["tile_h"])
                key = (tx, ty)
                if key in tiles_done:
                    continue

                try:
                    tile_arr = decode_tile(fields["png_b64"])
                    canvas[ty:ty+th, tx:tx+tw] = tile_arr

                    w_name    = fields.get("w", fields.get("worker", ""))
                    base_name = worker_base_name(w_name)
                    color     = WORKER_COLORS.get(base_name, DEFAULT_WORKER_COLOR)
                    overlays[key] = (color, time.time(), tw, th)
                    tiles_done.add(key)
                    throughput_count += 1
                except Exception as e:
                    print(f"[compositor] erro decod tile ({tx},{ty}): {e}")

        # Throughput
        now = time.time()
        dt = now - throughput_t
        if dt >= 1.0:
            tiles_per_sec    = throughput_count / dt
            throughput_count = 0
            throughput_t     = now

        # Render pygame
        if pygame is not None:
            # Monta surface nativa com overlays, depois escala para (disp_w, disp_h)
            surf = pygame.surfarray.make_surface(canvas.transpose(1, 0, 2))
            now_t = time.time()
            for (tx, ty), (color, hit_t, tw, th) in list(overlays.items()):
                age = now_t - hit_t
                if age < OVERLAY_DECAY:
                    alpha = int(OVERLAY_ALPHA * 255 * (1.0 - age / OVERLAY_DECAY))
                    ov = pygame.Surface((tw, th), pygame.SRCALPHA)
                    ov.fill((*color, alpha))
                    surf.blit(ov, (tx, ty))
                else:
                    overlays.pop((tx, ty), None)

            if sess.scale != 1.0:
                surf = pygame.transform.smoothscale(surf, (sess.disp_w, sess.disp_h))

            sess.screen.blit(surf, (0, 0))

            elapsed = int(now_t - t_start)
            _draw_stats_panel(
                sess, frame_id,
                len(tiles_done), tiles_total,
                tiles_per_sec, elapsed,
            )
            pygame.display.flip()

        if tiles_total > 0 and len(tiles_done) >= tiles_total:
            break

    if aborted:
        return False

    if output_path:
        Image.fromarray(canvas, "RGB").save(output_path)
        print(f"[compositor] salvo: {output_path} ({len(tiles_done)}/{tiles_total} tiles)")
    return True


# ═══════════════════════════════════════════════
#  CLI STANDALONE (1 frame)
# ═══════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description="Compositor visual do render farm")
    p.add_argument("--host",     default="192.168.1.20")
    p.add_argument("--port",     type=int, default=6379)
    p.add_argument("--width",    type=int, default=640)
    p.add_argument("--height",   type=int, default=360)
    p.add_argument("--frame-id", default="0001")
    p.add_argument("--output",   default=None)
    p.add_argument("--no-display", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    output_path = args.output or f"frame_{args.frame_id}.png"

    print(f"[compositor] conectando a {args.host}:{args.port}...")
    sess = open_session(args.host, args.port, args.width, args.height,
                        use_display=not args.no_display)

    aborted = {"v": False}

    def _handler(sig, frame):
        aborted["v"] = True
        print("\n[compositor] interrompido")

    signal.signal(signal.SIGINT,  _handler)
    signal.signal(signal.SIGTERM, _handler)

    try:
        render_one_frame(sess, args.frame_id, output_path=output_path)
    finally:
        close_session(sess)


if __name__ == "__main__":
    main()
