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

# Painel de stats abaixo do canvas: cabeçalho + 6 linhas de worker
STATS_HEADER_H = 30
STATS_ROW_H    = 24
STATS_PANEL_H  = STATS_HEADER_H + STATS_ROW_H * len(NODES_INFO) + 16

# Margens para não ultrapassar a tela (taskbar, título de janela)
SCREEN_MARGIN_W = 80
SCREEN_MARGIN_H = 160

STREAM_RESULTS = "{stream}:results"
CURR_KEY       = "{stream}:render:current"


# ═══════════════════════════════════════════════
#  UTIL
# ═══════════════════════════════════════════════

def connect_redis(host: str, port: int):
    from redis.cluster import RedisCluster
    return RedisCluster(host=host, port=port, decode_responses=True)


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
    """Thread que faz polling de /health e /queue/status de todos os nós."""
    prev_processed: Dict[str, int] = {}
    prev_t: Dict[str, float] = {}

    while not sess.stop_flag.is_set():
        for node in NODES_INFO:
            name = node["name"]
            ip   = node["ip"]
            entry = {"online": False, "leader": False, "temp": None,
                     "pending": 0, "processed_total": 0, "rate": 0.0,
                     "stream_len": 0}
            try:
                h = requests.get(f"http://{ip}:8000/health", timeout=0.8).json()
                entry["online"] = (h.get("status") == "ok")
                entry["leader"] = bool(h.get("is_leader"))
                entry["temp"]   = h.get("temp")
            except Exception:
                pass
            try:
                q = requests.get(f"http://{ip}:8000/queue/status", timeout=0.8).json()
                entry["pending"]         = int(q.get("pending", 0) or 0)
                entry["processed_total"] = int(q.get("processed_total", 0) or 0)
                entry["stream_len"]      = int(q.get("stream_length", 0) or 0)

                now = time.time()
                if name in prev_processed and name in prev_t:
                    dt = now - prev_t[name]
                    if dt > 0:
                        entry["rate"] = max(0.0, (entry["processed_total"] - prev_processed[name]) / dt)
                prev_processed[name] = entry["processed_total"]
                prev_t[name] = now
            except Exception:
                pass

            with sess.stats_lock:
                sess.stats[name] = entry

        # Wait total ~1s mas com wake-up rápido no stop_flag
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
    import pygame
    screen = sess.screen
    w = sess.disp_w
    y0 = sess.disp_h

    # Fundo do painel
    pygame.draw.rect(screen, (10, 14, 22), (0, y0, w, STATS_PANEL_H))
    pygame.draw.line(screen, (40, 50, 70), (0, y0), (w, y0), 1)

    # Cabeçalho: progresso do frame atual + tiles/s + líder
    with sess.stats_lock:
        snapshot = dict(sess.stats)

    leader_name = next((n for n, s in snapshot.items() if s.get("leader")), "?")
    hdr = (f"frame {frame_id}  |  {tiles_done}/{tiles_total} tiles  |  "
           f"{tiles_per_sec:.1f} tiles/s  |  {elapsed}s  |  líder: {leader_name}")
    txt = sess.font_hdr.render(hdr, True, (210, 225, 255))
    screen.blit(txt, (12, y0 + 6))

    # Linha separadora
    pygame.draw.line(screen, (30, 40, 60),
                     (8, y0 + STATS_HEADER_H - 2),
                     (w - 8, y0 + STATS_HEADER_H - 2), 1)

    # Uma linha por worker
    row_y = y0 + STATS_HEADER_H + 2
    for node in NODES_INFO:
        name = node["name"]
        color = WORKER_COLORS.get(name, DEFAULT_WORKER_COLOR)
        s = snapshot.get(name, {})

        online = s.get("online", False)
        is_leader = s.get("leader", False)
        temp = s.get("temp")
        pending = s.get("pending", 0)
        processed = s.get("processed_total", 0)
        rate = s.get("rate", 0.0)

        # Dot colorido do worker
        dot_col = color if online else (60, 60, 60)
        pygame.draw.circle(screen, dot_col, (18, row_y + STATS_ROW_H // 2), 6)

        name_col = (230, 235, 255) if online else (110, 110, 120)
        name_txt = sess.font_row.render(name, True, name_col)
        screen.blit(name_txt, (32, row_y + 4))

        # Badge de líder
        if is_leader:
            lb = sess.font_sm.render("LÍDER", True, (20, 20, 20))
            bx, by = 100, row_y + 5
            pygame.draw.rect(screen, (251, 191, 36), (bx, by, 44, 15), border_radius=3)
            screen.blit(lb, (bx + 6, by + 1))

        # Temperatura
        if temp is not None:
            if   temp >= 75: temp_col = (248, 113, 113)
            elif temp >= 65: temp_col = (251, 191, 36)
            else:            temp_col = (52, 211, 153)
            t_txt = sess.font_row.render(f"{temp:.1f}°C", True, temp_col)
        else:
            t_txt = sess.font_row.render("--°C", True, (110, 110, 120))
        screen.blit(t_txt, (155, row_y + 4))

        # Restante: pending / processed / rate
        info = (f"pend {_fmt_num(pending):>6}   "
                f"proc {_fmt_num(processed):>7}   "
                f"+{rate:7.0f}/s")
        i_txt = sess.font_row.render(info, True, (180, 195, 220) if online else (100, 100, 110))
        screen.blit(i_txt, (220, row_y + 4))

        row_y += STATS_ROW_H


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

        # Ler tiles da stream, avançando o last_id da sessão
        try:
            resp = r.xread({STREAM_RESULTS: sess.last_id}, count=100, block=500)
        except Exception as e:
            print(f"[compositor] Redis erro: {e}")
            time.sleep(1)
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
