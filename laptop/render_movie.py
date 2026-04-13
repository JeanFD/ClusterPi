"""
Orquestra renderização de um vídeo MP4 inteiro usando o cluster como render farm.

Itera por N frames, cada frame:
  1. Calcula camera para esse frame (órbita ao redor do target)
  2. POST /render/job no líder do cluster
  3. Spawna o compositor.py como subprocess para esse frame
  4. Espera o compositor terminar (frame salvo)
  5. Próximo frame
No fim, chama ffmpeg para gerar o MP4.

Uso:
    python render_movie.py --scene mandelbulb --frames 120 --fps 24
                           --width 640 --height 360 --duration 5
"""
import argparse
import math
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests

# Compositor in-process: uma única janela pygame persiste entre frames
sys.path.insert(0, str(Path(__file__).parent))
from compositor import open_session, close_session, render_one_frame


# ═══════════════════════════════════════════════
#  CONFIGURAÇÃO DA CÂMERA ORBITAL
# ═══════════════════════════════════════════════

def camera_for_frame(frame_idx: int, total_frames: int,
                     radius: float = 4.0, height: float = 1.0,
                     fov: float = 60.0) -> Dict:
    """
    Câmera orbitando em círculo no plano XZ ao redor de (0,0,0).
    Dá uma volta completa nos total_frames.
    """
    angle = 2.0 * math.pi * frame_idx / total_frames
    pos = [
        radius * math.cos(angle),
        height,
        radius * math.sin(angle),
    ]
    return {
        "pos":    pos,
        "target": [0.0, 0.0, 0.0],
        "fov":    fov,
    }


# ═══════════════════════════════════════════════
#  DETECÇÃO DO LÍDER
# ═══════════════════════════════════════════════

CLUSTER_IPS = [f"192.168.1.{i}" for i in range(20, 26)]

def find_leader(timeout: float = 5.0) -> Optional[str]:
    """
    Pergunta ao nó .20 (ou outros em sequência) quem é o líder.
    Retorna o IP do líder ou None.
    """
    for ip in CLUSTER_IPS:
        try:
            r = requests.get(f"http://{ip}:8000/health", timeout=timeout)
            data = r.json()
            leader = data.get("leader")
            if leader:
                return leader
        except Exception:
            continue
    return None


def api_url(leader_ip: str, path: str) -> str:
    return f"http://{leader_ip}:8000{path}"


# ═══════════════════════════════════════════════
#  ETÀ FORMATADO
# ═══════════════════════════════════════════════

def fmt_eta(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds // 60)}min {int(seconds % 60)}s"
    else:
        return f"{int(seconds // 3600)}h {int((seconds % 3600) // 60)}min"


# ═══════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description="Orquestra renderização MP4 no cluster")
    p.add_argument("--scene",       default="mandelbulb",     help="mandelbulb | menger | apollonian")
    p.add_argument("--frames",      type=int, default=120,    help="Total de frames do vídeo")
    p.add_argument("--fps",         type=int, default=24,     help="Frame rate do vídeo")
    p.add_argument("--width",       type=int, default=1920,    help="Largura do frame em pixels")
    p.add_argument("--height",      type=int, default=1080,    help="Altura do frame em pixels")
    p.add_argument("--tile-size",   type=int, default=64,     help="Tamanho do tile em pixels")
    p.add_argument("--duration",    type=float, default=5.0,  help="Duração do vídeo em segundos (define #frames se --frames=0)")
    p.add_argument("--orbit-radius",type=float, default=4.0,  help="Raio da órbita da câmera")
    p.add_argument("--orbit-height",type=float, default=1.0,  help="Altura Y da câmera")
    p.add_argument("--fov",         type=float, default=60.0, help="Field of view em graus")
    p.add_argument("--output-dir",  default="output",         help="Diretório de saída")
    p.add_argument("--leader-ip",   default=None,             help="IP do líder (detecta automaticamente se omitido)")
    p.add_argument("--no-display",  action="store_true",      help="Passar --no-display ao compositor")
    return p.parse_args()


def main():
    args = parse_args()

    # Se --duration fornecido e --frames=0, calcular frames
    total_frames = args.frames
    if total_frames == 0:
        total_frames = int(args.duration * args.fps)

    # Diretórios de saída
    out_dir    = Path(args.output_dir)
    frames_dir = out_dir / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    mp4_path = out_dir / "render.mp4"

    # Detectar líder
    leader_ip = args.leader_ip
    if not leader_ip:
        print("🔍 Detectando líder do cluster...", end=" ", flush=True)
        leader_ip = find_leader()
        if not leader_ip:
            print("FALHOU — não foi possível detectar o líder.")
            print("Tente especificar --leader-ip manualmente.")
            sys.exit(1)
        print(f"OK → {leader_ip}")

    # Mudar cluster para modo render
    print(f"🎬 Mudando cluster para modo render...")
    try:
        r = requests.post(api_url(leader_ip, "/mode"),
                          json={"mode": "render"}, timeout=5)
        print(f"   Resposta: {r.json()}")
    except Exception as e:
        print(f"   ⚠ Erro ao mudar modo: {e}")

    # Abre UMA janela pygame + UM cliente Redis reutilizados em todos os frames
    print(f"🖼  Abrindo compositor (janela única)...")
    sess = open_session(
        host=leader_ip, port=6379,
        full_w=args.width, full_h=args.height,
        use_display=not args.no_display,
    )

    t_movie_start     = time.time()
    frame_times: List[float] = []

    for frame_idx in range(total_frames):
        frame_id    = f"{frame_idx:04d}"
        output_path = str(frames_dir / f"frame_{frame_id}.png")

        # Tempo normalizado [0, 2π] para animações SDF
        time_val = 2.0 * math.pi * frame_idx / total_frames

        camera = camera_for_frame(
            frame_idx, total_frames,
            radius=args.orbit_radius,
            height=args.orbit_height,
            fov=args.fov,
        )

        # Progresso
        pct = 100.0 * frame_idx / total_frames
        if frame_times:
            avg_t = sum(frame_times) / len(frame_times)
            eta   = avg_t * (total_frames - frame_idx)
            eta_s = f" — eta {fmt_eta(eta)}"
        else:
            eta_s = ""
        print(f"\n📽  Frame {frame_idx+1}/{total_frames} ({pct:.0f}%){eta_s}")

        # Submeter job ao cluster
        job = {
            "frame_id":  frame_id,
            "scene":     args.scene,
            "camera":    camera,
            "full_w":    args.width,
            "full_h":    args.height,
            "tile_size": args.tile_size,
            "time":      time_val,
        }
        try:
            resp = requests.post(api_url(leader_ip, "/render/job"),
                                 json=job, timeout=10)
            data = resp.json()
            tiles_total = data.get("tiles_total", 0)
            print(f"   Job submetido: {tiles_total} tiles")
        except Exception as e:
            print(f"   ⚠ Erro ao submeter job: {e}")
            continue

        # Compositar este frame reutilizando a janela/sessão única
        t_frame_start = time.time()
        ok = render_one_frame(
            sess,
            frame_id=frame_id,
            output_path=output_path,
            tiles_total_hint=tiles_total,
            timeout_s=300.0,
        )
        if not ok:
            print(f"   ⚠ Compositor abortou no frame {frame_id}")
            break

        t_frame_elapsed = time.time() - t_frame_start
        frame_times.append(t_frame_elapsed)
        print(f"   Frame salvo em {output_path} ({t_frame_elapsed:.1f}s)")

    # Fechar janela do compositor
    close_session(sess)

    # Voltar para modo geometry
    print("\n🔄 Voltando cluster para modo geometry...")
    try:
        requests.post(api_url(leader_ip, "/mode"), json={"mode": "geometry"}, timeout=5)
    except Exception as e:
        print(f"   ⚠ Erro: {e}")

    # Montar MP4 com ffmpeg
    print(f"\n🎞  Montando MP4 com ffmpeg → {mp4_path}")
    ffmpeg_cmd = [
        "ffmpeg", "-y",
        "-framerate", str(args.fps),
        "-i", str(frames_dir / "frame_%04d.png"),
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        str(mp4_path),
    ]
    try:
        subprocess.run(ffmpeg_cmd, check=True)
        t_total = time.time() - t_movie_start
        print(f"\n✅ Vídeo gerado: {mp4_path}")
        print(f"   {total_frames} frames — {fmt_eta(t_total)} no total")
    except FileNotFoundError:
        print("⚠  ffmpeg não encontrado no PATH. Frames PNG salvos em", frames_dir)
    except subprocess.CalledProcessError as e:
        print(f"⚠  ffmpeg falhou: {e}")


if __name__ == "__main__":
    main()
