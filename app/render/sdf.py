"""
SDF ray marching — cenas 3D para render farm distribuído.

Versão VETORIZADA: cada SDF opera num batch de pontos (N, 3) de uma vez,
e o ray marching mantém máscaras booleanas de raios ativos. Sem dependência
de numba — numpy puro em float32 (amigável a NEON/SIMD no ARM).

Cenas disponíveis:
  - "mandelbulb"  : Mandelbulb power 8
  - "menger"      : Esponja de Menger infinita
  - "apollonian"  : Apollonian gasket
"""
import math
from typing import Dict, Callable

import numpy as np

# ═══════════════════════════════════════════════
#  CONSTANTES DE RENDER
# ═══════════════════════════════════════════════

MAX_STEPS = 64          # mandelbulb converge rápido — 64 basta
EPSILON   = np.float32(0.001)
MAX_DIST  = np.float32(20.0)

# ─── Iluminação ────────────────────────────────
LUZ1_DIR    = np.array([0.6,  0.8,  0.4],  dtype=np.float32)
LUZ1_COR    = np.array([0.9,  0.9,  1.0],  dtype=np.float32)
LUZ2_DIR    = np.array([-0.8, 0.2, -0.5],  dtype=np.float32)
LUZ2_COR    = np.array([1.0,  0.5,  0.15], dtype=np.float32)
AMBIENT_COR = np.array([0.04, 0.06, 0.12], dtype=np.float32)
MATERIAL    = np.array([0.55, 0.55, 0.60], dtype=np.float32)


def _norm3(v: np.ndarray) -> np.ndarray:
    """Normaliza vetor 3D simples (não batch)."""
    n = float(np.sqrt(v.dot(v)))
    return v / n if n > 1e-9 else v


def _norm_batch(v: np.ndarray) -> np.ndarray:
    """Normaliza (N,3) em-lugar-seguro. Retorna (N,3)."""
    n = np.linalg.norm(v, axis=1, keepdims=True)
    n = np.maximum(n, np.float32(1e-9))
    return v / n


# ═══════════════════════════════════════════════
#  SDFs DE CENA (BATCH)
# ═══════════════════════════════════════════════

def sdf_mandelbulb_batch(p: np.ndarray, time: float) -> np.ndarray:
    """
    p: (N, 3) float32. Retorna (N,) float32 com distância estimada.
    Referência: http://iquilezles.org/articles/mandelbulb/
    """
    p = p.astype(np.float32, copy=False)
    w = p.copy()
    dr = np.ones(p.shape[0], dtype=np.float32)
    m = (w * w).sum(axis=1)

    power = np.float32(8.0 + 0.5 * math.sin(time * 0.5))

    for _ in range(8):
        r = np.sqrt(m)
        alive = r < np.float32(1.5)
        if not alive.any():
            break

        # Evitar NaN: clamp do argumento do acos
        r_safe = r + np.float32(1e-12)
        theta = np.arccos(np.clip(w[:, 2] / r_safe, np.float32(-1.0), np.float32(1.0)))
        phi   = np.arctan2(w[:, 1], w[:, 0])

        dr_new = np.power(r, power - 1.0) * power * dr + np.float32(1.0)
        zr     = np.power(r, power)
        theta_p = theta * power
        phi_p   = phi * power

        sin_t = np.sin(theta_p)
        w_new = np.stack([
            zr * sin_t * np.cos(phi_p),
            zr * sin_t * np.sin(phi_p),
            zr * np.cos(theta_p),
        ], axis=1).astype(np.float32) + p

        # Atualizar apenas os vivos
        alive3 = alive[:, None]
        w  = np.where(alive3, w_new, w)
        dr = np.where(alive, dr_new, dr)
        m  = (w * w).sum(axis=1)

    m_safe = np.maximum(m, np.float32(1e-12))
    dr_safe = np.maximum(dr, np.float32(1e-12))
    return (np.float32(0.25) * np.log(m_safe) * np.sqrt(m_safe) / dr_safe).astype(np.float32)


def sdf_menger_batch(p: np.ndarray, _time: float) -> np.ndarray:
    """
    Esponja de Menger — IFS iterativo 5 níveis, vetorizado.
    """
    p = p.astype(np.float32, copy=False)
    # Dobrar o espaço em caixa [-1, 1]³ com repetição infinita
    q = np.abs(np.mod(p + np.float32(1.0), np.float32(2.0)) - np.float32(1.0))
    d = np.maximum(np.maximum(q[:, 0], q[:, 1]), q[:, 2]) - np.float32(1.0)

    s = np.float32(1.0)
    for _ in range(5):
        a = np.mod(q * s, np.float32(2.0)) - np.float32(1.0)
        s = s * np.float32(3.0)
        r = np.abs(a)
        r0, r1, r2 = r[:, 0], r[:, 1], r[:, 2]
        cross = np.maximum(
            np.maximum(np.minimum(r0, r1), np.minimum(r1, r2)),
            np.minimum(r0, r2),
        )
        c = (np.float32(1.0) / s) * (np.float32(1.0) - np.float32(3.0) * cross)
        d = np.maximum(d, c)

    return d.astype(np.float32)


def sdf_apollonian_batch(p: np.ndarray, time: float) -> np.ndarray:
    """
    Apollonian gasket (kaleidoscopic IFS) vetorizado.
    """
    p = p.astype(np.float32, copy=False)
    scale = np.float32(1.5 + 0.3 * math.sin(time * 0.3))
    q = p.copy()

    offset = np.array([scale - 1.0, scale - 1.0, scale - 1.0], dtype=np.float32)

    for _ in range(8):
        # Dobras em tetraedro (3 planos) via máscaras
        # if q[0]+q[1] < 0: swap-neg
        mask = (q[:, 0] + q[:, 1]) < 0
        new0 = np.where(mask, -q[:, 1], q[:, 0])
        new1 = np.where(mask, -q[:, 0], q[:, 1])
        q[:, 0] = new0
        q[:, 1] = new1

        mask = (q[:, 0] + q[:, 2]) < 0
        new0 = np.where(mask, -q[:, 2], q[:, 0])
        new2 = np.where(mask, -q[:, 0], q[:, 2])
        q[:, 0] = new0
        q[:, 2] = new2

        mask = (q[:, 1] + q[:, 2]) < 0
        new1 = np.where(mask, -q[:, 2], q[:, 1])
        new2 = np.where(mask, -q[:, 1], q[:, 2])
        q[:, 1] = new1
        q[:, 2] = new2

        q = q * scale - offset

    return (np.sqrt((q * q).sum(axis=1)) * (scale ** np.float32(-8.0))).astype(np.float32)


# Mapa de cenas (batch)
_SCENE_MAP_BATCH: Dict[str, Callable[[np.ndarray, float], np.ndarray]] = {
    "mandelbulb": sdf_mandelbulb_batch,
    "menger":     sdf_menger_batch,
    "apollonian": sdf_apollonian_batch,
}


def _get_sdf_batch(scene_name: str):
    return _SCENE_MAP_BATCH.get(scene_name, sdf_mandelbulb_batch)


# ═══════════════════════════════════════════════
#  NORMAL POR GRADIENTE FINITO (BATCH)
# ═══════════════════════════════════════════════

def normal_batch(sdf_fn, pos: np.ndarray, time: float) -> np.ndarray:
    """
    Gradiente central vetorizado. pos: (M, 3). Retorna (M, 3).
    6 avaliações do SDF, cada uma em M pontos.
    """
    e = np.float32(0.001)
    M = pos.shape[0]
    if M == 0:
        return np.zeros((0, 3), dtype=np.float32)

    dx = np.array([e, 0, 0], dtype=np.float32)
    dy = np.array([0, e, 0], dtype=np.float32)
    dz = np.array([0, 0, e], dtype=np.float32)

    nx = sdf_fn(pos + dx, time) - sdf_fn(pos - dx, time)
    ny = sdf_fn(pos + dy, time) - sdf_fn(pos - dy, time)
    nz = sdf_fn(pos + dz, time) - sdf_fn(pos - dz, time)

    n = np.stack([nx, ny, nz], axis=1).astype(np.float32)
    return _norm_batch(n)


# ═══════════════════════════════════════════════
#  AO (BATCH)
# ═══════════════════════════════════════════════

def ao_batch(sdf_fn, pos: np.ndarray, nor: np.ndarray, time: float) -> np.ndarray:
    """
    AO de 5 amostras ao longo da normal, vetorizado. Retorna (M,) float32.
    """
    M = pos.shape[0]
    if M == 0:
        return np.zeros((0,), dtype=np.float32)

    occ = np.zeros(M, dtype=np.float32)
    sca = np.float32(1.0)
    for i in range(1, 6):
        h = np.float32(0.01 + 0.12 * i / 5.0)
        d = sdf_fn(pos + nor * h, time)
        occ = occ + np.maximum(np.float32(0.0), h - d) * sca
        sca = sca * np.float32(0.95)

    return np.maximum(np.float32(0.0), np.float32(1.0) - np.float32(1.5) * occ)


# ═══════════════════════════════════════════════
#  RAY MARCHING (BATCH)
# ═══════════════════════════════════════════════

def ray_march_batch(sdf_fn, ro: np.ndarray, rd: np.ndarray, time: float):
    """
    ro, rd: (N, 3) float32. Retorna:
      hit: (N,) bool
      t:   (N,) float32
      pos: (N, 3) float32
    """
    N = ro.shape[0]
    t = np.zeros(N, dtype=np.float32)
    active = np.ones(N, dtype=bool)
    hit = np.zeros(N, dtype=bool)

    for _ in range(MAX_STEPS):
        if not active.any():
            break

        idx = np.where(active)[0]
        pos_a = ro[idx] + rd[idx] * t[idx, None]
        d = sdf_fn(pos_a, time)

        t_new = t[idx] + d
        t[idx] = t_new

        just_hit     = d < EPSILON
        just_escaped = t_new > MAX_DIST

        if just_hit.any():
            hit[idx[just_hit]] = True
        done = just_hit | just_escaped
        if done.any():
            active[idx[done]] = False

    pos = ro + rd * t[:, None]
    return hit, t, pos


# ═══════════════════════════════════════════════
#  SHADING (BATCH)
# ═══════════════════════════════════════════════

def shade_batch(sdf_fn, hit: np.ndarray, pos: np.ndarray, rd: np.ndarray, time: float) -> np.ndarray:
    """
    Calcula cor (N, 3) float32 em [0, 1].
    - hit==False: gradiente do céu
    - hit==True : ambient + 2 luzes direcionais + AO + Reinhard + gamma 2.2
    TODO: reintroduzir soft shadow vetorizado da luz 1 (removido nesta versão
          porque exigia loop por step com máscaras dentro da máscara de hits).
    """
    N = pos.shape[0]
    col = np.zeros((N, 3), dtype=np.float32)

    # ─── Background (miss) ───
    miss = ~hit
    if miss.any():
        t_sky = np.float32(0.5) * (rd[miss, 1] + np.float32(1.0))
        # gradient: preto → azul escuro
        sky_top = np.array([0.02, 0.04, 0.10], dtype=np.float32)
        col[miss] = t_sky[:, None] * sky_top[None, :]

    # ─── Hits ───
    if not hit.any():
        return col

    idx = np.where(hit)[0]
    pos_h = pos[idx]

    nor = normal_batch(sdf_fn, pos_h, time)
    ao  = ao_batch(sdf_fn, pos_h, nor, time)

    l1 = _norm3(LUZ1_DIR)
    l2 = _norm3(LUZ2_DIR)

    diff1 = np.maximum(np.float32(0.0), nor @ l1)
    diff2 = np.maximum(np.float32(0.0), nor @ l2)

    mat = MATERIAL  # (3,)
    ao3 = ao[:, None]

    shaded = (
        mat[None, :] * AMBIENT_COR[None, :] * ao3
        + mat[None, :] * LUZ1_COR[None, :] * (diff1[:, None] * ao3)
        + mat[None, :] * LUZ2_COR[None, :] * (diff2[:, None] * ao3) * np.float32(0.5)
    ).astype(np.float32)

    # Reinhard
    shaded = shaded / (shaded + np.float32(1.0))
    # Gamma 2.2
    shaded = np.clip(shaded, np.float32(0.0), np.float32(1.0)) ** np.float32(1.0 / 2.2)

    col[idx] = shaded
    return col


# ═══════════════════════════════════════════════
#  RENDER DE TILE
# ═══════════════════════════════════════════════

def render_tile(
    scene_name: str,
    tile_x: int, tile_y: int,
    tile_w: int, tile_h: int,
    full_w: int, full_h: int,
    camera: Dict,
    time: float,
) -> np.ndarray:
    """
    Renderiza um tile (tile_h, tile_w, 3) uint8. Assinatura inalterada
    em relação à versão anterior — só a implementação ficou vetorizada.
    """
    sdf_fn = _get_sdf_batch(scene_name)

    cam_pos    = np.array(camera.get("pos",    [0.0, 1.0, 4.0]), dtype=np.float32)
    cam_target = np.array(camera.get("target", [0.0, 0.0, 0.0]), dtype=np.float32)
    fov        = float(camera.get("fov", 60.0))

    forward = _norm3(cam_target - cam_pos)
    right   = _norm3(np.cross(forward, np.array([0.0, 1.0, 0.0], dtype=np.float32)))
    up      = np.cross(right, forward).astype(np.float32)

    tan_half = np.float32(math.tan(math.radians(fov * 0.5)))
    aspect   = np.float32(full_w) / np.float32(full_h)

    # Grid de pixels do tile (tile_h, tile_w)
    px_grid, py_grid = np.meshgrid(
        np.arange(tile_w, dtype=np.float32),
        np.arange(tile_h, dtype=np.float32),
        indexing="xy",
    )
    fx = np.float32(tile_x) + px_grid + np.float32(0.5)
    fy = np.float32(tile_y) + py_grid + np.float32(0.5)

    ndc_x = (np.float32(2.0) * fx / np.float32(full_w) - np.float32(1.0)) * aspect * tan_half
    ndc_y = (np.float32(1.0) - np.float32(2.0) * fy / np.float32(full_h)) * tan_half

    # Direções (N, 3)
    ndc_x_flat = ndc_x.reshape(-1, 1)
    ndc_y_flat = ndc_y.reshape(-1, 1)
    rd = (
        forward[None, :]
        + right[None, :] * ndc_x_flat
        + up[None, :] * ndc_y_flat
    ).astype(np.float32)
    rd = _norm_batch(rd)

    N = rd.shape[0]
    ro = np.broadcast_to(cam_pos, (N, 3)).astype(np.float32, copy=True)

    hit, t, pos = ray_march_batch(sdf_fn, ro, rd, float(time))
    col = shade_batch(sdf_fn, hit, pos, rd, float(time))

    img = (np.clip(col, 0.0, 1.0) * 255.0).astype(np.uint8)
    return img.reshape(tile_h, tile_w, 3)
