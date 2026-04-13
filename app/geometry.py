"""
Módulo unificado de geometrias para o cluster.
Suporta: torus, lorenz, mandelbulb, klein.

O daemon chama generate_batch(geom, batch_id, size) para criar tarefas.
O worker chama process_point(fields) ou process_batch(messages) para computar pontos 3D.

A geometria ativa é controlada via chave Redis {stream}:geometry
"""
import math
import time
import random
import logging
from typing import List, Dict, Tuple
import numpy as np

log = logging.getLogger("geometry")

# ═══════════════════════════════════════════════
#  PARÂMETROS POR GEOMETRIA
# ═══════════════════════════════════════════════

GEOMETRIES = {
    "torus": {
        "R": 2.0,       # raio maior
        "r": 0.7,       # raio menor
        "res_u": 80,
        "res_v": 40,
    },
    "lorenz": {
        "sigma": 10.0,
        "rho": 28.0,
        "beta": 8.0 / 3.0,
        "dt": 0.005,
        "scale": 0.08,
    },
    "mandelbulb": {
        "power": 8,
        "max_iter": 4,
        "scale": 2.2,
        "res_theta": 40,
        "res_phi": 40,
        "res_r": 15,
    },
    "klein": {
        "a": 2.5,
        "scale": 0.15,
        "res_u": 100,
        "res_v": 60,
    },
}

DEFAULT_GEOMETRY = "torus"

# ═══════════════════════════════════════════════
#  GERAÇÃO DE BATCHES (chamado pelo daemon/líder)
# ═══════════════════════════════════════════════

def generate_batch(geom: str, batch_id: int, size: int, fixed_t: float = None) -> List[Dict[str, str]]:
    """
    Gera um batch de tarefas para a geometria especificada.
    Cada item da lista vira um XADD no Redis Stream.
    Valores são strings (limitação do Redis Streams).
    """
    is_render = (fixed_t is not None)
    t = time.time() if fixed_t is None else fixed_t

    if geom == "torus":
        return _gen_torus(batch_id, size, t, is_render)
    elif geom == "lorenz":
        return _gen_lorenz(batch_id, size, t, is_render)
    elif geom == "mandelbulb":
        return _gen_mandelbulb(batch_id, size, t, is_render)
    elif geom == "klein":
        return _gen_klein(batch_id, size, t, is_render)
    else:
        return _gen_torus(batch_id, size, t, is_render)


def _gen_torus(batch_id, size, t, is_render=False):
    p = GEOMETRIES["torus"]
    out = []
    for k in range(size):
        if is_render:
            u = random.uniform(0, 2.0 * math.pi)
            v = random.uniform(0, 2.0 * math.pi)
        else:
            idx = (batch_id * size + k) % (p["res_u"] * p["res_v"])
            i, j = divmod(idx, p["res_v"])
            u = (i / p["res_u"]) * 2.0 * math.pi
            v = (j / p["res_v"]) * 2.0 * math.pi

        out.append({
            "geom": "torus",
            "u": f"{u:.4f}",
            "v": f"{v:.4f}",
            "t": f"{t:.4f}",
            "batch": str(batch_id),
        })
    return out


def _gen_lorenz(batch_id, size, t, is_render=False):
    out = []
    for k in range(size):
        if is_render:
            index = random.randint(100, 8000)
            offset = random.uniform(0, 0.2)
        else:
            index = (batch_id * size + k) % 5000
            offset = (batch_id % 6) * 0.02

        out.append({
            "geom": "lorenz",
            "index": str(index),
            "seed_offset": f"{offset:.4f}",
            "t": f"{t:.4f}",
            "batch": str(batch_id),
        })
    return out


def _gen_mandelbulb(batch_id, size, t, is_render=False):
    p = GEOMETRIES["mandelbulb"]
    out = []
    for k in range(size):
        if is_render:
            theta = random.uniform(0, 2.0 * math.pi)
            phi = random.uniform(-math.pi / 2.0, math.pi / 2.0)
            r = random.uniform(1.2, 1.2 + p["res_r"] * 0.02)
        else:
            total = p["res_theta"] * p["res_phi"] * p["res_r"]
            idx = (batch_id * size + k) % total
            ti, rest = divmod(idx, p["res_phi"] * p["res_r"])
            pi_idx, ri = divmod(rest, p["res_r"])

            theta = (ti / p["res_theta"]) * 2.0 * math.pi
            phi = -math.pi / 2 + (pi_idx / p["res_phi"]) * math.pi
            r = 1.2 + ri * 0.02

        out.append({
            "geom": "mandelbulb",
            "theta": f"{theta:.4f}",
            "phi": f"{phi:.4f}",
            "r": f"{r:.4f}",
            "t": f"{t:.4f}",
            "batch": str(batch_id),
        })
    return out


def _gen_klein(batch_id, size, t, is_render=False):
    p = GEOMETRIES["klein"]
    out = []
    for k in range(size):
        if is_render:
            u = random.uniform(0, 2.0 * math.pi)
            v = random.uniform(0, 2.0 * math.pi)
        else:
            idx = (batch_id * size + k) % (p["res_u"] * p["res_v"])
            i, j = divmod(idx, p["res_v"])
            u = (i / p["res_u"]) * 2.0 * math.pi
            v = (j / p["res_v"]) * 2.0 * math.pi

        out.append({
            "geom": "klein",
            "u": f"{u:.4f}",
            "v": f"{v:.4f}",
            "t": f"{t:.4f}",
            "batch": str(batch_id),
        })
    return out


# ═══════════════════════════════════════════════
#  PROCESSAMENTO DE PONTOS (chamado pelo worker)
# ═══════════════════════════════════════════════

def process_point(fields: Dict[str, str]) -> Dict[str, str]:
    """
    O worker chama isso para cada tarefa.
    Lê o campo 'geom' e despacha para o processador correto.
    Retorna dict com x, y, z como strings.
    """
    geom = fields.get("geom", "torus")

    if geom == "torus":
        return _proc_torus(fields)
    elif geom == "lorenz":
        return _proc_lorenz(fields)
    elif geom == "mandelbulb":
        return _proc_mandelbulb(fields)
    elif geom == "klein":
        return _proc_klein(fields)
    else:
        return _proc_torus(fields)


def _proc_torus(fields):
    """
    Torus paramétrico com perturbação temporal.
    Carga: trigonometria + harmônicas (~30-80µs em ARM)
    """
    p = GEOMETRIES["torus"]
    u = float(fields["u"])
    v = float(fields["v"])
    t = float(fields["t"])

    # Perturbação temporal — faz a superfície "respirar"
    phi = 0.5 * math.sin(2.0 * t * 0.3)
    rr = p["r"] * (1.0 + 0.15 * math.sin(3 * u + t * 0.7))

    x = (p["R"] + rr * math.cos(v + phi)) * math.cos(u)
    y = (p["R"] + rr * math.cos(v + phi)) * math.sin(u)
    z = rr * math.sin(v + phi)

    return {"x": f"{x:.4f}", "y": f"{y:.4f}", "z": f"{z:.4f}"}


def _proc_lorenz(fields):
    """
    Atrator de Lorenz — sistema caótico.
    Carga: N iterações de EDO (~200-500µs por ponto em ARM)
    """
    p = GEOMETRIES["lorenz"]
    index = int(fields["index"])
    offset = float(fields.get("seed_offset", "0"))

    x, y, z = 0.1 + offset, 0.0 + offset * 0.5, 0.0

    # Iterar o sistema caótico — ESTA é a carga pesada
    for _ in range(index):
        dx = p["sigma"] * (y - x)
        dy = x * (p["rho"] - z) - y
        dz = x * y - p["beta"] * z
        x += dx * p["dt"]
        y += dy * p["dt"]
        z += dz * p["dt"]

    s = p["scale"]
    return {
        "x": f"{x * s:.4f}",
        "y": f"{(z - 25) * s:.4f}",
        "z": f"{y * s:.4f}",
    }


def _proc_mandelbulb(fields):
    """
    Mandelbulb fractal 3D — SDF sampling.
    Carga: iterações de potência complexa (~300-800µs em ARM)
    """
    p = GEOMETRIES["mandelbulb"]
    theta = float(fields["theta"])
    phi = float(fields["phi"])
    r = float(fields["r"])
    t = float(fields["t"])
    n = p["power"]

    # Coordenada cartesiana do ponto de amostragem
    px = r * math.cos(phi) * math.cos(theta)
    py = r * math.cos(phi) * math.sin(theta)
    pz = r * math.sin(phi)

    # Iteração do Mandelbulb
    cx, cy, cz = px, py, pz
    escaped = False

    for _ in range(p["max_iter"]):
        r2 = cx * cx + cy * cy + cz * cz
        if r2 > 4:
            escaped = True
            break
        rMag = math.sqrt(r2)
        thetaM = math.atan2(math.sqrt(cx * cx + cy * cy), cz)
        phiM = math.atan2(cy, cx)
        rn = rMag ** n
        cx = rn * math.sin(thetaM * n) * math.cos(phiM * n) + px
        cy = rn * math.sin(thetaM * n) * math.sin(phiM * n) + py
        cz = rn * math.cos(thetaM * n) + pz

    if escaped:
        # Ponto fora do fractal — retorna ponto inválido que o frontend ignora
        return {"x": "999", "y": "999", "z": "999"}

    s = p["scale"]
    pulse = 1.0 + 0.05 * math.sin(t * 2 + theta * 3)
    return {
        "x": f"{px * s * pulse:.4f}",
        "y": f"{pz * s * pulse:.4f}",
        "z": f"{py * s * pulse:.4f}",
    }


def _proc_klein(fields):
    """
    Garrafa de Klein — imersão paramétrica em R³.
    Carga: trigonometria com branches (~50-100µs em ARM)
    """
    p = GEOMETRIES["klein"]
    u = float(fields["u"])
    v = float(fields["v"])
    t = float(fields["t"])

    cosU = math.cos(u)
    sinU = math.sin(u)
    cosV = math.cos(v)
    sinV = math.sin(v)
    a = p["a"]
    pulse = 1.0 + 0.05 * math.sin(t + u * 2)

    if u < math.pi:
        x = 3 * cosU * (1 + sinU) + a * (1 - cosU / 2) * cosU * cosV
        z = -8 * sinU - a * sinU * (1 - cosU / 2) * cosV
    else:
        x = 3 * cosU * (1 + sinU) + a * (1 - cosU / 2) * math.cos(v + math.pi)
        z = -8 * sinU

    y = a * (1 - cosU / 2) * sinV

    s = p["scale"] * pulse
    return {
        "x": f"{x * s:.4f}",
        "y": f"{y * s:.4f}",
        "z": f"{z * s + 0.8:.4f}",
    }


# ═══════════════════════════════════════════════
#  PROCESSAMENTO VETORIZADO (A4)
# ═══════════════════════════════════════════════

def process_batch(messages: List[Tuple[str, Dict[str, str]]]) -> List[Tuple[str, Dict[str, str]]]:
    """
    Recebe lista de (msg_id, fields), retorna lista de (msg_id, result_dict).
    Vetoriza torus e klein com numpy. Mantém lorenz e mandelbulb no loop
    Python (eles têm dependência iterativa que não vetoriza bem).
    Filtra pontos com x=='999' (mandelbulb fora do fractal).
    """
    if not messages:
        return []

    # Separar por geometria para vetorizar em bloco
    grupos: Dict[str, List[Tuple[int, str, Dict]]] = {}
    for orig_idx, (msg_id, fields) in enumerate(messages):
        geom = fields.get("geom", "torus")
        if geom not in grupos:
            grupos[geom] = []
        grupos[geom].append((orig_idx, msg_id, fields))

    # Pré-alocar lista de resultados com None (indexada pela posição original)
    resultados: List = [None] * len(messages)

    for geom, grupo in grupos.items():
        if geom == "torus":
            _batch_torus(grupo, resultados)
        elif geom == "klein":
            _batch_klein(grupo, resultados)
        else:
            # lorenz e mandelbulb — loop Python (dependência iterativa)
            for orig_idx, msg_id, fields in grupo:
                pt = process_point(fields)
                if pt.get("x") != "999":
                    resultados[orig_idx] = (msg_id, pt)
                # se x=='999' deixa None (será filtrado abaixo)

    # Retornar apenas os resultados não-None (filtra mandelbulb escapado)
    return [r for r in resultados if r is not None]


def _batch_torus(grupo, resultados):
    """
    Vetoriza o cálculo de torus com numpy para um grupo de mensagens.
    """
    p = GEOMETRIES["torus"]
    R = p["R"]
    r_minor = p["r"]

    indices = [i for i, _, _ in grupo]
    msg_ids = [m for _, m, _ in grupo]
    fields_list = [f for _, _, f in grupo]

    u_arr = np.fromiter((float(f["u"]) for f in fields_list), dtype=np.float64, count=len(fields_list))
    v_arr = np.fromiter((float(f["v"]) for f in fields_list), dtype=np.float64, count=len(fields_list))
    t_arr = np.fromiter((float(f["t"]) for f in fields_list), dtype=np.float64, count=len(fields_list))

    # Perturbação temporal
    phi = 0.5 * np.sin(2.0 * t_arr * 0.3)
    rr = r_minor * (1.0 + 0.15 * np.sin(3 * u_arr + t_arr * 0.7))

    x = (R + rr * np.cos(v_arr + phi)) * np.cos(u_arr)
    y = (R + rr * np.cos(v_arr + phi)) * np.sin(u_arr)
    z = rr * np.sin(v_arr + phi)

    for i, orig_idx in enumerate(indices):
        resultados[orig_idx] = (
            msg_ids[i],
            {"x": f"{x[i]:.4f}", "y": f"{y[i]:.4f}", "z": f"{z[i]:.4f}"},
        )


def _batch_klein(grupo, resultados):
    """
    Vetoriza o cálculo da garrafa de Klein com numpy para um grupo de mensagens.
    Como klein tem um branch (if u < pi), processamos os dois grupos separados.
    """
    p = GEOMETRIES["klein"]
    a = p["a"]
    scale = p["scale"]

    indices = [i for i, _, _ in grupo]
    msg_ids = [m for _, m, _ in grupo]
    fields_list = [f for _, _, f in grupo]

    u_arr = np.fromiter((float(f["u"]) for f in fields_list), dtype=np.float64, count=len(fields_list))
    v_arr = np.fromiter((float(f["v"]) for f in fields_list), dtype=np.float64, count=len(fields_list))
    t_arr = np.fromiter((float(f["t"]) for f in fields_list), dtype=np.float64, count=len(fields_list))

    cosU = np.cos(u_arr)
    sinU = np.sin(u_arr)
    cosV = np.cos(v_arr)
    sinV = np.sin(v_arr)
    pulse = 1.0 + 0.05 * np.sin(t_arr + u_arr * 2)

    mask = u_arr < math.pi

    x = np.where(
        mask,
        3 * cosU * (1 + sinU) + a * (1 - cosU / 2) * cosU * cosV,
        3 * cosU * (1 + sinU) + a * (1 - cosU / 2) * np.cos(v_arr + math.pi),
    )
    z = np.where(
        mask,
        -8 * sinU - a * sinU * (1 - cosU / 2) * cosV,
        -8 * sinU,
    )
    y = a * (1 - cosU / 2) * sinV

    s = scale * pulse
    for i, orig_idx in enumerate(indices):
        resultados[orig_idx] = (
            msg_ids[i],
            {
                "x": f"{x[i] * s[i]:.4f}",
                "y": f"{y[i] * s[i]:.4f}",
                "z": f"{z[i] * s[i] + 0.8:.4f}",
            },
        )