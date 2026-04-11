"""
Geração e processamento de pontos sobre um torus paramétrico.
A geração é feita pelo líder (em batches); o processamento é o que
o worker executa por ponto.
"""
import math, time
import random
from typing import List, Dict

R = 2.0   # raio maior
r = 0.7   # raio menor
RES_U = 80   # resolução em u
RES_V = 40   # resolução em v


def generate_torus_batch(batch_id: int, size: int) -> List[Dict[str, str]]:
    """
    O líder chama esta função. Cada item da lista vira um XADD.
    Os valores precisam ser strings (limitação do Redis Streams).
    """
    out = []
    t = time.time()
    for k in range(size):
        idx = (batch_id * size + k) % (RES_U * RES_V)
        i, j = divmod(idx, RES_V)
        u = (i / RES_U) * 2.0 * math.pi
        v = (j / RES_V) * 2.0 * math.pi
        out.append({
            "u": f"{u:.6f}",
            "v": f"{v:.6f}",
            "t": f"{t:.6f}",
            "batch": str(batch_id),
        })
    return out

def process_point(u, v, R=2, r=1):
    # Adiciona um pequeno ruído aleatório para dar volume à nuvem
    jitter = random.uniform(-0.1, 0.1)
    r_eff = r + jitter
    
    x = (R + r_eff * math.cos(v)) * math.cos(u)
    y = (R + r_eff * math.cos(v)) * math.sin(u)
    z = r_eff * math.sin(v)
    
    return x, y, z

# def process_point(fields: Dict[str, str]) -> Dict[str, float]:
#     """
#     O worker chama isso. Carga útil real: trigonometria + perturbação.
#     Em CPU ARM isso roda em ~30-80 µs por ponto. Aumentar a complexidade
#     aqui é o que estressa o cluster (ex: ray-marching, voxel sampling).
#     """
#     u = float(fields["u"])
#     v = float(fields["v"])
#     t = float(fields["t"])
#     age = time.time() - t

#     # perturbação temporal — faz a superfície "respirar"
#     phi = 0.5 * math.sin(2.0 * t * 0.3)
#     rr  = r * (1.0 + 0.15 * math.sin(3 * u + t * 0.7))

#     x = (R + rr * math.cos(v + phi)) * math.cos(u)
#     y = (R + rr * math.cos(v + phi)) * math.sin(u)
#     z = rr * math.sin(v + phi)

#     return {"x": x, "y": y, "z": z, "age_ms": age * 1000}