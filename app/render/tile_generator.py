"""
Gerador de tarefas de tile para o render farm.
Divide um frame completo em tiles e gera a lista de dicts para XADD no Redis.
"""
import math
from typing import List, Dict


def generate_render_tasks(
    frame_id: str,
    scene: str,
    camera: Dict,
    full_w: int,
    full_h: int,
    tile_size: int,
    time_val: float,
) -> List[Dict[str, str]]:
    """
    Divide um frame em tiles. Retorna lista de dicts para XADD.
    Tiles de borda podem ter tamanho menor que tile_size.

    Cada tarefa contém os campos:
        mode, frame_id, scene, tile_x, tile_y, tile_w, tile_h,
        full_w, full_h, cam_pos, cam_target, cam_fov, time
    """
    cam_pos    = camera.get("pos",    [0.0, 1.0, 4.0])
    cam_target = camera.get("target", [0.0, 0.0, 0.0])
    cam_fov    = float(camera.get("fov", 60.0))

    # Serializar câmera como strings "x,y,z"
    cam_pos_str    = ",".join(f"{v:.4f}" for v in cam_pos)
    cam_target_str = ",".join(f"{v:.4f}" for v in cam_target)

    tarefas = []
    cols = math.ceil(full_w / tile_size)
    rows = math.ceil(full_h / tile_size)

    for row in range(rows):
        for col in range(cols):
            tx = col * tile_size
            ty = row * tile_size
            # Tiles de borda podem ser menores
            tw = min(tile_size, full_w - tx)
            th = min(tile_size, full_h - ty)

            tarefas.append({
                "mode":       "render",
                "frame_id":   frame_id,
                "scene":      scene,
                "tile_x":     str(tx),
                "tile_y":     str(ty),
                "tile_w":     str(tw),
                "tile_h":     str(th),
                "full_w":     str(full_w),
                "full_h":     str(full_h),
                "cam_pos":    cam_pos_str,
                "cam_target": cam_target_str,
                "cam_fov":    f"{cam_fov:.2f}",
                "time":       f"{time_val:.4f}",
            })

    return tarefas
