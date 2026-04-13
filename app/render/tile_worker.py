"""
Tile worker — processa uma tarefa de render tile.
Chama sdf.render_tile e serializa o resultado em PNG base64 para o Redis.
"""
import os
import time
import io
import base64
from typing import Dict
import numpy as np

from config import MY_NAME
from render.sdf import render_tile


def process_render_task(fields: Dict[str, str]) -> Dict[str, str]:
    """
    Lê os campos de uma tarefa de render (do Redis Stream),
    chama sdf.render_tile, codifica o resultado como PNG base64
    e retorna um dict para ser gravado em {stream}:results.

    Importa PIL (Pillow) localmente — garante que falha clara se não instalado.
    """
    from PIL import Image  # noqa: importado aqui para falha explícita

    # Extrair parâmetros da tarefa
    frame_id  = fields["frame_id"]
    scene     = fields.get("scene", "mandelbulb")
    tile_x    = int(fields["tile_x"])
    tile_y    = int(fields["tile_y"])
    tile_w    = int(fields["tile_w"])
    tile_h    = int(fields["tile_h"])
    full_w    = int(fields["full_w"])
    full_h    = int(fields["full_h"])
    time_val  = float(fields.get("time", "0.0"))

    # Deserializar câmera de "x,y,z" → lista de floats
    def _parse_vec(s: str):
        return [float(v) for v in s.split(",")]

    camera = {
        "pos":    _parse_vec(fields.get("cam_pos",    "0,1,4")),
        "target": _parse_vec(fields.get("cam_target", "0,0,0")),
        "fov":    float(fields.get("cam_fov", "60")),
    }

    worker_id = os.environ.get("WORKER_ID", "0")
    worker_name = f"{MY_NAME}-w{worker_id}"

    t_start = time.perf_counter()

    # Render SDF do tile
    img_arr = render_tile(
        scene_name=scene,
        tile_x=tile_x, tile_y=tile_y,
        tile_w=tile_w, tile_h=tile_h,
        full_w=full_w, full_h=full_h,
        camera=camera,
        time=time_val,
    )

    compute_ms = (time.perf_counter() - t_start) * 1000.0

    # Codificar como PNG in-memory e converter para base64
    pil_img = Image.fromarray(img_arr.astype(np.uint8), mode="RGB")
    buf = io.BytesIO()
    pil_img.save(buf, format="PNG", optimize=False, compress_level=1)
    png_b64 = base64.b64encode(buf.getvalue()).decode("ascii")

    return {
        "mode":       "render",
        "frame_id":   frame_id,
        "tile_x":     str(tile_x),
        "tile_y":     str(tile_y),
        "tile_w":     str(tile_w),
        "tile_h":     str(tile_h),
        "w":          worker_name,
        "png_b64":    png_b64,
        "compute_ms": f"{compute_ms:.1f}",
    }
