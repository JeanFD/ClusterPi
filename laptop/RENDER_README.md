# ClusterPi — Modo Render Farm (SDF Ray Marching)

Documentação do modo de renderização distribuída.
O cluster divide frames em tiles e cada worker renderiza um tile via SDF ray marching.

---

## Pré-requisitos no laptop

```bash
pip install pygame pillow redis requests numpy
# ou usando o requirements.txt desta pasta:
pip install -r laptop/requirements.txt
```

**ffmpeg** deve estar no PATH para gerar o MP4 final:
- Windows: baixe em https://ffmpeg.org/download.html e adicione ao PATH
- Linux/macOS: `sudo apt install ffmpeg` ou `brew install ffmpeg`

---

## Como mudar para o modo render

O modo é controlado pela chave Redis `{stream}:mode`.  
Via curl (substituir IP pelo líder atual):

```bash
# Mudar para modo render
curl -X POST http://192.168.1.20:8000/mode \
     -H "Content-Type: application/json" \
     -d '{"mode": "render"}'

# Verificar modo atual
curl http://192.168.1.20:8000/mode

# Voltar para geometria
curl -X POST http://192.168.1.20:8000/mode \
     -H "Content-Type: application/json" \
     -d '{"mode": "geometry"}'
```

---

## Renderizar 1 frame (teste rápido)

### Passo 1 — Mudar o cluster para modo render

```bash
curl -X POST http://192.168.1.20:8000/mode \
     -H "Content-Type: application/json" \
     -d '{"mode": "render"}'
```

### Passo 2 — Submeter um job de frame

```bash
curl -X POST http://192.168.1.20:8000/render/job \
     -H "Content-Type: application/json" \
     -d '{
       "frame_id": "0001",
       "scene": "mandelbulb",
       "camera": {"pos": [0, 1, 4], "target": [0, 0, 0], "fov": 60},
       "full_w": 640,
       "full_h": 360,
       "tile_size": 64,
       "time": 0.0
     }'
```

Resposta esperada: `{"ok": true, "tiles_total": 60}`  
(640×360 com tiles de 64px = 10 colunas × 6 linhas = 60 tiles)

### Passo 3 — Monitorar progresso

```bash
# Ver quantos tiles foram concluídos
curl http://192.168.1.20:8000/render/progress
```

### Passo 4 — Rodar o compositor no laptop

```bash
python laptop/compositor.py \
    --host 192.168.1.20 \
    --width 640 --height 360 \
    --frame-id 0001 \
    --output frame_0001.png
```

O compositor abre uma janela pygame mostrando os tiles aparecendo em tempo real.
Ao completar todos os tiles, salva `frame_0001.png` e fecha.

---

## Como rodar o render_movie.py completo

```bash
# 5 segundos a 24fps = 120 frames, cena mandelbulb, tiles de 64px
python laptop/render_movie.py \
    --scene mandelbulb \
    --frames 120 \
    --fps 24 \
    --width 640 \
    --height 360 \
    --tile-size 64

# Sem janela pygame (headless, ideal para SSH)
python laptop/render_movie.py --scene menger --frames 60 --no-display

# Especificar líder manualmente (útil se a detecção automática falhar)
python laptop/render_movie.py --leader-ip 192.168.1.22 --scene apollonian
```

O vídeo é salvo em `output/render.mp4`.  
Os frames PNG intermediários ficam em `output/frames/frame_NNNN.png`.

---

## Cenas disponíveis

| Cena          | Descrição                                      |
|---------------|------------------------------------------------|
| `mandelbulb`  | Mandelbulb power 8 — fractal 3D animado        |
| `menger`      | Esponja de Menger — IFS iterativo 5 níveis     |
| `apollonian`  | Apollonian gasket — kaleidoscópico IFS         |

---

## Estimativa de tempo por tile

Em Raspberry Pi 5 (numpy puro, sem numba):

| Cena         | Tile 64×64 aprox.  |
|--------------|--------------------|
| `menger`     | 2–5s               |
| `apollonian` | 3–8s               |
| `mandelbulb` | 8–20s              |

Com 24 workers (6 hosts × 4 workers), um frame 640×360/64px = 60 tiles
pode ser concluído em paralelo — tempo ≈ tempo de 1 tile / workers disponíveis.

> **Dica de performance:** Reduza `--tile-size` para 32 para mais paralelismo
> (mais tiles = mais granularidade), ou use cenas mais rápidas como `menger`.

---

## Troubleshooting comum

### `CROSSSLOT` no Redis
As chaves `{stream}:mode`, `{stream}:render:job`, etc. **precisam** da hash tag
`{stream}` para cair no mesmo slot. Se aparecer erro de CROSSSLOT, verifique se
alguma chave foi adicionada sem o prefixo.

### Tiles não aparecem no compositor
1. Verifique que o cluster está em modo `render`: `curl .../mode`
2. Verifique os logs do worker: `journalctl -u cluster-worker@0 -f`
3. Confirme que Pillow está instalado no venv dos workers:
   ```bash
   ssh pi@192.168.1.20 "~/cluster/app/venv/bin/pip show Pillow"
   ```

### Janela pygame não abre
Use `--no-display` para rodar headless. O PNG ainda é salvo normalmente.

### ffmpeg não encontrado
Os frames PNG são salvos mesmo sem ffmpeg. Para montar o vídeo depois:
```bash
ffmpeg -y -framerate 24 -i output/frames/frame_%04d.png \
       -c:v libx264 -pix_fmt yuv420p output/render.mp4
```

### Compositor não encontra tiles antigos
O compositor usa `XREAD` a partir de `$` (momento atual). Se os tiles já foram
processados antes do compositor iniciar, eles não aparecerão.  
Para re-renderizar, submeta um novo job com um `frame_id` diferente.

---

## Como voltar ao modo geometry

```bash
curl -X POST http://192.168.1.20:8000/mode \
     -H "Content-Type: application/json" \
     -d '{"mode": "geometry"}'
```

O render_movie.py faz isso automaticamente ao terminar.

---

## Arquitetura resumida

```
Laptop                    Cluster (6× RPi5)
──────                    ─────────────────
render_movie.py
  │ POST /render/job ───► daemon.py (líder)
  │                           │ XADD {stream}:tasks (N tiles)
  │                           ▼
compositor.py             worker@0..3 (24 total)
  │ XREAD {stream}:results ◄── │ render_tile() → PNG b64
  │                             │ XADD {stream}:results
  ▼
monta imagem + salva PNG
  │
  ▼
ffmpeg → render.mp4
```
