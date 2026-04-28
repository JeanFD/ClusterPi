# ClusterPi — Render Farm & Visualizador 3D Distribuído

Cluster de 6× Raspberry Pi 5 para dois workloads paralelos:

- **Modo geometry** — gera pontos 3D em tempo real para um visualizador ao vivo (torus, lorenz, mandelbulb, klein)
- **Modo render** — fazenda de renderização por ray marching SDF; um frame é dividido em tiles, distribuído aos workers, e os resultados são compostos em PNGs / MP4

---

## Hardware

| Componente | Detalhes |
|---|---|
| Workers | 6× Raspberry Pi 5 |
| IPs | `192.168.1.20` – `192.168.1.25` |
| Hostnames | `worker-01` .. `worker-06` |
| Orquestrador | Laptop Windows (máquina de desenvolvimento) |

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│  LAPTOP (orquestrador)                                          │
│  cluster.ps1 · compositor.py · render_movie.py · watchdog.py   │
└────────────────────────────┬────────────────────────────────────┘
                             │ SSH / HTTP :8000
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
  ┌────────────┐      ┌────────────┐      ┌────────────┐
  │ worker-01  │      │ worker-02  │      │  ...06     │
  │ .1.20      │      │ .1.21      │      │            │
  │            │      │            │      │            │
  │ Redis ×3   │      │ Redis ×3   │      │ Redis ×3   │
  │ :6379/80/81│      │ :6379/80/81│      │ :6379/80/81│
  │            │      │            │      │            │
  │ Daemon     │◄────►│ Daemon     │◄────►│ Daemon     │
  │ (Raft 4321)│      │ (Raft 4321)│      │ (Raft 4321)│
  │            │      │            │      │            │
  │ API :8000  │      │ API :8000  │      │ API :8000  │
  │            │      │            │      │            │
  │ Worker ×4  │      │ Worker ×4  │      │ Worker ×4  │
  └────────────┘      └────────────┘      └────────────┘
```

### Stack por nó

| Componente | Tecnologia | Porta |
|---|---|---|
| Banco de dados | Redis 7 (3 instâncias, Docker) | 6379 / 6380 / 6381 |
| Eleição de líder | Raft via pysyncobj | 4321 |
| API REST | FastAPI + uvicorn | 8000 |
| Workers | 4 processos Python por nó | — |

**Total: 24 workers × 6 fetch threads = 144 threads consumindo tarefas em paralelo.**

### Fluxo de controle

```
Daemon líder
  └─► XADD round-robin → {s0}:tasks .. {s5}:tasks
                                            │
                          Workers (XREADGROUP por shard)
                                            │
                                     processa task
                                            │
                          XADD → {s0}:results .. {s5}:results
                                            │
                              Compositor (laptop) lê e monta frame
```

### Redis — layout de chaves

| Chave | Uso |
|---|---|
| `{s0}:tasks` .. `{s5}:tasks` | 6 streams de tarefas (um por master) |
| `{s0}:results` .. `{s5}:results` | Streams de resultados espelhados |
| `{ctrl}:mode` | `geometry` ou `render` |
| `{ctrl}:geometry` | Geometria ativa (torus, lorenz…) |
| `{ctrl}:cluster:leader` | IP do líder atual (TTL 5s) |
| `{ctrl}:render:job` | Parâmetros do frame em renderização |
| `{ctrl}:render:done:<frame_id>` | Contador de tiles concluídas |
| `{ctrl}:stats:proc:<node>` | Tarefas processadas por nó |

> **Regra crítica:** toda chave no plano de controle deve usar a tag `{ctrl}`. Chaves de tarefas/resultados usam `{s0}`..`{s5}`. Misturar tags em um pipeline causa erro `CROSSSLOT`.

---

## Estrutura de arquivos

```
cluster/
├── cluster.ps1                  # CLI principal (ponto de entrada no laptop)
├── watchdog.py                  # Dashboard terminal (polls /health cada 2s)
├── docker-compose.redis.yml     # Topologia Redis 6 masters × 2 réplicas
│
├── app/                         # Backend Python (roda nos Pis)
│   ├── config.py                # Detecção de IP, shards, chaves Redis
│   ├── daemon.py                # Raft + geração de tarefas (líder only)
│   ├── api.py                   # FastAPI: /health, /mode, /render/*, etc.
│   ├── worker.py                # Consumidor de tarefas (6 threads por processo)
│   ├── recovery.py              # XAUTOCLAIM — resgata tarefas travadas
│   ├── geometry.py              # Geradores: torus, lorenz, mandelbulb, klein
│   ├── requirements.txt
│   └── render/
│       ├── sdf.py               # Ray marching SDF vetorizado (NumPy)
│       ├── tile_generator.py    # Divide frame em tiles
│       └── tile_worker.py       # Renderiza e codifica um tile
│
├── laptop/                      # Ferramentas do orquestrador
│   ├── compositor.py            # Compositor de tiles ao vivo
│   ├── render_movie.py          # Driver multi-frame → MP4
│   └── requirements.txt
│
├── deploy/                      # Scripts de deploy e recuperação
│   ├── push.ps1                 # Distribui código + reinicia serviços
│   ├── setup-tmpfs.ps1          # Monta /data/redis-{a,b,c} como tmpfs
│   ├── recover-redis.ps1        # Reconstrói Redis Cluster do zero
│   ├── api.ps1                  # Chamadas REST diretas (debug)
│   ├── cluster-daemon.service   # systemd: daemon.py
│   ├── cluster-api.service      # systemd: uvicorn api:app
│   └── cluster-worker@.service  # systemd: worker.py (template @0..3)
│
└── output/                      # Frames renderizados (PNGs e MP4)
```

---

## Pré-requisitos

### Laptop (Windows)
- PowerShell 5.1+
- `ssh` no PATH (OpenSSH)
- Python 3.11+ com dependências de `laptop/requirements.txt`
- `ffmpeg` no PATH (para muxar MP4)

```powershell
pip install -r laptop/requirements.txt
```

### Raspberry Pi 5 (cada nó)
- Raspberry Pi OS (64-bit)
- Docker + Docker Compose
- Python 3.11 + venv em `/home/pi/cluster/app/venv/`
- `ffmpeg` instalado

---

## Configuração inicial (primeira vez)

### 1. Configurar SSH sem senha

No laptop, gere e copie a chave para todos os nós:

```powershell
ssh-keygen -t ed25519
20..25 | ForEach-Object { ssh-copy-id pi@192.168.1.$_ }
```

### 2. Montar tmpfs para o Redis em cada nó

```powershell
. .\cluster.ps1
# Executa setup-tmpfs em todos os nós
20..25 | ForEach-Object { ssh pi@192.168.1.$_ "sudo bash -s" < deploy/setup-tmpfs.ps1 }
```

Ou via script PowerShell:

```powershell
.\deploy\setup-tmpfs.ps1
```

### 3. Fazer o primeiro deploy do código

```powershell
. .\cluster.ps1
Cluster deploy
```

Isso executa `deploy/push.ps1`:
- Copia os arquivos Python via `scp`
- Copia os `.service` para `/etc/systemd/system/`
- Habilita e inicia os serviços

### 4. Subir o Redis Cluster

Necessário apenas na primeira vez ou após reset total:

```powershell
Cluster redis-fix
```

Isso executa `deploy/recover-redis.ps1`:
- Distribui `docker-compose.redis.yml` e `redis.conf`
- Sobe 3 containers Redis por nó (18 total)
- Cria o cluster com 6 masters e 2 réplicas por master
- Reinicia todos os serviços Python

### 5. Verificar o cluster

```powershell
Cluster status
```

Você deve ver 6 nós **Online** e um **Líder** marcado com `★`.

---

## Uso diário

### Carregar o módulo na sessão

```powershell
. .\cluster.ps1
```

Para carregar automaticamente, adicione ao `$PROFILE`:

```powershell
Add-Content $PROFILE '. "C:\Users\jean_\Documents\codigos\cluster\cluster.ps1"'
```

---

## Referência de comandos (`cluster.ps1`)

### Informação

```powershell
Cluster status                          # Saúde de todos os nós + líder + fila
Cluster watch                           # Status em loop (atualiza a cada 3s)
Cluster mode                            # Mostra o modo atual
Cluster mode geometry                   # Define modo geometry
Cluster mode render                     # Define modo render
Cluster geometry                        # Mostra geometria ativa
Cluster geometry torus                  # Troca geometria (torus|lorenz|mandelbulb|klein)
Cluster render-progress                 # Progresso do frame em renderização
```

### Deploy e serviços

```powershell
Cluster deploy                          # Distribui código e reinicia todos os nós
Cluster deploy -Nodes 20,21             # Deploy apenas nos nós especificados
Cluster start                           # systemctl start (todos os nós)
Cluster start -Nodes 20                 # systemctl start (nó específico)
Cluster stop                            # systemctl stop (todos os nós)
Cluster restart                         # systemctl restart (todos os nós)
Cluster restart -Nodes 22,23            # systemctl restart (nós específicos)
```

### Render

```powershell
# Renderiza 1 frame de teste (mandelbulb, 320×180)
Cluster render-frame

# Renderiza 1 frame com parâmetros específicos
Cluster render-frame -Scene mandelbulb -FrameId t1 -Width 640 -Height 360

# Renderiza vídeo completo (60 frames, 24fps)
Cluster render-movie -Scene menger -Frames 60 -Fps 24

# Cenas disponíveis: mandelbulb | menger | apollonian
```

### Hardware

```powershell
Cluster reboot 22                       # Reinicia o nó 22
Cluster reboot -Nodes 20,21,22 -Force   # Reinicia vários nós sem confirmação
Cluster shutdown                        # Desliga todos os nós (poweroff)
Cluster shutdown -Force                 # Desliga sem pedir confirmação
```

### Debug

```powershell
Cluster logs 20                                      # journalctl do daemon (nó 20)
Cluster logs 20 -Service cluster-api                 # Logs da API
Cluster logs 20 -Service cluster-worker@0 -Follow    # Logs do worker 0 em tempo real
Cluster ssh 20                                       # SSH interativo no nó 20
Cluster htop 20                                      # htop no nó 20
Cluster htop-all                                     # htop em grade no Windows Terminal
Cluster exec -Command "df -h /data"                  # Roda comando em todos os nós
Cluster exec -Nodes 20,21 -Command "uptime"          # Roda em nós específicos
```

### Redis

```powershell
Cluster redis-fix                       # Reconstrói Redis Cluster do zero
Cluster clear-streams                   # Apaga todos os streams de tasks e results
```

---

## Modo geometry

O modo geometry gera pontos 3D continuamente para um visualizador externo.

```powershell
Cluster mode geometry
Cluster geometry mandelbulb             # torus | lorenz | mandelbulb | klein
Cluster status                          # confirma que há fila crescendo
```

O líder gera batches de 2000 pontos por ciclo, respeitando um limite de 30.000 mensagens pendentes nas filas.

---

## Modo render

### Renderizar um único frame

```powershell
Cluster mode render
Cluster render-frame -Scene mandelbulb -Width 1280 -Height 720 -FrameId frame1
```

O frame resultante fica em `output/frames/`.

### Renderizar um vídeo completo

```powershell
python laptop/render_movie.py --scene menger --frames 60 --fps 24 --width 1280 --height 720
```

Ou via wrapper:

```powershell
Cluster render-movie -Scene menger -Frames 60 -Fps 24
```

O MP4 final fica em `output/render.mp4`.

### Como funciona o pipeline de render

1. O frame é dividido em tiles de 64×64 px pelo `tile_generator.py`
2. Cada tile vira uma tarefa `XADD` nas 6 filas
3. Os 24 workers executam ray marching SDF (`render/sdf.py`) e devolvem o tile codificado
4. O `compositor.py` no laptop lê os resultados em tempo real e monta o PNG
5. Quando todos os tiles chegam, o frame é salvo; `render_movie.py` avança para o próximo

---

## Monitoramento

### Dashboard terminal

```powershell
python watchdog.py
```

Mostra a cada 2s: status de cada nó, líder, temperatura, Redis, profundidade da fila.

### Status rápido

```powershell
Cluster watch          # loop automático com Cluster status a cada 3s
```

### Logs em tempo real

```powershell
Cluster logs 20 -Service cluster-worker@0 -Follow
Cluster logs 20 -Service cluster-daemon -Follow
```

---

## Troubleshooting

### Todos os nós OFFLINE após `Cluster status`

1. Verifique se os Pis estão na rede:
   ```powershell
   20..25 | ForEach-Object { "$_ : $(if(Test-Connection 192.168.1.$_ -Count 1 -Quiet){'OK'}else{'FAIL'})" }
   ```
2. Se ping OK → serviços caíram → `Cluster start`
3. Se ping FAIL → Pis desligados ou sem IP

### Nenhum líder eleito

O Raft precisa de quórum (≥4 de 6 nós online). Se poucos nós estão rodando:

```powershell
Cluster restart
```

Aguarde ~6s e rode `Cluster status` novamente.

### Erro CROSSSLOT no Redis

Uma chave foi criada sem hash tag. Toda chave do plano de controle precisa de `{ctrl}`, e shards precisam de `{s0}`..`{s5}`. Veja `app/config.py` para os helpers.

### Redis Cluster degradado ou quebrado

```powershell
Cluster redis-fix
```

Isso reconstrói o cluster inteiro. **Todos os dados nas filas serão perdidos.**

### Tile não aparece no compositor

O compositor lê resultados a partir do momento em que é iniciado (`$` = agora). Re-submeta o frame com um `frame_id` novo:

```powershell
Cluster render-frame -Scene mandelbulb -FrameId novo_id
```

### `ffmpeg` não encontrado

Instale o ffmpeg e adicione ao PATH do Windows. Necessário apenas para muxar o MP4 final em `render_movie.py`.

### Serviços caem em loop após deploy

Verifique importações no venv do Pi:

```powershell
Cluster exec -Command "cd /home/pi/cluster/app && venv/bin/python deploy/test_imports.py"
```

---

## Topologia Redis (referência)

6 masters, 2 réplicas por master, distribuídas em anel para tolerância a 2 falhas simultâneas:

| Nó | redis-a (master) | redis-b (réplica de) | redis-c (réplica de) |
|---|---|---|---|
| worker-01 (.20) | master s0 | master de .25 | master de .24 |
| worker-02 (.21) | master s1 | master de .20 | master de .25 |
| worker-03 (.22) | master s2 | master de .21 | master de .20 |
| worker-04 (.23) | master s3 | master de .22 | master de .21 |
| worker-05 (.24) | master s4 | master de .23 | master de .22 |
| worker-06 (.25) | master s5 | master de .24 | master de .23 |

Qualquer 2 nós podem cair sem perda de dados ou disponibilidade.

---

## Dependências

### app/requirements.txt (workers nos Pis)

```
redis==5.0.8
pysyncobj==0.3.12
fastapi==0.115.0
uvicorn[standard]==0.30.6
numpy==1.26.4
Pillow>=10.0
```

### laptop/requirements.txt (orquestrador)

```
pygame>=2.5
Pillow>=10.0
redis>=5.0
requests>=2.31
numpy>=1.26
```
