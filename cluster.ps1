# ═══════════════════════════════════════════════════════════════════════════
#  ClusterPi — Módulo de Gerenciamento
# ═══════════════════════════════════════════════════════════════════════════
#
# Carrega na sessão atual:
#   . .\cluster.ps1
#
# Ou carrega permanentemente adicionando ao $PROFILE:
#   Add-Content $PROFILE '. "C:\Users\jean_\Documents\codigos\cluster\cluster.ps1"'
#
# Comando principal:
#   Cluster <ação> [args]
#
# Exemplos:
#   Cluster status
#   Cluster deploy
#   Cluster deploy -Nodes 20,21
#   Cluster mode render
#   Cluster render-frame -Scene mandelbulb
#   Cluster render-movie -Scene menger -Frames 60
#   Cluster shutdown
#   Cluster reboot 22
#   Cluster logs 20
#   Cluster htop 20
#   Cluster ssh 20
#   Cluster help
#
# ═══════════════════════════════════════════════════════════════════════════

$ErrorActionPreference = "Continue"

# ─────────────────────────────────────────────────
#  CONFIGURAÇÃO GLOBAL
# ─────────────────────────────────────────────────

$global:CLUSTER_NODES   = 20..25
$global:CLUSTER_SUBNET  = "192.168.1"
$global:CLUSTER_USER    = "pi"
$global:CLUSTER_API_PORT = 8000
$global:CLUSTER_ROOT    = Split-Path -Parent $MyInvocation.MyCommand.Path

# Primeiro nó acessível, cacheia por sessão
$global:CLUSTER_ENTRY_IP = $null

# ─────────────────────────────────────────────────
#  HELPERS INTERNOS
# ─────────────────────────────────────────────────

function _Node-IP {
    param([int]$N)
    "$CLUSTER_SUBNET.$N"
}

function _Test-SSH {
    param([string]$Ip, [int]$Timeout = 3)
    $result = & ssh -o ConnectTimeout=$Timeout -o BatchMode=yes -o StrictHostKeyChecking=no "$CLUSTER_USER@$Ip" "echo ok" 2>$null
    return ($result -eq "ok")
}

function _Find-Entry {
    # Retorna o IP de um nó acessível (cacheia)
    if ($global:CLUSTER_ENTRY_IP) {
        if (_Test-SSH -Ip $global:CLUSTER_ENTRY_IP -Timeout 2) {
            return $global:CLUSTER_ENTRY_IP
        }
        $global:CLUSTER_ENTRY_IP = $null
    }
    foreach ($n in $CLUSTER_NODES) {
        $ip = _Node-IP $n
        if (_Test-SSH -Ip $ip) {
            $global:CLUSTER_ENTRY_IP = $ip
            return $ip
        }
    }
    return $null
}

function _Api-Get {
    param([string]$Path, [int]$Timeout = 5)
    $ip = _Find-Entry
    if (-not $ip) { throw "Nenhum nó acessível" }
    Invoke-RestMethod -Uri "http://${ip}:$CLUSTER_API_PORT$Path" -TimeoutSec $Timeout
}

function _Api-Post {
    param([string]$Path, $Body, [int]$Timeout = 10)
    $ip = _Find-Entry
    if (-not $ip) { throw "Nenhum nó acessível" }
    $json = $Body | ConvertTo-Json -Depth 6
    Invoke-RestMethod -Uri "http://${ip}:$CLUSTER_API_PORT$Path" -Method POST -Body $json -ContentType "application/json" -TimeoutSec $Timeout
}

function _Each-Node-Parallel {
    # Roda um scriptblock SSH em paralelo em todos os nós acessíveis
    param(
        [int[]]$Nodes,
        [string]$SshCommand,
        [int]$Timeout = 30
    )
    $jobs = @()
    foreach ($n in $Nodes) {
        $ip = _Node-IP $n
        $jobs += Start-Job -ScriptBlock {
            param($addr, $user, $cmd, $to)
            $result = & ssh -o ConnectTimeout=$to -o BatchMode=yes "$user@$addr" $cmd 2>&1
            [PSCustomObject]@{
                Ip     = $addr
                Output = ($result -join "`n")
                Exit   = $LASTEXITCODE
            }
        } -ArgumentList $ip, $CLUSTER_USER, $SshCommand, $Timeout
    }
    Wait-Job $jobs -Timeout ($Timeout + 5) | Out-Null
    $results = Receive-Job $jobs
    Remove-Job $jobs -Force
    return $results
}

function _Color-Status {
    param([string]$Text, [string]$Good = "ok|active|running|healthy")
    if ($Text -match $Good) { return "Green" }
    if ($Text -match "down|fail|error|dead|inactive") { return "Red" }
    return "Yellow"
}

# ─────────────────────────────────────────────────
#  AÇÕES
# ─────────────────────────────────────────────────

function _Cluster-Status {
    Write-Host ""
    Write-Host "═══ Cluster Status ═══" -ForegroundColor Cyan

    $online  = @()
    $offline = @()
    $leader  = $null
    $rows    = @()

    foreach ($n in $CLUSTER_NODES) {
        $ip = _Node-IP $n
        try {
            $h = Invoke-RestMethod -Uri "http://${ip}:$CLUSTER_API_PORT/health" -TimeoutSec 2
            $online += $ip
            if ($h.is_leader) { $leader = $ip }
            $rows += [PSCustomObject]@{
                Node     = $h.node
                IP       = $ip
                Leader   = if ($h.is_leader) { "★" } else { " " }
                Redis    = $h.redis
                Uptime   = "$($h.uptime_s)s"
                Temp     = if ($h.temp) { "$($h.temp)°C" } else { "-" }
                Geometry = $h.geometry
                Status   = $h.status
            }
        } catch {
            $offline += $ip
            $rows += [PSCustomObject]@{
                Node     = "worker-??"
                IP       = $ip
                Leader   = " "
                Redis    = "-"
                Uptime   = "-"
                Temp     = "-"
                Geometry = "-"
                Status   = "OFFLINE"
            }
        }
    }

    $rows | Format-Table -AutoSize

    # Resumo
    Write-Host "Online: $($online.Count)/$($CLUSTER_NODES.Count)   " -NoNewline -ForegroundColor $(if ($online.Count -eq $CLUSTER_NODES.Count) { "Green" } else { "Yellow" })
    if ($leader) {
        Write-Host "Líder: $leader   " -NoNewline -ForegroundColor Green
    } else {
        Write-Host "Líder: NENHUM   " -NoNewline -ForegroundColor Red
    }

    # Modo
    try {
        $mode = (_Api-Get "/mode").mode
        $color = if ($mode -eq "render") { "Magenta" } else { "Cyan" }
        Write-Host "Modo: $mode" -ForegroundColor $color
    } catch {
        Write-Host "Modo: ?" -ForegroundColor Yellow
    }

    # Fila
    try {
        $q = _Api-Get "/queue/status"
        Write-Host ""
        Write-Host "Fila: $($q.stream_length) entradas | pendentes: $($q.pending) | processadas: $($q.processed_total)" -ForegroundColor White
    } catch {}

    Write-Host ""
}

function _Cluster-Deploy {
    param([int[]]$Nodes)
    if (-not $Nodes) { $Nodes = $CLUSTER_NODES }

    $script = Join-Path $CLUSTER_ROOT "deploy\push.ps1"
    if (-not (Test-Path $script)) {
        Write-Host "Não achei $script" -ForegroundColor Red
        return
    }

    Push-Location $CLUSTER_ROOT
    try {
        & $script -Nodes $Nodes
    } finally {
        Pop-Location
    }
}

function _Cluster-Restart {
    param([int[]]$Nodes)
    if (-not $Nodes) { $Nodes = $CLUSTER_NODES }

    Write-Host "Reiniciando serviços cluster-* em $($Nodes.Count) nós..." -ForegroundColor Cyan
    $cmd = "sudo systemctl restart cluster-daemon cluster-worker@0 cluster-worker@1 cluster-worker@2 cluster-worker@3 cluster-api"
    $results = _Each-Node-Parallel -Nodes $Nodes -SshCommand $cmd -Timeout 15

    foreach ($r in $results) {
        if ($r.Exit -eq 0) {
            Write-Host "  OK   $($r.Ip)" -ForegroundColor Green
        } else {
            Write-Host "  FAIL $($r.Ip): $($r.Output)" -ForegroundColor Red
        }
    }
    Write-Host ""
    Write-Host "Aguardando eleição Raft (6s)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 6
    _Cluster-Status
}

function _Cluster-Start {
    param([int[]]$Nodes)
    if (-not $Nodes) { $Nodes = $CLUSTER_NODES }

    Write-Host "Iniciando serviços em $($Nodes.Count) nós..." -ForegroundColor Cyan
    $cmd = "sudo systemctl start cluster-daemon cluster-worker@0 cluster-worker@1 cluster-worker@2 cluster-worker@3 cluster-api"
    _Each-Node-Parallel -Nodes $Nodes -SshCommand $cmd -Timeout 10 | ForEach-Object {
        Write-Host "  $($_.Ip): $(if ($_.Exit -eq 0) { 'OK' } else { 'FAIL' })" -ForegroundColor $(if ($_.Exit -eq 0) { 'Green' } else { 'Red' })
    }
}

function _Cluster-Stop {
    param([int[]]$Nodes)
    if (-not $Nodes) { $Nodes = $CLUSTER_NODES }

    Write-Host "Parando serviços em $($Nodes.Count) nós..." -ForegroundColor Yellow
    $cmd = "sudo systemctl stop cluster-api cluster-worker@0 cluster-worker@1 cluster-worker@2 cluster-worker@3 cluster-daemon"
    _Each-Node-Parallel -Nodes $Nodes -SshCommand $cmd -Timeout 10 | ForEach-Object {
        Write-Host "  $($_.Ip): $(if ($_.Exit -eq 0) { 'OK' } else { 'FAIL' })" -ForegroundColor $(if ($_.Exit -eq 0) { 'Green' } else { 'Red' })
    }
}

function _Cluster-Shutdown {
    param(
        [int[]]$Nodes,
        [switch]$Force
    )
    if (-not $Nodes) { $Nodes = $CLUSTER_NODES }

    if (-not $Force) {
        Write-Host ""
        Write-Host "ATENCAO: vai desligar $($Nodes.Count) Raspberry Pi." -ForegroundColor Yellow
        $confirm = Read-Host "Confirma? (sim/NAO)"
        if ($confirm -ne "sim") {
            Write-Host "Abortado." -ForegroundColor Yellow
            return
        }
    }

    # Desligar em ordem reversa pra deixar o .20 (líder comum) por último
    $sorted = $Nodes | Sort-Object -Descending

    foreach ($n in $sorted) {
        $ip = _Node-IP $n
        Write-Host "  → Desligando $ip..." -ForegroundColor Red
        # Para os serviços primeiro, depois poweroff
        ssh -o ConnectTimeout=5 "$CLUSTER_USER@$ip" "sudo systemctl stop cluster-api cluster-worker@0 cluster-worker@1 cluster-worker@2 cluster-worker@3 cluster-daemon 2>/dev/null; sudo poweroff" 2>$null
        Start-Sleep -Seconds 1
    }
    Write-Host ""
    Write-Host "Comandos enviados. Os nós vão desligar em ~10 segundos." -ForegroundColor Yellow
}

function _Cluster-Reboot {
    param(
        [int[]]$Nodes,
        [switch]$Force
    )
    if (-not $Nodes) { $Nodes = $CLUSTER_NODES }

    if (-not $Force) {
        Write-Host "Vai reiniciar $($Nodes.Count) nó(s): $($Nodes -join ', ')" -ForegroundColor Yellow
        $confirm = Read-Host "Confirma? (sim/NAO)"
        if ($confirm -ne "sim") {
            Write-Host "Abortado." -ForegroundColor Yellow
            return
        }
    }

    foreach ($n in $Nodes) {
        $ip = _Node-IP $n
        Write-Host "  → Reboot $ip..." -ForegroundColor Yellow
        ssh -o ConnectTimeout=5 "$CLUSTER_USER@$ip" "sudo reboot" 2>$null
    }
    Write-Host ""
    Write-Host "Aguarde ~60s e rode 'Cluster status' pra verificar." -ForegroundColor Cyan
}

function _Cluster-Mode {
    param([string]$NewMode)

    if (-not $NewMode) {
        # Apenas mostra o modo atual
        try {
            $m = _Api-Get "/mode"
            Write-Host "Modo atual: $($m.mode)" -ForegroundColor Cyan
        } catch {
            Write-Host "Erro ao consultar modo: $_" -ForegroundColor Red
        }
        return
    }

    if ($NewMode -notin @("geometry","render")) {
        Write-Host "Modo inválido. Use 'geometry' ou 'render'." -ForegroundColor Red
        return
    }

    try {
        $r = _Api-Post "/mode" @{ mode = $NewMode }
        Write-Host "Modo alterado para: $($r.mode)" -ForegroundColor Green
    } catch {
        Write-Host "Erro: $_" -ForegroundColor Red
    }
}

function _Cluster-Geometry {
    param([string]$Geom)

    if (-not $Geom) {
        try {
            $g = _Api-Get "/geometry"
            Write-Host "Geometria atual: $($g.geometry)" -ForegroundColor Cyan
            Write-Host "Válidas: $($g.valid -join ', ')" -ForegroundColor Gray
        } catch {
            Write-Host "Erro: $_" -ForegroundColor Red
        }
        return
    }

    try {
        $r = _Api-Post "/geometry" @{ geometry = $Geom }
        if ($r.status -eq "ok") {
            Write-Host "Geometria alterada para: $($r.geometry)" -ForegroundColor Green
        } else {
            Write-Host "Erro: $($r.error)" -ForegroundColor Red
        }
    } catch {
        Write-Host "Erro: $_" -ForegroundColor Red
    }
}

function _Cluster-Render-Frame {
    param(
        [string]$FrameId = "test",
        [ValidateSet("mandelbulb","menger","apollonian")]
        [string]$Scene = "mandelbulb",
        [int]$Width = 640,
        [int]$Height = 360,
        [int]$TileSize = 64,
        [double]$Time = 0.0,
        [double[]]$CamPos = @(0, 1, 4),
        [double[]]$CamTarget = @(0, 0, 0),
        [double]$Fov = 60.0,
        [switch]$NoCompositor
    )

    # Garantir modo render
    Write-Host "Mudando para modo render..." -ForegroundColor Cyan
    _Api-Post "/mode" @{ mode = "render" } | Out-Null

    # Submeter job
    $job = @{
        frame_id  = $FrameId
        scene     = $Scene
        camera    = @{ pos = $CamPos; target = $CamTarget; fov = $Fov }
        full_w    = $Width
        full_h    = $Height
        tile_size = $TileSize
        time      = $Time
    }
    Write-Host "Submetendo job $FrameId ($Scene, ${Width}x${Height})..." -ForegroundColor Cyan
    $r = _Api-Post "/render/job" $job
    Write-Host "  $($r.tiles_total) tiles enfileirados" -ForegroundColor Green

    if ($NoCompositor) {
        Write-Host ""
        Write-Host "Job enviado. Monitore com: Cluster render-progress" -ForegroundColor Yellow
        return
    }

    # Spawnar compositor
    $ip = _Find-Entry
    $output = "frame_$FrameId.png"
    $compositor = Join-Path $CLUSTER_ROOT "laptop\compositor.py"
    if (-not (Test-Path $compositor)) {
        Write-Host "Compositor não encontrado em $compositor" -ForegroundColor Red
        return
    }

    Write-Host ""
    Write-Host "Abrindo compositor..." -ForegroundColor Cyan
    python $compositor --host $ip --width $Width --height $Height --frame-id $FrameId --output $output
    if (Test-Path $output) {
        Write-Host ""
        Write-Host "Frame salvo: $output" -ForegroundColor Green
        $abrir = Read-Host "Abrir imagem? (S/n)"
        if ($abrir -ne "n") { Start-Process $output }
    }
}

function _Cluster-Render-Movie {
    param(
        [ValidateSet("mandelbulb","menger","apollonian")]
        [string]$Scene = "mandelbulb",
        [int]$Frames = 120,
        [int]$Fps = 24,
        [int]$Width = 640,
        [int]$Height = 360,
        [int]$TileSize = 64,
        [double]$OrbitRadius = 4.0,
        [double]$OrbitHeight = 1.0,
        [double]$Fov = 60.0,
        [switch]$NoDisplay
    )

    $script = Join-Path $CLUSTER_ROOT "laptop\render_movie.py"
    if (-not (Test-Path $script)) {
        Write-Host "render_movie.py não encontrado em $script" -ForegroundColor Red
        return
    }

    $args = @(
        "--scene", $Scene,
        "--frames", $Frames,
        "--fps", $Fps,
        "--width", $Width,
        "--height", $Height,
        "--tile-size", $TileSize,
        "--orbit-radius", $OrbitRadius,
        "--orbit-height", $OrbitHeight,
        "--fov", $Fov
    )
    if ($NoDisplay) { $args += "--no-display" }

    Push-Location $CLUSTER_ROOT
    try {
        python $script @args
    } finally {
        Pop-Location
    }
}

function _Cluster-Render-Progress {
    try {
        $p = _Api-Get "/render/progress"
        if (-not $p.frame_id) {
            Write-Host "Nenhum frame ativo." -ForegroundColor Yellow
            return
        }
        $pct = if ($p.tiles_total -gt 0) { [int](100 * $p.tiles_done / $p.tiles_total) } else { 0 }
        Write-Host "Frame $($p.frame_id): $($p.tiles_done)/$($p.tiles_total) ($pct%)" -ForegroundColor Cyan
    } catch {
        Write-Host "Erro: $_" -ForegroundColor Red
    }
}

function _Cluster-Logs {
    param(
        [Parameter(Mandatory)][int]$Node,
        [string]$Service = "cluster-daemon",
        [int]$Lines = 50,
        [switch]$Follow
    )

    $ip = _Node-IP $Node
    $cmd = "sudo journalctl -u $Service -n $Lines --no-pager"
    if ($Follow) { $cmd = "sudo journalctl -u $Service -f" }

    Write-Host "Logs de $Service em $ip" -ForegroundColor Cyan
    Write-Host "─────────────────────────────────────────" -ForegroundColor DarkGray
    ssh "$CLUSTER_USER@$ip" $cmd
}

function _Cluster-Htop {
    param([Parameter(Mandatory)][int]$Node)
    $ip = _Node-IP $Node
    Write-Host "Conectando htop em $ip (q para sair)..." -ForegroundColor Cyan
    ssh -t "$CLUSTER_USER@$ip" "htop"
}

function _Cluster-Htop-All {
    # Abre 6 novas janelas do Windows Terminal com htop em cada nó
    $wtAvailable = $null -ne (Get-Command wt.exe -ErrorAction SilentlyContinue)
    if (-not $wtAvailable) {
        Write-Host "Windows Terminal (wt.exe) não encontrado." -ForegroundColor Red
        Write-Host "Instale da Microsoft Store ou abra htop manualmente:" -ForegroundColor Yellow
        foreach ($n in $CLUSTER_NODES) {
            Write-Host "  Cluster htop $n" -ForegroundColor Gray
        }
        return
    }

    # Monta um layout 2x3 com splits
    $nodes = @($CLUSTER_NODES)
    $firstIp = _Node-IP $nodes[0]
    $cmd = "wt new-tab --title 'Cluster htop' ssh -t $CLUSTER_USER@$firstIp htop"
    for ($i = 1; $i -lt $nodes.Count; $i++) {
        $ip = _Node-IP $nodes[$i]
        # Alterna vertical/horizontal pra fazer grade
        $split = if ($i -eq 3) { "-H" } else { "-V" }
        $cmd += " ``; split-pane $split ssh -t $CLUSTER_USER@$ip htop"
    }
    Write-Host "Abrindo htop em $($nodes.Count) painéis..." -ForegroundColor Cyan
    Invoke-Expression $cmd
}

function _Cluster-SSH {
    param([Parameter(Mandatory)][int]$Node)
    $ip = _Node-IP $Node
    ssh "$CLUSTER_USER@$ip"
}

function _Cluster-Exec {
    param(
        [Parameter(Mandatory)][string]$Command,
        [int[]]$Nodes
    )
    if (-not $Nodes) { $Nodes = $CLUSTER_NODES }

    Write-Host "Executando em $($Nodes.Count) nó(s): $Command" -ForegroundColor Cyan
    Write-Host "─────────────────────────────────────────" -ForegroundColor DarkGray
    $results = _Each-Node-Parallel -Nodes $Nodes -SshCommand $Command -Timeout 20

    foreach ($r in $results | Sort-Object Ip) {
        Write-Host ""
        Write-Host "[$($r.Ip)]" -ForegroundColor Yellow
        Write-Host $r.Output
    }
}

function _Cluster-Redis-Fix {
    Write-Host "Tentando reparar Redis Cluster..." -ForegroundColor Cyan
    $script = Join-Path $CLUSTER_ROOT "deploy\recover-redis.ps1"
    if (Test-Path $script) {
        & $script
    } else {
        Write-Host "Não achei recover-redis.ps1" -ForegroundColor Red
    }
}

function _Cluster-Clear-Streams {
    Write-Host "Limpando streams Redis (tasks e results)..." -ForegroundColor Yellow
    $ip = _Find-Entry
    if (-not $ip) {
        Write-Host "Sem nó acessível." -ForegroundColor Red
        return
    }
    ssh "$CLUSTER_USER@$ip" "docker exec cluster-redis redis-cli --cluster call $($ip):6379 del '{stream}:tasks' '{stream}:results' 2>/dev/null || docker exec cluster-redis redis-cli del '{stream}:tasks' '{stream}:results'"
    Write-Host "Streams limpas." -ForegroundColor Green
}

function _Cluster-Watch {
    # Loop de status que atualiza
    Write-Host "Monitorando cluster (Ctrl+C para sair)..." -ForegroundColor Cyan
    try {
        while ($true) {
            Clear-Host
            _Cluster-Status
            Write-Host "Atualiza em 3s... (Ctrl+C para sair)" -ForegroundColor DarkGray
            Start-Sleep -Seconds 3
        }
    } catch {
        Write-Host "Encerrado." -ForegroundColor Yellow
    }
}

function _Cluster-Help {
    Write-Host ""
    Write-Host "═══ Cluster — Comandos ═══" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  INFORMAÇÃO" -ForegroundColor Yellow
    Write-Host "    Cluster status                          Estado de todos os nós"
    Write-Host "    Cluster watch                           Status em loop (atualiza a cada 3s)"
    Write-Host "    Cluster mode [geometry|render]          Ler/definir modo de operação"
    Write-Host "    Cluster geometry [torus|klein|...]      Ler/definir geometria ativa"
    Write-Host ""
    Write-Host "  DEPLOY E SERVIÇOS" -ForegroundColor Yellow
    Write-Host "    Cluster deploy [-Nodes 20,21]           Distribui código e reinicia serviços"
    Write-Host "    Cluster start   [-Nodes ...]            Inicia serviços (systemctl start)"
    Write-Host "    Cluster stop    [-Nodes ...]            Para serviços (systemctl stop)"
    Write-Host "    Cluster restart [-Nodes ...]            Reinicia serviços"
    Write-Host ""
    Write-Host "  HARDWARE" -ForegroundColor Yellow
    Write-Host "    Cluster shutdown [-Force]               Desliga todos os nós (poweroff)"
    Write-Host '    Cluster reboot <N> [-Force]             Reinicia o nó N (ex: 22)'
    Write-Host "    Cluster reboot -Nodes 20,21,22 -Force   Reinicia vários"
    Write-Host ""
    Write-Host "  RENDER FARM" -ForegroundColor Yellow
    Write-Host "    Cluster render-frame                    Renderiza 1 frame de teste (mandelbulb)"
    Write-Host "    Cluster render-frame -Scene menger -FrameId t1"
    Write-Host "    Cluster render-movie -Frames 60         Renderiza vídeo completo"
    Write-Host "    Cluster render-progress                 Progresso do frame atual"
    Write-Host ""
    Write-Host "  DEBUG" -ForegroundColor Yellow
    Write-Host '    Cluster logs <N> [-Service X] [-Follow] Mostra logs do journalctl'
    Write-Host '    Cluster ssh <N>                         SSH interativo no nó'
    Write-Host '    Cluster htop <N>                        Abre htop no nó'
    Write-Host "    Cluster htop-all                        Abre htop em grade no Windows Terminal"
    Write-Host "    Cluster exec -Command 'uptime'          Roda comando em todos os nós"
    Write-Host ""
    Write-Host "  MANUTENÇÃO" -ForegroundColor Yellow
    Write-Host "    Cluster redis-fix                       Tenta reparar Redis Cluster"
    Write-Host "    Cluster clear-streams                   Limpa {stream}:tasks e {stream}:results"
    Write-Host ""
    Write-Host "  EXEMPLOS PRÁTICOS" -ForegroundColor Green
    Write-Host '    Cluster deploy -Nodes 20,21'
    Write-Host '    Cluster mode render'
    Write-Host '    Cluster render-frame -Scene mandelbulb -Width 320 -Height 180'
    Write-Host '    Cluster logs 20 -Service cluster-worker@0 -Follow'
    Write-Host '    Cluster exec -Command "df -h /data/redis"'
    Write-Host '    Cluster reboot 22 -Force'
    Write-Host ""
}

# ─────────────────────────────────────────────────
#  COMANDO PRINCIPAL
# ─────────────────────────────────────────────────

function Cluster {
    [CmdletBinding()]
    param(
        [Parameter(Position = 0)]
        [string]$Action = "help",

        [Parameter(Position = 1, ValueFromRemainingArguments = $true)]
        $Rest
    )

    switch ($Action.ToLower()) {
        "status"          { _Cluster-Status }
        "watch"           { _Cluster-Watch }
        "deploy"          { _Cluster-Deploy @Rest }
        "start"           { _Cluster-Start @Rest }
        "stop"            { _Cluster-Stop @Rest }
        "restart"         { _Cluster-Restart @Rest }
        "shutdown"        { _Cluster-Shutdown @Rest }
        "reboot"          {
            # Suporte simples: primeiro arg pode ser um int (nó único)
            if ($Rest -and $Rest[0] -match '^\d+$') {
                $n = [int]$Rest[0]
                $rest2 = $Rest[1..($Rest.Count-1)]
                _Cluster-Reboot -Nodes @($n) @rest2
            } else {
                _Cluster-Reboot @Rest
            }
        }
        "mode"            {
            $m = if ($Rest) { $Rest[0] } else { $null }
            _Cluster-Mode -NewMode $m
        }
        "geometry"        {
            $g = if ($Rest) { $Rest[0] } else { $null }
            _Cluster-Geometry -Geom $g
        }
        "render-frame"    { _Cluster-Render-Frame @Rest }
        "render-movie"    { _Cluster-Render-Movie @Rest }
        "render-progress" { _Cluster-Render-Progress }
        "logs"            {
            if (-not $Rest) { Write-Host 'Uso: Cluster logs <N>' -ForegroundColor Red; return }
            $n = [int]$Rest[0]
            $rest2 = if ($Rest.Count -gt 1) { $Rest[1..($Rest.Count-1)] } else { @() }
            _Cluster-Logs -Node $n @rest2
        }
        "ssh"             {
            if (-not $Rest) { Write-Host 'Uso: Cluster ssh <N>' -ForegroundColor Red; return }
            _Cluster-SSH -Node ([int]$Rest[0])
        }
        "htop"            {
            if (-not $Rest) { Write-Host 'Uso: Cluster htop <N>' -ForegroundColor Red; return }
            _Cluster-Htop -Node ([int]$Rest[0])
        }
        "htop-all"        { _Cluster-Htop-All }
        "exec"            { _Cluster-Exec @Rest }
        "redis-fix"       { _Cluster-Redis-Fix }
        "clear-streams"   { _Cluster-Clear-Streams }
        "help"            { _Cluster-Help }
        default {
            Write-Host "Ação desconhecida: $Action" -ForegroundColor Red
            _Cluster-Help
        }
    }
}

# Autocomplete para o parâmetro Action
Register-ArgumentCompleter -CommandName Cluster -ParameterName Action -ScriptBlock {
    param($cmdName, $paramName, $wordToComplete)
    @(
        "status", "watch", "deploy", "start", "stop", "restart",
        "shutdown", "reboot", "mode", "geometry",
        "render-frame", "render-movie", "render-progress",
        "logs", "ssh", "htop", "htop-all", "exec",
        "redis-fix", "clear-streams", "help"
    ) | Where-Object { $_ -like "$wordToComplete*" } | ForEach-Object {
        [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_)
    }
}

Write-Host "cluster.ps1 carregado. Digite 'Cluster help' para ver os comandos." -ForegroundColor Cyan