# deploy/recover-redis.ps1 — Restaura Redis e reinicia o cluster inteiro
# Uso: .\deploy\recover-redis.ps1
#
# O que faz:
#   1. Copia docker-compose.redis.yml e redis.conf para o nó líder (.20)
#   2. Cria o diretório /data/redis se não existir
#   3. Sobe o container Redis via Docker Compose
#   4. Aguarda o healthcheck do Redis
#   5. Limpa o rate-limit do systemd em TODOS os nós
#   6. Reinicia os 3 serviços (daemon, worker, api) em todos os nós
#   7. Verifica o status final

$ErrorActionPreference = "Continue"
$leader = "192.168.1.20"
$nodes  = 20..25

Write-Host "`n╔══════════════════════════════════════════╗" -ForegroundColor Red
Write-Host "║  Cluster RPi — RECOVERY MODE             ║" -ForegroundColor Red
Write-Host "╚══════════════════════════════════════════╝`n" -ForegroundColor Red

# ── Step 1: Verificar conectividade SSH ──────────────────────────
Write-Host "🔍 Verificando conectividade SSH..." -ForegroundColor Cyan
$reachable = 0
foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    $result = ssh -o ConnectTimeout=3 pi@$ip "echo ok" 2>&1
    if ($result -eq "ok") {
        Write-Host "  ✓ $ip" -ForegroundColor Green
        $reachable++
    } else {
        Write-Host "  ✗ $ip — UNREACHABLE" -ForegroundColor Red
    }
}
if ($reachable -eq 0) {
    Write-Host "`n❌ Nenhum nó acessível. Verifique a rede." -ForegroundColor Red
    exit 1
}
Write-Host "  $reachable/$($nodes.Count) nós acessíveis`n" -ForegroundColor Yellow

# ── Step 2: Restaurar arquivos do Redis no líder ─────────────────
Write-Host "📦 Restaurando infraestrutura Redis em $leader..." -ForegroundColor Cyan

ssh pi@$leader "mkdir -p ~/cluster/redis"
scp .\docker-compose.redis.yml pi@${leader}:~/cluster/
scp .\redis\redis.conf         pi@${leader}:~/cluster/redis/

# Criar diretório de dados
ssh pi@$leader "sudo mkdir -p /data/redis && sudo chown pi:pi /data/redis"
Write-Host "  ✓ Arquivos copiados" -ForegroundColor Green

# ── Step 3: Parar container antigo (se existir) e subir novo ────
Write-Host "`n🚀 Iniciando container Redis..." -ForegroundColor Cyan

ssh pi@$leader @"
cd ~/cluster
docker compose -f docker-compose.redis.yml down 2>/dev/null
docker compose -f docker-compose.redis.yml up -d
"@

# ── Step 4: Aguardar healthcheck ─────────────────────────────────
Write-Host "`n⏳ Aguardando Redis responder..." -ForegroundColor Yellow
$maxWait = 30
$elapsed = 0
$redisOk = $false

while ($elapsed -lt $maxWait) {
    Start-Sleep -Seconds 2
    $elapsed += 2
    $pong = ssh pi@$leader "redis-cli ping 2>/dev/null"
    if ($pong -match "PONG") {
        $redisOk = $true
        break
    }
    Write-Host "  ... tentando ($elapsed/$maxWait s)" -ForegroundColor DarkYellow
}

if (-not $redisOk) {
    Write-Host "`n❌ Redis não respondeu em ${maxWait}s. Verifique manualmente:" -ForegroundColor Red
    Write-Host "   ssh pi@$leader 'docker logs cluster-redis-1'" -ForegroundColor Yellow
    exit 1
}

Write-Host "  ✓ Redis PONG — online!" -ForegroundColor Green

# Mostrar info do cluster Redis
$clusterInfo = ssh pi@$leader "redis-cli cluster info 2>/dev/null | head -3"
Write-Host "  $clusterInfo" -ForegroundColor DarkGray

# ── Step 5: Reset systemd + restart services em todos os nós ─────
Write-Host "`n🔄 Reiniciando serviços em todos os nós..." -ForegroundColor Cyan

foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    Write-Host "  → $ip" -ForegroundColor Yellow
    ssh pi@$ip @"
sudo systemctl reset-failed cluster-daemon cluster-worker cluster-api 2>/dev/null
sudo systemctl restart cluster-daemon cluster-worker cluster-api
"@
}

# ── Step 6: Aguardar eleição Raft ────────────────────────────────
Write-Host "`n⏳ Aguardando eleição Raft (12s)..." -ForegroundColor Yellow
Start-Sleep -Seconds 12

# ── Step 7: Verificar status final ───────────────────────────────
Write-Host "`n✅ Status final:" -ForegroundColor Green
Write-Host ("  " + "-" * 50)
$allGood = $true

foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    $status = ssh pi@$ip "systemctl is-active cluster-daemon cluster-worker cluster-api 2>/dev/null | tr '\n' ' '"
    
    if ($status -match "active active active") {
        Write-Host "  ✓ $ip : $status" -ForegroundColor Green
    } else {
        Write-Host "  ✗ $ip : $status" -ForegroundColor Red
        $allGood = $false
    }
}

Write-Host ("  " + "-" * 50)

if ($allGood) {
    Write-Host "`n🎉 CLUSTER RECUPERADO COM SUCESSO!" -ForegroundColor Green
    Write-Host "   Dashboard: abra frontend/index.html" -ForegroundColor Cyan
    Write-Host "   Watchdog:  python watchdog.py`n" -ForegroundColor Cyan
} else {
    Write-Host "`n⚠ Alguns nós ainda estão com problemas." -ForegroundColor Yellow
    Write-Host "   Verifique logs: ssh pi@<ip> 'journalctl -u cluster-daemon -n 20 --no-pager'" -ForegroundColor Yellow
}
