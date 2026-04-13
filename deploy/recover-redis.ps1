# deploy/recover-redis.ps1 — Restaura Redis e reinicia o cluster inteiro
# Uso: .\deploy\recover-redis.ps1

$ErrorActionPreference = "Continue"
$nodes  = 20..25
$leader = "192.168.1.20"

Write-Host ""
Write-Host "===========================================" -ForegroundColor Red
Write-Host "  Cluster RPi -- RECOVERY MODE" -ForegroundColor Red
Write-Host "===========================================" -ForegroundColor Red
Write-Host ""

# ── Step 1: Verificar conectividade SSH ──────────────────────────
Write-Host "Verificando conectividade SSH..." -ForegroundColor Cyan
$reachable = 0
foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    $result = ssh -o ConnectTimeout=3 pi@$ip "echo ok" 2>&1
    if ($result -eq "ok") {
        Write-Host "  OK $ip" -ForegroundColor Green
        $reachable++
    } else {
        Write-Host "  FAIL $ip" -ForegroundColor Red
    }
}
if ($reachable -eq 0) {
    Write-Host "Nenhum no acessivel. Verifique a rede." -ForegroundColor Red
    exit 1
}
Write-Host "  $reachable / 6 nos acessiveis" -ForegroundColor Yellow

# ── Step 2: Distribuir redis.conf e docker-compose ───────────────
Write-Host ""
Write-Host "Distribuindo redis.conf e docker-compose..." -ForegroundColor Cyan
foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    ssh pi@$ip "mkdir -p ~/cluster/redis"
    scp .\docker-compose.redis.yml pi@${ip}:~/cluster/
    scp .\redis\redis.conf         pi@${ip}:~/cluster/redis/
}
Write-Host "  Arquivos distribuidos" -ForegroundColor Green

# ── Step 3: Garantir tmpfs + subir Redis em todos os nos ─────────
Write-Host ""
Write-Host "Subindo Redis em todos os nos..." -ForegroundColor Cyan

$redisCmd = "sudo mkdir -p /data/redis && " +
            "(mountpoint -q /data/redis || sudo mount -t tmpfs -o size=512M,mode=0755 tmpfs /data/redis); " +
            "cd ~/cluster && docker compose -f docker-compose.redis.yml down 2>/dev/null; " +
            "docker compose -f docker-compose.redis.yml up -d"

foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    Write-Host "  -> $ip" -ForegroundColor Yellow
    ssh pi@$ip $redisCmd
}

# ── Step 4: Aguardar Redis responder em todos os nos ─────────────
Write-Host ""
Write-Host "Aguardando Redis responder (ate 30s)..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

$allRedisOk = $true
foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    $ok = $false
    for ($i = 0; $i -lt 10; $i++) {
        $pong = ssh pi@$ip "docker exec cluster-redis-1 redis-cli ping 2>/dev/null"
        if ($pong -match "PONG") { $ok = $true; break }
        Start-Sleep -Seconds 2
    }
    if ($ok) {
        Write-Host "  OK $ip Redis PONG" -ForegroundColor Green
    } else {
        Write-Host "  FAIL $ip Redis nao respondeu" -ForegroundColor Red
        $allRedisOk = $false
    }
}

# ── Step 5: Criar Redis Cluster (6 masters, 0 replicas) ──────────
Write-Host ""
Write-Host "Criando Redis Cluster..." -ForegroundColor Cyan

$clusterNodes = (20..25 | ForEach-Object { "192.168.1.$_`:6379" }) -join " "
$createCmd    = "docker exec cluster-redis-1 redis-cli --cluster create $clusterNodes --cluster-replicas 0 --cluster-yes"
Write-Host "  $createCmd"
ssh pi@$leader $createCmd

Start-Sleep -Seconds 3

$clusterState = ssh pi@$leader "docker exec cluster-redis-1 redis-cli cluster info | head -3"
Write-Host "  $clusterState" -ForegroundColor DarkGray

# ── Step 6: Reiniciar servicos Python em todos os nos ────────────
Write-Host ""
Write-Host "Reiniciando servicos Python em todos os nos..." -ForegroundColor Cyan

$restartCmd = "sudo systemctl reset-failed 2>/dev/null || true; " +
              "sudo systemctl restart cluster-daemon cluster-worker@0 cluster-worker@1 cluster-worker@2 cluster-worker@3 cluster-api"

foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    Write-Host "  -> $ip" -ForegroundColor Yellow
    ssh pi@$ip $restartCmd
}

# ── Step 7: Aguardar eleicao Raft ────────────────────────────────
Write-Host ""
Write-Host "Aguardando eleicao Raft (12s)..." -ForegroundColor Yellow
Start-Sleep -Seconds 12

# ── Step 8: Status final ─────────────────────────────────────────
Write-Host ""
Write-Host "Status final:" -ForegroundColor Green
Write-Host ("  " + "-" * 52)
$allGood = $true

foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    $status = ssh pi@$ip "systemctl is-active cluster-daemon cluster-worker@0 cluster-worker@1 cluster-worker@2 cluster-worker@3 cluster-api 2>/dev/null | tr '\n' ' '"
    if ($status -match "active active active active active active") {
        Write-Host "  OK $ip : $status" -ForegroundColor Green
    } else {
        Write-Host "  FAIL $ip : $status" -ForegroundColor Red
        $allGood = $false
    }
}

Write-Host ("  " + "-" * 52)

if ($allGood) {
    Write-Host ""
    Write-Host "CLUSTER RECUPERADO COM SUCESSO!" -ForegroundColor Green
    Write-Host "  Health: .\deploy\api.ps1 -Action health" -ForegroundColor Cyan
    Write-Host "  Dashboard: abra frontend/index.html" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "Alguns nos ainda tem problemas." -ForegroundColor Yellow
    Write-Host "  Logs de um no com problema:" -ForegroundColor Yellow
    Write-Host "  ssh pi@192.168.1.20 journalctl -u cluster-daemon -n 30 --no-pager" -ForegroundColor Yellow
    Write-Host ""
}
