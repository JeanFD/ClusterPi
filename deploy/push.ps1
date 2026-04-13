# deploy/push.ps1 - Distribui codigo e reinicia servicos nos nos acessiveis
# Uso: .\deploy\push.ps1
# Ou para nos especificos: .\deploy\push.ps1 -Nodes 20,22,23

param(
    [int[]]$Nodes = (20..25)
)

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "=== Cluster RPi - Deploy ===" -ForegroundColor Blue
Write-Host ""

# --- Teste de conectividade SSH por no ---
Write-Host "Verificando conectividade..." -ForegroundColor Cyan
$reachable = @()
foreach ($n in $Nodes) {
    $ip = "192.168.1.$n"
    $result = & ssh -o ConnectTimeout=4 -o BatchMode=yes -o StrictHostKeyChecking=no pi@$ip "echo ok" 2>$null
    if ($result -eq "ok") {
        $reachable += $n
        Write-Host "  OK  $ip" -ForegroundColor Green
    } else {
        Write-Host "  SKIP $ip (inacessivel)" -ForegroundColor Yellow
    }
}

if ($reachable.Count -eq 0) {
    Write-Host "Nenhum no acessivel. Verifique a rede." -ForegroundColor Red
    exit 1
}

$nosStr = ($reachable | ForEach-Object { "192.168.1.$_" }) -join ", "
Write-Host "Deployando para: $nosStr" -ForegroundColor Yellow

# --- 1. Distribuir codigo Python + frontend ---
Write-Host ""
Write-Host "Distribuindo codigo..." -ForegroundColor Cyan
foreach ($n in $reachable) {
    $ip = "192.168.1.$n"
    Write-Host "  -> $ip" -ForegroundColor Yellow

    ssh pi@$ip "mkdir -p ~/cluster/app ~/cluster/frontend ~/cluster/app/render"
    scp .\app\*.py              pi@${ip}:~/cluster/app/
    scp .\app\requirements.txt  pi@${ip}:~/cluster/app/
    scp .\app\render\*.py       pi@${ip}:~/cluster/app/render/
    scp .\frontend\index.html   pi@${ip}:~/cluster/frontend/
    scp .\deploy\cluster-daemon.service  pi@${ip}:/tmp/
    scp .\deploy\cluster-worker@.service pi@${ip}:/tmp/
    scp .\deploy\cluster-api.service     pi@${ip}:/tmp/
}

# --- 2. Reiniciar servicos em paralelo ---
$restartCmd = 'sudo mv /tmp/cluster-daemon.service  /etc/systemd/system/ 2>/dev/null; ' +
              'sudo mv /tmp/cluster-api.service     /etc/systemd/system/ 2>/dev/null; ' +
              'sudo mv /tmp/cluster-worker@.service /etc/systemd/system/ 2>/dev/null; ' +
              'sudo systemctl daemon-reload; ' +
              'sudo systemctl disable --now cluster-worker 2>/dev/null; ' +
              'sudo systemctl enable  --now cluster-worker@0 cluster-worker@1 cluster-worker@2 cluster-worker@3; ' +
              'sudo systemctl restart cluster-daemon cluster-worker@0 cluster-worker@1 cluster-worker@2 cluster-worker@3 cluster-api'

Write-Host ""
Write-Host "Reiniciando servicos..." -ForegroundColor Cyan
$jobs = @()
foreach ($n in $reachable) {
    $ip = "192.168.1.$n"
    $jobs += Start-Job -ScriptBlock {
        param($addr, $cmd)
        ssh -o ConnectTimeout=15 pi@$addr $cmd 2>&1
    } -ArgumentList $ip, $restartCmd
}

Wait-Job $jobs | Out-Null
$output = Receive-Job $jobs
Remove-Job $jobs
$output | Where-Object { $_ -match "error|fail|Error|failed" } | ForEach-Object {
    Write-Host "  ! $_" -ForegroundColor Yellow
}

# --- 3. Aguardar Raft eleger lider ---
Write-Host ""
Write-Host "Aguardando eleicao do lider Raft (8s)..." -ForegroundColor Cyan
Start-Sleep -Seconds 8

# --- 4. Verificar status dos servicos ---
Write-Host ""
Write-Host "Status dos servicos:" -ForegroundColor Green
$leaderNode = $null
foreach ($n in $reachable) {
    $ip = "192.168.1.$n"
    Write-Host "  $ip : " -NoNewline
    $statCmd = "systemctl is-active cluster-daemon cluster-worker@0 cluster-worker@1 cluster-worker@2 cluster-worker@3 cluster-api"
    $status = (ssh -o ConnectTimeout=5 pi@$ip $statCmd 2>$null) -join " "
    $activeCount = ($status -split "\s+" | Where-Object { $_ -eq "active" }).Count
    if ($activeCount -ge 6) {
        Write-Host "OK ($activeCount/6 ativos)" -ForegroundColor Green
        if (-not $leaderNode) { $leaderNode = $ip }
    } else {
        Write-Host "PARCIAL ($activeCount/6 ativos): $status" -ForegroundColor Red
    }
}

# --- 5. Verificar saude do cluster via API ---
if ($leaderNode) {
    Write-Host ""
    Write-Host "Saude do cluster ($leaderNode):" -ForegroundColor Cyan
    try {
        $health = Invoke-RestMethod "http://${leaderNode}:8000/health" -TimeoutSec 5
        $redisColor = if ($health.redis -eq "ok") { "Green" } else { "Red" }
        $statusColor = if ($health.status -eq "ok") { "Green" } else { "Red" }
        Write-Host "  No     : $($health.node)" -ForegroundColor White
        Write-Host "  Lider  : $($health.leader)" -ForegroundColor White
        Write-Host "  Redis  : $($health.redis)" -ForegroundColor $redisColor
        Write-Host "  Status : $($health.status)" -ForegroundColor $statusColor
        Write-Host "  Geom   : $($health.geometry)" -ForegroundColor White
    } catch {
        Write-Host "  API nao respondeu em $leaderNode" -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "Abra o frontend: frontend\index.html" -ForegroundColor Cyan
Write-Host ""
