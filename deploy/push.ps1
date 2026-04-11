# deploy/push.ps1 — Distribui código e reinicia serviços em todos os nós
# Uso: .\deploy\push.ps1
# Ou para nós específicos: .\deploy\push.ps1 -Nodes 20,21,22

param(
    [int[]]$Nodes = (20..25)
)

$ErrorActionPreference = "Continue"

Write-Host "`n╔══════════════════════════════════════════╗" -ForegroundColor Blue
Write-Host "║  Cluster RPi — Deploy                    ║" -ForegroundColor Blue
Write-Host "╚══════════════════════════════════════════╝`n" -ForegroundColor Blue

# 1. Distribuir código Python + frontend
Write-Host "📦 Distribuindo código..." -ForegroundColor Cyan
foreach ($n in $Nodes) {
    $ip = "192.168.1.$n"
    Write-Host "  → $ip" -ForegroundColor Yellow
    
    # Criar diretórios se não existem
    ssh pi@$ip "mkdir -p ~/cluster/app ~/cluster/frontend"
    
    # Copiar todos os .py do app
    scp .\app\*.py          pi@${ip}:~/cluster/app/
    scp .\app\requirements.txt pi@${ip}:~/cluster/app/
    
    # Copiar frontend
    scp .\frontend\index.html pi@${ip}:~/cluster/frontend/
    
    # Copiar services
    scp .\deploy\*.service  pi@${ip}:/tmp/
}

# 2. Instalar/recarregar units e reiniciar (em paralelo)
Write-Host "`n🔄 Reiniciando serviços..." -ForegroundColor Cyan
$jobs = @()
foreach ($n in $Nodes) {
    $ip = "192.168.1.$n"
    $jobs += Start-Job -ScriptBlock { param($a)
        ssh pi@$a @"
sudo mv /tmp/cluster-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cluster-daemon cluster-worker cluster-api
sudo systemctl restart cluster-daemon cluster-worker cluster-api
"@
    } -ArgumentList $ip
}

Wait-Job $jobs | Out-Null
Receive-Job $jobs
Remove-Job  $jobs

# 3. Verificar status
Write-Host "`n✅ Status:" -ForegroundColor Green
foreach ($n in $Nodes) {
    $ip = "192.168.1.$n"
    Write-Host "  $ip : " -NoNewline
    $status = ssh pi@$ip "systemctl is-active cluster-daemon cluster-worker cluster-api | tr '\n' ' '"
    if ($status -match "active active active") {
        Write-Host $status -ForegroundColor Green
    } else {
        Write-Host $status -ForegroundColor Red
    }
}

Write-Host "`n🌐 Frontend: http://192.168.1.20:8000/../frontend/index.html" -ForegroundColor Cyan
Write-Host "   Ou abra local: frontend/index.html (vai se conectar automaticamente)`n"
