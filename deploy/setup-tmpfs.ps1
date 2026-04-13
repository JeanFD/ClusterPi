# deploy/setup-tmpfs.ps1 — Configura tmpfs em /data/redis em todos os nós
# A6: Redis em tmpfs elimina latência de I/O de disco
# Uso: .\deploy\setup-tmpfs.ps1
# Ou para nós específicos: .\deploy\setup-tmpfs.ps1 -Nodes 20,21,22

param(
    [int[]]$Nodes = (20..25)
)

$ErrorActionPreference = "Continue"

Write-Host "`n╔══════════════════════════════════════════╗" -ForegroundColor Blue
Write-Host "║  Cluster RPi — Setup tmpfs Redis         ║" -ForegroundColor Blue
Write-Host "╚══════════════════════════════════════════╝`n" -ForegroundColor Blue

# Comando bash montado em variável — evita problemas com heredoc indentado
$bashCmd = 'sudo mkdir -p /data/redis && ' +
           'sudo umount /data/redis 2>/dev/null || true; ' +
           'sudo mount -t tmpfs -o size=512M,mode=0755 tmpfs /data/redis && ' +
           'grep -q "tmpfs /data/redis" /etc/fstab || ' +
           'echo "tmpfs /data/redis tmpfs size=512M,mode=0755 0 0" | sudo tee -a /etc/fstab && ' +
           'echo "tmpfs configurado em /data/redis" && ' +
           'df -h /data/redis'

$jobs = @()
foreach ($n in $Nodes) {
    $ip = "192.168.1.$n"
    Write-Host "  → Configurando tmpfs em $ip" -ForegroundColor Yellow

    $jobs += Start-Job -ScriptBlock {
        param($addr, $cmd)
        ssh pi@$addr $cmd
    } -ArgumentList $ip, $bashCmd
}

Wait-Job $jobs | Out-Null
Receive-Job $jobs
Remove-Job  $jobs

Write-Host "`n✅ tmpfs configurado em todos os nós." -ForegroundColor Green
Write-Host "   Lembre-se: após reboot o Redis precisará re-inicializar o cluster." -ForegroundColor Yellow
Write-Host "   Use .\deploy\recover-redis.ps1 se necessário.`n"
