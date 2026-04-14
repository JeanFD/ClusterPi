# deploy/setup-tmpfs.ps1 — Configura tmpfs em /data/redis-{a,b,c}
# Redis em tmpfs elimina latência de I/O de disco. Três instâncias por nó
# (1 master + 2 replicas de vizinhos) → 3 mounts, 256M cada (768M por nó).
# Uso: .\deploy\setup-tmpfs.ps1
# Ou para nós específicos: .\deploy\setup-tmpfs.ps1 -Nodes 20,21,22

param(
    [int[]]$Nodes = (20..25)
)

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "=== Cluster RPi - Setup tmpfs (redis-a + redis-b + redis-c) ===" -ForegroundColor Blue
Write-Host ""

# Monta /data/redis-a e /data/redis-b (idempotente).
# Remove o antigo /data/redis do fstab se existir (sem remover o mount ativo
# para não quebrar um cluster já rodando).
$bashCmd = @'
set -e
for suf in a b c; do
  d="/data/redis-$suf"
  sudo mkdir -p "$d"
  if ! mountpoint -q "$d"; then
    sudo mount -t tmpfs -o size=256M,mode=0755 tmpfs "$d"
  fi
  grep -q "tmpfs $d " /etc/fstab || \
    echo "tmpfs $d tmpfs size=256M,mode=0755 0 0" | sudo tee -a /etc/fstab > /dev/null
done

# Limpa entrada legada do /data/redis (não remove o mount ao vivo)
sudo sed -i '\|^tmpfs /data/redis |d' /etc/fstab || true

echo "tmpfs OK em /data/redis-{a,b,c}"
df -h /data/redis-a /data/redis-b /data/redis-c
'@

$jobs = @()
foreach ($n in $Nodes) {
    $ip = "192.168.1.$n"
    Write-Host "  -> Configurando tmpfs em $ip" -ForegroundColor Yellow

    $jobs += Start-Job -ScriptBlock {
        param($addr, $cmd)
        ssh pi@$addr $cmd
    } -ArgumentList $ip, $bashCmd
}

Wait-Job $jobs | Out-Null
Receive-Job $jobs
Remove-Job  $jobs

Write-Host ""
Write-Host "tmpfs configurado em todos os nos." -ForegroundColor Green
Write-Host "   Apos reboot, rode .\deploy\recover-redis.ps1 para reiniciar o cluster Redis." -ForegroundColor Yellow
Write-Host ""
