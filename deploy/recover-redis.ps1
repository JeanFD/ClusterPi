# deploy/recover-redis.ps1 - Restaura Redis Cluster (6 masters x 2 replicas) e reinicia servicos
# Uso: .\deploy\recover-redis.ps1

$ErrorActionPreference = "Continue"
$nodes  = 20..25
$leader = "192.168.1.20"

Write-Host ""
Write-Host "===========================================" -ForegroundColor Red
Write-Host "  Cluster RPi -- RECOVERY MODE (6m x 2r)   " -ForegroundColor Red
Write-Host "===========================================" -ForegroundColor Red
Write-Host ""

# -- Step 1: Verificar conectividade SSH ------------------------------
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
if ($reachable -lt 6) {
    Write-Host "Precisa de todos os 6 nos para criar o cluster 6m x 2r." -ForegroundColor Red
    Write-Host "Reachable: $reachable / 6. Abortando." -ForegroundColor Red
    exit 1
}
Write-Host "  6 / 6 nos acessiveis" -ForegroundColor Green

# -- Step 2: Distribuir redis.conf e docker-compose -------------------
Write-Host ""
Write-Host "Distribuindo redis.conf e docker-compose..." -ForegroundColor Cyan
foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    ssh pi@$ip "mkdir -p ~/cluster/redis"
    scp .\docker-compose.redis.yml pi@${ip}:~/cluster/
    scp .\redis\redis.conf         pi@${ip}:~/cluster/redis/
}
Write-Host "  Arquivos distribuidos" -ForegroundColor Green

# -- Step 3: Garantir tmpfs (3 mounts), limpar estado, subir containers
Write-Host ""
Write-Host "Subindo Redis (3 instancias por no) em todos os nos..." -ForegroundColor Cyan

# IMPORTANTE: limpar nodes.conf de execucoes anteriores senao o cluster
# e iniciado com identity antiga e o --cluster create falha com
# "not empty" / "contains data".
$redisCmd = "sudo mkdir -p /data/redis-a /data/redis-b /data/redis-c && " +
            "(mountpoint -q /data/redis-a || sudo mount -t tmpfs -o size=256M,mode=0755 tmpfs /data/redis-a); " +
            "(mountpoint -q /data/redis-b || sudo mount -t tmpfs -o size=256M,mode=0755 tmpfs /data/redis-b); " +
            "(mountpoint -q /data/redis-c || sudo mount -t tmpfs -o size=256M,mode=0755 tmpfs /data/redis-c); " +
            "sudo rm -f /data/redis-a/nodes.conf /data/redis-a/dump.rdb /data/redis-b/nodes.conf /data/redis-b/dump.rdb /data/redis-c/nodes.conf /data/redis-c/dump.rdb; " +
            "docker rm -f cluster-redis-1 cluster-redis 2>/dev/null; " +
            "cd ~/cluster && docker compose -f docker-compose.redis.yml down --remove-orphans 2>/dev/null; " +
            "docker compose -f docker-compose.redis.yml up -d"

foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    Write-Host "  -> $ip" -ForegroundColor Yellow
    ssh pi@$ip $redisCmd
}

# -- Step 4: Aguardar ambas as instancias responderem em todos os nos -
Write-Host ""
Write-Host "Aguardando Redis responder (ate 30s)..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

$allRedisOk = $true
foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    $okA = $false; $okB = $false; $okC = $false
    for ($i = 0; $i -lt 10; $i++) {
        $pongA = ssh pi@$ip "docker exec cluster-redis-a redis-cli -p 6379 ping 2>/dev/null"
        $pongB = ssh pi@$ip "docker exec cluster-redis-b redis-cli -p 6380 ping 2>/dev/null"
        $pongC = ssh pi@$ip "docker exec cluster-redis-c redis-cli -p 6381 ping 2>/dev/null"
        if ($pongA -match "PONG") { $okA = $true }
        if ($pongB -match "PONG") { $okB = $true }
        if ($pongC -match "PONG") { $okC = $true }
        if ($okA -and $okB -and $okC) { break }
        Start-Sleep -Seconds 2
    }
    if ($okA -and $okB -and $okC) {
        Write-Host "  OK $ip  a:PONG b:PONG c:PONG" -ForegroundColor Green
    } else {
        $sA = if ($okA) { "PONG" } else { "DOWN" }
        $sB = if ($okB) { "PONG" } else { "DOWN" }
        $sC = if ($okC) { "PONG" } else { "DOWN" }
        Write-Host "  FAIL $ip  a:$sA b:$sB c:$sC" -ForegroundColor Red
        $allRedisOk = $false
    }
}
if (-not $allRedisOk) {
    Write-Host "Algumas instancias nao subiram. Verifique 'docker logs cluster-redis-{a,b,c}' no no problematico." -ForegroundColor Red
    exit 1
}

# -- Step 5: Criar Redis Cluster em ANEL (6m x 2r determinístico) ----
# Topologia em anel com offsets +1 e +2:
#   nó i: redis-a master, redis-b replica do master (i-1)%6, redis-c replica (i-2)%6
# Com 2 replicas por master em hosts distintos, o cluster tolera QUALQUER
# 2 nós caindo simultaneamente sem perder slot.
Write-Host ""
Write-Host "Criando Redis Cluster em anel (6m x 2r determinístico)..." -ForegroundColor Cyan

# 5-pre. Reset HARD em todas as 18 instâncias — garante que qualquer lixo
# de execuções anteriores (nodes.conf residual, epoch antigo, meet pendente)
# seja apagado antes do create.
Write-Host "  [5-pre] cluster reset hard em todas as 18 instâncias..." -ForegroundColor DarkGray
foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    $resetCmd = "docker exec cluster-redis-a redis-cli -p 6379 cluster reset hard 2>/dev/null; " +
                "docker exec cluster-redis-b redis-cli -p 6380 cluster reset hard 2>/dev/null; " +
                "docker exec cluster-redis-c redis-cli -p 6381 cluster reset hard 2>/dev/null; " +
                "docker exec cluster-redis-a redis-cli -p 6379 flushall 2>/dev/null; " +
                "docker exec cluster-redis-b redis-cli -p 6380 flushall 2>/dev/null; " +
                "docker exec cluster-redis-c redis-cli -p 6381 flushall 2>/dev/null"
    ssh pi@$ip $resetCmd | Out-Null
}
Start-Sleep -Seconds 2

# 5a. Cria cluster só com os 6 masters (6380/6381 ficam standalone).
$masters = (20..25 | ForEach-Object { "192.168.1.$_`:6379" }) -join " "
$createCmd = "docker exec cluster-redis-a redis-cli --cluster create $masters --cluster-yes"
Write-Host "  [5a] create masters: $createCmd" -ForegroundColor DarkGray
$createOut = (ssh pi@$leader $createCmd 2>&1 | Out-String)
Write-Host $createOut -ForegroundColor DarkGray
if ($createOut -notmatch "16384 slots covered") {
    Write-Host "  [5a] FALHOU: criação do cluster não cobriu os 16384 slots. Abortando." -ForegroundColor Red
    exit 1
}

Start-Sleep -Seconds 2

# 5b. Valida que todos os 6 masters se conhecem.
Write-Host "  [5b] validando cluster e coletando IDs dos masters..." -ForegroundColor DarkGray
$known = (ssh pi@$leader "docker exec cluster-redis-a redis-cli cluster info | grep cluster_known_nodes" 2>&1 | Out-String)
Write-Host "    $($known.Trim())" -ForegroundColor DarkGray
if ($known -notmatch "cluster_known_nodes:6") {
    Write-Host "  [5b] FALHOU: esperado 6 nós. Abortando." -ForegroundColor Red
    exit 1
}

$masterIds = @{}
foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    $id = ssh pi@$ip "docker exec cluster-redis-a redis-cli -p 6379 cluster myid"
    $masterIds[$n] = $id.Trim()
    Write-Host "    .$n → $($masterIds[$n])" -ForegroundColor DarkGray
}

# 5c. Anexa replicas em anel: redis-b com offset -1, redis-c com offset -2.
Write-Host "  [5c] anexando replicas em anel (offsets -1 e -2)..." -ForegroundColor DarkGray
$replicaSpecs = @(
    @{ Port = 6380; Offset = 1 },
    @{ Port = 6381; Offset = 2 }
)
foreach ($spec in $replicaSpecs) {
    $port   = $spec.Port
    $offset = $spec.Offset
    for ($i = 0; $i -lt 6; $i++) {
        $replicaNodeIdx = 20 + $i
        $masterNodeIdx  = 20 + ((($i - $offset) + 6) % 6)
        $replicaAddr    = "192.168.1.${replicaNodeIdx}:${port}"
        $masterAddr     = "192.168.1.${masterNodeIdx}:6379"
        $masterId       = $masterIds[$masterNodeIdx]
        $addCmd = "docker exec cluster-redis-a redis-cli --cluster add-node $replicaAddr $masterAddr --cluster-slave --cluster-master-id $masterId"
        $addOut = (ssh pi@$leader $addCmd 2>&1 | Out-String)
        if ($addOut -match "New node added correctly") {
            Write-Host "    .${replicaNodeIdx}:${port} → replica de .${masterNodeIdx}:6379 OK" -ForegroundColor DarkGreen
        } else {
            Write-Host "    .${replicaNodeIdx}:${port} → replica de .${masterNodeIdx}:6379 FALHOU" -ForegroundColor Red
            Write-Host $addOut -ForegroundColor DarkRed
        }
    }
}

Start-Sleep -Seconds 3

$clusterState = ssh pi@$leader "docker exec cluster-redis-a redis-cli cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_known_nodes'"
Write-Host "  ---" -ForegroundColor DarkGray
Write-Host "$clusterState" -ForegroundColor DarkGray
Write-Host "  ---" -ForegroundColor DarkGray

# -- Step 6: Reiniciar servicos Python em todos os nos ----------------
Write-Host ""
Write-Host "Reiniciando servicos Python em todos os nos..." -ForegroundColor Cyan

$restartCmd = "sudo systemctl reset-failed 2>/dev/null || true; " +
              "sudo systemctl restart cluster-daemon cluster-worker@0 cluster-worker@1 cluster-worker@2 cluster-worker@3 cluster-api"

foreach ($n in $nodes) {
    $ip = "192.168.1.$n"
    Write-Host "  -> $ip" -ForegroundColor Yellow
    ssh pi@$ip $restartCmd
}

# -- Step 7: Aguardar eleicao Raft ------------------------------------
Write-Host ""
Write-Host "Aguardando eleicao Raft (12s)..." -ForegroundColor Yellow
Start-Sleep -Seconds 12

# -- Step 8: Status final ---------------------------------------------
Write-Host ""
Write-Host "Status final:" -ForegroundColor Green
Write-Host ("  " + ("-" * 52))
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

Write-Host ("  " + ("-" * 52))

if ($allGood) {
    Write-Host ""
    Write-Host "CLUSTER RECUPERADO COM SUCESSO!" -ForegroundColor Green
    Write-Host "  Topologia: 6 masters x 2 replicas - tolera 2 nos caidos" -ForegroundColor Cyan
    Write-Host "  Verificar: ssh pi@$leader 'docker exec cluster-redis-a redis-cli cluster nodes'" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "Alguns nos ainda tem problemas." -ForegroundColor Yellow
    Write-Host "  Logs: ssh pi@192.168.1.20 journalctl -u cluster-daemon -n 30 --no-pager" -ForegroundColor Yellow
    Write-Host ""
}
