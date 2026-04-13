# deploy/api.ps1 — Helpers para chamar a API do cluster via PowerShell
# PowerShell não tem curl real — usa Invoke-RestMethod (irm)
#
# Uso:
#   .\deploy\api.ps1 -Action mode-get
#   .\deploy\api.ps1 -Action mode-set -Mode render
#   .\deploy\api.ps1 -Action mode-set -Mode geometry
#   .\deploy\api.ps1 -Action health
#   .\deploy\api.ps1 -Action render-job -Scene menger -FrameId 0001
#   .\deploy\api.ps1 -Action render-progress
#   .\deploy\api.ps1 -Action geometry-set -Geom torus

param(
    [string]$Action   = "health",
    [string]$NodeIp   = "192.168.1.20",   # $Host é reservado no PowerShell
    [int]   $Port     = 8000,
    [string]$Mode     = "geometry",
    [string]$Geom     = "torus",
    [string]$Scene    = "mandelbulb",
    [string]$FrameId  = "0001",
    [int]   $Width    = 640,
    [int]   $Height   = 360,
    [int]   $TileSize = 64,
    [float] $Time     = 0.0
)

$base = "http://${NodeIp}:${Port}"

switch ($Action) {

    "health" {
        Invoke-RestMethod "$base/health" | ConvertTo-Json
    }

    "mode-get" {
        Invoke-RestMethod "$base/mode" | ConvertTo-Json
    }

    "mode-set" {
        $body = @{ mode = $Mode } | ConvertTo-Json
        Invoke-RestMethod -Method POST -Uri "$base/mode" `
            -ContentType "application/json" -Body $body | ConvertTo-Json
    }

    "render-job" {
        # Câmera padrão em órbita — ajuste conforme necessário
        $body = @{
            frame_id  = $FrameId
            scene     = $Scene
            camera    = @{
                pos    = @(0, 1, 4)
                target = @(0, 0, 0)
                fov    = 60
            }
            full_w    = $Width
            full_h    = $Height
            tile_size = $TileSize
            time      = $Time
        } | ConvertTo-Json -Depth 5
        Invoke-RestMethod -Method POST -Uri "$base/render/job" `
            -ContentType "application/json" -Body $body | ConvertTo-Json
    }

    "render-progress" {
        Invoke-RestMethod "$base/render/progress" | ConvertTo-Json
    }

    "geometry-set" {
        $body = @{ geometry = $Geom } | ConvertTo-Json
        Invoke-RestMethod -Method POST -Uri "$base/geometry" `
            -ContentType "application/json" -Body $body | ConvertTo-Json
    }

    "queue" {
        Invoke-RestMethod "$base/queue/status" | ConvertTo-Json
    }

    default {
        Write-Host "Actions disponíveis:" -ForegroundColor Yellow
        Write-Host "  health, mode-get, mode-set, render-job, render-progress, geometry-set, queue"
    }
}
