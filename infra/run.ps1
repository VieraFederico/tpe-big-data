param(
    [Parameter(Position = 0)][string]$Command = "help",
    [Parameter(Position = 1, ValueFromRemainingArguments = $true)][string[]]$Rest = @()
)
$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$ComposeFile = "infra/docker-compose.yml"
$EnvFile = "config/.env"
$OutputDirs = @("bronze", "silver", "gold", "quarantine", "checkpoints")

function Show-Usage {
    @"
Uso: .\infra\run.ps1 <comando> [args]

  list                     Lista los pasos del pipeline y si están implementados
  run [pasos]              Corre el pipeline (todos, o p. ej. bronze_batch,bronze_stream)
  check                    Prueba la conexión a Cassandra/AstraDB
  query <archivo.cql> ...  Ejecuta una consulta y guarda evidencia (-p clave=valor)
  test [args]              Corre los tests (pytest)
  lint                     Chequea estilo y tipos (ruff check, ruff format --check, mypy)
  shell                    Abre una terminal dentro del contenedor
  clean                    Borra las salidas del lake (bronze, silver, gold, quarantine, checkpoints)
  down                     Baja los contenedores (Cassandra local incluida)
  build                    Reconstruye la imagen (tras cambiar config/requirements*.txt)
"@
}

function Invoke-Compose {
    & docker compose -f $ComposeFile @args
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

function Get-ServingTarget {
    if (-not (Test-Path $EnvFile)) { return "local" }
    $line = Get-Content $EnvFile | Where-Object { $_ -match '^\s*(export\s+)?SERVING_TARGET=' } | Select-Object -Last 1
    if (-not $line) { return "local" }
    return ($line -replace '.*=', '').Trim().Trim('"', "'")
}

function Initialize-Env {
    if (-not (Test-Path $EnvFile)) {
        Copy-Item "config/.env.example" $EnvFile
        Write-Host "Se creó $EnvFile desde el ejemplo; completalo si vas a usar AstraDB."
    }
}

function Start-CassandraIfLocal {
    if ((Get-ServingTarget) -eq "local") {
        Invoke-Compose --profile local up -d --wait cassandra
    }
}

function Invoke-Pipeline {
    Invoke-Compose run --rm pipeline @args
}

Initialize-Env

switch ($Command) {
    "list" { Invoke-Pipeline python -m src.pipeline --list }
    "run" {
        $steps = if ($Rest.Count -gt 0) { $Rest[0] } else { "" }
        if (-not $steps -or $steps -like "*serving*") { Start-CassandraIfLocal }
        if ($steps) { Invoke-Pipeline python -m src.pipeline --steps $steps }
        else { Invoke-Pipeline python -m src.pipeline }
    }
    "check" {
        Start-CassandraIfLocal
        Invoke-Pipeline python -m src.serving.check_connection
    }
    "query" {
        if ($Rest.Count -lt 1) { Write-Host "Falta el archivo .cql"; exit 1 }
        Start-CassandraIfLocal
        Invoke-Pipeline python -m src.serving.queries @Rest
    }
    "test" { Invoke-Pipeline python -m pytest tests @Rest }
    "lint" { Invoke-Pipeline sh -c "ruff check . && ruff format --check . && mypy" }
    "shell" { Invoke-Pipeline bash }
    "clean" {
        foreach ($dir in $OutputDirs) {
            $path = "data/datalake/$dir"
            if (Test-Path $path) { Remove-Item -Recurse -Force $path }
        }
        Write-Host "Salidas del lake borradas (landing intacto)."
    }
    "down" { Invoke-Compose --profile local down }
    "build" { Invoke-Compose build pipeline }
    { $_ -in "help", "-h", "--help" } { Show-Usage }
    default {
        Write-Host "Comando desconocido: $Command"
        Show-Usage
        exit 1
    }
}
