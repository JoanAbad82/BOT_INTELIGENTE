Param(
  [switch]$AllFiles = $true,   # ejecuta pre-commit en todo el repo
  [switch]$NoCommit            # no hace commit automático del autoupdate
)

# --- Forzar UTF-8 en Windows ---
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$OutputEncoding = [System.Text.UTF8Encoding]::new()

# --- Comprobaciones rápidas ---
if (-not (Test-Path ".pre-commit-config.yaml")) {
  Write-Error "No encuentro .pre-commit-config.yaml en el directorio actual. Ejecuta desde la raíz del repo."
  exit 1
}
if (-not (Get-Command poetry -ErrorAction SilentlyContinue)) {
  Write-Error "Poetry no está en PATH. Instálalo o abre el terminal del proyecto."
  exit 1
}

# --- Autoupdate ---
Write-Host ">> Autoupdate de hooks..." -ForegroundColor Cyan
poetry run python -X utf8 -m pre_commit.main autoupdate

# Si hubo cambios, mostrar diff breve y commitear (a menos que -NoCommit)
$changed = git diff --name-only .pre-commit-config.yaml
if ($changed) {
  git --no-pager diff -- .pre-commit-config.yaml
  if (-not $NoCommit) {
    git add .pre-commit-config.yaml
    git commit -m "chore(pre-commit): autoupdate hooks"
  }
} else {
  Write-Host "No hay cambios en .pre-commit-config.yaml."
}

# --- Refrescar entornos y ejecutar hooks ---
Write-Host ">> Limpiando entornos de pre-commit..." -ForegroundColor Cyan
poetry run pre-commit clean

Write-Host ">> Ejecutando pre-commit..." -ForegroundColor Cyan
if ($AllFiles) {
  poetry run pre-commit run --all-files
} else {
  poetry run pre-commit run
}

Write-Host ">> Listo." -ForegroundColor Green
