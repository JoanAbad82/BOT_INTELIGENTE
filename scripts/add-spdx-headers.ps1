Param(
  [ValidateSet("MIT","Apache-2.0")]
  [string]$LicenseId = "MIT",
  [int]$Year = 2025,
  [string]$Holder = "Joan Abad and contributors",
  [string[]]$IncludeDirs = @("src","tests","scripts"),
  [switch]$DryRun
)

$excludeRegex = "(\\|/)(\.git|\.venv|dist|build|data|\.mypy_cache|\.pytest_cache|\.ruff_cache|\.idea|\.vscode|vendor)(\\|/)"
$headerLine1 = "# SPDX-License-Identifier: $LicenseId"
$headerLine2 = "# Copyright (c) $Year $Holder"

function Should-ExcludeFile([string]$path) { return ($path -match $excludeRegex) }
function Join-LinesCRLF([string[]]$lines) { return ($lines -join "`r`n") }

# Descubrir directorios; si no hay, fallback a raíz
$foundDirs = @()
foreach ($d in $IncludeDirs) { if (Test-Path $d) { $foundDirs += $d } }
if (-not $foundDirs) {
  Write-Host "No se encontraron $($IncludeDirs -join ', '). Usando la raíz '.' como fallback." -ForegroundColor Yellow
  $foundDirs = @(".")
}

$files = Get-ChildItem -Path $foundDirs -Recurse -Filter *.py -File -ErrorAction SilentlyContinue |
  Where-Object { -not (Should-ExcludeFile $_.FullName) }

$modified = @()
$skipped  = @()

foreach ($f in $files) {
  $content = Get-Content -LiteralPath $f.FullName -Raw -Encoding UTF8
  if ($content -match 'SPDX-License-Identifier:\s*\S+') { $skipped += $f.FullName; continue }

  $logical = $content -split "\r?\n", -1

  $idx = 0
  if ($logical.Count -gt 0 -and $logical[0] -match '^\#!') { $idx++ }
  if ($logical.Count -gt $idx -and ($logical[$idx] -match 'coding[:=]\s*utf-?8' -or $logical[$idx] -match '-\*-\s*coding:\s*utf-?8\s*-\*-')) { $idx++ }

  $newLogical = @()
  if ($idx -gt 0) { $newLogical += $logical[0..($idx-1)] }
  $newLogical += @($headerLine1, $headerLine2)
  if ($idx -lt $logical.Count) { $newLogical += $logical[$idx..($logical.Count-1)] }

  $newContent = Join-LinesCRLF $newLogical

  if ($DryRun) {
    Write-Host "[DRY-RUN] Añadir SPDX en: $($f.FullName)"
  } else {
    try {
      if ($PSVersionTable.PSVersion.Major -ge 7) {
        Set-Content -LiteralPath $f.FullName -Value $newContent -Encoding utf8NoBOM
      } else {
        Set-Content -LiteralPath $f.FullName -Value $newContent -Encoding UTF8
      }
      $modified += $f.FullName
    } catch {
      Write-Warning "No se pudo escribir en $($f.FullName): $($_.Exception.Message)"
    }
  }
}

if ($DryRun) {
  Write-Host "`nDRY-RUN finalizado."
  Write-Host "Archivos que se modificarían: $($files.Count - $skipped.Count)"
} else {
  Write-Host "`nHecho. Archivos modificados: $($modified.Count)"
}
Write-Host "Omitidos (ya tenían SPDX): $($skipped.Count)"

# Evita que se cierre si lo lanzas con “Run with PowerShell”
if (-not ($Host.Name -match 'ConsoleHost')) { Read-Host "Terminado. Pulsa Enter para cerrar..." }
