param(
    [string]$PythonExe = "python"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = "$root\src"
Push-Location $root
try {
    & $PythonExe -m cybercar telegram recover @args
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
