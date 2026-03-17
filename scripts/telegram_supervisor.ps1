param(
    [string]$PythonExe = "python",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PassThru
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = "$root\src"
Push-Location $root
try {
    & $PythonExe -m cybercar telegram supervise @PassThru
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
