$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = "$root\src"
python -m cybercar migrate-legacy @args
exit $LASTEXITCODE
