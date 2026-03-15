$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = "$root\src"
python -m cybercar collect @args
exit $LASTEXITCODE
