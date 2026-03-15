$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = "$root\src"
python -m cybercar login status @args
exit $LASTEXITCODE
