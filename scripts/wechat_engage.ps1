$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = "$root\src"
python -m cybercar engage wechat @args
exit $LASTEXITCODE
