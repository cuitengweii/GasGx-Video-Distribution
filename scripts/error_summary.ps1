param(
    [string]$Workspace = "",
    [string]$ErrorFile = "",
    [int]$Hours = 24,
    [int]$Top = 10,
    [string]$Category = "",
    [switch]$ShowSamples,
    [int]$SampleLimit = 2,
    [switch]$ShowStack
)

$ErrorActionPreference = "Stop"

function Resolve-WorkspaceRoot {
    param([string]$InputWorkspace)
    if ([string]::IsNullOrWhiteSpace($InputWorkspace)) {
        return (Split-Path -Parent $PSScriptRoot)
    }
    return (Resolve-Path -Path $InputWorkspace).Path
}

function Resolve-ErrorLogPath {
    param(
        [string]$InputErrorFile,
        [string]$WorkspaceRoot
    )
    if (-not [string]::IsNullOrWhiteSpace($InputErrorFile)) {
        return (Resolve-Path -Path $InputErrorFile).Path
    }
    return (Join-Path $WorkspaceRoot "runtime\logs\telegram_command_worker_errors.jsonl")
}

function Parse-WorkerTs {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $null
    }
    try {
        return [datetime]::ParseExact(
            $Value.Trim(),
            "yyyy-MM-dd HH:mm:ss",
            [System.Globalization.CultureInfo]::InvariantCulture
        )
    }
    catch {
        return $null
    }
}

function Safe-Text {
    param([object]$Value)
    if ($null -eq $Value) {
        return ""
    }
    return [string]$Value
}

function Match-Category {
    param(
        [string]$Current,
        [string]$Filter
    )
    if ([string]::IsNullOrWhiteSpace($Filter)) {
        return $true
    }
    $value = Safe-Text $Current
    if ($Filter.Contains("*") -or $Filter.Contains("?")) {
        return $value -like $Filter
    }
    return $value -eq $Filter
}

$workspaceRoot = Resolve-WorkspaceRoot -InputWorkspace $Workspace
$errorLogPath = Resolve-ErrorLogPath -InputErrorFile $ErrorFile -WorkspaceRoot $workspaceRoot

if (-not (Test-Path -Path $errorLogPath -PathType Leaf)) {
    Write-Host ("[ErrorSummary] file not found: {0}" -f $errorLogPath)
    Write-Host "[ErrorSummary] start worker first, or pass -ErrorFile with a valid path."
    exit 0
}

$now = Get-Date
$windowStart = $now.AddHours(-1 * [Math]::Max(0, $Hours))
$normalizedTop = [Math]::Max(1, $Top)
$normalizedSampleLimit = [Math]::Max(1, $SampleLimit)

$events = New-Object System.Collections.Generic.List[object]
$invalidLineCount = 0

Get-Content -Path $errorLogPath -Encoding UTF8 | ForEach-Object {
    $line = Safe-Text $_
    $line = $line.Trim()
    if ([string]::IsNullOrWhiteSpace($line)) {
        return
    }

    try {
        $obj = $line | ConvertFrom-Json
    }
    catch {
        $script:invalidLineCount += 1
        return
    }

    if ($null -eq $obj) {
        return
    }

    $ts = Parse-WorkerTs -Value (Safe-Text $obj.ts)
    if ($null -ne $ts -and $ts -lt $windowStart) {
        return
    }
    if (-not (Match-Category -Current (Safe-Text $obj.category) -Filter $Category)) {
        return
    }

    $events.Add([pscustomobject]@{
            ts        = $ts
            category  = Safe-Text $obj.category
            severity  = Safe-Text $obj.severity
            retryable = Safe-Text $obj.retryable
            message   = Safe-Text $obj.message
            error     = Safe-Text $obj.error
            errorType = Safe-Text $obj.error_type
            stack     = Safe-Text $obj.stack
            raw       = $obj
        })
}

Write-Host ("[ErrorSummary] file={0}" -f $errorLogPath)
Write-Host ("[ErrorSummary] window_start={0} window_hours={1}" -f $windowStart.ToString("yyyy-MM-dd HH:mm:ss"), [Math]::Max(0, $Hours))
Write-Host ("[ErrorSummary] matched_events={0} invalid_lines={1}" -f $events.Count, $invalidLineCount)

if ($events.Count -eq 0) {
    Write-Host "[ErrorSummary] no matched events in current window."
    exit 0
}

$grouped = $events |
Group-Object -Property category |
ForEach-Object {
    $rows = $_.Group
    $lastEvent = $rows |
    Sort-Object -Property @{ Expression = { if ($null -ne $_.ts) { $_.ts } else { [datetime]::MinValue } } }, @{ Expression = { $_.message } } -Descending |
    Select-Object -First 1
    $retryableCount = ($rows | Where-Object { $_.retryable -eq "True" }).Count
    [pscustomobject]@{
        count             = $rows.Count
        category          = if ([string]::IsNullOrWhiteSpace($_.Name)) { "(uncategorized)" } else { $_.Name }
        last_ts           = if ($null -ne $lastEvent.ts) { $lastEvent.ts.ToString("yyyy-MM-dd HH:mm:ss") } else { "-" }
        retryable_count   = $retryableCount
        non_retryable_cnt = ($rows.Count - $retryableCount)
        sample_message    = Safe-Text $lastEvent.message
        sample_error      = Safe-Text $lastEvent.error
        sample_error_type = Safe-Text $lastEvent.errorType
        sample_stack      = Safe-Text $lastEvent.stack
        rows              = $rows
    }
} |
Sort-Object -Property count, last_ts -Descending

$topRows = $grouped | Select-Object -First $normalizedTop
$topRows |
Select-Object count, category, last_ts, retryable_count, non_retryable_cnt |
Format-Table -AutoSize

if ($ShowSamples) {
    Write-Host ""
    Write-Host "[ErrorSummary] Samples"
    foreach ($row in $topRows) {
        Write-Host ""
        Write-Host ("[{0}] count={1} last={2}" -f $row.category, $row.count, $row.last_ts)
        $samples = $row.rows |
        Sort-Object -Property @{ Expression = { if ($null -ne $_.ts) { $_.ts } else { [datetime]::MinValue } } } -Descending |
        Select-Object -First $normalizedSampleLimit

        foreach ($sample in $samples) {
            $tsText = if ($null -ne $sample.ts) { $sample.ts.ToString("yyyy-MM-dd HH:mm:ss") } else { "-" }
            $message = (Safe-Text $sample.message).Trim()
            $errorText = (Safe-Text $sample.error).Trim()
            $errorType = (Safe-Text $sample.errorType).Trim()
            $severity = Safe-Text $sample.severity
            $retryable = Safe-Text $sample.retryable
            if ([string]::IsNullOrWhiteSpace($severity)) { $severity = "-" }
            if ([string]::IsNullOrWhiteSpace($retryable)) { $retryable = "-" }

            Write-Host ("- {0} | severity={1} retryable={2}" -f $tsText, $severity, $retryable)
            if (-not [string]::IsNullOrWhiteSpace($message)) {
                Write-Host ("  message: {0}" -f $message)
            }
            if (-not [string]::IsNullOrWhiteSpace($errorType)) {
                Write-Host ("  error_type: {0}" -f $errorType)
            }
            if (-not [string]::IsNullOrWhiteSpace($errorText)) {
                Write-Host ("  error: {0}" -f $errorText)
            }
            if ($ShowStack) {
                $stackText = Safe-Text $sample.stack
                if (-not [string]::IsNullOrWhiteSpace($stackText)) {
                    $stackFirstLine = ($stackText -split "`r?`n" | Select-Object -First 1)
                    Write-Host ("  stack: {0}" -f $stackFirstLine)
                }
            }
        }
    }
}
