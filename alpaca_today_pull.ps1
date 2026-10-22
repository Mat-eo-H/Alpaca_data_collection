# alpaca_today_pull.ps1 - today's 1-minute Alpaca bars for the daily report's symbol list, in a VISIBLE window
# (2026-10-21, the report chat's "REPORT -> DATA 2026-10-21 (18:45)"; the user's timing: the report's list ~16:15,
# this pull 16:20, the report at 16:30 or when the marker says DONE).
#   alpaca_today_pull.py   waits for C:\StockData\report_requests\alpaca_today_<date>.txt, writes
#                          C:\StockData\alpaca_today\<date>\<SYM>_1.csv + _symbols.csv (the store is never touched)
# Tee log C:\StockData\alpaca_today\_pull_<stamp>.log (UTF-16), status marker C:\StockData\alpaca_today\_status.txt
# (RUNNING <date> ... -> DONE <date> ... | FAILED <date> ...), ends with ALPACA-TODAY-CHAIN-DONE.
# Registered by alpaca_today_pull_task.ps1.
#   .\alpaca_today_pull.ps1                          # today, the report's list (what the task runs)
#   .\alpaca_today_pull.ps1 -List C:\...\syms.txt    # another list (smoke test)
param(
    [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
    [string]$List = "",
    [string]$Stamp = (Get-Date -Format 'yyyyMMdd_HHmmss')
)
$host.UI.RawUI.WindowTitle = "ALPACA TODAY PULL $Stamp"
$env:PYTHONUTF8 = "1"
$env:PYTHONUNBUFFERED = "1"
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$py = "C:\Python314\python.exe"
$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$outRoot = "C:\StockData\alpaca_today"
New-Item -ItemType Directory -Force $outRoot | Out-Null
$log = Join-Path $outRoot "_pull_$Stamp.log"
$status = Join-Path $outRoot "_status.txt"
$argv = @("alpaca_today_pull.py", "--date", $Date)
if ($List) { $argv += @("--list", $List) }

"=== ALPACA TODAY PULL $Stamp date=$Date" | Tee-Object -FilePath $log -Append
Set-Location $repo
& $py @argv 2>&1 | Tee-Object -FilePath $log -Append
$code = $LASTEXITCODE
# the tool marks its own FAILED; this covers a crash before it could (no Python, an import error)
if ($code -ne 0 -and -not ((Get-Content $status -ErrorAction SilentlyContinue | Select-Object -First 1) -like "FAILED*")) {
    Set-Content $status "FAILED $Date exit $code" -Encoding utf8
}
"ALPACA-TODAY-CHAIN-DONE $(Get-Date -Format 'HH:mm:ss') exit=$code" | Tee-Object -FilePath $log -Append
