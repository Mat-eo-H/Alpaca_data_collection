<#
alpaca_options_earnings.ps1 - open alpaca_options_earnings.py in its OWN visible PowerShell window (normal priority,
-NoExit so the window stays readable, Ctrl+C or closing it stops the run) with a Tee log in
C:\StockData\optionsHistory\_logs. House rule: every long run visible and stoppable by hand.

usage:  .\alpaca_options_earnings.ps1                               # run: every pending event, newest first
        .\alpaca_options_earnings.ps1 -Cmd "run --days 90"
        .\alpaca_options_earnings.ps1 -Cmd "build-universe"
        .\alpaca_options_earnings.ps1 -Cmd "run --retry-errors"
The log name carries the command and a timestamp; the run ends with the line OPTIONS-HISTORY-DONE.
Watch it with Sync_AmiBroker\run_monitor.py --log <that log> --done-regex OPTIONS-HISTORY-DONE.
#>
param(
    [string]$Cmd = "run",
    [string]$Title = "Alpaca options history"
)
$py = "C:\Python314\python.exe"
$script = Join-Path $PSScriptRoot "alpaca_options_earnings.py"
$logDir = "C:\StockData\optionsHistory\_logs"
New-Item -ItemType Directory -Force $logDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$tag = ($Cmd -replace '[^A-Za-z0-9]+', '_').Trim('_')
$log = Join-Path $logDir ("alpaca_options_earnings_{0}_{1}.log" -f $tag, $stamp)
$inner = "`$host.UI.RawUI.WindowTitle = '$Title'; `$env:PYTHONUTF8 = '1'; " +
         "& '$py' '$script' $Cmd 2>&1 | Tee-Object -FilePath '$log'"
Start-Process powershell -ArgumentList '-NoExit', '-Command', $inner
Write-Host "started '$Title' ($Cmd); log: $log"
