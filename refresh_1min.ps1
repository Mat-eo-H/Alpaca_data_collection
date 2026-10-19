# refresh_1min.ps1 - top the two 1-minute stores off from Alpaca in a VISIBLE window, then resolve the basis changes the
# refresh parked (2026-10-18, user: "the store needs to be fixed and restarted as well" - the stores had stopped on
# 2026-09-25 because alpaca_1min_refresh.py was only ever run by hand; the daily chain tops off only the index files).
#   1. alpaca_1min_refresh.py --store <Store>   Alpaca SIP 1-minute bars after each file's last minute (today's bars only
#                                               after 20:10 ET, so the task runs at 21:30, after the bot's nightly job)
#   2. split_adjust.py pending --store 1min     the adjusted store's parked basis changes (the chain's forward step)
# Tee log C:\StockData\_refresh_1min_<stamp>.log (UTF-16), status marker C:\StockData\_refresh_1min_status.txt
# (RUNNING <step> ... DONE | DONE-WITH-ERRORS), ends with REFRESH-1MIN-CHAIN-DONE. Registered by refresh_1min_task.ps1.
#   .\refresh_1min.ps1                     # both stores (what the task runs)
#   .\refresh_1min.ps1 -Store adjusted     # one store (the 2026-10-18 catch-up: the adjusted store first, then raw)
param(
    [ValidateSet("adjusted", "raw", "both")][string]$Store = "both",
    [string]$Stamp = (Get-Date -Format 'yyyyMMdd_HHmmss')
)
$host.UI.RawUI.WindowTitle = "1-MINUTE STORE REFRESH $Stamp ($Store)"
$env:PYTHONUTF8 = "1"
$env:PYTHONUNBUFFERED = "1"
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$py = "C:\Python314\python.exe"
$root = "C:\Users\matth\OneDrive\VSCodeProjects"
$log = "C:\StockData\_refresh_1min_$Stamp.log"
$status = "C:\StockData\_refresh_1min_status.txt"
$failed = @()

function Mark($s) {
    Set-Content $status $s -Encoding utf8
    "=== $s $(Get-Date -Format 'HH:mm:ss')" | Tee-Object -FilePath $log -Append
}
function Step($name, $dir, $argv) {
    # ($args is PowerShell's automatic variable - naming the parameter that way runs python with NO script)
    Mark "RUNNING $name"
    Set-Location $dir
    & $py @argv 2>&1 | Tee-Object -FilePath $log -Append
    if ($LASTEXITCODE -ne 0) { $script:failed += $name; "!! step $name failed (exit $LASTEXITCODE)" | Tee-Object -FilePath $log -Append }
}

"=== 1-MINUTE STORE REFRESH $Stamp store=$Store" | Tee-Object -FilePath $log -Append
Step "alpaca-1min" (Join-Path $root "Alpaca_data_collection") @("alpaca_1min_refresh.py", "--store", $Store)
if ($Store -ne "raw") {
    Step "split-pending-1min" (Join-Path $root "AmiBroker_AFL\scripts") @("split_adjust.py", "pending", "--store", "1min")
}
if ($failed.Count -eq 0) { Mark "DONE" } else { Mark "DONE-WITH-ERRORS $($failed -join ',')" }
"REFRESH-1MIN-CHAIN-DONE $(Get-Date -Format 'HH:mm:ss') failed=$($failed.Count)" | Tee-Object -FilePath $log -Append
