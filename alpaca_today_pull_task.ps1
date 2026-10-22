# alpaca_today_pull_task.ps1 - register (or update) the Task Scheduler entry that runs alpaca_today_pull.ps1 every
# weekday afternoon in a VISIBLE console window (runs only while you are logged on, so the window can be watched).
# Modeled on refresh_1min_task.ps1 (2026-10-21, the report chat's request; the user's timing). 16:20: the report writes
# its list ~16:15 and builds at 16:30; the free plan serves SIP bars up to 16 minutes old, so 16:20 reaches 16:04 and
# the 16:00 closing-auction bar. Run once from here:
#
#   .\alpaca_today_pull_task.ps1                 # 16:20 local, Monday-Friday
#   .\alpaca_today_pull_task.ps1 -At 16:25
#   .\alpaca_today_pull_task.ps1 -Unregister     # remove it
param(
    [string]$At = "16:20",
    [switch]$Unregister
)
$name = "Alpaca today pull"
if ($Unregister) {
    Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "unregistered '$name'"
    exit 0
}
$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$script = Join-Path $repo "alpaca_today_pull.ps1"
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Normal -File `"$script`""
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday -At $At
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 30) -StartWhenAvailable -MultipleInstances IgnoreNew   # a 10-minute wait for the list + a short pull
Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Write-Host "registered '$name' Monday-Friday at $At (visible window while logged on); logs: C:\StockData\alpaca_today\_pull_<stamp>.log"
