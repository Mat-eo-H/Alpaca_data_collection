# refresh_1min_task.ps1 - register (or update) the Task Scheduler entry that runs refresh_1min.ps1 every weekday evening
# in a VISIBLE console window (runs only while you are logged on, so the window can be watched). Modeled on
# Sync_AmiBroker/daily_refresh_task.ps1 (2026-10-18, user: "the store needs to be fixed and restarted as well").
# 21:30: today's 1-minute bars are complete after 20:10 ET, and the bot's nightly job (20:30, one Alpaca news request
# per stored symbol, ~7,100) shares the 200-per-minute Alpaca budget, so this one starts after it. Run once from here:
#
#   .\refresh_1min_task.ps1                 # 21:30 local, Monday-Friday, both stores
#   .\refresh_1min_task.ps1 -At 22:00
#   .\refresh_1min_task.ps1 -Unregister     # remove it
param(
    [string]$At = "21:30",
    [switch]$Unregister
)
$name = "1-minute store refresh"
if ($Unregister) {
    Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "unregistered '$name'"
    exit 0
}
$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$script = Join-Path $repo "refresh_1min.ps1"
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Normal -File `"$script`""
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday -At $At
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 6) -StartWhenAvailable -MultipleInstances IgnoreNew   # two stores, ~12,000 paged requests each at 180 a minute
Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Write-Host "registered '$name' Monday-Friday at $At (visible window while logged on); logs: C:\StockData\_refresh_1min_<stamp>.log"
