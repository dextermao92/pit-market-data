# Registers the "pit-host-watch" scheduled task: python -m pit.host_watch every 5 minutes and at startup, for the
# current user. Run from the repo root (re-running replaces the task):
#   powershell -ExecutionPolicy Bypass -File ops\register_host_watch.ps1
#
# From an Administrator PowerShell it registers "run whether signed in or not" without storing a password (S4U), plus
# a boot trigger, so it also runs after a Windows Update restart left at the sign-in screen: the case it most needs
# to catch. Without admin (Windows refuses both then) it registers "only while signed in": re-run as admin to upgrade.
# Remove with: Unregister-ScheduledTask -TaskName pit-host-watch -Confirm:$false

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot
$pythonw = Join-Path $repo '.venv\Scripts\pythonw.exe'
if (-not (Test-Path $pythonw)) { throw "$pythonw not found: run 'uv sync' in the repo first" }

$action = New-ScheduledTaskAction -Execute $pythonw -Argument '-m pit.host_watch' -WorkingDirectory $repo
$every5 = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes 5)
$admin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)
$triggers = if ($admin) { @($every5, (New-ScheduledTaskTrigger -AtStartup)) } else { @($every5) }  # boot trigger needs admin
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 4) -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
$user = "$env:USERDOMAIN\$env:USERNAME"
$desc = 'pit-market-data: alerts on Discord when Docker / containers / the Airflow scheduler are down, or Windows waits to restart'

foreach ($logon in @('S4U') * $admin + 'Interactive') {
    $principal = New-ScheduledTaskPrincipal -UserId $user -LogonType $logon -RunLevel Limited
    try {
        Register-ScheduledTask -TaskName 'pit-host-watch' -Description $desc -Action $action -Trigger $triggers `
            -Settings $settings -Principal $principal -Force | Out-Null
        Write-Output "registered pit-host-watch ($logon)"
        if ($logon -eq 'Interactive') {
            Write-Output 'NOTE: runs only while you are signed in. For the sign-in-screen case, re-run this script as Administrator.'
        }
        break
    } catch {
        if ($logon -eq 'Interactive') { throw }
        Write-Output "S4U refused ($($_.Exception.Message.Trim())); falling back to Interactive"
    }
}
