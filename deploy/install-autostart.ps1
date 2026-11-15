param([string]$Project='orbita')
$ErrorActionPreference='Stop'
if($Project -notmatch '^[a-z0-9][a-z0-9_-]*$'){throw 'Invalid compose project name'}
$start=Join-Path $PSScriptRoot 'start.ps1'
$action=New-ScheduledTaskAction -Execute 'powershell.exe' -Argument ('-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "'+$start+'" -Project "'+$Project+'"')
$trigger=New-ScheduledTaskTrigger -AtLogOn -User ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)
$settings=New-ScheduledTaskSettingsSet -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName ('Orbita-'+$Project) -Action $action -Trigger $trigger -Settings $settings -Description 'Start Orbita after owner logon without opening a browser' -Force | Out-Null
