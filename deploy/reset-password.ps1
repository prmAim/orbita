param([string]$Project='orbita',[string]$EnvironmentFile,[string]$ComposeOverride)
$ErrorActionPreference='Stop'
$compose=@('compose','--project-name',$Project,'--project-directory',$PSScriptRoot)
if($EnvironmentFile){if(-not(Test-Path -LiteralPath $EnvironmentFile -PathType Leaf)){throw 'Environment file does not exist'};$compose+=@('--env-file',(Resolve-Path -LiteralPath $EnvironmentFile).Path)}
$compose+=@('-f',(Join-Path $PSScriptRoot 'compose.yaml'))
if($ComposeOverride){if(-not(Test-Path -LiteralPath $ComposeOverride -PathType Leaf)){throw 'Compose override does not exist'};$compose+=@('-f',(Resolve-Path -LiteralPath $ComposeOverride).Path)}
& docker @compose exec app java -jar /app/orbita.jar --spring.main.web-application-type=none --orbita.admin.reset-password=true --orbita.telegram.enabled=false --orbita.backup.enabled=false
if($LASTEXITCODE -ne 0){throw 'Password reset did not complete; access may remain locked'}
