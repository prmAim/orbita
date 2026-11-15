param([string]$Project='orbita',[switch]$Build,[string]$EnvironmentFile,[string]$ComposeOverride)
$ErrorActionPreference='Stop'
$options=@('up','-d')
if($Build){$options+='--build'}else{$options+='--no-build'}
$compose=@('compose','--project-name',$Project,'--project-directory',$PSScriptRoot)
if($EnvironmentFile){if(-not(Test-Path -LiteralPath $EnvironmentFile -PathType Leaf)){throw 'Environment file does not exist'};$compose+=@('--env-file',(Resolve-Path -LiteralPath $EnvironmentFile).Path)}
$compose+=@('-f',(Join-Path $PSScriptRoot 'compose.yaml'))
if($ComposeOverride){if(-not(Test-Path -LiteralPath $ComposeOverride -PathType Leaf)){throw 'Compose override does not exist'};$compose+=@('-f',(Resolve-Path -LiteralPath $ComposeOverride).Path)}
& docker @compose @options
if($LASTEXITCODE -ne 0){throw 'Orbita failed to start'}
