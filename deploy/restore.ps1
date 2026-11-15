param([Parameter(Mandatory)][string]$Backup,[string]$Project=('orbita-restored-'+(Get-Date -Format 'yyyyMMddHHmmss')),[int]$HttpPort=8087,[string]$EnvironmentFile,[string]$ComposeOverride)
$ErrorActionPreference='Stop'
if($Project -notmatch '^orbita-restored-[a-z0-9_-]+$'){throw 'Restore requires a NEW orbita-restored-* project'}
$source=(Resolve-Path -LiteralPath $Backup).Path
if([IO.Path]::GetExtension($source) -ne '.dump'){throw 'Expected a verified .dump archive'}
$checksum=$source+'.sha256'
if(-not(Test-Path -LiteralPath $checksum)){throw 'Verified SHA256 sidecar is required'}
$expected=(Get-Content -LiteralPath $checksum -Raw).Trim()
if((Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant() -ne $expected.ToLowerInvariant()){throw 'Backup checksum mismatch'}
$existing=& docker compose ls --all --format json | ConvertFrom-Json
if($existing | Where-Object Name -eq $Project){throw 'Target project already exists. Choose a fresh name.'}
$volumes=& docker volume ls --quiet --filter ('label=com.docker.compose.project='+$Project)
if($volumes){throw 'Target project already has persistent volumes. Choose a fresh name.'}
$env:ORBITA_HTTP_PORT=$HttpPort.ToString()
$compose=@('compose','--project-name',$Project,'--project-directory',$PSScriptRoot)
if($EnvironmentFile){if(-not(Test-Path -LiteralPath $EnvironmentFile -PathType Leaf)){throw 'Environment file does not exist'};$compose+=@('--env-file',(Resolve-Path -LiteralPath $EnvironmentFile).Path)}
$compose+=@('-f',(Join-Path $PSScriptRoot 'compose.yaml'))
if($ComposeOverride){if(-not(Test-Path -LiteralPath $ComposeOverride -PathType Leaf)){throw 'Compose override does not exist'};$compose+=@('-f',(Resolve-Path -LiteralPath $ComposeOverride).Path)}
$started=Get-Date
& docker @compose up -d --wait db
if($LASTEXITCODE -ne 0){throw 'New restore database failed to start'}
$container=& docker @compose ps -q db
& docker cp $source ($container+':/tmp/orbita-restore.dump')
if($LASTEXITCODE -ne 0){throw 'Cannot stage restore archive'}
& docker @compose exec -T db pg_restore --exit-on-error --no-owner --no-acl --clean --if-exists -U orbita -d orbita /tmp/orbita-restore.dump
if($LASTEXITCODE -ne 0){throw 'Restore failed; target remains isolated with app stopped'}
$sql=Get-Content -LiteralPath (Join-Path $PSScriptRoot 'restore-state.sql') -Raw -Encoding UTF8
$OutputEncoding=[Text.UTF8Encoding]::new($false)
$sql | & docker @compose exec -T db psql --set ON_ERROR_STOP=1 -U orbita -d orbita
if($LASTEXITCODE -ne 0){throw 'Cannot establish restored-copy safeguards; app stays stopped'}
& docker @compose up -d --build app
if($LASTEXITCODE -ne 0){throw 'Restored app failed to start'}
Write-Output "Restored into isolated project $Project. Set a new password with reset-password.ps1 -Project $Project."
Write-Output "Database restoration started $($started.ToString('o')); record readiness for reconciliation from the restored application before calculating R121 duration."
