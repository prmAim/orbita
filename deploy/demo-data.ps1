param(
    [Parameter(Mandatory)][ValidateSet('Generate','Remove')][string]$Action,
    [ValidatePattern('^[a-z0-9][a-z0-9_-]{0,62}$')][string]$Project='orbita',
    [string]$EnvironmentFile,
    [Parameter(Mandatory)][string]$ComposeOverride,
    [switch]$ValidateOnly,
    [string]$OwnerPasswordFile,
    [string]$MavenRepository,
    [string]$JavaHome,
    [ValidateSet('Standard','Large')][string]$Scenario='Standard'
)
$ErrorActionPreference='Stop'

# Compose/inspect contain local passwords. Keep their output in memory and never echo it.
function Read-DockerJson([string[]]$Arguments) {
    $result=@(& docker @Arguments 2>&1)
    if($LASTEXITCODE -ne 0){throw 'Docker configuration inspection failed; no demo SQL was executed'}
    try { return (($result -join "`n") | ConvertFrom-Json) }
    catch { throw 'Docker returned invalid inspection data; no demo SQL was executed' }
}
function Get-EnvironmentMap($Environment) {
    $values=@{}
    if($Environment -is [System.Array]) {
        foreach($entry in $Environment){$parts=$entry.Split('=',2);if($parts.Length -eq 2){$values[$parts[0]]=$parts[1]}}
    } elseif($null -ne $Environment) {
        foreach($property in $Environment.PSObject.Properties){$values[$property.Name]=[string]$property.Value}
    }
    return $values
}
function Assert-OfflineEnvironment($Environment) {
    $values=Get-EnvironmentMap $Environment
    foreach($name in @('ORBITA_BROKER_ENABLED','ORBITA_TELEGRAM_ENABLED','ORBITA_WORKER_ENABLED','ORBITA_BACKUP_ENABLED')) {
        if($values[$name] -cne 'false'){throw "Preview requires explicit $name=false in configuration and running app"}
    }
    foreach($name in @('ORBITA_SOURCE_ACCOUNT','ORBITA_MIRROR_ACCOUNT','ORBITA_BROKER_TOKEN_FILE','ORBITA_BROKER_TOKEN','ORBITA_TELEGRAM_TOKEN','ORBITA_TELEGRAM_CHAT_ID')) {
        if(-not [string]::IsNullOrWhiteSpace($values[$name])){throw "Preview requires empty $name"}
    }
    foreach($name in $values.Keys) {
        if($name -match '^(SPRING_(APPLICATION_JSON|CONFIG_.*|PROFILES_.*)|JAVA_TOOL_OPTIONS|JDK_JAVA_OPTIONS|_JAVA_OPTIONS|JAVA_OPTS)$' -and -not [string]::IsNullOrWhiteSpace($values[$name])) {
            throw 'Preview refuses additional Spring/JVM configuration overrides'
        }
    }
    if($values['SPRING_DATASOURCE_URL'] -cne 'jdbc:postgresql://db:5432/orbita' -or $values['SPRING_DATASOURCE_USERNAME'] -cne 'orbita') {
        throw 'Preview requires the local Compose orbita database and application role'
    }
}
function Read-RunningContainer([string]$Service) {
    $arguments=$compose+@('ps','--status','running','--quiet',$Service)
    $ids=@(& docker @arguments 2>&1)
    if($LASTEXITCODE -ne 0 -or $ids.Count -ne 1 -or [string]$ids[0] -notmatch '^[a-f0-9]{12,64}$') {
        throw "Preview requires exactly one running $Service container in the selected project"
    }
    $container=@(Read-DockerJson @('inspect',[string]$ids[0]))[0]
    if(-not $container.State.Running -or $container.Config.Labels.'com.docker.compose.project' -cne $Project -or $container.Config.Labels.'com.docker.compose.service' -cne $Service) {
        throw 'Container project/service identity does not match the requested preview'
    }
    return $container
}
function Invoke-DockerChecked([string[]]$Arguments,[string]$FailureMessage) {
    $result=@(& docker @Arguments 2>&1)
    if($LASTEXITCODE -ne 0){throw $FailureMessage}
}
function Invoke-IntegrationScenario {
    if(-not $OwnerPasswordFile -or -not(Test-Path -LiteralPath $OwnerPasswordFile -PathType Leaf)) {
        throw 'Generate requires the existing owner password file; the password is never reset'
    }
    $passwordPath=(Resolve-Path -LiteralPath $OwnerPasswordFile).Path
    if($MavenRepository -and -not(Test-Path -LiteralPath $MavenRepository -PathType Container)){throw 'Maven repository directory does not exist'}
    if($JavaHome -and -not(Test-Path -LiteralPath (Join-Path $JavaHome 'bin/java.exe') -PathType Leaf)){throw 'Java home must contain bin/java.exe'}
    $root=(Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
    $runId=(Get-Date -Format 'yyyyMMddTHHmmss')+'-'+[guid]::NewGuid().ToString('N').Substring(0,8)
    $runDirectory=Join-Path (Join-Path $root '.scratch/interface-preview/runs') $runId
    $null=New-Item -ItemType Directory -Path $runDirectory -Force
    $portOverride=Join-Path $runDirectory 'database-loopback.yaml'
    [IO.File]::WriteAllText($portOverride,@"
services:
  db:
    ports:
      - target: 5432
        published: "0"
        host_ip: 127.0.0.1
        protocol: tcp
"@,[Text.UTF8Encoding]::new($false))
    $temporaryCompose=$compose+@('-f',$portOverride)
    $applicationEnvironment=Get-EnvironmentMap $app.Config.Env
    if([string]::IsNullOrEmpty($applicationEnvironment['SPRING_DATASOURCE_PASSWORD'])){throw 'The selected app has no database password for loopback authentication'}
    $appStopped=$false;$databaseTouched=$false;$scenarioFailure=$null;$restorationFailure=$null
    try {
        $appStopped=$true
        Invoke-DockerChecked @('stop','--time','30',$app.Id) 'Could not stop the exact preview app container'
        $databaseTouched=$true
        Invoke-DockerChecked ($temporaryCompose+@('up','-d','--no-deps','--no-build','--pull','never','--wait','db')) 'Could not expose the preview database on loopback'
        $temporaryDatabase=Read-RunningContainer 'db'
        $bindings=@($temporaryDatabase.NetworkSettings.Ports.'5432/tcp')
        if($bindings.Count -ne 1 -or $bindings[0].HostIp -cne '127.0.0.1' -or $bindings[0].HostPort -notmatch '^\d+$' -or [int]$bindings[0].HostPort -le 0){throw 'Temporary database port is not an exclusive loopback binding'}
        foreach($binding in $temporaryDatabase.HostConfig.PortBindings.PSObject.Properties){if($binding.Name -cne '5432/tcp'){throw 'Unexpected temporary database publication'}}
        $environment=@{
            ORBITA_DEMO_JDBC_URL=('jdbc:postgresql://127.0.0.1:'+ $bindings[0].HostPort +'/orbita')
            ORBITA_DEMO_JDBC_USERNAME='orbita'
            ORBITA_DEMO_JDBC_PASSWORD=$applicationEnvironment['SPRING_DATASOURCE_PASSWORD']
            ORBITA_DEMO_RETAIN='true'
            ORBITA_DEMO_SCENARIO=$Scenario
            ORBITA_DEMO_OWNER_PASSWORD_FILE=$passwordPath
        }
        if($JavaHome){$environment['JAVA_HOME']=(Resolve-Path -LiteralPath $JavaHome).Path}
        $previousEnvironment=@{}
        $arguments=@('-f',(Join-Path $root 'backend/pom.xml'),'-Dtest=ru.orbita.broker.InterfaceDemoIT#wholeApplicationChainCreatesAndRemovesOnlyOwnedPreviewData',('-Dorbita.build.directory='+(Join-Path $runDirectory 'build')),'-DfailIfNoTests=true','test')
        if($MavenRepository){$arguments+=('-Dmaven.repo.local='+(Resolve-Path -LiteralPath $MavenRepository).Path)}
        try {
            foreach($name in $environment.Keys){$previousEnvironment[$name]=[Environment]::GetEnvironmentVariable($name,'Process');[Environment]::SetEnvironmentVariable($name,$environment[$name],'Process')}
            # Only paths/test selectors are process arguments; credentials remain child environment.
            & (Join-Path $root 'backend/mvnw.cmd') @arguments
            if($LASTEXITCODE -ne 0){throw 'The full integration scenario failed; owned partial data may remain for Remove'}
        } finally {
            foreach($name in $previousEnvironment.Keys){[Environment]::SetEnvironmentVariable($name,$previousEnvironment[$name],'Process')}
            $environment.Clear();$previousEnvironment.Clear();$applicationEnvironment.Clear()
        }
    } catch {$scenarioFailure=$_}
    finally {
        try {
            if($databaseTouched){
                Invoke-DockerChecked ($compose+@('up','-d','--no-deps','--no-build','--pull','never','--wait','db')) 'Could not restore the database without its temporary host port'
                $restoredDatabase=Read-RunningContainer 'db'
                if(@($restoredDatabase.HostConfig.PortBindings.PSObject.Properties).Count -gt 0){throw 'Database still has a published host port; app remains stopped'}
            }
            if($appStopped){Invoke-DockerChecked @('start',$app.Id) 'Could not restart the original offline preview app container'}
        } catch {$restorationFailure=$_}
        Remove-Item -LiteralPath $portOverride -ErrorAction SilentlyContinue
    }
    if($restorationFailure){throw 'Preview restoration failed: keep the app stopped and restore the original Compose profile locally before retrying'}
    if($scenarioFailure){throw 'Integration scenario failed; temporary database port was removed and the original offline app restarted. Review the local Maven reports.'}
    Write-Output "Full $Scenario integration scenario completed in project $Project; generated application facts retained. Reports: $runDirectory"
}
function Assert-GenerateBaseline {
    $query="select json_build_object('schema',(select version from public.flyway_schema_history where success order by installed_rank desc limit 1),'manifest',to_regnamespace('orbita_demo_preview') is not null)::text"
    $result=@(& docker exec $database.Id psql --no-psqlrc --tuples-only --no-align --set ON_ERROR_STOP=1 -U orbita -d orbita -c $query 2>&1)
    if($LASTEXITCODE -ne 0){throw 'Cannot qualify the selected schema before Generate; no harness was started'}
    try {$baseline=($result -join "`n") | ConvertFrom-Json} catch {throw 'Invalid schema qualification result; no harness was started'}
    if($baseline.manifest){throw 'Remove the existing preview before regeneration. A version 1 preview must be removed on schema 15 before upgrading.'}
    if($baseline.schema -cne '21'){throw 'Generate requires the updated application and qualified schema 21. Remove an old version 1 preview on schema 15 before upgrading.'}
}

if(-not(Test-Path -LiteralPath $ComposeOverride -PathType Leaf)){throw 'Compose override does not exist'}
$compose=@('compose','--project-name',$Project,'--project-directory',$PSScriptRoot)
if($EnvironmentFile) {
    if(-not(Test-Path -LiteralPath $EnvironmentFile -PathType Leaf)){throw 'Environment file does not exist'}
    $compose+=@('--env-file',(Resolve-Path -LiteralPath $EnvironmentFile).Path)
}
$compose+=@('-f',(Join-Path $PSScriptRoot 'compose.yaml'),'-f',(Resolve-Path -LiteralPath $ComposeOverride).Path)
$configuration=Read-DockerJson ($compose+@('config','--format','json'))
if($configuration.name -cne $Project){throw 'Resolved Compose project does not match'}
if(@($configuration.services.PSObject.Properties | Where-Object Name -notin @('app','db')).Count -gt 0){throw 'Preview requires only the app and db services'}
Assert-OfflineEnvironment $configuration.services.app.environment
if($configuration.services.app.command -or $configuration.services.app.entrypoint){throw 'Preview refuses Compose command/entrypoint overrides'}
foreach($network in $configuration.networks.PSObject.Properties) {
    if($network.Value.external){throw 'Preview refuses external networks'}
}
if($configuration.volumes.database.external -or $configuration.volumes.database.name -cne ($Project+'_database')) {
    throw 'Preview refuses a shared or renamed database volume'
}
$app=Read-RunningContainer 'app'
$database=Read-RunningContainer 'db'
Assert-OfflineEnvironment $app.Config.Env
if(($app.Config.Entrypoint -join '|') -cne 'java|-jar|/app/orbita.jar' -or $app.Config.Cmd) {
    throw 'Preview requires the standard Orbita application entry point without extra arguments'
}
$ports=@($app.HostConfig.PortBindings.'8080/tcp')
if($ports.Count -eq 0){throw 'Preview requires an owner interface bound to loopback'}
foreach($port in $ports){if($port.HostIp -cne '127.0.0.1'){throw 'Preview requires loopback-only HTTP exposure'}}
foreach($binding in $app.HostConfig.PortBindings.PSObject.Properties){if($binding.Name -cne '8080/tcp'){throw 'Preview refuses additional published application ports'}}
if(@($database.HostConfig.PortBindings.PSObject.Properties).Count -gt 0){throw 'Preview refuses a published database port'}
foreach($mount in $app.Mounts) {
    # The maintained runtime inherits the postgres image's anonymous VOLUME. It is not
    # an Orbita database or configuration mount and must not be supplied by Compose.
    if($mount.Destination -eq '/var/lib/postgresql/data' -and $mount.Type -eq 'volume' -and $mount.Name -cmatch '^[a-f0-9]{64}$') {
        if(@($configuration.services.app.volumes | Where-Object target -eq '/var/lib/postgresql/data').Count -gt 0){throw 'Preview refuses an explicitly mounted application PostgreSQL volume'}
        continue
    }
    if($mount.Destination -notin @('/usb','/var/lib/orbita/backups')){throw 'Preview refuses additional app configuration/credential mounts'}
}
$dataMount=@($database.Mounts | Where-Object Destination -eq '/var/lib/postgresql/data')
if($dataMount.Count -ne 1 -or $dataMount[0].Name -cne ($Project+'_database')){throw 'Running database volume identity does not match'}
if($ValidateOnly){Write-Output "Preview configuration verified for project $Project; no database changes.";return}
if($Action -eq 'Generate'){Assert-GenerateBaseline;Invoke-IntegrationScenario;return}

$sqlName='cleanup.sql'
$sqlPath=Join-Path (Join-Path $PSScriptRoot 'demo') $sqlName
if(-not(Test-Path -LiteralPath $sqlPath -PathType Leaf)){throw 'Demo SQL artifact is missing'}
$session=@"
SET orbita.demo.expected_database = 'orbita';
SET orbita.demo.expected_role = 'orbita';
SET orbita.demo.offline_confirmed = 'broker+telegram+worker+backup-disabled;accounts-empty';
"@
$sql=$session+"`n"+(Get-Content -LiteralPath $sqlPath -Raw -Encoding UTF8)
$OutputEncoding=[Text.UTF8Encoding]::new($false)
$result=@($sql | & docker exec -i $database.Id psql --no-psqlrc --set ON_ERROR_STOP=1 -U orbita -d orbita 2>&1)
if($LASTEXITCODE -ne 0){throw 'Demo transaction failed; inspect the selected database locally. Preview configuration stays disabled.'}
Write-Output "Demo $Action completed in project $Project. Keep the same preview override: broker, Telegram, workers and backups remain disabled."
