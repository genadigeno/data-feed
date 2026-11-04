# Follows a running session: prints its records as each part is committed, each part once.
# Stops when the session has ended and every part has been read.
#   powershell -ExecutionPolicy Bypass -File examples/clients/follow_session.ps1 -Session <session-id>
# With limits.max_workers: 1 in the session, records arrive in the session's own order.
param(
    [Parameter(Mandatory)] [string] $Session,
    [string] $Server = $(if ($env:DATAFEED_SERVER) { $env:DATAFEED_SERVER } else { "http://127.0.0.1:8080" }),
    [string] $Stream = "main",
    [int] $PollMs = 500
)
$ErrorActionPreference = "Stop"
$headers = @{}
if ($env:DATAFEED_TOKEN) { $headers.Authorization = "Bearer $env:DATAFEED_TOKEN" }
$api = "$Server/api/v1/sessions/$Session"
$seen = New-Object 'System.Collections.Generic.HashSet[string]'
while ($true) {
    $state = (Invoke-RestMethod $api -Headers $headers).state
    $parts = @((Invoke-RestMethod "$api/artifacts/manifest" -Headers $headers).parts | Where-Object { $_.stream -eq $Stream })
    $new = 0
    for ($n = 0; $n -lt $parts.Count; $n++) {
        $p = $parts[$n]
        $key = "$($p.unit_seq)/$($p.seq)"
        if ($seen.Contains($key)) { continue }
        $r = Invoke-WebRequest "$api/artifacts/$Stream/parts/$n" -Headers $headers -UseBasicParsing
        # Part numbers shift when several units run at once: the checksum says whether this is the part we meant.
        if ($r.Headers['ETag'].Trim('"') -ne $p.sha256) { break }
        [Text.Encoding]::UTF8.GetString($r.RawContentStream.ToArray()).Split("`n") | Where-Object { $_ } | Write-Output
        [void]$seen.Add($key)
        $new++
    }
    if ($new -eq 0) {
        if ($state -in 'COMPLETED', 'STOPPED', 'FAILED') { break }
        Start-Sleep -Milliseconds $PollMs
    }
}
