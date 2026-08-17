<#
.SYNOPSIS
Generates the large performance/memory fixture used by Invoke-CompareVerification.ps1 -Mode Memory.

.DESCRIPTION
100,000 rows x 50 columns by default, values padded to 20-30 characters, producing roughly 500 Adds,
10,000 Updates, 927 Deletes and 89,073 unchanged rows. Deterministic - regenerating gives
byte-identical files.

Generated rather than stored so the repo folder does not carry the fixture's own size in test data.

This shape - not the original 20,000 x 20 - is what -Mode Memory now uses (G3, 2026-08-09): at 20K x
20 the ~70 MB PowerShell baseline dominated the measurement and the large-vs-small margin was only a
few MB, close enough to run-to-run noise to be worth strengthening. -Cols and -ValueWidth exist so a
shape closer to a real extract can also be requested; 20 narrow columns is small
enough that the scripts look more alike than they are.

.PARAMETER Rows
Data rows in the Previous file. Current gets the same rows less deletions, plus 500 additions.

.PARAMETER Cols
Total columns, including the ID/anchor column.

.PARAMETER ValueWidth
Minimum characters per generated value. Each value is padded to between ValueWidth and
ValueWidth + 10 characters, varying per cell. 0 disables padding, reproducing the original narrow
fixture shape (with -Rows 20000 -Cols 20). Widening multiplies both file size and generation cost.

.PARAMETER OutFolder
Where prev.csv and curr.csv are written.

.EXAMPLE
.\New-LargeFixture.ps1
.\New-LargeFixture.ps1 -Rows 50000 -Cols 30
.\New-LargeFixture.ps1 -Rows 25000 -Cols 50 -ValueWidth 20 -OutFolder .\fixtures\profile-generated
#>
[CmdletBinding()]
Param(
    [int]$Rows = 100000,
    [int]$Cols = 50,
    [int]$ValueWidth = 20,
    [string]$OutFolder = (Join-Path (Join-Path $PSScriptRoot 'fixtures') 'large-generated')
)
$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force $OutFolder | Out-Null
# .NET APIs resolve relative paths against the process current directory, not PowerShell's location.
$OutFolder = (Resolve-Path -LiteralPath $OutFolder).ProviderPath
$repoRoot  = (Resolve-Path -LiteralPath (Split-Path $PSScriptRoot -Parent)).ProviderPath

$dataCols = $Cols - 1                      # one column is the ID/anchor
$header   = 'ID,' + ((1..$dataCols | ForEach-Object { "col$_" }) -join ',')

# Streamed rather than accumulated, and plain loops rather than a pipeline per cell. The previous
# version held both files in StringBuilders and built each row through ForEach-Object: measured
# 839 MB peak and 61.3 s to produce 61 MB, which put the fixture sizes worth profiling out of reach.
$enc = New-Object System.Text.UTF8Encoding($true)
$wp = $null
$wc = $null
try {
    $wp = New-Object System.IO.StreamWriter((Join-Path $OutFolder 'prev.csv'), $false, $enc)
    $wc = New-Object System.IO.StreamWriter((Join-Path $OutFolder 'curr.csv'), $false, $enc)
    $wp.NewLine = "`r`n"; $wc.NewLine = "`r`n"
    $wp.WriteLine($header); $wc.WriteLine($header)

    $fields = [string[]]::new($dataCols)
    # PadRight is a no-op once a value is long enough, so ValueWidth 0 leaves every value byte-exact.
    for ($i = 1; $i -le $Rows; $i++) {
        for ($c = 1; $c -le $dataCols; $c++) {
            $b = "val$($i)_$c"
            $fields[$c - 1] = if ($ValueWidth -gt 0) { $b.PadRight($ValueWidth + (($i + $c) % 11), 'x') } else { $b }
        }
        $line = "$i," + ($fields -join ',')
        $wp.WriteLine($line)
        if ($i % 10 -eq 0) {
            # every 10th row changes col3 -> Update
            $saved = $fields[2]
            $b = "CHANGED$i"
            $fields[2] = if ($ValueWidth -gt 0) { $b.PadRight($ValueWidth + (($i + 3) % 11), 'x') } else { $b }
            $wc.WriteLine("$i," + ($fields -join ','))
            $fields[2] = $saved
        }
        elseif ($i % 97 -ne 0) {
            $wc.WriteLine($line)            # unchanged
        }
        # every 97th row (not already an Update) is omitted from Current -> Delete
    }
    for ($i = $Rows + 1; $i -le $Rows + 500; $i++) {
        for ($c = 1; $c -le $dataCols; $c++) {
            $b = "new$($i)_$c"
            $fields[$c - 1] = if ($ValueWidth -gt 0) { $b.PadRight($ValueWidth + (($i + $c) % 11), 'x') } else { $b }
        }
        $wc.WriteLine("$i," + ($fields -join ','))   # Add
    }
}
finally {
    if ($wp) { $wp.Dispose() }
    if ($wc) { $wc.Dispose() }
}

$kb = 1024
# Printed path is repo-relative so output captured into a log does not carry the local folder
# structure of whoever ran it. $OutFolder itself stays absolute - it is still what every .NET
# call above used.
$outDisplay = if ($OutFolder.StartsWith($repoRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    $OutFolder.Substring($repoRoot.Length).TrimStart('\','/')
} else {
    $OutFolder
}
"prev.csv {0:N0} KB   curr.csv {1:N0} KB   ->  $outDisplay" -f `
    ((Get-Item (Join-Path $OutFolder 'prev.csv')).Length/$kb), ((Get-Item (Join-Path $OutFolder 'curr.csv')).Length/$kb)