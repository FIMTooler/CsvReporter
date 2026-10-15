# Proof harness for Compare-ImportedCsv. Cross-checks it against Compare-CsvData - the file-path
# sibling this function's design is diffable against - for equivalence over this repo's own
# fixture corpus, then covers the row-shape validation and deserialized-row handling that only
# this function needs, since Import-Csv never hands Compare-CsvData anything to validate.
#Requires -Version 7
. (Join-Path $PSScriptRoot 'Compare-CsvData.ps1')
. (Join-Path $PSScriptRoot 'Compare-ImportedCsv.ps1')
$PSDefaultParameterValues['Compare-CsvData:Encoding'] = 'UTF8'
$PSDefaultParameterValues['Compare-CsvData:DelimiterName'] = 'comma'
$fx = Join-Path $PSScriptRoot '..\tests\fixtures'
$pass = 0; $fail = 0
function Ok($label, $cond, $detail) {
    if ($cond) { $script:pass++; "  PASS  $label" } else { $script:fail++; "  FAIL  $label -- $detail" }
}
$w = Join-Path $env:TEMP 'importedcsvharness'
if (Test-Path $w) { Remove-Item $w -Recurse -Force }
New-Item -ItemType Directory -Force $w | Out-Null
$utf8Bom = New-Object System.Text.UTF8Encoding($true)
function W($n, $t) { $p = Join-Path $w $n; [System.IO.File]::WriteAllText($p, $t, $utf8Bom); $p }

# Order-insensitive content string for a Changes array - neither function guarantees row order.
function Get-SortedContent($Rows) {
    (@($Rows) | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
}

# Runs Compare-CsvData (against the file paths) and Compare-ImportedCsv (against Import-Csv of the
# same files) and asserts they agree. Both functions share every line of comparison logic -
# Compare-ImportedCsv.ps1 is diffable against Compare-CsvData.ps1 bar input acquisition and entry
# validation - so a disagreement here means the diff introduced a real behavioral difference, not
# that the design allows one.
function Test-Equivalence {
    Param($Label, $PrevPath, $CurrPath, $DelimiterName = 'comma', $FileArgs = @{}, $RowArgs = @{})
    $delimChar = if ($DelimiterName -eq 'tab') { "`t" } else { ',' }
    $prevRows = @(Import-Csv -LiteralPath $PrevPath -Encoding UTF8 -Delimiter $delimChar)
    $currRows = @(Import-Csv -LiteralPath $CurrPath -Encoding UTF8 -Delimiter $delimChar)
    $viaFile = Compare-CsvData -PreviousCsvPath $PrevPath -CurrentCsvPath $CurrPath -AnchorColumn 'ID' -DelimiterName $DelimiterName -IncludeSummary @FileArgs
    $viaRows = Compare-ImportedCsv -PreviousRows $prevRows -CurrentRows $currRows -AnchorColumn 'ID' -IncludeSummary @RowArgs
    $sameSummary = $viaFile.Summary.Adds -eq $viaRows.Summary.Adds -and $viaFile.Summary.Updates -eq $viaRows.Summary.Updates -and $viaFile.Summary.Deletes -eq $viaRows.Summary.Deletes -and $viaFile.Summary.Unchanged -eq $viaRows.Summary.Unchanged
    $sameRows = (Get-SortedContent $viaFile.Changes) -ceq (Get-SortedContent $viaRows.Changes)
    Ok "$Label - file and row versions agree" ($sameSummary -and $sameRows) `
       "file: A=$($viaFile.Summary.Adds) U=$($viaFile.Summary.Updates) D=$($viaFile.Summary.Deletes) N=$($viaFile.Summary.Unchanged) -- rows: A=$($viaRows.Summary.Adds) U=$($viaRows.Summary.Updates) D=$($viaRows.Summary.Deletes) N=$($viaRows.Summary.Unchanged) -- rowContentMatch=$sameRows"
}

# The rejection paths (duplicate anchor, mismatched columns) are ported verbatim too, so a genuine
# rejection fixture must throw the IDENTICAL message either way, not just an equally-shaped one.
function Test-RejectionEquivalence($Label, $PrevPath, $CurrPath, $FileArgs = @{}, $RowArgs = @{}) {
    $prevRows = @(Import-Csv -LiteralPath $PrevPath -Encoding UTF8)
    $currRows = @(Import-Csv -LiteralPath $CurrPath -Encoding UTF8)
    $fileMsg = $null; $rowMsg = $null
    try { $null = Compare-CsvData -PreviousCsvPath $PrevPath -CurrentCsvPath $CurrPath -AnchorColumn 'ID' @FileArgs } catch { $fileMsg = $_.Exception.Message }
    try { $null = Compare-ImportedCsv -PreviousRows $prevRows -CurrentRows $currRows -AnchorColumn 'ID' @RowArgs } catch { $rowMsg = $_.Exception.Message }
    Ok "$Label - both reject, identical message" ($null -ne $fileMsg -and $fileMsg -ceq $rowMsg) "file='$fileMsg' rows='$rowMsg'"
}

# Runs a body of statements (the last of which should call Compare-ImportedCsv) inside a fresh
# CHILD process on each host and asserts it throws a message matching $Expect. $Body is inserted
# by variable interpolation, not re-parsed, so its own '$' characters reach the child untouched
# regardless of how $Body itself was authored.
function ThrowsBothHosts($Label, $Body, $Expect) {
    $probe = @"
. '$PSScriptRoot\Compare-ImportedCsv.ps1'
try {
$Body
    'ACCEPTED'
} catch { 'REJECTED: ' + `$_.Exception.Message }
"@
    foreach ($hostExe in @(@('7','pwsh'),@('51','powershell'))) {
        $result = & $hostExe[1] -NoProfile -NonInteractive -Command $probe 2>&1
        Ok "$Label -- PS$($hostExe[0])" ("$result" -match "REJECTED: .*$Expect") "$result"
    }
}

# Marshals a batch of Compare-CsvData/Compare-ImportedCsv calls through a PS5.1 CHILD process back
# to this PS7 outer host, via Export-Clixml/Import-Clixml - the mechanism Test-CompareCsvData.ps1
# proves in its own section 0, reused unchanged here for the return shape (Changes + Summary),
# which both functions build with the identical code. $Calls maps a label to a PowerShell
# EXPRESSION as a string; Test-EquivString/Test-RejectString below (defined in the child, not
# here) do the real work so each label stays a one-line call.
function Invoke-OnPS51([hashtable]$Calls) {
    $xmlPath = Join-Path $w "ps51_$([guid]::NewGuid().ToString('N')).xml"
    $lines = foreach ($label in $Calls.Keys) {
        "try { `$results['$label'] = $($Calls[$label]) } catch { `$results['$label'] = ""ERROR: `$(`$_.Exception.Message)"" }"
    }
    $childScript = @"
`$results = @{}
. '$PSScriptRoot\Compare-CsvData.ps1'
. '$PSScriptRoot\Compare-ImportedCsv.ps1'
`$PSDefaultParameterValues['Compare-CsvData:Encoding'] = 'UTF8'
`$PSDefaultParameterValues['Compare-CsvData:DelimiterName'] = 'comma'
function Get-SortedContent(`$Rows) { (@(`$Rows) | ForEach-Object { (`$_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "``n" }
function Test-EquivString([string]`$PrevPath, [string]`$CurrPath, [string]`$DelimiterName, [hashtable]`$FileArgs, [hashtable]`$RowArgs) {
    `$delimChar = if (`$DelimiterName -eq 'tab') { "``t" } else { ',' }
    `$vp = @(Import-Csv -LiteralPath `$PrevPath -Encoding UTF8 -Delimiter `$delimChar)
    `$vc = @(Import-Csv -LiteralPath `$CurrPath -Encoding UTF8 -Delimiter `$delimChar)
    `$rf = Compare-CsvData -PreviousCsvPath `$PrevPath -CurrentCsvPath `$CurrPath -AnchorColumn 'ID' -DelimiterName `$DelimiterName -IncludeSummary @FileArgs
    `$rr = Compare-ImportedCsv -PreviousRows `$vp -CurrentRows `$vc -AnchorColumn 'ID' -IncludeSummary @RowArgs
    `$sf = Get-SortedContent `$rf.Changes
    `$sr = Get-SortedContent `$rr.Changes
    "A=`$(`$rf.Summary.Adds),U=`$(`$rf.Summary.Updates),D=`$(`$rf.Summary.Deletes),N=`$(`$rf.Summary.Unchanged)|A2=`$(`$rr.Summary.Adds),U2=`$(`$rr.Summary.Updates),D2=`$(`$rr.Summary.Deletes),N2=`$(`$rr.Summary.Unchanged)|match=`$(`$sf -ceq `$sr)"
}
function Test-RejectString([string]`$PrevPath, [string]`$CurrPath, [hashtable]`$FileArgs = @{}, [hashtable]`$RowArgs = @{}) {
    `$vp = @(Import-Csv -LiteralPath `$PrevPath -Encoding UTF8)
    `$vc = @(Import-Csv -LiteralPath `$CurrPath -Encoding UTF8)
    `$fm = `$null; `$rm = `$null
    try { `$null = Compare-CsvData -PreviousCsvPath `$PrevPath -CurrentCsvPath `$CurrPath -AnchorColumn 'ID' @FileArgs } catch { `$fm = `$_.Exception.Message }
    try { `$null = Compare-ImportedCsv -PreviousRows `$vp -CurrentRows `$vc -AnchorColumn 'ID' @RowArgs } catch { `$rm = `$_.Exception.Message }
    "match=`$(`$null -ne `$fm -and `$fm -ceq `$rm)|file=`$fm|rows=`$rm"
}
$($lines -join "`n")
`$results | Export-Clixml -LiteralPath '$xmlPath' -Depth 5
"@
    $childOut = & powershell -NoProfile -NonInteractive -Command $childScript 2>&1
    if (-not (Test-Path -LiteralPath $xmlPath)) { throw "PS5.1 child produced no results file. Child output: $childOut" }
    $imported = Import-Clixml -LiteralPath $xmlPath
    Remove-Item -LiteralPath $xmlPath -Force
    $imported
}
function OkPs51Equiv($Label, $Val) {
    Ok "$Label -- PS5.1" ($Val -is [string] -and $Val -notmatch '^ERROR' -and $Val -match 'match=True') "value='$Val'"
}

"PSVersion: $($PSVersionTable.PSVersion)"
""
"=== 0. self-check: the content comparison this suite relies on can actually detect disagreement ==="
"    proven before section 1 leans on it for every fixture"
$rowsA = @([pscustomobject]@{ ChangeType = 'Update'; ID = '1'; Name = 'Alice' })
$rowsB = @([pscustomobject]@{ ChangeType = 'Update'; ID = '1'; Name = 'Alicia' })
Ok 'Get-SortedContent distinguishes genuinely different rows' ((Get-SortedContent $rowsA) -cne (Get-SortedContent $rowsB)) 'flagged a real difference as none'
Ok 'Get-SortedContent agrees on genuinely identical rows' ((Get-SortedContent $rowsA) -ceq (Get-SortedContent @([pscustomobject]@{ ChangeType = 'Update'; ID = '1'; Name = 'Alice' }))) 'flagged identical rows as different'

""
"=== 1. equivalence: Compare-CsvData (file paths) and Compare-ImportedCsv (Import-Csv of the same"
"    files) agree - over the fixtures Compare-CsvData's own suite already exercises, plus the"
"    parameters (-ExpectedColumns, -ChangeTypeColumnName, -CaseSensitive, -ExcludeFromComparison)"
"    that carry over unchanged ==="
Test-Equivalence 'sparse'         "$fx\sparse\prev.csv"         "$fx\sparse\curr.csv"
Test-Equivalence 'newlines'       "$fx\newlines\prev.csv"       "$fx\newlines\curr.csv"
Test-Equivalence 'symmetric'      "$fx\symmetric\prev.csv"      "$fx\symmetric\curr.csv"
Test-Equivalence 'collation'      "$fx\collation\prev.csv"      "$fx\collation\curr.csv"
Test-Equivalence 'column-order'   "$fx\column-order\prev.csv"   "$fx\column-order\curr.csv"
Test-Equivalence 'empty-values'   "$fx\empty-values\prev.csv"   "$fx\empty-values\curr.csv"
Test-Equivalence 'single-row'     "$fx\single-row\prev.csv"     "$fx\single-row\curr.csv"
Test-Equivalence 'newline-diff'   "$fx\newline-diff\prev.csv"   "$fx\newline-diff\curr.csv"
Test-Equivalence 'terminators (crlf)' "$fx\terminators\crlf_prev.csv" "$fx\terminators\crlf_curr.csv"
Test-Equivalence 'terminators (lf)'   "$fx\terminators\lf_prev.csv"   "$fx\terminators\lf_curr.csv"
Test-Equivalence 'delimiters (tab)' "$fx\delimiters\prev.csv" "$fx\delimiters\curr.csv" -DelimiterName 'tab'
# tests/fixtures/casing/ is NOT used here: it puts 'abc' and 'ABC' on the SAME side, which
# Compare-CsvData always rejects as a duplicate anchor regardless of -CaseSensitive - anchor
# matching is hardcoded case-insensitive there, the switch reaches values only. That fixture was
# built for the family scripts' different design; these inline pairs exercise the switch the way
# Compare-CsvData's own suite does, with the anchor collision on the CROSS-side identity instead.
$caseAnchorPrev = W 'case_anchor_p.csv' "ID,Dept`r`nE1,Ops`r`n"
$caseAnchorCurr = W 'case_anchor_c.csv' "ID,Dept`r`ne1,Ops`r`n"
$caseValuePrev  = W 'case_value_p.csv' "ID,Dept`r`nE1,Ops`r`n"
$caseValueCurr  = W 'case_value_c.csv' "ID,Dept`r`nE1,OPS`r`n"
Test-Equivalence 'anchor case difference, switch off' $caseAnchorPrev $caseAnchorCurr
Test-Equivalence 'anchor case difference, switch on'  $caseAnchorPrev $caseAnchorCurr -FileArgs @{ CaseSensitive = $true } -RowArgs @{ CaseSensitive = $true }
Test-Equivalence 'value case difference, switch off'  $caseValuePrev $caseValueCurr
Test-Equivalence 'value case difference, switch on'   $caseValuePrev $caseValueCurr -FileArgs @{ CaseSensitive = $true } -RowArgs @{ CaseSensitive = $true }
Test-Equivalence 'column-order with -ExpectedColumns' "$fx\column-order\prev.csv" "$fx\column-order\curr.csv" `
    -FileArgs @{ ExpectedColumns = @('ID','Alpha','Beta','Gamma') } -RowArgs @{ ExpectedColumns = @('ID','Alpha','Beta','Gamma') }
Test-Equivalence 'sparse with -ChangeTypeColumnName' "$fx\sparse\prev.csv" "$fx\sparse\curr.csv" `
    -FileArgs @{ ChangeTypeColumnName = 'Verdict' } -RowArgs @{ ChangeTypeColumnName = 'Verdict' }
# exclude-columns has OldOnly in Previous only and NewOnly in Current only, so only an exclusion
# lets it run. The case pair puts 'stamp' on one side and 'Stamp' on the other: two columns, since
# names match exactly, though member access would read either one for the other.
$exAll = @('Stamp','NewOnly','OldOnly')
$exCasePrev = W 'ex_case_p.csv' "ID,Name,stamp`r`n1,A,prevStamp1`r`n2,B,prevStamp2`r`n"
$exCaseCurr = W 'ex_case_c.csv' "ID,Name,Stamp`r`n1,A2,currStamp1`r`n"
Test-Equivalence 'exclude-columns with -ExcludeFromComparison' "$fx\exclude-columns\prev.csv" "$fx\exclude-columns\curr.csv" `
    -FileArgs @{ ExcludeFromComparison = $exAll } -RowArgs @{ ExcludeFromComparison = $exAll }
Test-Equivalence 'exclude-columns, excluded names declared in -ExpectedColumns' "$fx\exclude-columns\prev.csv" "$fx\exclude-columns\curr.csv" `
    -FileArgs @{ ExcludeFromComparison = $exAll; ExpectedColumns = @('ID','Name','Dept','Stamp','NewOnly','OldOnly') } `
    -RowArgs  @{ ExcludeFromComparison = $exAll; ExpectedColumns = @('ID','Name','Dept','Stamp','NewOnly','OldOnly') }
Test-Equivalence 'exclude-columns, excluded names left out of -ExpectedColumns' "$fx\exclude-columns\prev.csv" "$fx\exclude-columns\curr.csv" `
    -FileArgs @{ ExcludeFromComparison = $exAll; ExpectedColumns = @('ID','Name','Dept') } `
    -RowArgs  @{ ExcludeFromComparison = $exAll; ExpectedColumns = @('ID','Name','Dept') }
Test-Equivalence 'exclude-columns, every non-anchor column excluded' "$fx\exclude-columns\prev.csv" "$fx\exclude-columns\curr.csv" `
    -FileArgs @{ ExcludeFromComparison = @('Name','Dept','Stamp','NewOnly','OldOnly') } -RowArgs @{ ExcludeFromComparison = @('Name','Dept','Stamp','NewOnly','OldOnly') }
Test-Equivalence 'Stamp and stamp excluded, one per side' $exCasePrev $exCaseCurr `
    -FileArgs @{ ExcludeFromComparison = @('Stamp','stamp') } -RowArgs @{ ExcludeFromComparison = @('Stamp','stamp') }

"    PS5.1 twin. Each label runs entirely inside the child - Import-Csv, both function calls, and"
"    the comparison all happen there, since Compare-ImportedCsv takes row objects, not paths, so"
"    there is nothing to marshal INTO the child, only the verdict back out"
$calls1 = @{
    sparse          = "Test-EquivString '$fx\sparse\prev.csv' '$fx\sparse\curr.csv' 'comma' @{} @{}"
    newlines        = "Test-EquivString '$fx\newlines\prev.csv' '$fx\newlines\curr.csv' 'comma' @{} @{}"
    symmetric       = "Test-EquivString '$fx\symmetric\prev.csv' '$fx\symmetric\curr.csv' 'comma' @{} @{}"
    collation       = "Test-EquivString '$fx\collation\prev.csv' '$fx\collation\curr.csv' 'comma' @{} @{}"
    columnOrder     = "Test-EquivString '$fx\column-order\prev.csv' '$fx\column-order\curr.csv' 'comma' @{} @{}"
    emptyValues     = "Test-EquivString '$fx\empty-values\prev.csv' '$fx\empty-values\curr.csv' 'comma' @{} @{}"
    singleRow       = "Test-EquivString '$fx\single-row\prev.csv' '$fx\single-row\curr.csv' 'comma' @{} @{}"
    newlineDiff     = "Test-EquivString '$fx\newline-diff\prev.csv' '$fx\newline-diff\curr.csv' 'comma' @{} @{}"
    terminatorsCrlf = "Test-EquivString '$fx\terminators\crlf_prev.csv' '$fx\terminators\crlf_curr.csv' 'comma' @{} @{}"
    terminatorsLf   = "Test-EquivString '$fx\terminators\lf_prev.csv' '$fx\terminators\lf_curr.csv' 'comma' @{} @{}"
    delimitersTab   = "Test-EquivString '$fx\delimiters\prev.csv' '$fx\delimiters\curr.csv' 'tab' @{} @{}"
    anchorCaseOff   = "Test-EquivString '$caseAnchorPrev' '$caseAnchorCurr' 'comma' @{} @{}"
    anchorCaseOn    = "Test-EquivString '$caseAnchorPrev' '$caseAnchorCurr' 'comma' @{CaseSensitive=`$true} @{CaseSensitive=`$true}"
    valueCaseOff    = "Test-EquivString '$caseValuePrev' '$caseValueCurr' 'comma' @{} @{}"
    valueCaseOn     = "Test-EquivString '$caseValuePrev' '$caseValueCurr' 'comma' @{CaseSensitive=`$true} @{CaseSensitive=`$true}"
    expectedColumns = "Test-EquivString '$fx\column-order\prev.csv' '$fx\column-order\curr.csv' 'comma' @{ExpectedColumns=@('ID','Alpha','Beta','Gamma')} @{ExpectedColumns=@('ID','Alpha','Beta','Gamma')}"
    changeTypeName  = "Test-EquivString '$fx\sparse\prev.csv' '$fx\sparse\curr.csv' 'comma' @{ChangeTypeColumnName='Verdict'} @{ChangeTypeColumnName='Verdict'}"
    excludeColumns  = "Test-EquivString '$fx\exclude-columns\prev.csv' '$fx\exclude-columns\curr.csv' 'comma' @{ExcludeFromComparison=@('Stamp','NewOnly','OldOnly')} @{ExcludeFromComparison=@('Stamp','NewOnly','OldOnly')}"
    excludeDeclAll  = "Test-EquivString '$fx\exclude-columns\prev.csv' '$fx\exclude-columns\curr.csv' 'comma' @{ExcludeFromComparison=@('Stamp','NewOnly','OldOnly');ExpectedColumns=@('ID','Name','Dept','Stamp','NewOnly','OldOnly')} @{ExcludeFromComparison=@('Stamp','NewOnly','OldOnly');ExpectedColumns=@('ID','Name','Dept','Stamp','NewOnly','OldOnly')}"
    excludeDeclNone = "Test-EquivString '$fx\exclude-columns\prev.csv' '$fx\exclude-columns\curr.csv' 'comma' @{ExcludeFromComparison=@('Stamp','NewOnly','OldOnly');ExpectedColumns=@('ID','Name','Dept')} @{ExcludeFromComparison=@('Stamp','NewOnly','OldOnly');ExpectedColumns=@('ID','Name','Dept')}"
    excludeEvery    = "Test-EquivString '$fx\exclude-columns\prev.csv' '$fx\exclude-columns\curr.csv' 'comma' @{ExcludeFromComparison=@('Name','Dept','Stamp','NewOnly','OldOnly')} @{ExcludeFromComparison=@('Name','Dept','Stamp','NewOnly','OldOnly')}"
    excludeCasePair = "Test-EquivString '$exCasePrev' '$exCaseCurr' 'comma' @{ExcludeFromComparison=@('Stamp','stamp')} @{ExcludeFromComparison=@('Stamp','stamp')}"
}
$results1 = Invoke-OnPS51 $calls1
foreach ($key in $calls1.Keys) { OkPs51Equiv $key $results1[$key] }

""
"=== 2. equivalence: rejection paths throw the identical message either way, since the anchor and"
"    column-set checks are ported verbatim, not just similarly shaped ==="
Test-RejectionEquivalence 'duplicates'                    "$fx\duplicates\prev.csv"               "$fx\duplicates\curr.csv"
Test-RejectionEquivalence 'mismatch: extra in Current'    "$fx\mismatched-columns\prev.csv"        "$fx\mismatched-columns\curr.csv"
Test-RejectionEquivalence 'mismatch: extra in Previous'   "$fx\mismatched-columns\prev_extra.csv"  "$fx\mismatched-columns\curr_extra.csv"
Test-RejectionEquivalence 'mismatch: renamed column'      "$fx\mismatched-columns\prev_renamed.csv" "$fx\mismatched-columns\curr_renamed.csv"
# The -ExcludeFromComparison rejections. Its -ExpectedColumns mismatch is left out: that message
# says "file" in one function and "collection" in the other, by design.
Test-RejectionEquivalence 'exclude-columns, Stamp alone excluded' "$fx\exclude-columns\prev.csv" "$fx\exclude-columns\curr.csv" `
    -FileArgs @{ ExcludeFromComparison = @('Stamp') } -RowArgs @{ ExcludeFromComparison = @('Stamp') }
Test-RejectionEquivalence 'exclude-columns, a name on neither side' "$fx\exclude-columns\prev.csv" "$fx\exclude-columns\curr.csv" `
    -FileArgs @{ ExcludeFromComparison = @('stamp') } -RowArgs @{ ExcludeFromComparison = @('stamp') }
Test-RejectionEquivalence 'exclude-columns, the anchor excluded' "$fx\exclude-columns\prev.csv" "$fx\exclude-columns\curr.csv" `
    -FileArgs @{ ExcludeFromComparison = @('ID') } -RowArgs @{ ExcludeFromComparison = @('ID') }
$exVerdictPrev = W 'ex_verdict_p.csv' "ID,Name`r`n1,A`r`n"
$exVerdictCurr = W 'ex_verdict_c.csv' "ID,changetype,Name`r`n1,x,A`r`n"
Test-RejectionEquivalence 'an excluded Current-only changetype column collides' $exVerdictPrev $exVerdictCurr `
    -FileArgs @{ ExcludeFromComparison = @('changetype') } -RowArgs @{ ExcludeFromComparison = @('changetype') }

"    PS5.1 twin"
$calls2 = @{
    duplicates       = "Test-RejectString '$fx\duplicates\prev.csv' '$fx\duplicates\curr.csv'"
    mismatchCurrent  = "Test-RejectString '$fx\mismatched-columns\prev.csv' '$fx\mismatched-columns\curr.csv'"
    mismatchPrevious = "Test-RejectString '$fx\mismatched-columns\prev_extra.csv' '$fx\mismatched-columns\curr_extra.csv'"
    mismatchRenamed  = "Test-RejectString '$fx\mismatched-columns\prev_renamed.csv' '$fx\mismatched-columns\curr_renamed.csv'"
    excludeStampOnly = "Test-RejectString '$fx\exclude-columns\prev.csv' '$fx\exclude-columns\curr.csv' @{ExcludeFromComparison=@('Stamp')} @{ExcludeFromComparison=@('Stamp')}"
    excludeWrongCase = "Test-RejectString '$fx\exclude-columns\prev.csv' '$fx\exclude-columns\curr.csv' @{ExcludeFromComparison=@('stamp')} @{ExcludeFromComparison=@('stamp')}"
    excludeAnchor    = "Test-RejectString '$fx\exclude-columns\prev.csv' '$fx\exclude-columns\curr.csv' @{ExcludeFromComparison=@('ID')} @{ExcludeFromComparison=@('ID')}"
    excludeVerdict   = "Test-RejectString '$exVerdictPrev' '$exVerdictCurr' @{ExcludeFromComparison=@('changetype')} @{ExcludeFromComparison=@('changetype')}"
}
$results2 = Invoke-OnPS51 $calls2
foreach ($key in $calls2.Keys) { OkPs51Equiv $key $results2[$key] }

""
"=== 3. entry validation: row-shape rejections only this function can see, since Import-Csv never"
"    hands Compare-CsvData anything to validate ==="
$body3a1 = @'
$prev = @(@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bob"})
$curr = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bobby"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a hashtable row in Previous is rejected, never read as columns' $body3a1 "Previous row 1 is System\.Collections\.Hashtable, not"

$body3a2 = @'
$prev = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bob"})
$curr = @([pscustomobject]@{ID="1";Name="Alice"}, @{ID="2";Name="Bobby"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a hashtable row in Current is rejected too' $body3a2 "Current row 2 is System\.Collections\.Hashtable, not"

$body3b1 = @'
$prev = @([pscustomobject]@{ID=1;Name="Alice"}, [pscustomobject]@{ID="2";Name="Bob"})
$curr = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bobby"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a non-string anchor cell is caught before .Trim() can throw on it' $body3b1 "Previous row 1, column 'ID' is a System\.Int32, not a string"

$body3b2 = @'
$prev = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bob"})
$curr = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID=2;Name="Bobby"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a non-string anchor cell in Current is caught too' $body3b2 "Current row 2, column 'ID' is a System\.Int32, not a string"

$body3c1 = @'
$prev = @([pscustomobject]@{ID="1";Name=$true}, [pscustomobject]@{ID="2";Name="Bob"})
$curr = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bobby"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a non-string non-anchor cell names its own column, not the anchor' $body3c1 "Previous row 1, column 'Name' is a System\.Boolean, not a string"

$body3c2 = @'
$prev = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bob"})
$curr = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name=$true})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a non-string non-anchor cell in Current is caught too' $body3c2 "Current row 2, column 'Name' is a System\.Boolean, not a string"

$body3d1 = @'
$prev = @([pscustomobject]@{ID="1";Name=@("x","y")}, [pscustomobject]@{ID="2";Name="Bob"})
$curr = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bobby"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'an array-valued cell is rejected' $body3d1 "Previous row 1, column 'Name' is a System\.Object\[\], not a string"

$body3d2 = @'
$prev = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bob"})
$curr = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name=@("z")})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a single-element array cell is caught too, not silently unwrapped to its one value' $body3d2 "Current row 2, column 'Name' is a System\.Object\[\], not a string"

$body3e1 = @'
$prev = @([pscustomobject]@{ID="1";Name="Alice";Dept="HR"}, [pscustomobject]@{ID="2";Name="Bob"})
$curr = @([pscustomobject]@{ID="1";Name="Alice";Dept="HR"}, [pscustomobject]@{ID="2";Name="Bobby";Dept="IT"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a Previous row missing a property row 1 carries is rejected' $body3e1 "Previous row 2's columns do not match row 1's"

$body3e2 = @'
$prev = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bob"})
$curr = @([pscustomobject]@{ID="1";Name="Alice"}, [pscustomobject]@{ID="2";Name="Bobby";Extra="x"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a Current row carrying an extra property is rejected too' $body3e2 "Current row 2's columns do not match row 1's"

""
"=== 4. entry validation: an empty side is the parameter binder's rejection, not this function's -"
"    Mandatory alone on an [object[]] parameter already rejects `$null, @(), and a `$null element,"
"    all three, before the function body ever runs (verified directly before writing these) ==="
$body4a1 = @'
$curr = @([pscustomobject]@{ID="1";Name="Alice"})
$null = Compare-ImportedCsv -PreviousRows @() -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'an empty Previous collection is rejected by the parameter binder' $body4a1 "parameter 'PreviousRows' because it is an empty array"

$body4a2 = @'
$prev = @([pscustomobject]@{ID="1";Name="Alice"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows @() -AnchorColumn "ID"
'@
ThrowsBothHosts 'an empty Current collection is rejected the same way' $body4a2 "parameter 'CurrentRows' because it is an empty array"

$body4b1 = @'
$curr = @([pscustomobject]@{ID="1";Name="Alice"})
$null = Compare-ImportedCsv -PreviousRows $null -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a $null Previous is rejected by the parameter binder' $body4b1 "parameter 'PreviousRows' because it is null"

$body4b2 = @'
$prev = @([pscustomobject]@{ID="1";Name="Alice"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $null -AnchorColumn "ID"
'@
ThrowsBothHosts 'a $null Current is rejected the same way' $body4b2 "parameter 'CurrentRows' because it is null"

$body4c1 = @'
$prev = @([pscustomobject]@{ID="1";Name="Alice"}, $null)
$curr = @([pscustomobject]@{ID="1";Name="Alice"})
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a $null element inside Previous is rejected before Test-RowShapes ever sees it' $body4c1 "parameter 'PreviousRows' because it is null"

$body4c2 = @'
$prev = @([pscustomobject]@{ID="1";Name="Alice"})
$curr = @([pscustomobject]@{ID="1";Name="Alice"}, $null)
$null = Compare-ImportedCsv -PreviousRows $prev -CurrentRows $curr -AnchorColumn "ID"
'@
ThrowsBothHosts 'a $null element inside Current is rejected the same way' $body4c2 "parameter 'CurrentRows' because it is null"

"    the real-world instance: Import-Csv of a header-only file yields zero rows, which hits the"
"    SAME binder rejection above - not Compare-CsvData's own 'yielded no rows' file-specific"
"    message, since Compare-ImportedCsv's body never runs to produce it. The one documented gap"
"    between the two functions"
$body4d = @"
`$prev = @(Import-Csv -LiteralPath '$fx\malformed\header_only.csv' -Encoding UTF8)
`$curr = @([pscustomobject]@{ID="1";Name="Alice"})
`$null = Compare-ImportedCsv -PreviousRows `$prev -CurrentRows `$curr -AnchorColumn "ID"
"@
ThrowsBothHosts 'a header-only file, once imported, is rejected as an empty collection - not with Compare-CsvData''s file-specific message' $body4d "parameter 'PreviousRows' because it is an empty array"
$fileMsgHeaderOnly = $null
try { $null = Compare-CsvData -PreviousCsvPath "$fx\malformed\header_only.csv" -CurrentCsvPath "$fx\malformed\good.csv" -AnchorColumn 'ID' } catch { $fileMsgHeaderOnly = $_.Exception.Message }
Ok 'the two messages genuinely differ, confirming this is a real design gap, not a duplicate check' `
   ($null -ne $fileMsgHeaderOnly -and $fileMsgHeaderOnly -notmatch 'empty array') "file version's message: '$fileMsgHeaderOnly'"

""
"=== 5. a row that crossed Export-Clixml/Import-Clixml - losing its live type identity, not just"
"    its column set - still validates and compares correctly ==="
"    PSObject.TypeNames carries the 'Deserialized.' marker after the round trip; GetType() does"
"    not (checked directly before relying on it - the marker lives in TypeNames, not the CLR"
"    type), so the probe below reads TypeNames to prove the row genuinely crossed the boundary"
"    before trusting that Compare-ImportedCsv still accepts it"
$deserialProbe = @"
. '$PSScriptRoot\Compare-CsvData.ps1'
. '$PSScriptRoot\Compare-ImportedCsv.ps1'
`$prevLive = @(Import-Csv -LiteralPath '$fx\sparse\prev.csv' -Encoding UTF8)
`$currLive = @(Import-Csv -LiteralPath '$fx\sparse\curr.csv' -Encoding UTF8)
`$xml = [System.IO.Path]::GetTempFileName()
`$prevLive | Export-Clixml -LiteralPath `$xml -Depth 5
`$prevDeser = @(Import-Clixml -LiteralPath `$xml)
Remove-Item -LiteralPath `$xml -Force
`$isDeserialized = `$prevDeser[0].PSObject.TypeNames[0] -eq 'Deserialized.System.Management.Automation.PSCustomObject'
`$isStillPSCO = `$prevDeser[0] -is [pscustomobject]
`$rLive = Compare-ImportedCsv -PreviousRows `$prevLive -CurrentRows `$currLive -AnchorColumn 'ID' -IncludeSummary
`$rDeser = Compare-ImportedCsv -PreviousRows `$prevDeser -CurrentRows `$currLive -AnchorColumn 'ID' -IncludeSummary
`$sameSummary = `$rLive.Summary.Adds -eq `$rDeser.Summary.Adds -and `$rLive.Summary.Updates -eq `$rDeser.Summary.Updates -and `$rLive.Summary.Deletes -eq `$rDeser.Summary.Deletes -and `$rLive.Summary.Unchanged -eq `$rDeser.Summary.Unchanged
"deserialized=`$isDeserialized|stillPSCO=`$isStillPSCO|resultsMatch=`$sameSummary"
"@
foreach ($hostExe in @(@('7','pwsh'),@('51','powershell'))) {
    $result = & $hostExe[1] -NoProfile -NonInteractive -Command $deserialProbe 2>&1
    Ok "the round trip actually crossed a type boundary -- PS$($hostExe[0])" ("$result" -match 'deserialized=True') "$result"
    Ok "a deserialized row still passes -is [pscustomobject] -- PS$($hostExe[0])" ("$result" -match 'stillPSCO=True') "$result"
    Ok "a deserialized row compares identically to the original -- PS$($hostExe[0])" ("$result" -match 'resultsMatch=True') "$result"
}

""
# A check that silently stops running is worse than one that fails - assert the denominator, the
# same discipline this repo's own verification harnesses hold themselves to.
# Counted BEFORE this assertion runs, so the total printed below is this number plus one.
# Update it deliberately when adding a check - that is the point.
$expected = 103
Ok "all $expected preceding checks ran (guards against a check vanishing)" (($pass + $fail) -eq $expected) "ran $($pass + $fail)"

""
"TOTAL: $pass passed, $fail failed"