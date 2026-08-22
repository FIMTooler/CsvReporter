# Proof harness for Compare-CsvData. Exercises the function against this repo's own test
# fixtures and baselines, and cross-checks several behaviors against CompareCSVs_Delta.ps1, the
# sibling script this function's design is derived from.
#Requires -Version 7
. (Join-Path $PSScriptRoot 'Compare-CsvData.ps1')
# Every call needs -Encoding UTF8 now that it is mandatory. Supplied once here so the call
# sites below stay readable; section 7 proves the parameter is still genuinely enforced.
$PSDefaultParameterValues['Compare-CsvData:Encoding'] = 'UTF8'
$PSDefaultParameterValues['Compare-CsvData:DelimiterName'] = 'comma'
$fx = Join-Path $PSScriptRoot '..\tests\fixtures'
$bl = Join-Path $PSScriptRoot '..\tests\baselines'
$pass = 0; $fail = 0
function Ok($label, $cond, $detail) {
    if ($cond) { $script:pass++; "  PASS  $label" } else { $script:fail++; "  FAIL  $label -- $detail" }
}
function Throws($label, $sb, $expect) {
    try { $null = & $sb; Ok $label $false 'did not throw' }
    catch { Ok $label ($_.Exception.Message -match $expect) "msg='$($_.Exception.Message)'" }
}
$w = Join-Path $env:TEMP 'protoharness'
if (Test-Path $w) { Remove-Item $w -Recurse -Force }
New-Item -ItemType Directory -Force $w | Out-Null
$e8 = New-Object System.Text.UTF8Encoding($true)
function W($n,$t){ $p = Join-Path $w $n; [System.IO.File]::WriteAllText($p,$t,$e8); $p }

# Marshals a batch of Compare-CsvData calls through a PS5.1 CHILD process back to this PS7 outer
# host, via Export-Clixml/Import-Clixml - the same architecture tests/Invoke-CompareVerification.ps1
# already uses for cross-version checks. Round-trip fidelity for a row array, the -IncludeSummary
# hashtable and a caught exception's .Message is proven in section 0, not assumed here. $Calls maps
# a label to a PowerShell EXPRESSION as a string, not a scriptblock - it has to run in a separate
# process. A thrown exception is caught inside the child and comes back under the same label as the
# string "ERROR: <message>", never a process crash.
function Invoke-OnPS51([hashtable]$Calls) {
    $xmlPath = Join-Path $w "ps51_$([guid]::NewGuid().ToString('N')).xml"
    $lines = foreach ($label in $Calls.Keys) {
        "try { `$results['$label'] = $($Calls[$label]) } catch { `$results['$label'] = ""ERROR: `$(`$_.Exception.Message)"" }"
    }
    $childScript = @"
`$results = @{}
. '$PSScriptRoot\Compare-CsvData.ps1'
`$PSDefaultParameterValues['Compare-CsvData:Encoding'] = 'UTF8'
`$PSDefaultParameterValues['Compare-CsvData:DelimiterName'] = 'comma'
$($lines -join "`n")
`$results | Export-Clixml -LiteralPath '$xmlPath' -Depth 5
"@
    $childOut = & powershell -NoProfile -NonInteractive -Command $childScript 2>&1
    if (-not (Test-Path -LiteralPath $xmlPath)) { throw "PS5.1 child produced no results file. Child output: $childOut" }
    $imported = Import-Clixml -LiteralPath $xmlPath
    Remove-Item -LiteralPath $xmlPath -Force
    $imported
}
function ThrowsPs51($label, $val, $expect) {
    Ok $label ($val -is [string] -and $val.StartsWith('ERROR: ') -and $val -match $expect) "value='$val'"
}

"PSVersion: $($PSVersionTable.PSVersion)"
""
"=== 0. PS5.1 marshal mechanism: round-trip fidelity proven before being relied on ==="
"    every PS5.1 check from here on depends on Invoke-OnPS51 - a PS5.1 CHILD process runs"
"    Compare-CsvData and hands results back to this PS7 outer host via Export-Clixml/Import-Clixml."
"    Proven here for all three shapes this suite needs, not assumed."
$m0 = Invoke-OnPS51 @{
    arr     = "Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID'"
    summary = "Compare-CsvData -PreviousCsvPath '$fx\column-order\prev.csv' -CurrentCsvPath '$fx\column-order\curr.csv' -AnchorColumn 'ID' -IncludeSummary"
    thrown  = "Compare-CsvData -PreviousCsvPath '$fx\duplicates\prev.csv' -CurrentCsvPath '$fx\duplicates\curr.csv' -AnchorColumn 'ID'"
}
$arr0Live = Compare-CsvData -PreviousCsvPath "$fx\sparse\prev.csv" -CurrentCsvPath "$fx\sparse\curr.csv" -AnchorColumn 'ID'
$arr0LiveSorted = ($arr0Live | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
$arr0Ps51Sorted = ($m0.arr    | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
Ok 'mechanism: array shape - row count survives the round trip' (@($m0.arr).Count -eq @($arr0Live).Count) "PS5.1=$(@($m0.arr).Count) PS7=$(@($arr0Live).Count)"
Ok 'mechanism: array shape - property NAMES and ORDER survive the round trip' `
   ((@($m0.arr[0].PSObject.Properties.Name) -join ',') -ceq (@($arr0Live[0].PSObject.Properties.Name) -join ',')) `
   "PS5.1=$(@($m0.arr[0].PSObject.Properties.Name) -join ',') PS7=$(@($arr0Live[0].PSObject.Properties.Name) -join ',')"
Ok 'mechanism: array shape - row content is byte-exact after the round trip' ($arr0Ps51Sorted -ceq $arr0LiveSorted) "PS5.1=[$arr0Ps51Sorted] PS7=[$arr0LiveSorted]"

$sum0Live = Compare-CsvData -PreviousCsvPath "$fx\column-order\prev.csv" -CurrentCsvPath "$fx\column-order\curr.csv" -AnchorColumn 'ID' -IncludeSummary
Ok 'mechanism: -IncludeSummary shape - Changes + Summary keys survive the round trip' `
   ((($m0.summary.Keys | Sort-Object) -join ',') -ceq (($sum0Live.Keys | Sort-Object) -join ',')) "PS5.1 keys: $(($m0.summary.Keys | Sort-Object) -join ',')"
Ok 'mechanism: -IncludeSummary shape - nested Adds/Updates/Deletes/Unchanged counts survive the round trip' `
   ($m0.summary.Summary.Adds -eq $sum0Live.Summary.Adds -and $m0.summary.Summary.Updates -eq $sum0Live.Summary.Updates -and $m0.summary.Summary.Deletes -eq $sum0Live.Summary.Deletes -and $m0.summary.Summary.Unchanged -eq $sum0Live.Summary.Unchanged) `
   "PS5.1: A=$($m0.summary.Summary.Adds) U=$($m0.summary.Summary.Updates) D=$($m0.summary.Summary.Deletes) N=$($m0.summary.Summary.Unchanged) -- PS7: A=$($sum0Live.Summary.Adds) U=$($sum0Live.Summary.Updates) D=$($sum0Live.Summary.Deletes) N=$($sum0Live.Summary.Unchanged)"

$dupLiveMsg = $null
try { $null = Compare-CsvData -PreviousCsvPath "$fx\duplicates\prev.csv" -CurrentCsvPath "$fx\duplicates\curr.csv" -AnchorColumn 'ID' }
catch { $dupLiveMsg = $_.Exception.Message }
Ok 'mechanism: exception shape - a caught .Message survives the round trip as a labeled ERROR string' `
   ($m0.thrown -is [string] -and $m0.thrown.StartsWith('ERROR: ')) "type=$($m0.thrown.GetType().Name) value=$($m0.thrown)"
Ok 'mechanism: exception shape - the marshaled message text is byte-exact with what PS7 throws directly' `
   ($m0.thrown -ceq "ERROR: $dupLiveMsg") "PS5.1=[$($m0.thrown)] PS7=[ERROR: $dupLiveMsg]"

""
"=== 1. output is byte-identical to CompareCSVs_Delta.ps1's own recorded baselines ==="
"    (exported straight from the return value - no reshaping)"
foreach ($name in 'sparse','newlines','symmetric','collation','column-order') {
    try {
        $rows = Compare-CsvData -PreviousCsvPath "$fx\$name\prev.csv" -CurrentCsvPath "$fx\$name\curr.csv" -AnchorColumn 'ID'
        $mine = Join-Path $w "$name.csv"
        $rows | Export-Csv -LiteralPath $mine -NoTypeInformation -Encoding UTF8
        $a = @(Get-Content $mine); $b = @(Get-Content "$bl\${name}_delta.csv")
        $hdr  = $a[0] -ceq $b[0]
        $body = (($a[1..($a.Count-1)] | Sort-Object) -join "`n") -ceq (($b[1..($b.Count-1)] | Sort-Object) -join "`n")
        Ok "$name header + rows match Delta" ($hdr -and $body) "header=$hdr rows=$body"
    } catch { Ok $name $false "THREW: $($_.Exception.Message)" }
}

"    PS5.1 twin: same five fixtures and baseline comparison,"
"    computed by a PS5.1 CHILD process and marshaled back - mechanism proven in section 0"
$ps51_1 = Invoke-OnPS51 @{
    sparse         = "Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID'"
    newlines       = "Compare-CsvData -PreviousCsvPath '$fx\newlines\prev.csv' -CurrentCsvPath '$fx\newlines\curr.csv' -AnchorColumn 'ID'"
    symmetric      = "Compare-CsvData -PreviousCsvPath '$fx\symmetric\prev.csv' -CurrentCsvPath '$fx\symmetric\curr.csv' -AnchorColumn 'ID'"
    collation      = "Compare-CsvData -PreviousCsvPath '$fx\collation\prev.csv' -CurrentCsvPath '$fx\collation\curr.csv' -AnchorColumn 'ID'"
    'column-order' = "Compare-CsvData -PreviousCsvPath '$fx\column-order\prev.csv' -CurrentCsvPath '$fx\column-order\curr.csv' -AnchorColumn 'ID'"
}
foreach ($name in 'sparse','newlines','symmetric','collation','column-order') {
    $rows51 = $ps51_1[$name]
    if ($rows51 -is [string] -and $rows51.StartsWith('ERROR: ')) { Ok "$name header + rows match Delta -- PS5.1" $false $rows51; continue }
    $mine51 = Join-Path $w "${name}_ps51.csv"
    $rows51 | Export-Csv -LiteralPath $mine51 -NoTypeInformation -Encoding UTF8
    $a51 = @(Get-Content $mine51); $b51 = @(Get-Content "$bl\${name}_delta.csv")
    $hdr51  = $a51[0] -ceq $b51[0]
    $body51 = (($a51[1..($a51.Count-1)] | Sort-Object) -join "`n") -ceq (($b51[1..($b51.Count-1)] | Sort-Object) -join "`n")
    Ok "$name header + rows match Delta -- PS5.1" ($hdr51 -and $body51) "header=$hdr51 rows=$body51"
}

""
"=== 1b. an embedded multi-line value that actually changes registers as Update (TEST-PLAN-Compare-CsvData.md 1.1) ==="
"    section 1's newlines/symmetric fixtures carry a multi-line value identical on both sides;"
"    newline-diff (built for the family's own A8) is the only fixture where it actually differs"
$mlr = Compare-CsvData -PreviousCsvPath "$fx\newline-diff\prev.csv" -CurrentCsvPath "$fx\newline-diff\curr.csv" -AnchorColumn 'ID' -IncludeSummary
$ml1 = $mlr.Changes | Where-Object ID -eq '1'
$ml2 = $mlr.Changes | Where-Object ID -eq '2'
$ml3 = $mlr.Changes | Where-Object ID -eq '3'
Ok 'summary: two Updates, one Unchanged control row' ($mlr.Summary.Updates -eq 2 -and $mlr.Summary.Unchanged -eq 1) "U=$($mlr.Summary.Updates) N=$($mlr.Summary.Unchanged)"
Ok 'a change strictly inside the multi-line value is an Update, full value intact' ($ml1.ChangeType -ceq 'Update' -and $ml1.Notes -ceq "Line1`nLineTHREE") "ChangeType=$($ml1.ChangeType) Notes='$($ml1.Notes)'"
Ok 'unchanged multi-line value beside a real change still carries its own (unchanged) value' ($ml2.ChangeType -ceq 'Update' -and $ml2.Notes -ceq "Same`nSame2") "ChangeType=$($ml2.ChangeType) Notes='$($ml2.Notes)'"
Ok 'fully unchanged control row is absent, not present-and-marked' ($null -eq $ml3) "found: $($null -ne $ml3)"

"    PS5.1 twin"
$ps51_1b = Invoke-OnPS51 @{
    mlr = "Compare-CsvData -PreviousCsvPath '$fx\newline-diff\prev.csv' -CurrentCsvPath '$fx\newline-diff\curr.csv' -AnchorColumn 'ID' -IncludeSummary"
}
$mlr51 = $ps51_1b.mlr
$ml1_51 = $mlr51.Changes | Where-Object ID -eq '1'
$ml2_51 = $mlr51.Changes | Where-Object ID -eq '2'
$ml3_51 = $mlr51.Changes | Where-Object ID -eq '3'
Ok 'summary: two Updates, one Unchanged control row -- PS5.1' ($mlr51.Summary.Updates -eq 2 -and $mlr51.Summary.Unchanged -eq 1) "U=$($mlr51.Summary.Updates) N=$($mlr51.Summary.Unchanged)"
Ok 'a change strictly inside the multi-line value is an Update, full value intact -- PS5.1' ($ml1_51.ChangeType -ceq 'Update' -and $ml1_51.Notes -ceq "Line1`nLineTHREE") "ChangeType=$($ml1_51.ChangeType) Notes='$($ml1_51.Notes)'"
Ok 'unchanged multi-line value beside a real change still carries its own (unchanged) value -- PS5.1' ($ml2_51.ChangeType -ceq 'Update' -and $ml2_51.Notes -ceq "Same`nSame2") "ChangeType=$($ml2_51.ChangeType) Notes='$($ml2_51.Notes)'"
Ok 'fully unchanged control row is absent, not present-and-marked -- PS5.1' ($null -eq $ml3_51) "found: $($null -ne $ml3_51)"

""
"=== 2. return shapes ==="
$rows = Compare-CsvData -PreviousCsvPath "$fx\column-order\prev.csv" -CurrentCsvPath "$fx\column-order\curr.csv" -AnchorColumn 'ID'
Ok 'default returns an array of rows' ($rows -is [array] -and $rows.Count -eq 3) "type=$($rows.GetType().Name) count=$($rows.Count)"
Ok 'row carries ChangeType then source columns' ((@($rows[0].PSObject.Properties.Name) -join ',') -ceq 'ChangeType,Gamma,ID,Alpha,Beta') "$(@($rows[0].PSObject.Properties.Name) -join ',')"
$r = Compare-CsvData -PreviousCsvPath "$fx\column-order\prev.csv" -CurrentCsvPath "$fx\column-order\curr.csv" -AnchorColumn 'ID' -IncludeSummary
Ok '-IncludeSummary returns Changes + Summary only' ((($r.Keys | Sort-Object) -join ',') -ceq 'Changes,Summary') "keys: $(($r.Keys | Sort-Object) -join ',')"
Ok 'summary counts are right' ($r.Summary.Adds -eq 1 -and $r.Summary.Updates -eq 1 -and $r.Summary.Deletes -eq 1 -and $r.Summary.Unchanged -eq 1) `
   "A=$($r.Summary.Adds) U=$($r.Summary.Updates) D=$($r.Summary.Deletes) N=$($r.Summary.Unchanged)"
Ok 'plan: single return value' ((@($r)).Count -eq 1) "got $((@($r)).Count)"

# An empty result must stay an empty array, and a single result must not collapse to a bare object.
$same = "ID,dept`r`nE1,Ops`r`nE2,HR`r`n"
$zp = W 'zp.csv' $same; $zc = W 'zc.csv' $same
$zero = Compare-CsvData -PreviousCsvPath $zp -CurrentCsvPath $zc -AnchorColumn 'ID'
Ok 'zero changes returns an empty array, not $null' ($null -ne $zero -and $zero -is [array] -and $zero.Count -eq 0) "type=$(if($null -eq $zero){'null'}else{$zero.GetType().Name})"
$op = W 'op.csv' "ID,dept`r`nE1,Ops`r`n"; $oc = W 'oc.csv' "ID,dept`r`nE1,Sales`r`n"
$one = Compare-CsvData -PreviousCsvPath $op -CurrentCsvPath $oc -AnchorColumn 'ID'
Ok 'one change stays an array' ($one -is [array] -and $one.Count -eq 1) "type=$($one.GetType().Name) count=$($one.Count)"

"    PS5.1 twin. A Clixml round trip does not preserve the exact"
"    .NET array type (proven in section 0 - it comes back as ArrayList, not object[]), so these"
"    check count and shape, the same behavioral guarantee, rather than re-asserting a container type"
"    that is a property of the marshal channel, not of Compare-CsvData running under PS5.1"
$ps51_2 = Invoke-OnPS51 @{
    rows        = "Compare-CsvData -PreviousCsvPath '$fx\column-order\prev.csv' -CurrentCsvPath '$fx\column-order\curr.csv' -AnchorColumn 'ID'"
    summary     = "Compare-CsvData -PreviousCsvPath '$fx\column-order\prev.csv' -CurrentCsvPath '$fx\column-order\curr.csv' -AnchorColumn 'ID' -IncludeSummary"
    singleCount = "@(Compare-CsvData -PreviousCsvPath '$fx\column-order\prev.csv' -CurrentCsvPath '$fx\column-order\curr.csv' -AnchorColumn 'ID' -IncludeSummary).Count"
    zero        = "Compare-CsvData -PreviousCsvPath '$zp' -CurrentCsvPath '$zc' -AnchorColumn 'ID'"
    one         = "Compare-CsvData -PreviousCsvPath '$op' -CurrentCsvPath '$oc' -AnchorColumn 'ID'"
}
Ok 'default returns 3 rows -- PS5.1' (@($ps51_2.rows).Count -eq 3) "count=$(@($ps51_2.rows).Count)"
Ok 'row carries ChangeType then source columns -- PS5.1' ((@($ps51_2.rows[0].PSObject.Properties.Name) -join ',') -ceq 'ChangeType,Gamma,ID,Alpha,Beta') "$(@($ps51_2.rows[0].PSObject.Properties.Name) -join ',')"
Ok '-IncludeSummary returns Changes + Summary only -- PS5.1' ((($ps51_2.summary.Keys | Sort-Object) -join ',') -ceq 'Changes,Summary') "keys: $(($ps51_2.summary.Keys | Sort-Object) -join ',')"
Ok 'summary counts are right -- PS5.1' ($ps51_2.summary.Summary.Adds -eq 1 -and $ps51_2.summary.Summary.Updates -eq 1 -and $ps51_2.summary.Summary.Deletes -eq 1 -and $ps51_2.summary.Summary.Unchanged -eq 1) `
   "A=$($ps51_2.summary.Summary.Adds) U=$($ps51_2.summary.Summary.Updates) D=$($ps51_2.summary.Summary.Deletes) N=$($ps51_2.summary.Summary.Unchanged)"
Ok 'plan: single return value -- PS5.1' ($ps51_2.singleCount -eq 1) "got $($ps51_2.singleCount)"
Ok 'zero changes returns an empty collection, not $null -- PS5.1' ($null -ne $ps51_2.zero -and @($ps51_2.zero).Count -eq 0) "type=$(if($null -eq $ps51_2.zero){'null'}else{$ps51_2.zero.GetType().Name})"
Ok 'one change stays a one-item collection -- PS5.1' (@($ps51_2.one).Count -eq 1) "count=$(@($ps51_2.one).Count)"

""
"=== 3. -CaseSensitive reaches VALUES only, never anchor identity ==="
$ap = W 'ap.csv' "ID,dept`r`nE1,Ops`r`n"; $ac = W 'ac.csv' "ID,dept`r`ne1,Ops`r`n"
$aOff = Compare-CsvData -PreviousCsvPath $ap -CurrentCsvPath $ac -AnchorColumn 'ID' -IncludeSummary
$aOn  = Compare-CsvData -PreviousCsvPath $ap -CurrentCsvPath $ac -AnchorColumn 'ID' -IncludeSummary -CaseSensitive
Ok 'anchor E1/e1 is ONE row, switch off' ($aOff.Summary.Unchanged -eq 1 -and $aOff.Summary.Adds -eq 0 -and $aOff.Summary.Deletes -eq 0) "A=$($aOff.Summary.Adds) D=$($aOff.Summary.Deletes) N=$($aOff.Summary.Unchanged)"
Ok 'anchor E1/e1 is ONE row, switch ON'  ($aOn.Summary.Unchanged  -eq 1 -and $aOn.Summary.Adds  -eq 0 -and $aOn.Summary.Deletes  -eq 0) "A=$($aOn.Summary.Adds) D=$($aOn.Summary.Deletes) N=$($aOn.Summary.Unchanged) - switch must NOT reach the anchor"
$vp = W 'vp.csv' "ID,dept`r`nE1,Ops`r`n"; $vc = W 'vc.csv' "ID,dept`r`nE1,OPS`r`n"
$vOff = Compare-CsvData -PreviousCsvPath $vp -CurrentCsvPath $vc -AnchorColumn 'ID' -IncludeSummary
$vOn  = Compare-CsvData -PreviousCsvPath $vp -CurrentCsvPath $vc -AnchorColumn 'ID' -IncludeSummary -CaseSensitive
Ok 'value Ops/OPS unchanged, switch off' ($vOff.Summary.Unchanged -eq 1) "N=$($vOff.Summary.Unchanged)"
Ok 'value Ops/OPS is an Update, switch ON' ($vOn.Summary.Updates -eq 1) "U=$($vOn.Summary.Updates) - switch MUST reach values"

"    PS5.1 twin"
$ps51_3 = Invoke-OnPS51 @{
    aOff = "Compare-CsvData -PreviousCsvPath '$ap' -CurrentCsvPath '$ac' -AnchorColumn 'ID' -IncludeSummary"
    aOn  = "Compare-CsvData -PreviousCsvPath '$ap' -CurrentCsvPath '$ac' -AnchorColumn 'ID' -IncludeSummary -CaseSensitive"
    vOff = "Compare-CsvData -PreviousCsvPath '$vp' -CurrentCsvPath '$vc' -AnchorColumn 'ID' -IncludeSummary"
    vOn  = "Compare-CsvData -PreviousCsvPath '$vp' -CurrentCsvPath '$vc' -AnchorColumn 'ID' -IncludeSummary -CaseSensitive"
}
Ok 'anchor E1/e1 is ONE row, switch off -- PS5.1' ($ps51_3.aOff.Summary.Unchanged -eq 1 -and $ps51_3.aOff.Summary.Adds -eq 0 -and $ps51_3.aOff.Summary.Deletes -eq 0) `
   "A=$($ps51_3.aOff.Summary.Adds) D=$($ps51_3.aOff.Summary.Deletes) N=$($ps51_3.aOff.Summary.Unchanged)"
Ok 'anchor E1/e1 is ONE row, switch ON -- PS5.1' ($ps51_3.aOn.Summary.Unchanged -eq 1 -and $ps51_3.aOn.Summary.Adds -eq 0 -and $ps51_3.aOn.Summary.Deletes -eq 0) `
   "A=$($ps51_3.aOn.Summary.Adds) D=$($ps51_3.aOn.Summary.Deletes) N=$($ps51_3.aOn.Summary.Unchanged) - switch must NOT reach the anchor"
Ok 'value Ops/OPS unchanged, switch off -- PS5.1' ($ps51_3.vOff.Summary.Unchanged -eq 1) "N=$($ps51_3.vOff.Summary.Unchanged)"
Ok 'value Ops/OPS is an Update, switch ON -- PS5.1' ($ps51_3.vOn.Summary.Updates -eq 1) "U=$($ps51_3.vOn.Summary.Updates) - switch MUST reach values"

""
"=== 4. rejection cases ==="
Throws 'duplicate anchor (duplicates fixture)' { Compare-CsvData -PreviousCsvPath "$fx\duplicates\prev.csv" -CurrentCsvPath "$fx\duplicates\curr.csv" -AnchorColumn 'ID' } 'Duplicate anchor'
Throws 'header-only Previous'                  { Compare-CsvData -PreviousCsvPath "$fx\malformed\header_only.csv" -CurrentCsvPath "$fx\malformed\good.csv" -AnchorColumn 'ID' } 'yielded no rows'
Throws 'header-only Current'                   { Compare-CsvData -PreviousCsvPath "$fx\malformed\good.csv" -CurrentCsvPath "$fx\malformed\header_only.csv" -AnchorColumn 'ID' } 'yielded no rows'
# The fixture names are counter-intuitive: curr_extra.csv is the COUNTERPART to prev_extra.csv.
# These are the source repo's own pairings.
Throws 'mismatch: extra in Current'  { Compare-CsvData -PreviousCsvPath "$fx\mismatched-columns\prev.csv"         -CurrentCsvPath "$fx\mismatched-columns\curr.csv"         -AnchorColumn 'ID' } 'Column sets differ'
Throws 'mismatch: extra in Previous' { Compare-CsvData -PreviousCsvPath "$fx\mismatched-columns\prev_extra.csv"   -CurrentCsvPath "$fx\mismatched-columns\curr_extra.csv"   -AnchorColumn 'ID' } 'Column sets differ'
Throws 'mismatch: renamed column'    { Compare-CsvData -PreviousCsvPath "$fx\mismatched-columns\prev_renamed.csv" -CurrentCsvPath "$fx\mismatched-columns\curr_renamed.csv" -AnchorColumn 'ID' } 'Column sets differ'
Throws 'anchor name cased wrong'     { Compare-CsvData -PreviousCsvPath "$fx\sparse\prev.csv" -CurrentCsvPath "$fx\sparse\curr.csv" -AnchorColumn 'id' } 'not found'
Throws 'anchor name absent'          { Compare-CsvData -PreviousCsvPath "$fx\sparse\prev.csv" -CurrentCsvPath "$fx\sparse\curr.csv" -AnchorColumn 'Nope' } 'not found'
$bp = W 'bp.csv' "ID,dept`r`n,Ops`r`n"; $bc = W 'bc.csv' "ID,dept`r`nE1,Ops`r`n"
Throws 'blank anchor value in Previous' { Compare-CsvData -PreviousCsvPath $bp -CurrentCsvPath $bc -AnchorColumn 'ID' } 'is empty'
# A column name containing the character a joined-string check would use.
$cp = W 'cp.csv' "ID,`"A,B`",C`r`n1,x,y`r`n"; $cc = W 'cc.csv' "ID,A,`"B,C`"`r`n1,x,y`r`n"
Throws 'comma inside a column name still throws' { Compare-CsvData -PreviousCsvPath $cp -CurrentCsvPath $cc -AnchorColumn 'ID' } 'Column sets differ'
$hp = "$fx\column-order\prev.csv"
$ht = W 'hcase.csv' (([System.IO.File]::ReadAllText("$fx\column-order\curr.csv")) -creplace 'Alpha','alpha')
Throws 'header differing only in case throws' { Compare-CsvData -PreviousCsvPath $hp -CurrentCsvPath $ht -AnchorColumn 'ID' } 'Column sets differ'

"    PS5.1 twin"
$ps51_4 = Invoke-OnPS51 @{
    dup         = "Compare-CsvData -PreviousCsvPath '$fx\duplicates\prev.csv' -CurrentCsvPath '$fx\duplicates\curr.csv' -AnchorColumn 'ID'"
    hdrOnlyP    = "Compare-CsvData -PreviousCsvPath '$fx\malformed\header_only.csv' -CurrentCsvPath '$fx\malformed\good.csv' -AnchorColumn 'ID'"
    hdrOnlyC    = "Compare-CsvData -PreviousCsvPath '$fx\malformed\good.csv' -CurrentCsvPath '$fx\malformed\header_only.csv' -AnchorColumn 'ID'"
    mmCurrent   = "Compare-CsvData -PreviousCsvPath '$fx\mismatched-columns\prev.csv' -CurrentCsvPath '$fx\mismatched-columns\curr.csv' -AnchorColumn 'ID'"
    mmPrevious  = "Compare-CsvData -PreviousCsvPath '$fx\mismatched-columns\prev_extra.csv' -CurrentCsvPath '$fx\mismatched-columns\curr_extra.csv' -AnchorColumn 'ID'"
    mmRenamed   = "Compare-CsvData -PreviousCsvPath '$fx\mismatched-columns\prev_renamed.csv' -CurrentCsvPath '$fx\mismatched-columns\curr_renamed.csv' -AnchorColumn 'ID'"
    caseWrong   = "Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'id'"
    absent      = "Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'Nope'"
    blankAnchor = "Compare-CsvData -PreviousCsvPath '$bp' -CurrentCsvPath '$bc' -AnchorColumn 'ID'"
    commaName   = "Compare-CsvData -PreviousCsvPath '$cp' -CurrentCsvPath '$cc' -AnchorColumn 'ID'"
    caseHeader  = "Compare-CsvData -PreviousCsvPath '$hp' -CurrentCsvPath '$ht' -AnchorColumn 'ID'"
}
ThrowsPs51 'duplicate anchor (duplicates fixture) -- PS5.1' $ps51_4.dup 'Duplicate anchor'
ThrowsPs51 'header-only Previous -- PS5.1'                  $ps51_4.hdrOnlyP 'yielded no rows'
ThrowsPs51 'header-only Current -- PS5.1'                   $ps51_4.hdrOnlyC 'yielded no rows'
ThrowsPs51 'mismatch: extra in Current -- PS5.1'  $ps51_4.mmCurrent 'Column sets differ'
ThrowsPs51 'mismatch: extra in Previous -- PS5.1' $ps51_4.mmPrevious 'Column sets differ'
ThrowsPs51 'mismatch: renamed column -- PS5.1'    $ps51_4.mmRenamed 'Column sets differ'
ThrowsPs51 'anchor name cased wrong -- PS5.1'     $ps51_4.caseWrong 'not found'
ThrowsPs51 'anchor name absent -- PS5.1'          $ps51_4.absent 'not found'
ThrowsPs51 'blank anchor value in Previous -- PS5.1' $ps51_4.blankAnchor 'is empty'
ThrowsPs51 'comma inside a column name still throws -- PS5.1' $ps51_4.commaName 'Column sets differ'
ThrowsPs51 'header differing only in case throws -- PS5.1' $ps51_4.caseHeader 'Column sets differ'

""
"=== 5. runs clean under StrictMode 2.0 ==="
$strictProbe = "Set-StrictMode -Version 2.0; . '$PSScriptRoot\Compare-CsvData.ps1'; (Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID' -Encoding UTF8 -DelimiterName comma).Count"
foreach ($hostExe in @(@('7','pwsh'),@('51','powershell'))) {
    $strict = & $hostExe[1] -NoProfile -Command $strictProbe 2>&1
    Ok "StrictMode 2.0 -- PS$($hostExe[0])" ("$strict" -match '^\d+$') "output: $strict"
}

""
"=== 6. a claim this design makes about the SOURCE script ==="
"    (finding 1 was an unverified citation - this keeps it honest)"
$so = Join-Path $w 'srcout'; New-Item -ItemType Directory -Force $so | Out-Null
$sp = W 'sp.csv' "ID,Alpha`r`n1,a1`r`n2,a2`r`n"
$sc = W 'sc.csv' "ID,alpha`r`n1,a1`r`n2,CHANGED`r`n"
$deltaScript = Join-Path $PSScriptRoot '..\CompareCSVs_Delta.ps1'
$null = & pwsh -NoProfile -File $deltaScript -PreviousCSVFile $sp -CurrentCSVFile $sc -AnchorColumn ID -OutputFolder $so 2>&1
Ok 'source script ACCEPTS a casing-only header change' ([bool](Get-ChildItem $so -Filter *.csv -ErrorAction SilentlyContinue)) 'this design deliberately differs - keep the correction box honest'

""
"=== 6b. -ExpectedColumns catches a column missing from BOTH files ==="
# Export-Csv takes the header from the first object only. Here 'Dept' is absent from the first
# row on both days, so both files lack the column and agree with each other perfectly.
$xp = Join-Path $w 'x_prev.csv'; $xc = Join-Path $w 'x_curr.csv'
@([pscustomobject]@{ ID='E1'; Name='Alice' }, [pscustomobject]@{ ID='E2'; Name='Bob';   Dept='IT' }) | Export-Csv -LiteralPath $xp -NoTypeInformation -Encoding UTF8
@([pscustomobject]@{ ID='E1'; Name='Alice' }, [pscustomobject]@{ ID='E2'; Name='Bobby'; Dept='HR' }) | Export-Csv -LiteralPath $xc -NoTypeInformation -Encoding UTF8
$noDecl = Compare-CsvData -PreviousCsvPath $xp -CurrentCsvPath $xc -AnchorColumn 'ID' -IncludeSummary
Ok 'without -ExpectedColumns the dropped column is invisible' ($noDecl.Summary.Updates -eq 1) "U=$($noDecl.Summary.Updates) - Name change seen, Dept change lost"
Throws 'with -ExpectedColumns it is caught' { Compare-CsvData -PreviousCsvPath $xp -CurrentCsvPath $xc -AnchorColumn 'ID' -ExpectedColumns 'ID','Name','Dept' } 'do not match -ExpectedColumns'
# A matching declaration must not break a good run.
$okRun = Compare-CsvData -PreviousCsvPath "$fx\column-order\prev.csv" -CurrentCsvPath "$fx\column-order\curr.csv" -AnchorColumn 'ID' -ExpectedColumns 'Gamma','ID','Alpha','Beta'
Ok 'a matching declaration passes, order irrelevant' ($okRun.Count -eq 3) "count=$($okRun.Count)"
Throws 'declaration cased wrong is rejected' { Compare-CsvData -PreviousCsvPath "$fx\column-order\prev.csv" -CurrentCsvPath "$fx\column-order\curr.csv" -AnchorColumn 'ID' -ExpectedColumns 'gamma','ID','Alpha','Beta' } 'do not match -ExpectedColumns'

""
"=== 6c. -DelimiterName handles all four of Delta's delimiters ==="
$recs = @([pscustomobject]@{ ID='E1'; Name='Alice, A.'; Dept='HR' }, [pscustomobject]@{ ID='E2'; Name='Bob'; Dept='IT' })
foreach ($d in @(@('comma',','), @('tab',"`t"), @('semicolon',';'), @('pipe','|'))) {
    $dp = Join-Path $w "d_$($d[0])_p.csv"; $dc = Join-Path $w "d_$($d[0])_c.csv"
    $recs | Export-Csv -LiteralPath $dp -NoTypeInformation -Encoding UTF8 -Delimiter $d[1]
    @($recs[0], [pscustomobject]@{ ID='E2'; Name='Bobby'; Dept='IT' }) | Export-Csv -LiteralPath $dc -NoTypeInformation -Encoding UTF8 -Delimiter $d[1]
    $dr = Compare-CsvData -PreviousCsvPath $dp -CurrentCsvPath $dc -AnchorColumn 'ID' -DelimiterName $d[0] -IncludeSummary
    Ok "$($d[0]): one Update, comma inside a value survives" ($dr.Summary.Updates -eq 1 -and $dr.Changes[0].Name -ceq 'Bobby') "U=$($dr.Summary.Updates) Name=$($dr.Changes[0].Name)"
}
Throws 'wrong -DelimiterName is rejected by the anchor check' { Compare-CsvData -PreviousCsvPath (Join-Path $w 'd_semicolon_p.csv') -CurrentCsvPath (Join-Path $w 'd_semicolon_c.csv') -AnchorColumn 'ID' -DelimiterName comma } 'not found'
Throws 'an unsupported delimiter name is rejected' { Compare-CsvData -PreviousCsvPath "$fx\sparse\prev.csv" -CurrentCsvPath "$fx\sparse\curr.csv" -AnchorColumn 'ID' -DelimiterName colon } 'does not belong to the set'

""
"=== 7. -Encoding is genuinely mandatory and genuinely restricted ==="
"    (the harness supplies it via `$PSDefaultParameterValues, so prove it is still enforced)"
$ansiProbe = @"
. '$PSScriptRoot\Compare-CsvData.ps1'
try { `$null = Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID' -DelimiterName comma -Encoding ansi -ErrorAction Stop; 'ACCEPTED' }
catch { 'REJECTED: ' + `$_.Exception.Message }
"@
$mandProbe = @"
. '$PSScriptRoot\Compare-CsvData.ps1'
try { `$null = Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID' -DelimiterName comma -ErrorAction Stop; 'ACCEPTED' }
catch { 'BLOCKED' }
"@
foreach ($hostExe in @(@('7','pwsh'),@('51','powershell'))) {
    $probe = & $hostExe[1] -NoProfile -Command $ansiProbe 2>&1
    Ok "-Encoding ansi is rejected -- PS$($hostExe[0])" ("$probe" -match 'REJECTED') "$probe"
    $mand = & $hostExe[1] -NoProfile -NonInteractive -Command $mandProbe 2>&1
    Ok "-Encoding cannot be omitted -- PS$($hostExe[0])" ("$mand" -notmatch 'ACCEPTED') "$mand"
}

""
"=== 8. both sides of every per-row check ==="
"    (the Current-side checks run first, so a fixture with faults on both sides"
"     never reaches the Previous-side branch - each needs a one-sided case)"
$g1p = W 'g1_p.csv' "ID,dept`r`nE1,Ops`r`n"
$g1c = W 'g1_c.csv' "ID,dept`r`n,Ops`r`n"
Throws 'blank anchor in CURRENT'   { Compare-CsvData -PreviousCsvPath $g1p -CurrentCsvPath $g1c -AnchorColumn 'ID' } 'is empty in Current'
$g2p = W 'g2_p.csv' "ID,dept`r`nE1,Ops`r`nE1,HR`r`n"
$g2c = W 'g2_c.csv' "ID,dept`r`nE1,Ops`r`n"
Throws 'duplicate anchor in PREVIOUS' { Compare-CsvData -PreviousCsvPath $g2p -CurrentCsvPath $g2c -AnchorColumn 'ID' } 'Duplicate anchor .* in Previous'
$g3p = W 'g3_p.csv' "ID,dept`r`nE1,Ops`r`n"
$g3c = W 'g3_c.csv' "ID,dept`r`nE1,Ops`r`nE1,HR`r`n"
Throws 'duplicate anchor in CURRENT'  { Compare-CsvData -PreviousCsvPath $g3p -CurrentCsvPath $g3c -AnchorColumn 'ID' } 'Duplicate anchor .* in Current'

"    PS5.1 twin"
$ps51_8 = Invoke-OnPS51 @{
    blankCurrent = "Compare-CsvData -PreviousCsvPath '$g1p' -CurrentCsvPath '$g1c' -AnchorColumn 'ID'"
    dupPrevious  = "Compare-CsvData -PreviousCsvPath '$g2p' -CurrentCsvPath '$g2c' -AnchorColumn 'ID'"
    dupCurrent   = "Compare-CsvData -PreviousCsvPath '$g3p' -CurrentCsvPath '$g3c' -AnchorColumn 'ID'"
}
ThrowsPs51 'blank anchor in CURRENT -- PS5.1' $ps51_8.blankCurrent 'is empty in Current'
ThrowsPs51 'duplicate anchor in PREVIOUS -- PS5.1' $ps51_8.dupPrevious 'Duplicate anchor .* in Previous'
ThrowsPs51 'duplicate anchor in CURRENT -- PS5.1' $ps51_8.dupCurrent 'Duplicate anchor .* in Current'

""
"=== 9. absent-value normalisation (null / empty / whitespace count as equal) ==="
"    the one deliberate divergence from CompareCSVs_Delta.ps1, previously untested"
function NormCase($label, $prevVal, $currVal, $expectUpdate) {
    $np = W "n_$($label -replace '\W','')_p.csv" "ID,A,B`r`nE1,$prevVal,keep`r`n"
    $nc = W "n_$($label -replace '\W','')_c.csv" "ID,A,B`r`nE1,$currVal,keep`r`n"
    $nr = Compare-CsvData -PreviousCsvPath $np -CurrentCsvPath $nc -AnchorColumn 'ID' -IncludeSummary
    $got = $nr.Summary.Updates -eq 1
    Ok $label ($got -eq $expectUpdate) "expected update=$expectUpdate got U=$($nr.Summary.Updates) N=$($nr.Summary.Unchanged)"
}
NormCase 'empty vs empty -> unchanged'          ''      ''      $false
NormCase 'whitespace vs empty -> unchanged'     '"   "' ''      $false
NormCase 'whitespace vs whitespace -> unchanged' '" "'  '"    "' $false
NormCase 'empty vs value -> Update (added)'     ''      'Ops'   $true
NormCase 'value vs empty -> Update (cleared)'   'Ops'   ''      $true
NormCase 'value vs value differing -> Update'   'Ops'   'Sales' $true

"    PS5.1 twin. Same six value pairs NormCase builds inline, written to their own files here"
"    since NormCase's file paths aren't exposed for reuse across a process boundary"
$n1p = W 'n51_1_p.csv' "ID,A,B`r`nE1,,keep`r`n";           $n1c = W 'n51_1_c.csv' "ID,A,B`r`nE1,,keep`r`n"
$n2p = W 'n51_2_p.csv' "ID,A,B`r`nE1,`"   `",keep`r`n";    $n2c = W 'n51_2_c.csv' "ID,A,B`r`nE1,,keep`r`n"
$n3p = W 'n51_3_p.csv' "ID,A,B`r`nE1,`" `",keep`r`n";      $n3c = W 'n51_3_c.csv' "ID,A,B`r`nE1,`"    `",keep`r`n"
$n4p = W 'n51_4_p.csv' "ID,A,B`r`nE1,,keep`r`n";           $n4c = W 'n51_4_c.csv' "ID,A,B`r`nE1,Ops,keep`r`n"
$n5p = W 'n51_5_p.csv' "ID,A,B`r`nE1,Ops,keep`r`n";        $n5c = W 'n51_5_c.csv' "ID,A,B`r`nE1,,keep`r`n"
$n6p = W 'n51_6_p.csv' "ID,A,B`r`nE1,Ops,keep`r`n";        $n6c = W 'n51_6_c.csv' "ID,A,B`r`nE1,Sales,keep`r`n"
$ps51_9 = Invoke-OnPS51 @{
    c1 = "Compare-CsvData -PreviousCsvPath '$n1p' -CurrentCsvPath '$n1c' -AnchorColumn 'ID' -IncludeSummary"
    c2 = "Compare-CsvData -PreviousCsvPath '$n2p' -CurrentCsvPath '$n2c' -AnchorColumn 'ID' -IncludeSummary"
    c3 = "Compare-CsvData -PreviousCsvPath '$n3p' -CurrentCsvPath '$n3c' -AnchorColumn 'ID' -IncludeSummary"
    c4 = "Compare-CsvData -PreviousCsvPath '$n4p' -CurrentCsvPath '$n4c' -AnchorColumn 'ID' -IncludeSummary"
    c5 = "Compare-CsvData -PreviousCsvPath '$n5p' -CurrentCsvPath '$n5c' -AnchorColumn 'ID' -IncludeSummary"
    c6 = "Compare-CsvData -PreviousCsvPath '$n6p' -CurrentCsvPath '$n6c' -AnchorColumn 'ID' -IncludeSummary"
}
Ok 'empty vs empty -> unchanged -- PS5.1' ($ps51_9.c1.Summary.Updates -eq 0) "U=$($ps51_9.c1.Summary.Updates) N=$($ps51_9.c1.Summary.Unchanged)"
Ok 'whitespace vs empty -> unchanged -- PS5.1' ($ps51_9.c2.Summary.Updates -eq 0) "U=$($ps51_9.c2.Summary.Updates) N=$($ps51_9.c2.Summary.Unchanged)"
Ok 'whitespace vs whitespace -> unchanged -- PS5.1' ($ps51_9.c3.Summary.Updates -eq 0) "U=$($ps51_9.c3.Summary.Updates) N=$($ps51_9.c3.Summary.Unchanged)"
Ok 'empty vs value -> Update (added) -- PS5.1' ($ps51_9.c4.Summary.Updates -eq 1) "U=$($ps51_9.c4.Summary.Updates) N=$($ps51_9.c4.Summary.Unchanged)"
Ok 'value vs empty -> Update (cleared) -- PS5.1' ($ps51_9.c5.Summary.Updates -eq 1) "U=$($ps51_9.c5.Summary.Updates) N=$($ps51_9.c5.Summary.Unchanged)"
Ok 'value vs value differing -> Update -- PS5.1' ($ps51_9.c6.Summary.Updates -eq 1) "U=$($ps51_9.c6.Summary.Updates) N=$($ps51_9.c6.Summary.Unchanged)"

""
"=== 10. a ragged row does not manufacture a false Update ==="
"    Import-Csv gives `$null for a short row's missing trailing field; `$null -ne '' without"
"    the normalisation, so this would otherwise report a change on an empty-either-way column"
$rp = W 'r_p.csv' "ID,A,B`r`nE1,x,`r`n"
$rc = W 'r_c.csv' "ID,A,B`r`nE1,x`r`n"
$rr = Compare-CsvData -PreviousCsvPath $rp -CurrentCsvPath $rc -AnchorColumn 'ID' -IncludeSummary
Ok 'short row vs empty cell is Unchanged' ($rr.Summary.Unchanged -eq 1 -and $rr.Summary.Updates -eq 0) "U=$($rr.Summary.Updates) N=$($rr.Summary.Unchanged)"
$r2p = W 'r2_p.csv' "ID,A,B`r`nE1,x,HR`r`n"
$r2c = W 'r2_c.csv' "ID,A,B`r`nE1,x`r`n"
$rr2 = Compare-CsvData -PreviousCsvPath $r2p -CurrentCsvPath $r2c -AnchorColumn 'ID' -IncludeSummary
Ok 'short row against a real value is still an Update' ($rr2.Summary.Updates -eq 1) "U=$($rr2.Summary.Updates) - a cleared value must still register"

"    PS5.1 twin"
$ps51_10 = Invoke-OnPS51 @{
    ragged1 = "Compare-CsvData -PreviousCsvPath '$rp' -CurrentCsvPath '$rc' -AnchorColumn 'ID' -IncludeSummary"
    ragged2 = "Compare-CsvData -PreviousCsvPath '$r2p' -CurrentCsvPath '$r2c' -AnchorColumn 'ID' -IncludeSummary"
}
Ok 'short row vs empty cell is Unchanged -- PS5.1' ($ps51_10.ragged1.Summary.Unchanged -eq 1 -and $ps51_10.ragged1.Summary.Updates -eq 0) "U=$($ps51_10.ragged1.Summary.Updates) N=$($ps51_10.ragged1.Summary.Unchanged)"
Ok 'short row against a real value is still an Update -- PS5.1' ($ps51_10.ragged2.Summary.Updates -eq 1) "U=$($ps51_10.ragged2.Summary.Updates) - a cleared value must still register"

""
"=== 11. verdict-column collision (found by independent review, 2026-08-19) ==="
"    a source column named ChangeType overwrote the verdict with its own data, silently"
$vp = W 'v_p.csv' "ID,ChangeType,dept`r`nE1,Active,Ops`r`n"
$vc = W 'v_c.csv' "ID,ChangeType,dept`r`nE1,Active,Sales`r`n"
Throws 'source column ChangeType is rejected' { Compare-CsvData -PreviousCsvPath $vp -CurrentCsvPath $vc -AnchorColumn 'ID' } 'collides with the verdict column'
$lp = W 'l_p.csv' "ID,changetype,dept`r`nE1,Active,Ops`r`n"
$lc = W 'l_c.csv' "ID,changetype,dept`r`nE1,Active,Sales`r`n"
Throws 'lowercase changetype also rejected' { Compare-CsvData -PreviousCsvPath $lp -CurrentCsvPath $lc -AnchorColumn 'ID' } 'collides with the verdict column'
# Renaming the verdict column is the way through, exactly as Delta allows. The name below is an
# arbitrary caller-chosen string with no significance - it is here only to show that whatever is
# passed becomes the column name, leaving the source's own ChangeType column untouched beside it.
$anyName = 'PickAnyNameYouLike'
$vr = Compare-CsvData -PreviousCsvPath $vp -CurrentCsvPath $vc -AnchorColumn 'ID' -ChangeTypeColumnName $anyName
Ok '-ChangeTypeColumnName resolves the collision' ((@($vr[0].PSObject.Properties.Name) -join ',') -ceq "$anyName,ID,ChangeType,dept") "$(@($vr[0].PSObject.Properties.Name) -join ',')"
Ok 'renamed verdict still carries the verdict' ($vr[0].$anyName -ceq 'Update' -and $vr[0].ChangeType -ceq 'Active') "$anyName=$($vr[0].$anyName) ChangeType=$($vr[0].ChangeType)"
Throws 'blank -ChangeTypeColumnName rejected' { Compare-CsvData -PreviousCsvPath "$fx\sparse\prev.csv" -CurrentCsvPath "$fx\sparse\curr.csv" -AnchorColumn 'ID' -ChangeTypeColumnName '   ' } 'cannot be empty or whitespace'
# The default name must still be what Delta emits.
$dv = Compare-CsvData -PreviousCsvPath "$fx\column-order\prev.csv" -CurrentCsvPath "$fx\column-order\curr.csv" -AnchorColumn 'ID'
Ok 'default verdict column is still ChangeType' ((@($dv[0].PSObject.Properties.Name)[0]) -ceq 'ChangeType') "$(@($dv[0].PSObject.Properties.Name)[0])"

"    PS5.1 twin"
$ps51_11 = Invoke-OnPS51 @{
    collision      = "Compare-CsvData -PreviousCsvPath '$vp' -CurrentCsvPath '$vc' -AnchorColumn 'ID'"
    collisionLower = "Compare-CsvData -PreviousCsvPath '$lp' -CurrentCsvPath '$lc' -AnchorColumn 'ID'"
    renamed        = "Compare-CsvData -PreviousCsvPath '$vp' -CurrentCsvPath '$vc' -AnchorColumn 'ID' -ChangeTypeColumnName '$anyName'"
    blankName      = "Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID' -ChangeTypeColumnName '   '"
    defaultCol     = "Compare-CsvData -PreviousCsvPath '$fx\column-order\prev.csv' -CurrentCsvPath '$fx\column-order\curr.csv' -AnchorColumn 'ID'"
}
ThrowsPs51 'source column ChangeType is rejected -- PS5.1' $ps51_11.collision 'collides with the verdict column'
ThrowsPs51 'lowercase changetype also rejected -- PS5.1' $ps51_11.collisionLower 'collides with the verdict column'
Ok '-ChangeTypeColumnName resolves the collision -- PS5.1' ((@($ps51_11.renamed[0].PSObject.Properties.Name) -join ',') -ceq "$anyName,ID,ChangeType,dept") "$(@($ps51_11.renamed[0].PSObject.Properties.Name) -join ',')"
Ok 'renamed verdict still carries the verdict -- PS5.1' ($ps51_11.renamed[0].$anyName -ceq 'Update' -and $ps51_11.renamed[0].ChangeType -ceq 'Active') "$anyName=$($ps51_11.renamed[0].$anyName) ChangeType=$($ps51_11.renamed[0].ChangeType)"
ThrowsPs51 'blank -ChangeTypeColumnName rejected -- PS5.1' $ps51_11.blankName 'cannot be empty or whitespace'
Ok 'default verdict column is still ChangeType -- PS5.1' ((@($ps51_11.defaultCol[0].PSObject.Properties.Name)[0]) -ceq 'ChangeType') "$(@($ps51_11.defaultCol[0].PSObject.Properties.Name)[0])"

""
"=== 12. padded anchor values (found by independent review, 2026-08-19) ==="
"    the CsvReporter family scripts split this into a Delete plus an Add, not a single Update"
$pp = W 'pad_p.csv' "ID,dept`r`n`" E1`",Ops`r`nE2,HR`r`n"
$pc = W 'pad_c.csv' "ID,dept`r`nE1,Ops`r`nE2,HR`r`n"
$pr = Compare-CsvData -PreviousCsvPath $pp -CurrentCsvPath $pc -AnchorColumn 'ID' -IncludeSummary
Ok 'padded anchor is ONE row, not Add+Delete' ($pr.Summary.Unchanged -eq 2 -and $pr.Summary.Adds -eq 0 -and $pr.Summary.Deletes -eq 0) `
   "A=$($pr.Summary.Adds) D=$($pr.Summary.Deletes) N=$($pr.Summary.Unchanged)"
# Trimming is identity-only: an emitted row keeps the file's own value.
$dp = W 'pad_d_p.csv' "ID,dept`r`n`" E9 `",Ops`r`n"
$dc = W 'pad_d_c.csv' "ID,dept`r`nE1,Ops`r`n"
$dr = Compare-CsvData -PreviousCsvPath $dp -CurrentCsvPath $dc -AnchorColumn 'ID'
$del = $dr | Where-Object ChangeType -eq 'Delete'
Ok 'a Delete row keeps the untrimmed anchor value' ($del.ID -ceq ' E9 ') "got '$($del.ID)'"
# Two spellings of one anchor inside ONE file are now a duplicate, which is correct.
$sp = W 'pad_s_p.csv' "ID,dept`r`n`" E1`",Ops`r`nE1,HR`r`n"
$sc = W 'pad_s_c.csv' "ID,dept`r`nE1,Ops`r`n"
Throws 'padded + unpadded in one file is a duplicate' { Compare-CsvData -PreviousCsvPath $sp -CurrentCsvPath $sc -AnchorColumn 'ID' } 'Duplicate anchor'
# The asymmetry must hold: values are NOT trimmed.
$vp2 = W 'pad_v_p.csv' "ID,dept`r`nE1,`"Ops `"`r`n"
$vc2 = W 'pad_v_c.csv' "ID,dept`r`nE1,Ops`r`n"
$vr2 = Compare-CsvData -PreviousCsvPath $vp2 -CurrentCsvPath $vc2 -AnchorColumn 'ID' -IncludeSummary
Ok 'a padded VALUE is still a real difference' ($vr2.Summary.Updates -eq 1) "U=$($vr2.Summary.Updates) - values must not be trimmed"

"    PS5.1 twin"
$ps51_12 = Invoke-OnPS51 @{
    padAnchor = "Compare-CsvData -PreviousCsvPath '$pp' -CurrentCsvPath '$pc' -AnchorColumn 'ID' -IncludeSummary"
    padDelete = "Compare-CsvData -PreviousCsvPath '$dp' -CurrentCsvPath '$dc' -AnchorColumn 'ID'"
    padDup    = "Compare-CsvData -PreviousCsvPath '$sp' -CurrentCsvPath '$sc' -AnchorColumn 'ID'"
    padValue  = "Compare-CsvData -PreviousCsvPath '$vp2' -CurrentCsvPath '$vc2' -AnchorColumn 'ID' -IncludeSummary"
}
Ok 'padded anchor is ONE row, not Add+Delete -- PS5.1' ($ps51_12.padAnchor.Summary.Unchanged -eq 2 -and $ps51_12.padAnchor.Summary.Adds -eq 0 -and $ps51_12.padAnchor.Summary.Deletes -eq 0) `
   "A=$($ps51_12.padAnchor.Summary.Adds) D=$($ps51_12.padAnchor.Summary.Deletes) N=$($ps51_12.padAnchor.Summary.Unchanged)"
$del51 = @($ps51_12.padDelete) | Where-Object ChangeType -eq 'Delete'
Ok 'a Delete row keeps the untrimmed anchor value -- PS5.1' ($del51.ID -ceq ' E9 ') "got '$($del51.ID)'"
ThrowsPs51 'padded + unpadded in one file is a duplicate -- PS5.1' $ps51_12.padDup 'Duplicate anchor'
Ok 'a padded VALUE is still a real difference -- PS5.1' ($ps51_12.padValue.Summary.Updates -eq 1) "U=$($ps51_12.padValue.Summary.Updates) - values must not be trimmed"

""
"=== 13. a bad -ExpectedColumns blames the declaration, not the file ==="
"    every case below uses two perfectly correct files"
$ep = W 'e_p.csv' "ID,Name`r`nE1,Alice`r`n"
$ec = W 'e_c.csv' "ID,Name`r`nE1,Alicia`r`n"
Throws 'whitespace-only entry names itself' { Compare-CsvData -PreviousCsvPath $ep -CurrentCsvPath $ec -AnchorColumn 'ID' -ExpectedColumns 'ID','Name','   ' } 'empty or whitespace-only entry'
# Assert the content - which spellings are named - not the surrounding phrasing. Two checks here
# broke on a wording change once already, which is what asserting on prose gets you.
Throws 'duplicated entry names the duplicate' { Compare-CsvData -PreviousCsvPath $ep -CurrentCsvPath $ec -AnchorColumn 'ID' -ExpectedColumns 'ID','Name','Name' } "'Name' twice"
Throws 'case-only duplicate is caught too' { Compare-CsvData -PreviousCsvPath $ep -CurrentCsvPath $ec -AnchorColumn 'ID' -ExpectedColumns 'ID','Name','name' } "'Name' and 'name'"
# The declaration is checked before either file is opened - a nonexistent path must not be reached.
$nope = Join-Path $PSScriptRoot 'nope\nope.csv'
Throws 'declaration validated before any file read' { Compare-CsvData -PreviousCsvPath $nope -CurrentCsvPath $nope -AnchorColumn 'ID' -ExpectedColumns 'ID','   ' } 'empty or whitespace-only entry'
# Names are quoted in every set message, so padding and whitespace are visible.
try { $null = Compare-CsvData -PreviousCsvPath $ep -CurrentCsvPath $ec -AnchorColumn 'ID' -ExpectedColumns 'ID',' Name'
      Ok 'a padded name is visible in the message' $false 'did not throw' }
catch { Ok 'a padded name is visible in the message' ($_.Exception.Message -match "' Name'") "$($_.Exception.Message)" }
$mp = W 'm_p.csv' "ID,Name`r`nE1,Alice`r`n"
$mc = W 'm_c.csv' "ID,Dept`r`nE1,HR`r`n"
try { $null = Compare-CsvData -PreviousCsvPath $mp -CurrentCsvPath $mc -AnchorColumn 'ID'
      Ok 'file-vs-file message quotes names too' $false 'did not throw' }
catch { Ok 'file-vs-file message quotes names too' ($_.Exception.Message -match "'ID' \| 'Name'") "$($_.Exception.Message)" }

"    PS5.1 twin"
$ps51_13 = Invoke-OnPS51 @{
    wsEntry    = "Compare-CsvData -PreviousCsvPath '$ep' -CurrentCsvPath '$ec' -AnchorColumn 'ID' -ExpectedColumns 'ID','Name','   '"
    dupEntry   = "Compare-CsvData -PreviousCsvPath '$ep' -CurrentCsvPath '$ec' -AnchorColumn 'ID' -ExpectedColumns 'ID','Name','Name'"
    caseDup    = "Compare-CsvData -PreviousCsvPath '$ep' -CurrentCsvPath '$ec' -AnchorColumn 'ID' -ExpectedColumns 'ID','Name','name'"
    beforeRead = "Compare-CsvData -PreviousCsvPath '$nope' -CurrentCsvPath '$nope' -AnchorColumn 'ID' -ExpectedColumns 'ID','   '"
    paddedName = "Compare-CsvData -PreviousCsvPath '$ep' -CurrentCsvPath '$ec' -AnchorColumn 'ID' -ExpectedColumns 'ID',' Name'"
    fileVsFile = "Compare-CsvData -PreviousCsvPath '$mp' -CurrentCsvPath '$mc' -AnchorColumn 'ID'"
}
ThrowsPs51 'whitespace-only entry names itself -- PS5.1' $ps51_13.wsEntry 'empty or whitespace-only entry'
ThrowsPs51 'duplicated entry names the duplicate -- PS5.1' $ps51_13.dupEntry "'Name' twice"
ThrowsPs51 'case-only duplicate is caught too -- PS5.1' $ps51_13.caseDup "'Name' and 'name'"
ThrowsPs51 'declaration validated before any file read -- PS5.1' $ps51_13.beforeRead 'empty or whitespace-only entry'
ThrowsPs51 'a padded name is visible in the message -- PS5.1' $ps51_13.paddedName "' Name'"
ThrowsPs51 'file-vs-file message quotes names too -- PS5.1' $ps51_13.fileVsFile "'ID' \| 'Name'"

""
"=== 14. round-2 review findings ==="
# The collision check must trim both sides, as CompareCSVs_Delta.ps1:455-456 does. Without it a
# padded -ChangeTypeColumnName emits two columns whose names differ only by a space.
$tp = W 't_p.csv' "ID,ChangeType,dept`r`nE1,Active,Ops`r`n"
$tc = W 't_c.csv' "ID,ChangeType,dept`r`nE1,Active,Sales`r`n"
Throws 'padded -ChangeTypeColumnName still collides' { Compare-CsvData -PreviousCsvPath $tp -CurrentCsvPath $tc -AnchorColumn 'ID' -ChangeTypeColumnName 'ChangeType ' } 'collides with the verdict column'
# A duplicate caused by padding must name both raw spellings, not the trimmed key.
$qp = W 'q_p.csv' "ID,dept`r`nE1,Ops`r`n"
$qc = W 'q_c.csv' "ID,dept`r`n`"  E7  `",Ops`r`nE7,HR`r`n"
try { $null = Compare-CsvData -PreviousCsvPath $qp -CurrentCsvPath $qc -AnchorColumn 'ID'
      Ok 'padding-caused duplicate names both spellings' $false 'did not throw' }
catch { Ok 'padding-caused duplicate names both spellings' (($_.Exception.Message -match "'  E7  '") -and ($_.Exception.Message -match "'E7'")) "$($_.Exception.Message)" }
# An ordinary duplicate, where both spellings are identical, keeps the simpler message.
$sp2 = W 's2_p.csv' "ID,dept`r`nE1,Ops`r`n"
$sc2 = W 's2_c.csv' "ID,dept`r`nE9,Ops`r`nE9,HR`r`n"
try { $null = Compare-CsvData -PreviousCsvPath $sp2 -CurrentCsvPath $sc2 -AnchorColumn 'ID'
      Ok 'plain duplicate keeps the simple message' $false 'did not throw' }
catch { Ok 'plain duplicate keeps the simple message' ($_.Exception.Message -match "^Duplicate anchor 'E9' in Current") "$($_.Exception.Message)" }

"    PS5.1 twin"
$ps51_14 = Invoke-OnPS51 @{
    paddedCollide = "Compare-CsvData -PreviousCsvPath '$tp' -CurrentCsvPath '$tc' -AnchorColumn 'ID' -ChangeTypeColumnName 'ChangeType '"
    paddedDup     = "Compare-CsvData -PreviousCsvPath '$qp' -CurrentCsvPath '$qc' -AnchorColumn 'ID'"
    plainDup      = "Compare-CsvData -PreviousCsvPath '$sp2' -CurrentCsvPath '$sc2' -AnchorColumn 'ID'"
}
ThrowsPs51 'padded -ChangeTypeColumnName still collides -- PS5.1' $ps51_14.paddedCollide 'collides with the verdict column'
Ok 'padding-caused duplicate names both spellings -- PS5.1' `
   ($ps51_14.paddedDup -is [string] -and $ps51_14.paddedDup.StartsWith('ERROR: ') -and $ps51_14.paddedDup -match "'  E7  '" -and $ps51_14.paddedDup -match "'E7'") `
   "value='$($ps51_14.paddedDup)'"
ThrowsPs51 'plain duplicate keeps the simple message -- PS5.1' $ps51_14.plainDup "^ERROR: Duplicate anchor 'E9' in Current"

""
"=== 14b. a collision message must not blame the wrong mechanism (round 3) ==="
# The both-spellings wording was written for padding and inherited by the case path, which said
# "once trimmed" about two values that differ only in letter case.
$kp = W 'k_p.csv' "ID,dept`r`nE1,Ops`r`n"
$kc = W 'k_c.csv' "ID,dept`r`nE7,Ops`r`ne7,HR`r`n"
try { $null = Compare-CsvData -PreviousCsvPath $kp -CurrentCsvPath $kc -AnchorColumn 'ID'
      Ok 'case-only duplicate does not blame trimming' $false 'did not throw' }
catch {
    $m = $_.Exception.Message
    Ok 'case-only duplicate does not blame trimming' (($m -notmatch 'once trimmed') -and ($m -match "'e7'") -and ($m -match "'E7'")) "$m"
}
try { $null = Compare-CsvData -PreviousCsvPath $kp -CurrentCsvPath $kp -AnchorColumn 'ID' -ExpectedColumns 'ID','dept','DEPT'
      Ok 'case-only declared duplicate shows both spellings' $false 'did not throw' }
catch {
    $m = $_.Exception.Message
    Ok 'case-only declared duplicate shows both spellings' (($m -match "'dept'") -and ($m -match "'DEPT'")) "$m"
}

"    PS5.1 twin"
$ps51_14b = Invoke-OnPS51 @{
    caseDupMsg  = "Compare-CsvData -PreviousCsvPath '$kp' -CurrentCsvPath '$kc' -AnchorColumn 'ID'"
    caseDeclDup = "Compare-CsvData -PreviousCsvPath '$kp' -CurrentCsvPath '$kp' -AnchorColumn 'ID' -ExpectedColumns 'ID','dept','DEPT'"
}
Ok 'case-only duplicate does not blame trimming -- PS5.1' `
   ($ps51_14b.caseDupMsg -is [string] -and $ps51_14b.caseDupMsg.StartsWith('ERROR: ') -and $ps51_14b.caseDupMsg -notmatch 'once trimmed' -and $ps51_14b.caseDupMsg -match "'e7'" -and $ps51_14b.caseDupMsg -match "'E7'") `
   "value='$($ps51_14b.caseDupMsg)'"
Ok 'case-only declared duplicate shows both spellings -- PS5.1' `
   ($ps51_14b.caseDeclDup -is [string] -and $ps51_14b.caseDeclDup.StartsWith('ERROR: ') -and $ps51_14b.caseDeclDup -match "'dept'" -and $ps51_14b.caseDeclDup -match "'DEPT'") `
   "value='$($ps51_14b.caseDeclDup)'"

""
"=== 14c. an invented column name must not ship (round 4) ==="
# A blank header field becomes 'H1'. Both days get the same treatment, so the schema check passes
# and the delta would carry an invented name in its own header.
$ip = W 'i_p.csv' "ID, ,dept`r`nE1,x,Ops`r`n"
$ic = W 'i_c.csv' "ID, ,dept`r`nE1,x,Sales`r`n"
Throws 'blank column name is rejected' { Compare-CsvData -PreviousCsvPath $ip -CurrentCsvPath $ic -AnchorColumn 'ID' } 'not parsed cleanly'
Throws 'the message names the invented-name cause' { Compare-CsvData -PreviousCsvPath $ip -CurrentCsvPath $ic -AnchorColumn 'ID' } "such as 'H1'"
# It must fire on the Current side too, not just Previous.
$jp = W 'j_p.csv' "ID,dept`r`nE1,Ops`r`n"
$jc = W 'j_c.csv' "ID, ,dept`r`nE1,x,Ops`r`n"
Throws 'blank column name in Current is rejected' { Compare-CsvData -PreviousCsvPath $jp -CurrentCsvPath $jc -AnchorColumn 'ID' } 'Current file was not parsed cleanly'
# And it must still fire when the host has suppressed warnings entirely - the case that made this
# silent in the first place.
$suppressedProbe = @"
`$WarningPreference = 'SilentlyContinue'
. '$PSScriptRoot\Compare-CsvData.ps1'
try { `$null = Compare-CsvData -PreviousCsvPath '$ip' -CurrentCsvPath '$ic' -AnchorColumn 'ID' -Encoding UTF8 -DelimiterName comma; 'SHIPPED' }
catch { 'THREW' }
"@
foreach ($hostExe in @(@('7','pwsh'),@('51','powershell'))) {
    $suppressed = & $hostExe[1] -NoProfile -NonInteractive -Command $suppressedProbe 2>&1
    Ok "still rejected under `$WarningPreference=SilentlyContinue -- PS$($hostExe[0])" ("$suppressed" -match 'THREW') "$suppressed"
}
# A clean file must not trip it.
$okRun2 = Compare-CsvData -PreviousCsvPath "$fx\sparse\prev.csv" -CurrentCsvPath "$fx\sparse\curr.csv" -AnchorColumn 'ID'
Ok 'a clean file is unaffected' ($okRun2.Count -gt 0) "rows=$($okRun2.Count)"

"    PS5.1 twin. Check 4 (`$WarningPreference) is already its own two-host loop above,"
"    matching sections 5 and 7's shape - not duplicated again here"
$ps51_14c = Invoke-OnPS51 @{
    blankName    = "Compare-CsvData -PreviousCsvPath '$ip' -CurrentCsvPath '$ic' -AnchorColumn 'ID'"
    blankCurrent = "Compare-CsvData -PreviousCsvPath '$jp' -CurrentCsvPath '$jc' -AnchorColumn 'ID'"
    clean        = "Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID'"
}
ThrowsPs51 'blank column name is rejected -- PS5.1' $ps51_14c.blankName 'not parsed cleanly'
ThrowsPs51 'the message names the invented-name cause -- PS5.1' $ps51_14c.blankName "such as 'H1'"
ThrowsPs51 'blank column name in Current is rejected -- PS5.1' $ps51_14c.blankCurrent 'Current file was not parsed cleanly'
Ok 'a clean file is unaffected -- PS5.1' (@($ps51_14c.clean).Count -gt 0) "rows=$(@($ps51_14c.clean).Count)"

""
"=== 15. runs inside a host that sets its own preferences ==="
# StrictMode, ErrorActionPreference and PSDefaultParameterValues all set by the caller, as they
# would be in a real script. Previously only StrictMode was covered, and only in isolation.
# Tightened from a loose 'rows=\d+ total=\d+' pattern match to the real counts, computed once
# here directly, so a wrong number can no longer pass as long as it merely looks like a number.
$sparseLive15 = Compare-CsvData -PreviousCsvPath "$fx\sparse\prev.csv" -CurrentCsvPath "$fx\sparse\curr.csv" -AnchorColumn 'ID' -IncludeSummary
$expected15 = "rows=$($sparseLive15.Changes.Count) total=$($sparseLive15.Summary.Total) adds=$($sparseLive15.Summary.Adds) updates=$($sparseLive15.Summary.Updates) deletes=$($sparseLive15.Summary.Deletes) unchanged=$($sparseLive15.Summary.Unchanged)"
$hostProbe = @"
Set-StrictMode -Version 2.0
`$ErrorActionPreference = 'Stop'
`$PSDefaultParameterValues['Compare-CsvData:Encoding'] = 'UTF8'
`$PSDefaultParameterValues['Compare-CsvData:DelimiterName'] = 'comma'
. '$PSScriptRoot\Compare-CsvData.ps1'
`$r = Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID' -IncludeSummary
'rows=' + `$r.Changes.Count + ' total=' + `$r.Summary.Total + ' adds=' + `$r.Summary.Adds + ' updates=' + `$r.Summary.Updates + ' deletes=' + `$r.Summary.Deletes + ' unchanged=' + `$r.Summary.Unchanged
"@
foreach ($hostExe in @(@('7','pwsh'),@('51','powershell'))) {
    $res = & $hostExe[1] -NoProfile -NonInteractive -Command $hostProbe 2>&1
    Ok "host preferences honoured on PS$($hostExe[0]) - actual row/summary counts match" ("$res" -ceq $expected15) "got=[$res] expected=[$expected15]"
}

""
"=== 16. CRLF- and LF-terminated input carrying identical content produce identical results (TEST-PLAN-Compare-CsvData.md 1.2) ==="
'    every fixture this suite builds inline uses `r`n literally - only tests/fixtures/terminators/'
"    carries genuine LF-only content reaching Import-Csv through this harness"
$crlfR = Compare-CsvData -PreviousCsvPath "$fx\terminators\crlf_prev.csv" -CurrentCsvPath "$fx\terminators\crlf_curr.csv" -AnchorColumn 'ID' -IncludeSummary
$lfR   = Compare-CsvData -PreviousCsvPath "$fx\terminators\lf_prev.csv"   -CurrentCsvPath "$fx\terminators\lf_curr.csv"   -AnchorColumn 'ID' -IncludeSummary
Ok 'CRLF and LF summaries agree' ($crlfR.Summary.Adds -eq $lfR.Summary.Adds -and $crlfR.Summary.Updates -eq $lfR.Summary.Updates -and $crlfR.Summary.Deletes -eq $lfR.Summary.Deletes -and $crlfR.Summary.Unchanged -eq $lfR.Summary.Unchanged) `
   "crlf: A=$($crlfR.Summary.Adds) U=$($crlfR.Summary.Updates) D=$($crlfR.Summary.Deletes) N=$($crlfR.Summary.Unchanged) -- lf: A=$($lfR.Summary.Adds) U=$($lfR.Summary.Updates) D=$($lfR.Summary.Deletes) N=$($lfR.Summary.Unchanged)"
$crlfSorted = ($crlfR.Changes | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
$lfSorted   = ($lfR.Changes   | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
Ok 'CRLF and LF rows are content-identical (order-insensitive)' ($crlfSorted -ceq $lfSorted) "crlf=[$crlfSorted] lf=[$lfSorted]"

"    PS5.1 twin"
$ps51_16 = Invoke-OnPS51 @{
    crlf = "Compare-CsvData -PreviousCsvPath '$fx\terminators\crlf_prev.csv' -CurrentCsvPath '$fx\terminators\crlf_curr.csv' -AnchorColumn 'ID' -IncludeSummary"
    lf   = "Compare-CsvData -PreviousCsvPath '$fx\terminators\lf_prev.csv' -CurrentCsvPath '$fx\terminators\lf_curr.csv' -AnchorColumn 'ID' -IncludeSummary"
}
$crlfR51 = $ps51_16.crlf
$lfR51   = $ps51_16.lf
Ok 'CRLF and LF summaries agree -- PS5.1' ($crlfR51.Summary.Adds -eq $lfR51.Summary.Adds -and $crlfR51.Summary.Updates -eq $lfR51.Summary.Updates -and $crlfR51.Summary.Deletes -eq $lfR51.Summary.Deletes -and $crlfR51.Summary.Unchanged -eq $lfR51.Summary.Unchanged) `
   "crlf: A=$($crlfR51.Summary.Adds) U=$($crlfR51.Summary.Updates) D=$($crlfR51.Summary.Deletes) N=$($crlfR51.Summary.Unchanged) -- lf: A=$($lfR51.Summary.Adds) U=$($lfR51.Summary.Updates) D=$($lfR51.Summary.Deletes) N=$($lfR51.Summary.Unchanged)"
$crlfSorted51 = (@($crlfR51.Changes) | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
$lfSorted51   = (@($lfR51.Changes)   | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
Ok 'CRLF and LF rows are content-identical (order-insensitive) -- PS5.1' ($crlfSorted51 -ceq $lfSorted51) "crlf=[$crlfSorted51] lf=[$lfSorted51]"

""
"=== 17. an accented (non-ASCII) value compares correctly (TEST-PLAN-Compare-CsvData.md 2.1) ==="
"    narrower than the family's A2: -Encoding here is mandatory UTF8-only, so there is no ANSI"
"    mis-decode trap to catch - what's untested is whether an accented value works at all through"
"    Import-Csv and the function's own anchor lookup and value comparison"
$accCafe  = 'Caf' + [char]0xE9         # Cafe (accented) - built from a char code, keeps this .ps1 pure ASCII
$accCreme = 'Cr' + [char]0xE8 + 'me'   # Creme (accented, different character)
$acp = W 'acc_p.csv' "ID,Name`r`n$accCafe,x`r`nE2,$accCafe`r`n"
$acc = W 'acc_c.csv' "ID,Name`r`n$accCafe,x`r`nE2,$accCreme`r`n"
$accR = Compare-CsvData -PreviousCsvPath $acp -CurrentCsvPath $acc -AnchorColumn 'ID' -IncludeSummary
Ok 'an accented anchor value matches itself, unchanged' ($accR.Summary.Unchanged -eq 1) "N=$($accR.Summary.Unchanged)"
$accUpd = $accR.Changes | Where-Object ID -eq 'E2'
Ok 'a value changing between two accented values registers as Update, byte-exact' ($accUpd.ChangeType -ceq 'Update' -and $accUpd.Name -ceq $accCreme) "ChangeType=$($accUpd.ChangeType) Name='$($accUpd.Name)'"

"    PS5.1 twin"
$ps51_17 = Invoke-OnPS51 @{
    acc = "Compare-CsvData -PreviousCsvPath '$acp' -CurrentCsvPath '$acc' -AnchorColumn 'ID' -IncludeSummary"
}
$accR51 = $ps51_17.acc
Ok 'an accented anchor value matches itself, unchanged -- PS5.1' ($accR51.Summary.Unchanged -eq 1) "N=$($accR51.Summary.Unchanged)"
$accUpd51 = @($accR51.Changes) | Where-Object ID -eq 'E2'
Ok 'a value changing between two accented values registers as Update, byte-exact -- PS5.1' ($accUpd51.ChangeType -ceq 'Update' -and $accUpd51.Name -ceq $accCreme) "ChangeType=$($accUpd51.ChangeType) Name='$($accUpd51.Name)'"

""
"=== 18. a literal 0-byte file is rejected with a message distinguishable from a header-only"
"    file's rejection (TEST-PLAN-Compare-CsvData.md 2.2) ==="
"    Import-Csv leaves both silent with Count=0 - Compare-CsvData.ps1 now checks file length"
"    directly, inside the existing zero-row branch, so a missing file's own error path is untouched"
$zeroFile = Join-Path $w 'zero_18.csv'
[System.IO.File]::WriteAllBytes($zeroFile, @())
$headerOnlyFile = W 'header_only_18.csv' "ID,dept`r`n"
$goodFile18 = W 'good_18.csv' "ID,dept`r`nE1,Ops`r`n"
Throws '0-byte Previous: distinct empty-file message' { Compare-CsvData -PreviousCsvPath $zeroFile -CurrentCsvPath $goodFile18 -AnchorColumn 'ID' } 'Previous file is empty; no header line found'
Throws '0-byte Current: distinct empty-file message'  { Compare-CsvData -PreviousCsvPath $goodFile18 -CurrentCsvPath $zeroFile -AnchorColumn 'ID' } 'Current file is empty; no header line found'
Throws 'header-only Previous: stays the OTHER message' { Compare-CsvData -PreviousCsvPath $headerOnlyFile -CurrentCsvPath $goodFile18 -AnchorColumn 'ID' } 'Previous file yielded no rows'
Throws 'header-only Current: stays the OTHER message'  { Compare-CsvData -PreviousCsvPath $goodFile18 -CurrentCsvPath $headerOnlyFile -AnchorColumn 'ID' } 'Current file yielded no rows'

"    PS5.1 twin"
$ps51_18 = Invoke-OnPS51 @{
    zeroPrev    = "Compare-CsvData -PreviousCsvPath '$zeroFile' -CurrentCsvPath '$goodFile18' -AnchorColumn 'ID'"
    zeroCurr    = "Compare-CsvData -PreviousCsvPath '$goodFile18' -CurrentCsvPath '$zeroFile' -AnchorColumn 'ID'"
    hdrOnlyPrev = "Compare-CsvData -PreviousCsvPath '$headerOnlyFile' -CurrentCsvPath '$goodFile18' -AnchorColumn 'ID'"
    hdrOnlyCurr = "Compare-CsvData -PreviousCsvPath '$goodFile18' -CurrentCsvPath '$headerOnlyFile' -AnchorColumn 'ID'"
}
ThrowsPs51 '0-byte Previous: distinct empty-file message -- PS5.1' $ps51_18.zeroPrev 'Previous file is empty; no header line found'
ThrowsPs51 '0-byte Current: distinct empty-file message -- PS5.1'  $ps51_18.zeroCurr 'Current file is empty; no header line found'
ThrowsPs51 'header-only Previous: stays the OTHER message -- PS5.1' $ps51_18.hdrOnlyPrev 'Previous file yielded no rows'
ThrowsPs51 'header-only Current: stays the OTHER message -- PS5.1'  $ps51_18.hdrOnlyCurr 'Current file yielded no rows'

""
"=== 19. a decorated path still works, and a missing input file is rejected clearly"
"    (TEST-PLAN-Compare-CsvData.md 2.3, mirrors the family's Group E) ==="
"    the function passes -LiteralPath to Import-Csv, so this should work by construction - never"
"    actually checked until now"
$decoRoot = Join-Path $w "deco [b] caf$([char]0xE9)"
if (Test-Path -LiteralPath $decoRoot) { Remove-Item -LiteralPath $decoRoot -Recurse -Force }
New-Item -ItemType Directory -Force $decoRoot | Out-Null
$decoPrev = Join-Path $decoRoot 'prev.csv'; $decoCurr = Join-Path $decoRoot 'curr.csv'
Copy-Item -LiteralPath "$fx\sparse\prev.csv" -Destination $decoPrev -Force
Copy-Item -LiteralPath "$fx\sparse\curr.csv" -Destination $decoCurr -Force
$ctrl19 = Compare-CsvData -PreviousCsvPath "$fx\sparse\prev.csv" -CurrentCsvPath "$fx\sparse\curr.csv" -AnchorColumn 'ID' -IncludeSummary
$deco19 = Compare-CsvData -PreviousCsvPath $decoPrev -CurrentCsvPath $decoCurr -AnchorColumn 'ID' -IncludeSummary
$ctrlSorted19 = ($ctrl19.Changes | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
$decoSorted19 = ($deco19.Changes | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
Ok 'a path decorated with [ ], and a non-ASCII character, is content-identical to an undecorated run' `
   ($decoSorted19 -ceq $ctrlSorted19 -and $deco19.Summary.Total -eq $ctrl19.Summary.Total) "decoTotal=$($deco19.Summary.Total) ctrlTotal=$($ctrl19.Summary.Total)"
$missingInput19 = Join-Path $w 'e19_missing_input.csv'
Throws 'a missing input file is rejected with a clear, file-naming message' { Compare-CsvData -PreviousCsvPath $missingInput19 -CurrentCsvPath $decoCurr -AnchorColumn 'ID' } 'Could not find file'

"    PS5.1 twin"
$ps51_19 = Invoke-OnPS51 @{
    ctrl    = "Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID' -IncludeSummary"
    deco    = "Compare-CsvData -PreviousCsvPath '$decoPrev' -CurrentCsvPath '$decoCurr' -AnchorColumn 'ID' -IncludeSummary"
    missing = "Compare-CsvData -PreviousCsvPath '$missingInput19' -CurrentCsvPath '$decoCurr' -AnchorColumn 'ID'"
}
$ctrl19_51 = $ps51_19.ctrl
$deco19_51 = $ps51_19.deco
$ctrlSorted19_51 = (@($ctrl19_51.Changes) | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
$decoSorted19_51 = (@($deco19_51.Changes) | ForEach-Object { ($_.PSObject.Properties.Value -join '|') } | Sort-Object) -join "`n"
Ok 'a path decorated with [ ], and a non-ASCII character, is content-identical to an undecorated run -- PS5.1' `
   ($decoSorted19_51 -ceq $ctrlSorted19_51 -and $deco19_51.Summary.Total -eq $ctrl19_51.Summary.Total) "decoTotal=$($deco19_51.Summary.Total) ctrlTotal=$($ctrl19_51.Summary.Total)"
ThrowsPs51 'a missing input file is rejected with a clear, file-naming message -- PS5.1' $ps51_19.missing 'Could not find file'

""
# A check that silently stops running is worse than one that fails - assert the denominator, the
# same discipline the repo's own verification harnesses hold themselves to.
# Counted BEFORE this assertion runs, so the total printed below is this number plus one.
# Update it deliberately when adding a check - that is the point.
$expected = 182
Ok "all $expected preceding checks ran (guards against a check vanishing)" (($pass + $fail) -eq $expected) "ran $($pass + $fail)"

""
"TOTAL: $pass passed, $fail failed"