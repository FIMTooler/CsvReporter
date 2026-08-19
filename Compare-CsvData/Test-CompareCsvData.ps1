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

"PSVersion: $($PSVersionTable.PSVersion)"
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

""
"=== 5. runs clean under StrictMode 2.0 ==="
$strict = & pwsh -NoProfile -Command "Set-StrictMode -Version 2.0; . '$PSScriptRoot\Compare-CsvData.ps1'; (Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID' -Encoding UTF8 -DelimiterName comma).Count" 2>&1
Ok 'StrictMode 2.0' ("$strict" -match '^\d+$') "output: $strict"

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
$probe = & pwsh -NoProfile -Command @"
. '$PSScriptRoot\Compare-CsvData.ps1'
try { `$null = Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID' -DelimiterName comma -Encoding ansi -ErrorAction Stop; 'ACCEPTED' }
catch { 'REJECTED: ' + `$_.Exception.Message }
"@ 2>&1
Ok '-Encoding ansi is rejected' ("$probe" -match 'REJECTED') "$probe"
$mand = & pwsh -NoProfile -NonInteractive -Command @"
. '$PSScriptRoot\Compare-CsvData.ps1'
try { `$null = Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID' -DelimiterName comma -ErrorAction Stop; 'ACCEPTED' }
catch { 'BLOCKED' }
"@ 2>&1
Ok '-Encoding cannot be omitted' ("$mand" -notmatch 'ACCEPTED') "$mand"

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
$suppressed = & pwsh -NoProfile -NonInteractive -Command @"
`$WarningPreference = 'SilentlyContinue'
. '$PSScriptRoot\Compare-CsvData.ps1'
try { `$null = Compare-CsvData -PreviousCsvPath '$ip' -CurrentCsvPath '$ic' -AnchorColumn 'ID' -Encoding UTF8 -DelimiterName comma; 'SHIPPED' }
catch { 'THREW' }
"@ 2>&1
Ok 'still rejected under $WarningPreference=SilentlyContinue' ("$suppressed" -match 'THREW') "$suppressed"
# A clean file must not trip it.
$okRun2 = Compare-CsvData -PreviousCsvPath "$fx\sparse\prev.csv" -CurrentCsvPath "$fx\sparse\curr.csv" -AnchorColumn 'ID'
Ok 'a clean file is unaffected' ($okRun2.Count -gt 0) "rows=$($okRun2.Count)"

""
"=== 15. runs inside a host that sets its own preferences ==="
# StrictMode, ErrorActionPreference and PSDefaultParameterValues all set by the caller, as they
# would be in a real script. Previously only StrictMode was covered, and only in isolation.
$hostProbe = @"
Set-StrictMode -Version 2.0
`$ErrorActionPreference = 'Stop'
`$PSDefaultParameterValues['Compare-CsvData:Encoding'] = 'UTF8'
`$PSDefaultParameterValues['Compare-CsvData:DelimiterName'] = 'comma'
. '$PSScriptRoot\Compare-CsvData.ps1'
`$r = Compare-CsvData -PreviousCsvPath '$fx\sparse\prev.csv' -CurrentCsvPath '$fx\sparse\curr.csv' -AnchorColumn 'ID' -IncludeSummary
'rows=' + `$r.Changes.Count + ' total=' + `$r.Summary.Total
"@
foreach ($hostExe in @(@('7','pwsh'),@('51','powershell'))) {
    $res = & $hostExe[1] -NoProfile -NonInteractive -Command $hostProbe 2>&1
    Ok "host preferences honoured on PS$($hostExe[0])" ("$res" -match 'rows=\d+ total=\d+') "$res"
}

""
# A check that silently stops running is worse than one that fails - assert the denominator, the
# same discipline the repo's own verification harnesses hold themselves to.
# Counted BEFORE this assertion runs, so the total printed below is this number plus one.
# Update it deliberately when adding a check - that is the point.
$expected = 80
Ok "all $expected preceding checks ran (guards against a check vanishing)" (($pass + $fail) -eq $expected) "ran $($pass + $fail)"

""
"TOTAL: $pass passed, $fail failed"