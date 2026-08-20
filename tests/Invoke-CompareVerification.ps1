<#
.SYNOPSIS
Verification harness for the CsvReporter scripts. Runs them against the shared fixtures on both
PowerShell versions and reports agreement.

.DESCRIPTION
Four modes:

  -Mode Agreement   (default) Run every script on every behavioural fixture, on PS 5.1 and 7, and
                    assert that same-output-shape scripts agree and that each script gives the same
                    bytes on both versions. A script producing no file fails outright rather than
                    counting as agreement. Also asserts every script's output against a recorded
                    baseline in tests\baselines\ - the sibling checks are
                    blind to all four scripts drifting together; the baseline check is not.
                    medium, Detailed and small no longer sort their output by anchor (G11/G12), so
                    their agreement/baseline checks compare sorted row content instead of raw bytes -
                    large is the only script in the family that still sorts, so it is now compared the
                    same order-insensitive way against small/medium and against the baseline.
  -Mode Malformed   Feed every bad input and every mismatched-column pair to every script, on PS 5.1
                    and 7, and assert the expected rejection fires and no report is written. Both
                    versions because this mode asserts on error TEXT, which they render differently.
  -Mode Memory      Peak-working-set comparison on the large generated fixture. Fresh process per
                    run, warm-up discarded, best of N. Asserts CompareCSVs_large.ps1 peaks below
                    CompareCSVs_small.ps1, which is the only reason the external-sort version exists.
                    Requires New-LargeFixture.ps1 to have been run.
                    PowerShell 7 ONLY, deliberately: the assertion compares two scripts within one
                    runtime, so a second version re-confirms an architectural property rather than
                    testing anything new, and would need its own baseline process. Profiling memory
                    across both versions is a separate exercise, not this check.
  -Mode Core        TEST-PLAN.md cases with no better-fitting mode, on PS 5.1 and 7: a 0-byte
                    Previous/Current file (C1); a single-data-row fixture on both sides (C3);
                    CompareCSVs_Detailed.ps1's -ValueTransforms, -DateFormats and -IgnoreColumns
                    against the newlines\ fixture (C4-C6); and a path decorated with wildcard
                    metacharacters or non-ASCII characters, compared byte-identical against an
                    undecorated run (E1/E2).

Every mode prints PASS/FAIL per check, ends with a RESULT line, and exits non-zero if any check
failed.

Expected agreement: small == medium == large (standard output shape). small != Detailed - different
shapes, not a failure.

Paths default to repo-relative and need no arguments: -ScriptFolder defaults to the repo root and
-FixtureFolder to tests\fixtures. Both can be overridden to test scripts elsewhere.

Note -Scripts is an array: invoke this script directly, not via pwsh -File, which binds the whole
list as one string.

.PARAMETER UpdateBaseline
Agreement mode only. Regenerates tests\baselines\ from this run's own output instead of checking
against it. Never runs implicitly - baselines are read-only on every normal invocation. Prints the
old-versus-new diff for every fixture before overwriting, and refuses outright (writes nothing) if
any check other than the baseline comparison itself failed on this run, so a baseline is never
re-recorded from a state the rest of the suite already considers broken.

.EXAMPLE
.\Invoke-CompareVerification.ps1
.\Invoke-CompareVerification.ps1 -Mode Malformed
.\Invoke-CompareVerification.ps1 -Mode Memory -Reps 2
.\Invoke-CompareVerification.ps1 -Mode Core
.\Invoke-CompareVerification.ps1 -UpdateBaseline
#>
# This script itself must run under PowerShell 7+ as the outer host - not Windows PowerShell 5.1,
# even though 5.1 is one of the two runtimes every mode exercises as a CHILD process. PS5.1 as the
# OUTER host reinterprets a child pwsh.exe process's own stderr as a terminating exception instead of
# plain text to capture, breaking every mode confusingly deep inside a run.
# #Requires is enforced before Param() below even evaluates, so this also blocks an unrelated
# $PSScriptRoot-under-5.1 bug from ever being reached via this script.
#Requires -Version 7
[CmdletBinding()]
Param(
    [ValidateSet('Agreement','Malformed','Memory','Core')]
    [string]$Mode = 'Agreement',
    # Repo-relative: this script lives in tests\, the CompareCSVs_*.ps1 scripts live at the root.
    [string]$ScriptFolder = (Split-Path $PSScriptRoot -Parent),
    [string]$FixtureFolder = (Join-Path $PSScriptRoot 'fixtures'),
    [string[]]$Scripts = @('small','medium','large','Detailed','Delta'),
    [int]$Reps = 2,
    [switch]$UpdateBaseline
)
$ErrorActionPreference = 'Stop'
$work = Join-Path $PSScriptRoot '_work'
New-Item -ItemType Directory -Force $work | Out-Null

# Every failed check increments this; the script exits non-zero if it ends above zero. Without an
# exit code "did it pass" is only answerable by reading prose, which is no use to a caller.
$failures = 0
function Assert-Check([string]$label, [bool]$ok, [string]$detail = '') {
    if (-not $ok) { $script:failures++ }
    "    {0,-24} {1}{2}" -f $label, $(if ($ok) { 'PASS' } else { 'FAIL' }), $(if ($detail) { "   $detail" } else { '' })
}

function Clear-Dir([string]$p) {
    if (Test-Path $p) { Get-ChildItem -Path $p -Filter '*.csv' | ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force } }
    New-Item -ItemType Directory -Force $p | Out-Null
}
function Invoke-One([string]$script,[string]$exe,[string]$prev,[string]$curr,[string]$out) {
    Clear-Dir $out
    & $exe -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$script.ps1") `
        -PreviousCSVFile $prev -CurrentCSVFile $curr -AnchorColumn ID -OutputFolder $out *>&1 | Out-Null
    $f = Get-ChildItem -Path $out -Filter '*.csv' -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($f) { return (Get-FileHash $f.FullName -Algorithm SHA256).Hash.Substring(0,12) }
    return 'NOFILE'
}

# Prints the old-versus-new diff before overwriting a baseline, so
# re-blessing requires reading what changed rather than just typing a flag. The comparison uses
# Get-FileHash, not Compare-Object, matching the rest of this harness. The overwrite is a pure byte
# copy - never a read-as-text/write-as-text round trip - so it cannot alter the baseline's encoding.
function Update-Baseline([string]$Label, [string]$SourcePath, [string]$BaselinePath) {
    if (Test-Path -LiteralPath $BaselinePath) {
        $oldHash = (Get-FileHash -LiteralPath $BaselinePath -Algorithm SHA256).Hash
        $newHash = (Get-FileHash -LiteralPath $SourcePath -Algorithm SHA256).Hash
        if ($oldHash -eq $newHash) {
            "    $Label : unchanged"
            return
        }
        "    $Label : DIFFERS"
        $oldLines = [System.IO.File]::ReadAllText($BaselinePath) -split "`r`n"
        $newLines = [System.IO.File]::ReadAllText($SourcePath) -split "`r`n"
        $max = [Math]::Max($oldLines.Count, $newLines.Count)
        for ($i = 0; $i -lt $max; $i++) {
            $o = if ($i -lt $oldLines.Count) { $oldLines[$i] } else { '<no line>' }
            $n = if ($i -lt $newLines.Count) { $newLines[$i] } else { '<no line>' }
            if ($o -ne $n) {
                "      line $($i + 1):"
                "        old: $o"
                "        new: $n"
            }
        }
    } else {
        "    $Label : no existing baseline, creating new"
    }
    [System.IO.File]::Copy($SourcePath, $BaselinePath, $true)
}

# medium/Detailed no longer sort their output by anchor (G11), so their agreement/baseline checks
# can't use Get-FileHash on the raw file - two content-identical outputs can differ in row order.
# Strips the fixed header line(s) (1 for medium's shape; 2 for Detailed's - header + SUMMARY, which
# is not a sortable data row), sorts the remaining rows as plain text, then returns both as one
# array so the caller can compare them.
function Get-ComparableLines([string]$Path, [int]$HeaderLineCount) {
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    $lines = [System.IO.File]::ReadAllText($Path) -split "`r`n"
    if ($lines.Count -gt 0 -and $lines[-1] -eq '') { $lines = $lines[0..($lines.Count - 2)] }
    if ($lines.Count -le $HeaderLineCount) { return $lines }
    $header = $lines[0..($HeaderLineCount - 1)]
    $rows = @($lines[$HeaderLineCount..($lines.Count - 1)] | Sort-Object)
    return @($header + $rows)
}

# Not Get-FileHash (order can legitimately differ) and not Compare-Object (its PSObject-wrapping
# makes it unsafe at scale - moot at these fixture sizes, but there's no reason to
# use the one idiom this codebase specifically avoids). A manual index-by-index array comparison,
# same technique Update-Baseline above already uses for its own diff - always run, not a
# hash-then-diff-on-failure split, since these fixtures are a handful of lines either way.
function Test-ContentEqual([string]$Label, [string]$PathA, [string]$PathB, [int]$HeaderLineCount) {
    $a = Get-ComparableLines $PathA $HeaderLineCount
    $b = Get-ComparableLines $PathB $HeaderLineCount
    if ($null -eq $a -or $null -eq $b) {
        Assert-Check $Label $false "missing file: A exists=$($null -ne $a) B exists=$($null -ne $b)"
        return
    }
    $equal = ($a.Count -eq $b.Count)
    if ($equal) {
        for ($i = 0; $i -lt $a.Count; $i++) {
            if ($a[$i] -ne $b[$i]) { $equal = $false; break }
        }
    }
    Assert-Check $Label $equal
    if (-not $equal) {
        $max = [Math]::Max($a.Count, $b.Count)
        for ($i = 0; $i -lt $max; $i++) {
            $x = if ($i -lt $a.Count) { $a[$i] } else { '<no line>' }
            $y = if ($i -lt $b.Count) { $b[$i] } else { '<no line>' }
            if ($x -ne $y) {
                "      line $($i + 1):"
                "        a: $x"
                "        b: $y"
            }
        }
    }
}

# Delta has no independent oracle today - its own checks above compare it only
# against its own previously-blessed baseline, which would enshrine a classification bug forever if
# one existed the day the baseline was blessed. medium/small/large already write an explicit
# ChangeType column using the identical Add/Update/Delete literals Delta uses, so Delta's
# classification can be cross-checked against any one of them directly, no mapping layer needed.
# Reads by header name, not fixed column position - required on both sides, since Delta's anchor is
# never hoisted to column 0 (column-order exists specifically to exercise this) and Import-Csv
# indexes by header regardless of physical column order, so this one function serves as the reader
# for both the standard shape (AnchorColumn, ChangeType, ...) and Delta's shape (ChangeType first,
# anchor wherever it naturally sits) - no separate per-shape reader needed.
# OrdinalIgnoreCase key comparer to match the anchor comparer every script itself uses when
# -CaseSensitive is not passed (never is, in this harness) - collation's anchors ('A-1' vs 'a_1')
# would otherwise risk a mismatch against how the scripts under test actually keyed them.
# -SkipChangeType excludes rows carrying that literal ChangeType value from the map. Needed for the
# standard shape's "None" rows: medium/small/large write one row per Current record regardless of
# verdict, but Delta writes only Add/Update/Delete rows and never a "None" one - without this filter,
# every unchanged record would look like a false "missing from Delta" mismatch.
function Get-AnchorChangeTypeMap([string]$Path, [string]$AnchorColumn, [string]$ChangeTypeColumn = 'ChangeType', [string]$SkipChangeType) {
    $map = [System.Collections.Generic.Dictionary[string,string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($row in (Import-Csv -LiteralPath $Path)) {
        $ct = $row.$ChangeTypeColumn
        if ($SkipChangeType -and $ct -eq $SkipChangeType) { continue }
        $map.Add($row.$AnchorColumn, $ct)
    }
    return $map
}

# Anchor-keyed dictionary diff, not raw line/byte comparison - row order (Delta's vs. the standard
# script's) never matters, only the (anchor, ChangeType) association does. Reports every mismatching
# or one-sided anchor, not just the first, same "show everything that differs" style as
# Test-ContentEqual above.
function Test-ChangeTypeAgreement([string]$Label, [string]$StandardPath, [string]$DeltaPath, [string]$AnchorColumn) {
    $std = Get-AnchorChangeTypeMap -Path $StandardPath -AnchorColumn $AnchorColumn -SkipChangeType 'None'
    $delta = Get-AnchorChangeTypeMap -Path $DeltaPath -AnchorColumn $AnchorColumn
    $mismatches = [System.Collections.Generic.List[string]]::new()
    foreach ($anchor in $std.Keys) {
        if (-not $delta.ContainsKey($anchor)) {
            $mismatches.Add("anchor '$anchor': standard=$($std[$anchor]) delta=<missing>")
        } elseif ($std[$anchor] -ne $delta[$anchor]) {
            $mismatches.Add("anchor '$anchor': standard=$($std[$anchor]) delta=$($delta[$anchor])")
        }
    }
    foreach ($anchor in $delta.Keys) {
        if (-not $std.ContainsKey($anchor)) {
            $mismatches.Add("anchor '$anchor': standard=<missing> delta=$($delta[$anchor])")
        }
    }
    Assert-Check $Label ($mismatches.Count -eq 0) "$($std.Count) standard (non-None) / $($delta.Count) delta anchors"
    foreach ($m in $mismatches) { "      $m" }
}

if ($Mode -eq 'Agreement') {
    # Allowlist, not blocklist: a blocklist here once let 14 leftover profiling fixtures
    # (profile-generated-*, up to 488 MB each) silently join this sweep, turning a documented
    # "seconds" run into 100+ minutes across 19 fixtures instead of 5. Naming the five behavioural
    # fixtures explicitly means anything else that shows up in tests\fixtures\ is ignored, regardless
    # of its name.
    $behaviouralFixtures = @('sparse','newlines','symmetric','collation','duplicates','column-order')
    $fixtures = Get-ChildItem $FixtureFolder -Directory |
                Where-Object { $_.Name -in $behaviouralFixtures -and (Test-Path (Join-Path $_.FullName 'prev.csv')) }
    "Fixtures: $($fixtures.Count) of $($behaviouralFixtures.Count) expected ($((($fixtures.Name | Sort-Object)) -join ', '))"
    if ($fixtures.Count -ne $behaviouralFixtures.Count) {
        Assert-Check 'all behavioural fixtures present' $false "found $($fixtures.Count) of $($behaviouralFixtures.Count): $((($fixtures.Name | Sort-Object)) -join ', ')"
    }

    # The sibling checks below catch one script disagreeing with its peers,
    # but are structurally blind to all four drifting together - verified by corrupting a fixture and
    # watching the suite stay green. A recorded baseline per fixture per shape closes that gap.
    # 'small' and 'Detailed' are the recorded representatives for their shape; which script generates
    # the baseline does not matter since the sibling checks already require all of a shape to agree.
    $baselineFolder = Join-Path $PSScriptRoot 'baselines'
    New-Item -ItemType Directory -Force $baselineFolder | Out-Null
    $baselineSources = @{}   # fixture name -> @{ standard = path; detailed = path }; used only by -UpdateBaseline

    foreach ($fx in $fixtures) {
        $prev = Join-Path $fx.FullName 'prev.csv'; $curr = Join-Path $fx.FullName 'curr.csv'
        $h = @{}
        $stdSource = $null; $detSource = $null; $medSource = $null; $deltaSource = $null; $largeSource = $null
        foreach ($s in $Scripts) {
            foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
                $outDir = Join-Path $work ("{0}_{1}_{2}" -f $fx.Name,$s,$v[0])
                $h["$s$($v[0])"] = Invoke-One $s $v[1] $prev $curr $outDir
                if ($v[0] -eq '7' -and $h["$s$($v[0])"] -ne 'NOFILE') {
                    $srcFile = Get-ChildItem -Path $outDir -Filter '*.csv' -ErrorAction SilentlyContinue | Select-Object -First 1
                    if ($s -eq 'small')    { $stdSource = $srcFile.FullName }
                    if ($s -eq 'medium')   { $medSource = $srcFile.FullName }
                    if ($s -eq 'Detailed') { $detSource = $srcFile.FullName }
                    if ($s -eq 'Delta')    { $deltaSource = $srcFile.FullName }
                    if ($s -eq 'large')    { $largeSource = $srcFile.FullName }
                }
            }
        }
        # $stdSource (small) is what -UpdateBaseline records for the standard shape - medium's and
        # large's own PS7 output are never recorded as a baseline source, since none of small/medium/
        # large guarantee row order and only need to agree on content.
        # Delta is its own recorded shape representative, same as Detailed - no sibling produces its
        # shape, so there is no "which script generates the baseline" question for it either.
        $baselineSources[$fx.Name] = @{ standard = $stdSource; detailed = $detSource; delta = $deltaSource }
        "=== $($fx.Name) ==="
        foreach ($s in $Scripts) {
            # NOFILE is an explicit failure here, not a value to compare. Two scripts that both
            # produced nothing are not in agreement - they failed identically.
            $produced = ($h["${s}7"] -ne 'NOFILE' -and $h["${s}51"] -ne 'NOFILE')
            Assert-Check "$s versions agree" ($produced -and $h["${s}7"] -eq $h["${s}51"]) "PS7=$($h["${s}7"])  PS5.1=$($h["${s}51"])"
        }
        if ($Scripts -contains 'small' -and $Scripts -contains 'medium') {
            if ($h['small7'] -ne 'NOFILE' -and $h['medium7'] -ne 'NOFILE') {
                Test-ContentEqual 'small == medium' $stdSource $medSource 1
            } else {
                Assert-Check 'small == medium' $false "small7=$($h['small7']) medium7=$($h['medium7'])"
            }
        }
        if ($Scripts -contains 'small' -and $Scripts -contains 'large') {
            if ($h['small7'] -ne 'NOFILE' -and $h['large7'] -ne 'NOFILE') {
                Test-ContentEqual 'small == large' $stdSource $largeSource 1
            } else {
                Assert-Check 'small == large' $false "small7=$($h['small7']) large7=$($h['large7'])"
            }
        }
        if ($Scripts -contains 'Delta') {
            # Delta always writes a file, even for a zero-change run - unlike every sibling, NOFILE is
            # a failure for this script specifically rather than a valid "nothing changed" state.
            Assert-Check 'Delta never NOFILE' ($h['Delta7'] -ne 'NOFILE' -and $h['Delta51'] -ne 'NOFILE') "PS7=$($h['Delta7'])  PS5.1=$($h['Delta51'])"
        }
        # G14: Delta's own baseline check further below only proves it agrees with its own past self -
        # this is Delta's independent oracle, proving its ChangeType classification agrees with
        # medium's on this run's actual input. medium is the default source purely for convenience (no
        # sort/accumulation step to strip back out); small/medium/large are already proven mutually
        # equal above, so by transitivity this also proves agreement with small and large.
        if ($Scripts -contains 'Delta' -and $Scripts -contains 'medium') {
            if ($h['Delta7'] -ne 'NOFILE' -and $h['medium7'] -ne 'NOFILE') {
                Test-ChangeTypeAgreement 'Delta ChangeType == medium' $medSource $deltaSource 'ID'
            } else {
                Assert-Check 'Delta ChangeType == medium' $false "Delta7=$($h['Delta7']) medium7=$($h['medium7'])"
            }
        }

        # Skipped entirely under -UpdateBaseline: comparing against a baseline you are about to
        # overwrite would always "fail" on the very first bless, and is not what that switch checks.
        if (-not $UpdateBaseline) {
            $standardBaseline = Join-Path $baselineFolder "$($fx.Name)_standard.csv"
            if (Test-Path -LiteralPath $standardBaseline) {
                if ($Scripts -contains 'small') {
                    if ($h['small7'] -ne 'NOFILE') {
                        Test-ContentEqual 'small matches baseline' $stdSource $standardBaseline 1
                    } else {
                        Assert-Check 'small matches baseline' $false "PS7=$($h['small7'])"
                    }
                }
                if ($Scripts -contains 'large') {
                    if ($h['large7'] -ne 'NOFILE') {
                        Test-ContentEqual 'large matches baseline' $largeSource $standardBaseline 1
                    } else {
                        Assert-Check 'large matches baseline' $false "PS7=$($h['large7'])"
                    }
                }
                if ($Scripts -contains 'medium') {
                    if ($h['medium7'] -ne 'NOFILE') {
                        Test-ContentEqual 'medium matches baseline' $medSource $standardBaseline 1
                    } else {
                        Assert-Check 'medium matches baseline' $false "PS7=$($h['medium7'])"
                    }
                }
            } elseif ($Scripts -contains 'small' -or $Scripts -contains 'medium' -or $Scripts -contains 'large') {
                Assert-Check 'standard baseline exists' $false "missing: $standardBaseline - run -UpdateBaseline once to create it"
            }
            $detailedBaseline = Join-Path $baselineFolder "$($fx.Name)_detailed.csv"
            if (Test-Path -LiteralPath $detailedBaseline) {
                if ($Scripts -contains 'Detailed') {
                    if ($h['Detailed7'] -ne 'NOFILE') {
                        Test-ContentEqual 'Detailed matches baseline' $detSource $detailedBaseline 2
                    } else {
                        Assert-Check 'Detailed matches baseline' $false "PS7=$($h['Detailed7'])"
                    }
                }
            } elseif ($Scripts -contains 'Detailed') {
                Assert-Check 'detailed baseline exists' $false "missing: $detailedBaseline - run -UpdateBaseline once to create it"
            }
            $deltaBaseline = Join-Path $baselineFolder "$($fx.Name)_delta.csv"
            if (Test-Path -LiteralPath $deltaBaseline) {
                if ($Scripts -contains 'Delta') {
                    if ($h['Delta7'] -ne 'NOFILE') {
                        # Header line count 1: Delta has no SUMMARY row, so Get-ComparableLines needs
                        # no new behaviour here beyond the correct count.
                        Test-ContentEqual 'Delta matches baseline' $deltaSource $deltaBaseline 1
                    } else {
                        Assert-Check 'Delta matches baseline' $false "PS7=$($h['Delta7'])"
                    }
                }
            } elseif ($Scripts -contains 'Delta') {
                Assert-Check 'delta baseline exists' $false "missing: $deltaBaseline - run -UpdateBaseline once to create it"
            }
        }
    }

    # Only after every fixture above has been checked, and only if nothing else failed. Regenerating
    # from a state the rest of the suite already considers broken is exactly the blind re-blessing
    # this switch exists to prevent - $failures at this point reflects only the checks above, since
    # the baseline-comparison checks that would trivially fail on a first bless were never run.
    if ($UpdateBaseline) {
        ""
        if ($failures -gt 0) {
            "REFUSED: $failures other check(s) failed above; no baseline written. Fix those first."
        } else {
            "Updating baselines in $baselineFolder"
            foreach ($fx in $fixtures) {
                $src = $baselineSources[$fx.Name]
                if ($src['standard']) {
                    Update-Baseline -Label "$($fx.Name)_standard" -SourcePath $src['standard'] -BaselinePath (Join-Path $baselineFolder "$($fx.Name)_standard.csv")
                }
                if ($src['detailed']) {
                    Update-Baseline -Label "$($fx.Name)_detailed" -SourcePath $src['detailed'] -BaselinePath (Join-Path $baselineFolder "$($fx.Name)_detailed.csv")
                }
                if ($src['delta']) {
                    Update-Baseline -Label "$($fx.Name)_delta" -SourcePath $src['delta'] -BaselinePath (Join-Path $baselineFolder "$($fx.Name)_delta.csv")
                }
            }
        }
    }
}
elseif ($Mode -eq 'Malformed') {
    $bad = Join-Path $FixtureFolder 'malformed'
    $good = Join-Path $bad 'good.csv'
    # Both versions, same loop shape as Agreement. This mode asserts on ERROR TEXT, and the two
    # versions render it differently: 7 prefixes the record with "Exception:", 5.1 prefixes it with
    # the script path and splits the record across several output objects. Where-Object tests each
    # object, so a pattern matching text anywhere in the message still matches on both - the wrap
    # itself is harmless. What is NOT harmless is a pattern anchored to either version's framing.
    # Verified 2026-08-06: searching 'Exception:' passes all three throw cases on 7 and fails all
    # four on 5.1. That divergence is invisible if only one version is run.
    #
    # TEST-PLAN.md Group B: every case above always passes the malformed fixture as Previous with
    # good.csv as Current, so only the Previous validation branch has ever executed. These four
    # reuse the same fixtures with the roles reversed, and assert that the message actually names
    # the Current side rather than just that some error fired - "an error occurred" would not catch
    # someone editing one branch and not its twin.
    # bad_quotes' expected path is pre-resolved through the same mechanism CompareCSVs_*.ps1 itself
    # uses (Resolve-Path -LiteralPath ... .ProviderPath inside Resolve-FullPath), so the assertion
    # compares against what the script actually produces, not the raw Join-Path string.
    $badQuotesResolved = (Resolve-Path -LiteralPath (Join-Path $bad 'bad_quotes.csv')).ProviderPath
    # dup_anchor has no ExpectFile: TEST-PLAN.md's B4 only specifies the warning message, unlike
    # B1-B3 which explicitly require no report written. Whether a report is written here is
    # incidental to what B4 tests - dup_anchor.csv deduplicates (first-occurrence-wins) to the exact
    # same two records good.csv already holds, so there is genuinely zero net difference, and
    # whether a zero-diff run still writes a report is a per-script property already covered
    # elsewhere (Delta always writes; small/medium/large/Detailed don't when nothing changed) -
    # asserting it here would test that instead of the side-naming this case exists to check.
    $groupBCases = @(
        @{ Case = 'short_row';  Pattern = 'Row 2 in Current file has 2 field\(s\), expected 3'; ExpectFile = $false }
        @{ Case = 'long_row';   Pattern = 'Row 2 in Current file has 4 field\(s\), expected 3'; ExpectFile = $false }
        @{ Case = 'bad_quotes'; Pattern = [regex]::Escape("Malformed CSV in '$badQuotesResolved' at line 3"); ExpectFile = $false }
        @{ Case = 'dup_anchor'; Pattern = "Duplicate anchor '1' in Current file\. Using row 1; ignoring row\(s\): 2"; ExpectFile = $null }
    )
    # TEST-PLAN.md E3: three ways a path guard should reject at parameter binding rather than let a
    # bad path reach the comparison - non-zero exit, no report written, and the message names the
    # offending parameter. B7 (fixed 2026-08-07, all five scripts) closed Test-Path -Path silently
    # rejecting real files with [ or ] in their name; nothing has asserted the guard's basic
    # reject-shape stays intact until now. "Both directions" in TEST-PLAN.md's Group E means
    # accept-a-decorated-path (E1/E2, under -Mode Core) versus reject-an-invalid-one (E3, here) - not
    # Previous-versus-Current, so each sub-case below exercises one side only.
    # Exit code is asserted explicitly here via $LASTEXITCODE, unlike the message-only checks above -
    # TEST-PLAN.md's E3 wording calls out "non-zero exit" as its own assertion, distinct from the
    # message text.
    $missingInputFile = Join-Path $work 'e3_missing_input.csv'
    $e3PathCases = @(
        @{ Case = 'missing_input'; Prev = $missingInputFile; Curr = $good; Param = 'PreviousCSVFile' }
        @{ Case = 'dir_as_file';   Prev = $bad;               Curr = $good; Param = 'PreviousCSVFile' }
    )
    foreach ($s in $Scripts) {
        "=== CompareCSVs_$s.ps1 ==="
        foreach ($case in 'short_row','long_row','bad_quotes','dup_anchor') {
            foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
                $out = Join-Path $work ("mal_{0}_{1}_{2}" -f $s,$case,$v[0])
                Clear-Dir $out
                $r = & $v[1] -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$s.ps1") `
                        -PreviousCSVFile (Join-Path $bad "$case.csv") -CurrentCSVFile $good `
                        -AnchorColumn ID -OutputFolder $out 2>&1
                $line = $r | Where-Object { "$_" -match 'field\(s\)|Malformed|Duplicate anchor' } | Select-Object -First 1
                Assert-Check "$case PS$($v[0])" ([bool]$line) ("$line".Trim())
            }
        }
        # Group B, reversed roles: fixture as Current, good.csv as Previous. Joins and collapses
        # whitespace in the captured output before matching - verified 2026-08-20 that PS5.1 wraps a
        # long thrown message across several captured lines (bad_quotes' message split between
        # "...Malformed CSV in" and "'<path>' at line 3...."), so a per-line substring match would
        # miss a message that genuinely fired. PS7's single-line output is unaffected by the join.
        foreach ($b in $groupBCases) {
            foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
                $out = Join-Path $work ("malB_{0}_{1}_{2}" -f $s,$b.Case,$v[0])
                Clear-Dir $out
                $r = & $v[1] -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$s.ps1") `
                        -PreviousCSVFile $good -CurrentCSVFile (Join-Path $bad "$($b.Case).csv") `
                        -AnchorColumn ID -OutputFolder $out 2>&1
                $joined = (($r -join ' ') -replace '\s+', ' ').Trim()
                $matched = $joined -match $b.Pattern
                $wrote = (Get-ChildItem -Path $out -Filter '*.csv' -ErrorAction SilentlyContinue).Count -gt 0
                $ok = $matched
                if ($null -ne $b.ExpectFile) { $ok = $ok -and ($wrote -eq $b.ExpectFile) }
                Assert-Check "B:$($b.Case) PS$($v[0])" $ok "matched=$matched fileWritten=$wrote"
            }
        }
        # Mismatched column sets. Kept out of Agreement mode: every script correctly writes no file,
        # which that mode would score as "they agree" - a green result meaning "all failed alike".
        $mm = Join-Path $FixtureFolder 'mismatched-columns'
        foreach ($case in @(@('extra_in_curr','prev.csv','curr.csv'),
                            @('extra_in_prev','prev_extra.csv','curr_extra.csv'),
                            @('renamed_col','prev_renamed.csv','curr_renamed.csv'))) {
            foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
                $out = Join-Path $work ("mm_{0}_{1}_{2}" -f $s,$case[0],$v[0])
                Clear-Dir $out
                $r = & $v[1] -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$s.ps1") `
                        -PreviousCSVFile (Join-Path $mm $case[1]) -CurrentCSVFile (Join-Path $mm $case[2]) `
                        -AnchorColumn ID -OutputFolder $out 2>&1
                $detected = ($r | Where-Object { "$_" -match 'Column mismatch detected' }).Count -gt 0
                $wrote = (Get-ChildItem -Path $out -Filter '*.csv' -ErrorAction SilentlyContinue).Count
                Assert-Check "$($case[0]) PS$($v[0])" ($detected -and $wrote -eq 0) "detected=$detected fileWritten=$($wrote -gt 0)"
            }
        }
        # Empty-input throws, via header_only.csv (header line only, byte-identical to good.csv's
        # header so the column-set check can't fire first and mask the empty-record throw this
        # exists to test). Same shape as the mismatched-columns block above - explicit Previous/
        # Current pairs, not the main loop's always-good-as-Current shape, since both sides need a
        # turn being empty. All scripts are expected to already have both throws; if one doesn't,
        # that's a finding to report, not something to fix here.
        $headerOnly = Join-Path $bad 'header_only.csv'
        foreach ($case in @(@('empty_previous',$headerOnly,$good,'No records found in Previous CSV file\.'),
                            @('empty_current',$good,$headerOnly,'No records found in Current CSV file\.'))) {
            foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
                $out = Join-Path $work ("empty_{0}_{1}_{2}" -f $s,$case[0],$v[0])
                Clear-Dir $out
                $r = & $v[1] -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$s.ps1") `
                        -PreviousCSVFile $case[1] -CurrentCSVFile $case[2] `
                        -AnchorColumn ID -OutputFolder $out 2>&1
                $detected = ($r | Where-Object { "$_" -match $case[3] }).Count -gt 0
                $wrote = (Get-ChildItem -Path $out -Filter '*.csv' -ErrorAction SilentlyContinue).Count
                Assert-Check "$($case[0]) PS$($v[0])" ($detected -and $wrote -eq 0) "detected=$detected fileWritten=$($wrote -gt 0)"
            }
        }
        # E3, continued: missing_input and dir_as_file share a shape (valid -OutputFolder, one bad
        # -PreviousCSVFile) with the checks above, so they reuse it directly. missing_output cannot -
        # a folder that was never created has nothing to Get-ChildItem - so it is asserted separately
        # below by checking the folder still doesn't exist.
        foreach ($e in $e3PathCases) {
            foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
                $out = Join-Path $work ("e3_{0}_{1}_{2}" -f $s,$e.Case,$v[0])
                Clear-Dir $out
                $r = & $v[1] -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$s.ps1") `
                        -PreviousCSVFile $e.Prev -CurrentCSVFile $e.Curr `
                        -AnchorColumn ID -OutputFolder $out 2>&1
                $exitCode = $LASTEXITCODE
                $joined = (($r -join ' ') -replace '\s+', ' ').Trim()
                $matched = $joined -match [regex]::Escape("Cannot validate argument on parameter '$($e.Param)'")
                $wrote = (Get-ChildItem -Path $out -Filter '*.csv' -ErrorAction SilentlyContinue).Count -gt 0
                Assert-Check "E3:$($e.Case) PS$($v[0])" ($exitCode -ne 0 -and $matched -and -not $wrote) "exit=$exitCode matched=$matched fileWritten=$wrote"
            }
        }
        foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
            $missingOut = Join-Path $work ("e3_missing_output_{0}_{1}" -f $s,$v[0])
            if (Test-Path -LiteralPath $missingOut) { Remove-Item -LiteralPath $missingOut -Recurse -Force }
            $r = & $v[1] -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$s.ps1") `
                    -PreviousCSVFile $good -CurrentCSVFile $good `
                    -AnchorColumn ID -OutputFolder $missingOut 2>&1
            $exitCode = $LASTEXITCODE
            $joined = (($r -join ' ') -replace '\s+', ' ').Trim()
            $matched = $joined -match [regex]::Escape("Cannot validate argument on parameter 'OutputFolder'")
            $created = Test-Path -LiteralPath $missingOut
            Assert-Check "E3:missing_output PS$($v[0])" ($exitCode -ne 0 -and $matched -and -not $created) "exit=$exitCode matched=$matched folderCreated=$created"
        }
    }
}
elseif ($Mode -eq 'Core') {
    # TEST-PLAN.md C1: a 0-byte file as Previous/Current throws the empty-file message naming the
    # right side, and writes no report. Generated into $work rather than committed to
    # tests\fixtures\ - trivial to create, and an empty file sitting in the repo reads oddly on its
    # own. C1's own spec also includes a header-only file still throwing the DISTINCT
    # "No records found in..." message, proving the two failure modes stay separate - that half is
    # already asserted by -Mode Malformed's existing empty_previous/empty_current checks against
    # header_only.csv, so it is not duplicated here under a new label.
    "=== C1: 0-byte file ==="
    $malformedDir = Join-Path $FixtureFolder 'malformed'
    $goodFile = Join-Path $malformedDir 'good.csv'
    $emptyFile = Join-Path $work 'c1_empty.csv'
    [System.IO.File]::WriteAllBytes($emptyFile, @())
    foreach ($s in $Scripts) {
        foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
            $outPrev = Join-Path $work ("c1_prev_{0}_{1}" -f $s,$v[0])
            Clear-Dir $outPrev
            $rPrev = & $v[1] -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$s.ps1") `
                    -PreviousCSVFile $emptyFile -CurrentCSVFile $goodFile `
                    -AnchorColumn ID -OutputFolder $outPrev 2>&1
            $joinedPrev = (($rPrev -join ' ') -replace '\s+', ' ').Trim()
            $matchedPrev = $joinedPrev -match [regex]::Escape('Previous CSV file is empty; no header line found.')
            $wrotePrev = (Get-ChildItem -Path $outPrev -Filter '*.csv' -ErrorAction SilentlyContinue).Count -gt 0
            Assert-Check "C1:$s empty-Previous PS$($v[0])" ($matchedPrev -and -not $wrotePrev) "matched=$matchedPrev fileWritten=$wrotePrev"

            $outCurr = Join-Path $work ("c1_curr_{0}_{1}" -f $s,$v[0])
            Clear-Dir $outCurr
            $rCurr = & $v[1] -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$s.ps1") `
                    -PreviousCSVFile $goodFile -CurrentCSVFile $emptyFile `
                    -AnchorColumn ID -OutputFolder $outCurr 2>&1
            $joinedCurr = (($rCurr -join ' ') -replace '\s+', ' ').Trim()
            $matchedCurr = $joinedCurr -match [regex]::Escape('Current CSV file is empty; no header line found.')
            $wroteCurr = (Get-ChildItem -Path $outCurr -Filter '*.csv' -ErrorAction SilentlyContinue).Count -gt 0
            Assert-Check "C1:$s empty-Current PS$($v[0])" ($matchedCurr -and -not $wroteCurr) "matched=$matchedCurr fileWritten=$wroteCurr"
        }
    }

    # TEST-PLAN.md C3: guards the `return ,$rows` single-element-array-unrolling trap
    # (ENGINEERING-NOTES.md Part 2 section G) - a report must still be produced, with exactly one
    # data row, when each side holds exactly one. single-row\ has one row per side differing in one
    # column, so the report also proves the comparison itself ran, not just that nothing crashed.
    # SUMMARY-row exclusion only matters for Detailed; a harmless no-op for the other four.
    "=== C3: single data row ==="
    $singleRowDir = Join-Path $FixtureFolder 'single-row'
    $srPrev = Join-Path $singleRowDir 'prev.csv'; $srCurr = Join-Path $singleRowDir 'curr.csv'
    if (-not (Test-Path $srPrev) -or -not (Test-Path $srCurr)) {
        Assert-Check 'single-row fixture present' $false "expected prev.csv/curr.csv under $singleRowDir"
    } else {
        foreach ($s in $Scripts) {
            foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
                $out = Join-Path $work ("c3_{0}_{1}" -f $s,$v[0])
                Clear-Dir $out
                & $v[1] -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$s.ps1") `
                    -PreviousCSVFile $srPrev -CurrentCSVFile $srCurr `
                    -AnchorColumn ID -OutputFolder $out *>&1 | Out-Null
                $f = Get-ChildItem -Path $out -Filter '*.csv' -ErrorAction SilentlyContinue | Select-Object -First 1
                if (-not $f) { Assert-Check "C3:$s PS$($v[0]) report written" $false; continue }
                $rows = @(Import-Csv -LiteralPath $f.FullName | Where-Object { $_.ID -ne 'SUMMARY' })
                Assert-Check "C3:$s PS$($v[0]) exactly one data row" ($rows.Count -eq 1) "found $($rows.Count)"
            }
        }
    }

    # -ValueTransforms, -DateFormats and -IgnoreColumns applied to
    # CompareCSVs_Detailed.ps1 - the three cases needing no new fixture. newlines\ already carries a
    # Status column (Active/Inactive vs 1/0, for the transform case) and a HireDate column in two
    # formats (MM/dd/yyyy vs yyyy-MM-dd, for the date case); the ignore case uses Dept, present in
    # every fixture.
    #
    # Hashtable parameters cannot cross a -File process boundary as command-line text - PowerShell
    # renders an object argument by its ToString() ("System.Collections.Hashtable"), not as a real
    # object on the far side. C4 and C5 route through a small driver .ps1 that builds the hashtable
    # in-process and calls Detailed via splatting instead, one driver per PowerShell version, keeping
    # the same fresh-child-process discipline as every other mode. C6's -IgnoreColumns is a
    # [string[]] and crosses the boundary fine as plain text, so it runs directly.
    $newlines = Join-Path $FixtureFolder 'newlines'
    $prev = Join-Path $newlines 'prev.csv'; $curr = Join-Path $newlines 'curr.csv'
    $detailedScript = Join-Path $ScriptFolder 'CompareCSVs_Detailed.ps1'

    if (-not (Test-Path $prev) -or -not (Test-Path $curr)) {
        Assert-Check 'newlines fixture present' $false "expected prev.csv/curr.csv under $newlines"
    } else {

    function Invoke-CoreDriver([string]$label, [string]$body, [string]$exe, [string]$out) {
        Clear-Dir $out
        $driver = Join-Path $work "core_driver_$label.ps1"
        [System.IO.File]::WriteAllText($driver, $body, [System.Text.Encoding]::ASCII)
        & $exe -NoProfile -File $driver *>&1 | Out-Null
        return Get-ChildItem -Path $out -Filter '*.csv' -ErrorAction SilentlyContinue | Select-Object -First 1
    }

    "=== C4: -ValueTransforms ==="
    foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
        $out = Join-Path $work "core_c4_$($v[0])"
        $body = @"
`$t = @{ 'status' = @{ 'Active' = '1'; 'Inactive' = '0'; '*' = 'Unknown' } }
& '$detailedScript' -PreviousCSVFile '$prev' -CurrentCSVFile '$curr' -AnchorColumn ID -OutputFolder '$out' -ValueTransforms `$t
"@
        $f = Invoke-CoreDriver "c4_$($v[0])" $body $v[1] $out
        if (-not $f) { Assert-Check "C4 PS$($v[0]) report written" $false; continue }
        $rows = Import-Csv -LiteralPath $f.FullName
        $matched = @($rows | Where-Object { $_.ID -in @('1','2','4') })
        Assert-Check "C4 PS$($v[0]) matched-row count" ($matched.Count -eq 3) "found $($matched.Count)"
        $allMatch = ($matched.Count -gt 0) -and -not ($matched | Where-Object { $_.'match status' -ne 'True' })
        Assert-Check "C4 PS$($v[0]) match status True for reconciled rows" ([bool]$allMatch)
        $summary = $rows | Where-Object { $_.ID -eq 'SUMMARY' }
        $expectSummary = "Active->1 (2 applied)`nInactive->0 (1 applied)`n*->Unknown (0 applied)"
        Assert-Check "C4 PS$($v[0]) SUMMARY rule inventory" ($summary.'old status' -ceq $expectSummary) "$($summary.'old status')"
    }

    "=== C5: -DateFormats ==="
    foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
        $outNo = Join-Path $work "core_c5_no_$($v[0])"
        $bodyNo = "& '$detailedScript' -PreviousCSVFile '$prev' -CurrentCSVFile '$curr' -AnchorColumn ID -OutputFolder '$outNo'"
        $fNo = Invoke-CoreDriver "c5no_$($v[0])" $bodyNo $v[1] $outNo
        $matchNo = $null
        if ($fNo) { $matchNo = ($(Import-Csv -LiteralPath $fNo.FullName) | Where-Object { $_.ID -eq 'SUMMARY' }).'match hiredate' }
        Assert-Check "C5 PS$($v[0]) baseline 3 of 3 FALSE (no -DateFormats)" ($matchNo -eq '3 of 3 FALSE') "$matchNo"

        $outYes = Join-Path $work "core_c5_yes_$($v[0])"
        $bodyYes = @"
`$d = @{ 'hiredate' = @{ Previous = 'MM/dd/yyyy'; Current = 'yyyy-MM-dd'; Output = 'yyyy-MM-dd' } }
& '$detailedScript' -PreviousCSVFile '$prev' -CurrentCSVFile '$curr' -AnchorColumn ID -OutputFolder '$outYes' -DateFormats `$d
"@
        $fYes = Invoke-CoreDriver "c5yes_$($v[0])" $bodyYes $v[1] $outYes
        if (-not $fYes) { Assert-Check "C5 PS$($v[0]) report written" $false; continue }
        $rowsYes = Import-Csv -LiteralPath $fYes.FullName
        $summaryYes = $rowsYes | Where-Object { $_.ID -eq 'SUMMARY' }
        Assert-Check "C5 PS$($v[0]) 0 of 3 FALSE (with -DateFormats)" ($summaryYes.'match hiredate' -eq '0 of 3 FALSE') "$($summaryYes.'match hiredate')"
        $id1 = $rowsYes | Where-Object { $_.ID -eq '1' }
        Assert-Check "C5 PS$($v[0]) old/new hiredate stay un-normalized" `
            ($id1.'old hiredate' -eq '01/15/2020' -and $id1.'new hiredate' -eq '2020-01-15') `
            "old=$($id1.'old hiredate') new=$($id1.'new hiredate')"
    }

    "=== C6: -IgnoreColumns ==="
    foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
        $out = Join-Path $work "core_c6_$($v[0])"
        Clear-Dir $out
        & $v[1] -NoProfile -File $detailedScript -PreviousCSVFile $prev -CurrentCSVFile $curr `
            -AnchorColumn ID -OutputFolder $out -IgnoreColumns Dept *>&1 | Out-Null
        $f = Get-ChildItem -Path $out -Filter '*.csv' -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $f) { Assert-Check "C6 PS$($v[0]) report written" $false; continue }
        $header = Get-Content -LiteralPath $f.FullName -TotalCount 1
        $noDeptTriplet = ($header -notmatch '"old dept"' -and $header -notmatch '"new dept"' -and $header -notmatch '"match dept"')
        $stillHasOtherTriplet = ($header -match '"old hiredate"' -and $header -match '"match hiredate"')
        Assert-Check "C6 PS$($v[0]) ignored column has no old/new/match triplet" $noDeptTriplet "$header"
        Assert-Check "C6 PS$($v[0]) other columns still triplet-shaped" $stillHasOtherTriplet
    }

    }

    # TEST-PLAN.md E1/E2: a path decorated with wildcard metacharacters (E1) or non-ASCII characters
    # (E2) must be accepted and produce byte-identical output to the same fixture run from an
    # undecorated path - proving the decoration reaches neither the comparison nor the output. B7
    # (fixed 2026-08-07, all five scripts) closed Test-Path -Path treating [ and ] as wildcards and
    # silently rejecting real files; nothing has asserted the fix stays intact until now.
    #
    # Both prev.csv/curr.csv AND -OutputFolder are decorated, per TEST-PLAN.md's wording. large.ps1
    # stays in the loop deliberately - it also spools run files into -OutputFolder mid-run
    # (DECISIONS.md #8), giving a path defect more surface there than in the other four.
    #
    # Get-ChildItem -LiteralPath, not the -Path pattern the rest of this file uses (Invoke-One,
    # Clear-Dir, every inline "did it write a file" check) - confirmed 2026-08-20 that -Path silently
    # finds nothing against E1's bracketed -OutputFolder even though CompareCSVs_large.ps1 wrote the
    # report correctly and printed the right path: -Path treats [ and ] as wildcards, the exact defect
    # class B7 fixed in the scripts themselves, now surfacing in the harness's own file lookup instead.
    # E2's plain non-ASCII paths don't trigger this (no wildcard metacharacters), but using the same
    # literal-path lookup for both keeps the two cases identical in shape.
    function Get-HashLiteral([string]$dir) {
        $f = Get-ChildItem -LiteralPath $dir -Filter '*.csv' -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($f) { return (Get-FileHash -LiteralPath $f.FullName -Algorithm SHA256).Hash.Substring(0,12) }
        return 'NOFILE'
    }

    "=== E1/E2: decorated paths ==="
    $sparseDir = Join-Path $FixtureFolder 'sparse'
    $sparsePrev = Join-Path $sparseDir 'prev.csv'; $sparseCurr = Join-Path $sparseDir 'curr.csv'
    if (-not (Test-Path $sparsePrev) -or -not (Test-Path $sparseCurr)) {
        Assert-Check 'sparse fixture present' $false "expected prev.csv/curr.csv under $sparseDir"
    } else {
        # E2's accented character is built from a char code, not typed literally - every .ps1 in this
        # repo must stay pure ASCII (STATUS.md), and $() around the backtick-escaped $ in E1's suffix
        # keeps that string literal too (no expansion, no non-ASCII byte in source either way).
        $decorations = @(
            @{ Group = 'E1'; Suffix = "[b] `$d 'q'" }
            @{ Group = 'E2'; Suffix = "caf$([char]0xE9)" }
        )
        foreach ($s in $Scripts) {
            foreach ($v in @(@('7','pwsh'),@('51','powershell'))) {
                # Undecorated control, once per script/version - both E1 and E2 below compare against
                # this same run rather than each computing their own.
                $ctrlOut = Join-Path $work ("e1e2_ctrl_{0}_{1}" -f $s,$v[0])
                $ctrlHash = Invoke-One $s $v[1] $sparsePrev $sparseCurr $ctrlOut

                foreach ($d in $decorations) {
                    # Recreated fresh every run (not Clear-Dir, which is -Path-based and therefore
                    # bracket-unsafe for E1) - decoRoot is a scratch leaf this test alone owns, so a
                    # recursive delete-then-recreate is simpler than reproducing Clear-Dir's
                    # csv-only-preserve-the-rest semantics, which exists for dirs reused across
                    # fixtures within one run; each decoRoot name here is already unique per
                    # (group, script, version).
                    $decoRoot = Join-Path $work ("{0}_{1}_{2} {3}" -f $d.Group,$s,$v[0],$d.Suffix)
                    if (Test-Path -LiteralPath $decoRoot) { Remove-Item -LiteralPath $decoRoot -Recurse -Force }
                    New-Item -ItemType Directory -Force $decoRoot | Out-Null
                    $decoPrev = Join-Path $decoRoot 'prev.csv'; $decoCurr = Join-Path $decoRoot 'curr.csv'
                    Copy-Item -LiteralPath $sparsePrev -Destination $decoPrev -Force
                    Copy-Item -LiteralPath $sparseCurr -Destination $decoCurr -Force
                    $decoOut = Join-Path $decoRoot ("out {0}" -f $d.Suffix)
                    New-Item -ItemType Directory -Force $decoOut | Out-Null

                    & $v[1] -NoProfile -File (Join-Path $ScriptFolder "CompareCSVs_$s.ps1") `
                            -PreviousCSVFile $decoPrev -CurrentCSVFile $decoCurr `
                            -AnchorColumn ID -OutputFolder $decoOut *>&1 | Out-Null
                    $exitCode = $LASTEXITCODE
                    $decoHash = Get-HashLiteral $decoOut
                    $ok = ($exitCode -eq 0 -and $decoHash -ne 'NOFILE' -and $decoHash -eq $ctrlHash)
                    Assert-Check "$($d.Group):$s PS$($v[0])" $ok "exit=$exitCode deco=$decoHash ctrl=$ctrlHash"
                }
            }
        }
    }
}
else {  # Memory
    $big = Join-Path $FixtureFolder 'large-generated'
    if (-not (Test-Path (Join-Path $big 'prev.csv'))) {
        throw "Large fixture missing. Run .\New-LargeFixture.ps1 first."
    }
    function Measure-Peak([string]$script,[string]$out) {
        Clear-Dir $out
        $a = @('-NoProfile','-File',(Join-Path $ScriptFolder "CompareCSVs_$script.ps1"),
               '-PreviousCSVFile',(Join-Path $big 'prev.csv'),'-CurrentCSVFile',(Join-Path $big 'curr.csv'),
               '-AnchorColumn','ID','-OutputFolder',$out)
        $p = Start-Process pwsh -ArgumentList $a -PassThru -NoNewWindow -RedirectStandardOutput (Join-Path $out 'run.log')
        $peak = 0
        while (-not $p.WaitForExit(75)) { try { $p.Refresh(); if ($p.WorkingSet64 -gt $peak) { $peak = $p.WorkingSet64 } } catch {} }
        return [math]::Round($peak/1MB)
    }
    $bl = Start-Process pwsh -ArgumentList @('-NoProfile','-Command','exit') -PassThru -NoNewWindow
    $blPeak = 0
    while (-not $bl.WaitForExit(50)) { try { $bl.Refresh(); if ($bl.WorkingSet64 -gt $blPeak) { $blPeak = $bl.WorkingSet64 } } catch {} }
    "pwsh baseline peak: {0} MB (subtract this for the real figure)" -f [math]::Round($blPeak/1MB)
    foreach ($s in $Scripts) { $null = Measure-Peak $s (Join-Path $work "warm_$s") }   # warm-up, discarded
    $best = @{}
    foreach ($r in 1..$Reps) {
        foreach ($s in $Scripts) {
            $mb = Measure-Peak $s (Join-Path $work ("mem_{0}_{1}" -f $s,$r))
            if (-not $best.ContainsKey($s) -or $mb -lt $best[$s]) { $best[$s] = $mb }
        }
    }
    foreach ($s in $Scripts) { "    {0,-11} peak {1,5} MB   ({2} MB above baseline)" -f $s,$best[$s],($best[$s]-[math]::Round($blPeak/1MB)) }
    # The reason CompareCSVs_large.ps1 exists is to use less memory than the in-memory script. If it
    # ever stops doing so, that is a result worth failing on rather than printing.
    if ($Scripts -contains 'small' -and $Scripts -contains 'large') {
        Assert-Check 'large peak < small peak' ($best['large'] -lt $best['small']) "large=$($best['large']) MB  small=$($best['small']) MB"
    }
}

""
if ($failures -eq 0) {
    "RESULT: all checks passed"
} else {
    "RESULT: $failures check(s) FAILED"
}
exit $(if ($failures -gt 0) { 1 } else { 0 })