function Compare-CsvData {
<#
.SYNOPSIS
Compares two CSV exports on an anchor column and returns the rows that changed, whole.

.DESCRIPTION
Joins both files on -AnchorColumn and returns one object per changed row: Add, Update or Delete.
Unchanged rows are counted, never returned. Add and Update rows carry today's values; Deletes
carry the previous day's.

Rows come back in the output shape already - a verdict column, then the current file's columns in
own order - so they pipe straight to Export-Csv.

THE RETURN SHAPE DEPENDS ON -IncludeSummary. SEE OUTPUTS.

.PARAMETER PreviousCsvPath
Path to the previous day's CSV.

.PARAMETER CurrentCsvPath
Path to today's CSV.

.PARAMETER AnchorColumn
The anchor column - the column that identifies a row, such as EmployeeID. Both files are
joined on it. Matched exactly, letter case included.

.PARAMETER Encoding
Encoding both files are read with; must match what your Export-Csv writes. 'UTF8' is the only value
both PowerShell versions accept. Mandatory, so the coupling is visible at the call site.

Always pass -Encoding to your own Export-Csv too: 5.1's default destroys accented characters.

.PARAMETER DelimiterName
comma, tab, semicolon or pipe; must match what your Export-Csv wrote. Named rather than a raw
character, because a literal tab is invisible in a diff.

A mismatch throws, but blames the anchor - supply -ExpectedColumns for a message showing both column
sets instead. Avoid -UseCulture: its separator is a comma on en-US and a semicolon on other locales.

.PARAMETER ExpectedColumns
Optional. The column names your transform produces. Both files must carry exactly this set, same
letter case, or the run throws naming the side that diverged.

It is the only thing that catches a column missing from BOTH files: Export-Csv builds its header from
the first object alone, so a property absent there vanishes from every day's file alike.

Give SOURCE column names including the anchor. Order is irrelevant. Do not list the verdict column.
Empty, whitespace-only and duplicate entries are rejected before either file is opened.

.PARAMETER ChangeTypeColumnName
Name of the leading verdict column. Default 'ChangeType'.

Cannot be blank, and cannot collide with a source column - the run throws rather than silently
renaming either one. Checked ignoring letter case and surrounding whitespace.

.PARAMETER CaseSensitive
Compares FIELD VALUES case-sensitively. Off by default, so 'Ops' and 'OPS' are the same value.

It does not reach row matching. Anchors always match case-insensitively and trimmed, so 'E1042',
'e1042' and ' E1042 ' are one row; splitting them would emit a Delete plus an Add.

Trimming is identity only - rows carry the file's own value - and field values are never trimmed.
Column names are always matched exactly.

.PARAMETER IncludeSummary
Changes the return shape. See OUTPUTS.

.INPUTS
None. Paths are passed as parameters.

.OUTPUTS
WITHOUT -IncludeSummary (the default): the changed rows themselves, one object each.

    $changes = Compare-CsvData -PreviousCsvPath $yesterday -CurrentCsvPath $today `
                                -AnchorColumn EmployeeID -Encoding UTF8 -DelimiterName comma
    $changes | Export-Csv -LiteralPath $out -NoTypeInformation -Encoding UTF8

    "ChangeType","EmployeeID","GivenName","dept","title"
    "Update","E1002","Bob","Sales","Director"
    "Delete","E1004","Dan","IT","Engineer"
    "Add","E1005","Erin","HR","Recruiter"

WITH -IncludeSummary: a hashtable of two keys - Changes (the same rows) and Summary (Adds, Updates,
Deletes, Unchanged, Total, PreviousCount, CurrentCount).

    $r.Changes | Export-Csv -LiteralPath $out -NoTypeInformation -Encoding UTF8
    "Unchanged: $($r.Summary.Unchanged)"

Unchanged, PreviousCount and CurrentCount cannot be recovered from the rows afterwards.

No changes returns an empty array, not $null. What to write in that case is yours to decide.

ROW ORDER IS NOT GUARANTEED. Sort them yourself if order matters to whatever reads them.

.EXAMPLE
$changes = Compare-CsvData -PreviousCsvPath .\yesterday.csv -CurrentCsvPath .\today.csv `
                            -AnchorColumn EmployeeID -Encoding UTF8 -DelimiterName comma
$changes | Where-Object ChangeType -eq 'Delete' | ForEach-Object { Remove-Account $_.EmployeeID }

.EXAMPLE
$r = Compare-CsvData -PreviousCsvPath .\yesterday.csv -CurrentCsvPath .\today.csv `
                         -AnchorColumn EmployeeID -Encoding UTF8 -DelimiterName comma -IncludeSummary
if ($r.Summary.PreviousCount -lt ($r.Summary.CurrentCount * 0.9)) { throw 'Previous file looks truncated.' }

.NOTES
Both files must be produced by the same export code: same column names and letter case, same
delimiter, written with -Encoding UTF8 and -NoTypeInformation.
#>
    [CmdletBinding()]
    Param(
        [Parameter(Mandatory)][string]$PreviousCsvPath,
        [Parameter(Mandatory)][string]$CurrentCsvPath,
        [Parameter(Mandatory)][string]$AnchorColumn,
        [Parameter(Mandatory)][ValidateSet('UTF8')][string]$Encoding,
        [Parameter(Mandatory)][ValidateSet('comma','tab','semicolon','pipe')][string]$DelimiterName,
        [ValidateNotNullOrEmpty()][string[]]$ExpectedColumns,
        [ValidateNotNullOrEmpty()][string]$ChangeTypeColumnName = 'ChangeType',
        [switch]$CaseSensitive,
        [switch]$IncludeSummary
    )

    # [ValidateNotNullOrEmpty()] admits '   ', which would name the output column with whitespace.
    if ([string]::IsNullOrWhiteSpace($ChangeTypeColumnName)) {
        throw "Parameter -ChangeTypeColumnName cannot be empty or whitespace."
    }

    # Checked before either file is read: a malformed declaration would otherwise report
    # "the file does not match" about a file that is correct.
    if ($PSBoundParameters.ContainsKey('ExpectedColumns')) {
        $blankEntries = @($ExpectedColumns | Where-Object { [string]::IsNullOrWhiteSpace($_) })
        if ($blankEntries.Count -gt 0) {
            throw "-ExpectedColumns contains $($blankEntries.Count) empty or whitespace-only entry(s). Every entry must be a column name."
        }
        # Case-INsensitive deliberately: Import-Csv rejects a header carrying both 'Name' and 'name',
        # so declaring both is always wrong.
        $dupGroups = @($ExpectedColumns | Group-Object | Where-Object { $_.Count -gt 1 })
        if ($dupGroups.Count -gt 0) {
            # Name every spelling: reporting one of 'dept'/'DEPT' hides why they collided.
            $detail = ($dupGroups | ForEach-Object {
                $spellings = @($_.Group | Select-Object -Unique)
                if ($spellings.Count -gt 1) { ($spellings | ForEach-Object { "'$_'" }) -join ' and ' }
                else { "'$($spellings[0])' twice" }
            }) -join '; '
            throw "-ExpectedColumns lists $detail, naming the same column. Names are compared here ignoring letter case, because Import-Csv cannot produce a row carrying two members whose names differ only by case."
        }
    }

    # Quote names in any set we report: padded and whitespace-only entries are otherwise invisible.
    function Format-NameSet($Names) { ($Names | ForEach-Object { "'$_'" }) -join ' | ' }

    # Named rather than a raw character: a literal tab at a call site is invisible in a diff.
    $delimiter = switch ($DelimiterName) {
        'comma'     { ',' }
        'tab'       { "`t" }
        'semicolon' { ';' }
        'pipe'      { '|' }
    }

    # A blank column name becomes 'H1', announced only as a WARNING - a stream this function does not
    # own and a runbook routinely suppresses. Both files get the same invented name, so the schema check
    # passes and 'H1' ships in this function's own output header. -WarningVariable captures it regardless.
    $previous = @(Import-Csv -LiteralPath $PreviousCsvPath -Encoding $Encoding -Delimiter $delimiter -WarningAction SilentlyContinue -WarningVariable prevWarn)
    $current  = @(Import-Csv -LiteralPath $CurrentCsvPath  -Encoding $Encoding -Delimiter $delimiter -WarningAction SilentlyContinue -WarningVariable currWarn)
    foreach ($side in @(@('Previous', $prevWarn, $PreviousCsvPath), @('Current', $currWarn, $CurrentCsvPath))) {
        if ($side[1]) {
            throw "$($side[0]) file was not parsed cleanly: $($side[1] -join ' / ') The usual cause is a blank column name in the header, which is replaced with an invented one such as 'H1' - and that name would travel into this function's own output header. File: $($side[2])"
        }
    }
    if ($previous.Count -eq 0) { throw "Previous file yielded no rows: $PreviousCsvPath" }
    if ($current.Count  -eq 0) { throw "Current file yielded no rows: $CurrentCsvPath" }

    # Element by element, never a joined string: a name containing the join character would let two
    # genuinely different sets compare equal.
    function Test-NameSetsEqual($A, $B) {
        if ($A.Count -ne $B.Count) { return $false }
        for ($n = 0; $n -lt $A.Count; $n++) { if ($A[$n] -cne $B[$n]) { return $false } }
        return $true
    }

    $prevNames = @($previous[0].PSObject.Properties.Name | Sort-Object)
    $currNames = @($current[0].PSObject.Properties.Name  | Sort-Object)

    if ($PSBoundParameters.ContainsKey('ExpectedColumns')) {
        # Catches a column missing from BOTH files, which comparing them to each other cannot.
        # Export-Csv takes its header from the first object only.
        $expected = @($ExpectedColumns | Sort-Object)
        if (-not (Test-NameSetsEqual $expected $prevNames)) {
            throw "Previous file's columns do not match -ExpectedColumns. Expected: $(Format-NameSet $expected) || Previous: $(Format-NameSet $prevNames)"
        }
        if (-not (Test-NameSetsEqual $expected $currNames)) {
            throw "Current file's columns do not match -ExpectedColumns. Expected: $(Format-NameSet $expected) || Current: $(Format-NameSet $currNames)"
        }
        # Both now equal $expected, so they equal each other - no separate pairwise check needed.
    }
    elseif (-not (Test-NameSetsEqual $prevNames $currNames)) {
        throw "Column sets differ. Previous: $(Format-NameSet $prevNames) || Current: $(Format-NameSet $currNames)"
    }
    if ($prevNames -cnotcontains $AnchorColumn) {
        throw "Anchor column '$AnchorColumn' not found (exact match required). Columns: $(Format-NameSet $prevNames)"
    }

    # A source column of this name overwrites the verdict with that row's own data, silently.
    # Compared case-insensitively and trimmed both sides, so 'changetype' and 'ChangeType ' both
    # collide. Never auto-renamed - that would be an undetectable contract change.
    $verdictNorm = $ChangeTypeColumnName.Trim()
    $collision = @($currNames | Where-Object { $_.Trim() -ieq $verdictNorm }) | Select-Object -First 1
    if ($collision) {
        throw "Source column '$collision' collides with the verdict column name '$ChangeTypeColumnName'. Pass a different -ChangeTypeColumnName to resolve this."
    }

    # Always case-insensitive; -CaseSensitive does not reach it. A case-sensitive anchor would split
    # one employee into a Delete plus an Add; a wrong value comparison costs only a stray Update.
    $anchorComparer = [System.StringComparer]::OrdinalIgnoreCase

    # Output column order is Current's physical order. Built once.
    $outputColumns = @($current[0].PSObject.Properties.Name)
    $compareNames  = @($outputColumns | Where-Object { $_ -cne $AnchorColumn })

    # Trimmed for IDENTITY only - a row emits whatever the file held. ' E1' and 'E1' are one employee;
    # splitting them would produce a Delete plus an Add. Field values are never trimmed.
    $currentLookup = [System.Collections.Generic.Dictionary[string,object]]::new($anchorComparer)
    $i = 0
    foreach ($rec in $current) {
        $i++
        $raw = $rec.$AnchorColumn
        if ([string]::IsNullOrWhiteSpace($raw)) { throw "Anchor '$AnchorColumn' is empty in Current row $i." }
        $key = $raw.Trim()
        if ($currentLookup.ContainsKey($key)) {
            # Report the RAW spellings - the trimmed anchor alone hides why the two collided.
            $firstRaw = $currentLookup[$key].$AnchorColumn
            if ($firstRaw -cne $raw) {
                throw "Duplicate anchor in Current at row $($i): '$raw' and '$firstRaw' are the same identity. Anchors match ignoring letter case and surrounding whitespace."
            }
            throw "Duplicate anchor '$raw' in Current at row $i."
        }
        $currentLookup[$key] = $rec
    }

    # One row per change: verdict column, then Current's columns in Current's order. Pipes straight
    # to Export-Csv.
    function New-DeltaRow($Verdict, $Source, $Columns, $VerdictColumn) {
        $o = [ordered]@{ $VerdictColumn = $Verdict }
        foreach ($n in $Columns) { $o[$n] = $Source.$n }
        [pscustomobject]$o
    }

    $changes = [System.Collections.Generic.List[object]]::new()
    $adds = 0; $updates = 0; $deletes = 0; $unchanged = 0
    # Maps the trimmed anchor to the raw spelling that claimed it, so a duplicate can show both.
    $seen = [System.Collections.Generic.Dictionary[string,string]]::new($anchorComparer)

    $i = 0
    foreach ($prev in $previous) {
        $i++
        $raw = $prev.$AnchorColumn
        if ([string]::IsNullOrWhiteSpace($raw)) { throw "Anchor '$AnchorColumn' is empty in Previous row $i." }
        $key = $raw.Trim()   # identity only - see the note at the Current lookup above
        if ($seen.ContainsKey($key)) {
            $firstRaw = $seen[$key]
            if ($firstRaw -cne $raw) {
                throw "Duplicate anchor in Previous at row $($i): '$raw' and '$firstRaw' are the same identity. Anchors match ignoring letter case and surrounding whitespace."
            }
            throw "Duplicate anchor '$raw' in Previous at row $i."
        }
        $seen[$key] = $raw

        if ($currentLookup.ContainsKey($key)) {
            $curr = $currentLookup[$key]
            # Stop at the first difference - the row is emitted whole either way.
            $isUpdate = $false
            foreach ($n in $compareNames) {
                $a = $prev.$n; $b = $curr.$n
                if ([string]::IsNullOrWhiteSpace($a)) { $a = '' }
                if ([string]::IsNullOrWhiteSpace($b)) { $b = '' }
                $differs = if ($CaseSensitive) { $a -cne $b } else { $a -ine $b }
                if ($differs) { $isUpdate = $true; break }
            }
            [void]$currentLookup.Remove($key)
            if ($isUpdate) {
                $updates++
                $changes.Add((New-DeltaRow 'Update' $curr $outputColumns $ChangeTypeColumnName))
            } else { $unchanged++ }
        } else {
            # No Current row, so a Delete row carries Previous's values - the one place the file
            # mixes two points in time.
            $deletes++
            $changes.Add((New-DeltaRow 'Delete' $prev $outputColumns $ChangeTypeColumnName))
        }
    }

    # Survivors were never in Previous. Enumeration happens to give Current's file order, but
    # Dictionary promises nothing - hence the help's caveat. Forcing it by re-walking $current costs
    # ~500ms per run on PS7 at 10k rows, for an ordering nothing consumes.
    foreach ($key in $currentLookup.Keys) {
        $adds++
        $changes.Add((New-DeltaRow 'Add' $currentLookup[$key] $outputColumns $ChangeTypeColumnName))
    }

    # The leading comma stops PowerShell unrolling the list: an empty result stays an array, and a
    # single row does not collapse to a bare object.
    if (-not $IncludeSummary) { return ,$changes.ToArray() }

    # Unchanged, PreviousCount and CurrentCount cannot be recovered from the rows, and this function
    # writes nothing to the console.
    @{
        Changes = $changes.ToArray()
        Summary = [pscustomobject]@{
            Adds = $adds; Updates = $updates; Deletes = $deletes; Unchanged = $unchanged
            Total = $adds + $updates + $deletes + $unchanged
            PreviousCount = $previous.Count; CurrentCount = $current.Count
        }
    }
}