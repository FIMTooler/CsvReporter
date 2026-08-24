function Compare-ImportedCsv {
<#
.SYNOPSIS
Compares two collections of imported CSV rows on an anchor column and returns the rows that changed,
whole.

.DESCRIPTION
The object-input companion to Compare-CsvData: the same comparison, the same anchor identity and
absent-value rules, taking two collections of rows instead of two file paths. The caller owns
Import-Csv (and, for the result, Export-Csv) - this function does no file I/O of its own.

Joins both collections on -AnchorColumn and returns one object per changed row: Add, Update or Delete.
Unchanged rows are counted, never returned. Add and Update rows carry today's values; Deletes carry
the previous day's.

Rows come back in the output shape already - a verdict column, then the current collection's columns
in their own order - so they pipe straight to Export-Csv.

THE RETURN SHAPE DEPENDS ON -IncludeSummary. SEE OUTPUTS.

.PARAMETER PreviousRows
The previous day's rows - rows exactly as Import-Csv produces them: [pscustomobject] rows, every cell
a string or absent. Every row is checked against this before anything else runs; the check names the
row and, for a bad cell, the column.

.PARAMETER CurrentRows
Today's rows. Same contract as -PreviousRows.

.PARAMETER AnchorColumn
The anchor column - the column that identifies a row, such as EmployeeID. Both collections are
joined on it. Matched exactly, letter case included.

.PARAMETER ExpectedColumns
Optional. The column names your transform produces. Both collections must carry exactly this set,
same letter case, or the run throws naming the side that diverged.

It is the only thing that catches a column missing from BOTH sides: if the file either collection was
read from was written by Export-Csv, its header came from the first object alone, so a property absent
there vanishes from every day's file alike.

Give SOURCE column names including the anchor. Order is irrelevant. Do not list the verdict column.
Empty, whitespace-only and duplicate entries are rejected before either collection is examined.

.PARAMETER ChangeTypeColumnName
Name of the leading verdict column. Default 'ChangeType'.

Cannot be blank, and cannot collide with a source column - the run throws rather than silently
renaming either one. Checked ignoring letter case and surrounding whitespace.

.PARAMETER CaseSensitive
Compares FIELD VALUES case-sensitively. Off by default, so 'Ops' and 'OPS' are the same value.

It does not reach row matching. Anchors always match case-insensitively and trimmed, so 'E1042',
'e1042' and ' E1042 ' are one row; splitting them would emit a Delete plus an Add.

Trimming is identity only - rows carry the collection's own value - and field values are never
trimmed. Column names are always matched exactly.

.PARAMETER IncludeSummary
Changes the return shape. See OUTPUTS.

.INPUTS
None. Rows are passed as parameters, not the pipeline - two same-shaped collections down one pipeline
would leave which one is Previous and which is Current ambiguous.

.OUTPUTS
WITHOUT -IncludeSummary (the default): the changed rows themselves, one object each.

    $previous = Import-Csv -LiteralPath $yesterday -Encoding UTF8
    $current  = Import-Csv -LiteralPath $today -Encoding UTF8
    $changes = Compare-ImportedCsv -PreviousRows $previous -CurrentRows $current -AnchorColumn EmployeeID
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
$previous = Import-Csv -LiteralPath .\yesterday.csv -Encoding UTF8
$current  = Import-Csv -LiteralPath .\today.csv -Encoding UTF8
$changes = Compare-ImportedCsv -PreviousRows $previous -CurrentRows $current -AnchorColumn EmployeeID
$changes | Where-Object ChangeType -eq 'Delete' | ForEach-Object { Remove-Account $_.EmployeeID }

.EXAMPLE
$r = Compare-ImportedCsv -PreviousRows $previous -CurrentRows $current -AnchorColumn EmployeeID `
                          -IncludeSummary
if ($r.Summary.PreviousCount -lt ($r.Summary.CurrentCount * 0.9)) { throw 'Previous collection looks truncated.' }

.NOTES
Both collections must hold [pscustomobject] rows with the same column names and letter case
throughout - every row, not just the first; a ragged or mismatched row is rejected before comparison
starts, naming the side and the row, and, for a bad cell, the column too.

-PreviousRows and -CurrentRows are Mandatory array parameters, so PowerShell itself rejects $null, an
empty collection, or a collection containing a $null element, on either one, before this function
runs - with its own message, not this function's.
#>
    [CmdletBinding()]
    Param(
        # [object[]], not [pscustomobject[]] - binding an element into a [pscustomobject[]] array
        # wraps it in PSObject, and a wrapped Hashtable then passes -is [pscustomobject] as True.
        # [object[]] leaves a bad element's real type intact for Test-RowShapes below to catch.
        [Parameter(Mandatory)][object[]]$PreviousRows,
        [Parameter(Mandatory)][object[]]$CurrentRows,
        [Parameter(Mandatory)][string]$AnchorColumn,
        [ValidateNotNullOrEmpty()][string[]]$ExpectedColumns,
        [ValidateNotNullOrEmpty()][string]$ChangeTypeColumnName = 'ChangeType',
        [switch]$CaseSensitive,
        [switch]$IncludeSummary
    )

    # [ValidateNotNullOrEmpty()] admits '   ', which would name the output column with whitespace.
    if ([string]::IsNullOrWhiteSpace($ChangeTypeColumnName)) {
        throw "Parameter -ChangeTypeColumnName cannot be empty or whitespace."
    }

    # Checked before either collection is examined: a malformed declaration would otherwise report
    # "the collection does not match" about a collection that is correct.
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

    # Element by element, never a joined string: a name containing the join character would let two
    # genuinely different sets compare equal. Defined here, ahead of the file version's position for
    # it, because the row-shape check below needs it before row 0's schema is ever trusted.
    function Test-NameSetsEqual($A, $B) {
        if ($A.Count -ne $B.Count) { return $false }
        for ($n = 0; $n -lt $A.Count; $n++) { if ($A[$n] -cne $B[$n]) { return $false } }
        return $true
    }

    $previous = @($PreviousRows)
    $current  = @($CurrentRows)

    # Import-Csv guarantees pscustomobject rows with a uniform, string-or-absent property set; a
    # caller-supplied collection does not, so every row is checked here before anything downstream
    # trusts row 0's shape for the whole side. Fails on the first bad row found.
    function Test-RowShapes($Rows, $Side) {
        $refNames = $null
        for ($i = 0; $i -lt $Rows.Count; $i++) {
            $rowNum = $i + 1
            $row = $Rows[$i]
            # $row is never $null here: -PreviousRows/-CurrentRows are Mandatory array parameters,
            # and PowerShell rejects a $null element at the binder before this function runs.
            if ($row -isnot [pscustomobject]) {
                throw "$Side row $rowNum is $($row.GetType().FullName), not [pscustomobject]. Compare-ImportedCsv accepts rows exactly as Import-Csv produces them."
            }
            $names = @($row.PSObject.Properties.Name | Sort-Object)
            if ($null -eq $refNames) {
                $refNames = $names
            } elseif (-not (Test-NameSetsEqual $refNames $names)) {
                throw "$Side row $rowNum's columns do not match row 1's. Row 1: $(Format-NameSet $refNames) || Row ${rowNum}: $(Format-NameSet $names)"
            }
            foreach ($p in $row.PSObject.Properties) {
                if ($null -ne $p.Value -and $p.Value -isnot [string]) {
                    throw "$Side row $rowNum, column '$($p.Name)' is a $($p.Value.GetType().FullName), not a string. Compare-ImportedCsv accepts rows exactly as Import-Csv produces them - every cell must be a string or absent."
                }
            }
        }
    }
    Test-RowShapes $previous 'Previous'
    Test-RowShapes $current  'Current'

    $prevNames = @($previous[0].PSObject.Properties.Name | Sort-Object)
    $currNames = @($current[0].PSObject.Properties.Name  | Sort-Object)

    if ($PSBoundParameters.ContainsKey('ExpectedColumns')) {
        # Catches a column missing from BOTH sides, which comparing them to each other cannot.
        # Export-Csv takes its header from the first object only.
        $expected = @($ExpectedColumns | Sort-Object)
        if (-not (Test-NameSetsEqual $expected $prevNames)) {
            throw "Previous collection's columns do not match -ExpectedColumns. Expected: $(Format-NameSet $expected) || Previous: $(Format-NameSet $prevNames)"
        }
        if (-not (Test-NameSetsEqual $expected $currNames)) {
            throw "Current collection's columns do not match -ExpectedColumns. Expected: $(Format-NameSet $expected) || Current: $(Format-NameSet $currNames)"
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

    # Trimmed for IDENTITY only - a row emits whatever the collection held. ' E1' and 'E1' are one
    # employee; splitting them would produce a Delete plus an Add. Field values are never trimmed.
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
            # No Current row, so a Delete row carries Previous's values - the one place the collection
            # mixes two points in time.
            $deletes++
            $changes.Add((New-DeltaRow 'Delete' $prev $outputColumns $ChangeTypeColumnName))
        }
    }

    # Survivors were never in Previous. Enumeration happens to give Current's collection order, but
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