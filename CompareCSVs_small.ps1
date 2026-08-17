<#
.SYNOPSIS
Compares two CSV files and writes a changes report (Adds, Updates, Deletes).

.DESCRIPTION
In-memory comparison:
- Robust header parsing (quoted headers, embedded delimiters).
- Strict anchor presence; fails if the anchor column is missing.
- Detects and warns about duplicate anchor values (uses first occurrence only).
- Case-sensitive or insensitive comparisons per -CaseSensitive.
- Outputs a CSV with ChangeType and old/new values for changed columns.
  - On Update rows only the columns that actually changed are populated. Unchanged columns are
    left as empty unquoted fields, which keeps a mostly-unchanged report small and makes the
    changed cells easy to spot. An explicitly empty value is written as "" instead.
- Rows are written in the order they are found: Current's row order, then deletions in whatever
  order the lookup enumerates them. Output is not sorted by anchor.
- Prints a one-line summary with counts and elapsed time.

.PARAMETER PreviousCSVFile
Path to the "Previous" CSV file.

.PARAMETER CurrentCSVFile
Path to the "Current" CSV file.

.PARAMETER AnchorColumn
Header name of the key/anchor column used to join rows.

.PARAMETER OutputFolder
Folder where the changes CSV will be written.

.PARAMETER DelimiterName
Logical delimiter name: comma, tab, semicolon, or pipe.

.PARAMETER EncodingName
Input/output encoding. One of: auto, ascii, ansi, default, oem, unicode, utf8BOM, utf8NoBOM.
Default: utf8BOM. Every option produces the same bytes on Windows PowerShell 5.1 and PowerShell 7+.

Reading: a byte-order mark in the input file always wins. This value only decides how a file with
no BOM is decoded. The default therefore behaves as "use the BOM if present, otherwise UTF-8",
which is also what 'auto' does - the two are equivalent on every input.

IMPORTANT: a file with no BOM is assumed to be UTF-8. A legacy Windows (ANSI/code page) export
with no BOM will be decoded incorrectly, turning accented and other non-ASCII characters into
replacement characters. Because this script compares values, that shows up as spurious differences
rather than as an error. Pass -EncodingName ansi for such files.

Writing: the output file is always written in the encoding named here; 'auto' writes UTF-8 with a
BOM. Keep the BOM ('utf8BOM') if the report will be opened in Excel, which otherwise reads UTF-8
output as ANSI.

'default' is an alias for 'ansi' (the system ANSI code page), kept for backward compatibility.

.PARAMETER CaseSensitive
Use case-sensitive comparisons when set.

.PARAMETER RejectDuplicateAnchors
Fail the run instead of warning when a duplicate anchor value is found. Default behaviour (this
switch absent) is to warn and continue, using the first occurrence and ignoring the rest. With this
switch, the run throws on the first duplicate it finds and writes no report - useful when the anchor
is meant to be unique, since a duplicate is then a data-quality problem (often a wrong
-AnchorColumn) rather than something to quietly work around.

.INPUTS
None. You cannot pipe objects to this script.

.OUTPUTS
None. Writes a changes CSV to -OutputFolder and summary messages to the console.

.EXAMPLE
.\CompareCSVs_small.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -DelimiterName comma -EncodingName utf8BOM -CaseSensitive

.NOTES
Requires Microsoft.VisualBasic for TextFieldParser header parsing.
Duplicate anchor detection: When duplicates are found, the script warns with yellow text
showing the anchor value and row numbers, then processes only the first occurrence - unless
-RejectDuplicateAnchors is passed, in which case it fails the run on the first one instead.
#>
[CmdletBinding()]
Param(
    [Parameter(Mandatory=$true)]
    [ValidateScript({Test-Path -LiteralPath $_ -PathType Leaf })]
    [String]$PreviousCSVFile,
    [Parameter(Mandatory=$true)]
    [ValidateScript({Test-Path -LiteralPath $_ -PathType Leaf })]
    [String]$CurrentCSVFile,
    [Parameter(Mandatory=$true)]
    [ValidateNotNullOrEmpty()]
    [String]$AnchorColumn,
    [Parameter(Mandatory=$true)]
    [ValidateScript({Test-Path -LiteralPath $_ -PathType Container })]
    [String]$OutputFolder,
    [ValidateSet('comma','tab','semicolon','pipe')]
    [string]$DelimiterName = 'comma',
    [ValidateSet('auto','ascii','ansi','default','oem','unicode','utf8BOM','utf8NoBOM')]
    [string]$EncodingName = 'utf8BOM',
    [switch]$CaseSensitive,
    [switch]$RejectDuplicateAnchors
)
try {
    $scriptStartTime = Get-Date

    # Resolve delimiter from name
    $Delimiter = switch ($DelimiterName) {
        'comma'     { ',' }
        'tab'       { "`t" }
        'semicolon' { ';' }
        'pipe'      { '|' }
    }
    $anchorComparer = if ($CaseSensitive) { [System.StringComparer]::Ordinal } else { [System.StringComparer]::OrdinalIgnoreCase }

    # Validate AnchorColumn early
    if ([string]::IsNullOrWhiteSpace($AnchorColumn)) {
        throw "Parameter -AnchorColumn cannot be empty or whitespace."
    }

    # Resolve $OutputFolder once to a full, literal path
    try {
        $OutputFolder = (Resolve-Path -LiteralPath $OutputFolder -ErrorAction Stop).ProviderPath
    } catch {
        throw "Output folder not found: $OutputFolder"
    }

    # Ensure Microsoft.VisualBasic (TextFieldParser) is available
    $tfpTypeName = "Microsoft.VisualBasic.FileIO.TextFieldParser, Microsoft.VisualBasic"
    if (-not [Type]::GetType($tfpTypeName, $false)) {
        try {
            Add-Type -AssemblyName Microsoft.VisualBasic -ErrorAction Stop
        } catch {
            throw "Microsoft.VisualBasic assembly is required for robust header parsing (TextFieldParser). $($_.Exception.Message)"
        }
    }

    # All CSV reading goes through TextFieldParser, for both the header and the rows. It honours
    # quoted newlines, handles CRLF/LF/CR terminators, rejects malformed quoting instead of silently
    # mangling it, and returns a string[] per record whose Length is the real field count - which is
    # what makes the ragged-row check below able to fire at all.
    function New-CsvParser {
        param([string]$FullPath, [string]$Delimiter, [System.Text.Encoding]$DefaultEncoding)
        # detectEncoding:$true so a byte-order mark wins over $DefaultEncoding. That is the behaviour
        # documented for -EncodingName: the BOM decides, and this setting is only the fallback.
        $parser = New-Object Microsoft.VisualBasic.FileIO.TextFieldParser($FullPath, $DefaultEncoding, $true)
        $parser.TextFieldType = [Microsoft.VisualBasic.FileIO.FieldType]::Delimited
        $parser.SetDelimiters(@($Delimiter))
        $parser.HasFieldsEnclosedInQuotes = $true
        $parser.TrimWhiteSpace = $false
        return $parser
    }

    function Resolve-FullPath {
        param([string]$Path)
        try {
            return (Resolve-Path -LiteralPath $Path -ErrorAction Stop).ProviderPath
        } catch {
            throw "File not found: $Path"
        }
    }

    function Get-CsvHeaderFields {
        # $Side is the role the caller passed this file as - 'Previous' or 'Current'. The path alone
        # does not say which of the two arguments was at fault, and a job that generates both paths
        # can produce two that look alike. Naming it here keeps the failure to one sentence instead
        # of one message wrapped inside another. Not Mandatory: a missing value would make PowerShell
        # prompt, which hangs an unattended run.
        param([string]$Path, [string]$Delimiter, [System.Text.Encoding]$DefaultEncoding, [string]$Side)
        $fullPath = Resolve-FullPath -Path $Path
        $parser = $null
        try {
            $parser = New-CsvParser -FullPath $fullPath -Delimiter $Delimiter -DefaultEncoding $DefaultEncoding
            $parser.ReadFields()
        } catch {
            throw "Cannot read $Side CSV header from '$fullPath': $($_.Exception.Message)"
        } finally {
            if ($parser) { $parser.Close() }
        }
    }

    function Read-CsvRows {
        param([string]$Path, [string]$Delimiter, [System.Text.Encoding]$DefaultEncoding)
        $fullPath = Resolve-FullPath -Path $Path
        $rows = [System.Collections.Generic.List[string[]]]::new()
        $parser = $null
        try {
            $parser = New-CsvParser -FullPath $fullPath -Delimiter $Delimiter -DefaultEncoding $DefaultEncoding
            $null = $parser.ReadFields()   # header; validated separately
            while (-not $parser.EndOfData) { $rows.Add($parser.ReadFields()) }
        } catch [Microsoft.VisualBasic.FileIO.MalformedLineException] {
            throw "Malformed CSV in '$fullPath' at line $($parser.ErrorLineNumber). Unbalanced quotes are the usual cause. Line reads: $($parser.ErrorLine)"
        } catch {
            throw "Cannot read CSV rows from '$fullPath': $($_.Exception.Message)"
        } finally {
            if ($parser) { $parser.Close() }
        }
        # Comma prevents PowerShell from unrolling the List on return, which would collapse a
        # single-row file into one string[] and make .Count report the field count.
        return ,$rows
    }

    # Encoding helpers
    function Get-AnsiCodePage {
        $cp = [System.Globalization.CultureInfo]::CurrentCulture.TextInfo.ANSICodePage
        if ($cp -le 0) { $cp = 65001 }
        return $cp
    }
    function Get-OemCodePage {
        $cp = [System.Globalization.CultureInfo]::CurrentCulture.TextInfo.OEMCodePage
        if ($cp -le 0) { $cp = 65001 }
        return $cp
    }

    # Logical encoding name -> an explicit Encoding object, used for BOTH reading and writing. Built
    # from .NET types rather than either PowerShell version's encoding names, so behaviour does not
    # depend on the version. On read it is the default only; a byte-order mark overrides it.
    function Resolve-Encoding {
        param([Parameter(Mandatory)][string]$EncodingName)
        $name = if ($EncodingName -eq 'default') { 'ansi' } else { $EncodingName }
        try {
            switch ($name) {
                'auto'      { New-Object System.Text.UTF8Encoding($true) }
                'ascii'     { New-Object System.Text.ASCIIEncoding }
                'ansi'      { [System.Text.Encoding]::GetEncoding((Get-AnsiCodePage)) }
                'oem'       { [System.Text.Encoding]::GetEncoding((Get-OemCodePage)) }
                'unicode'   { New-Object System.Text.UnicodeEncoding($false, $true) }
                'utf8BOM'   { New-Object System.Text.UTF8Encoding($true) }
                'utf8NoBOM' { New-Object System.Text.UTF8Encoding($false) }
            }
        } catch {
            throw "Cannot resolve encoding '$EncodingName': $($_.Exception.Message)"
        }
    }

    # Format one record as a CSV line. Every field is quoted (matching Windows PowerShell 5.1's
    # Export-Csv), so escaping reduces to doubling embedded quotes. A $null field is written as an
    # empty *unquoted* field, which is how unchanged columns render on Update rows.
    function ConvertTo-CsvLine {
        # Not Mandatory: a missing value would make PowerShell prompt, which hangs an unattended run.
        # [object[]], not [string[]]: a string[] parameter coerces an assigned $null to "".
        param([object[]]$Fields, [string]$Delimiter)
        $out = [string[]]::new($Fields.Length)
        for ($i = 0; $i -lt $Fields.Length; $i++) {
            $f = $Fields[$i]
            if ($null -eq $f) {
                $out[$i] = ''
            } else {
                # Only call Replace when there is actually a quote: it allocates a new string either
                # way, and most fields have none. Measured 1.30x faster on a 20K x 20 export.
                # Do NOT rewrite this loop with a StringBuilder - each Append goes through
                # PowerShell's method dispatch, which measured 4.2x SLOWER than array + -join.
                $s = [string]$f
                if ($s.IndexOf('"') -ge 0) { $s = $s.Replace('"','""') }
                $out[$i] = '"' + $s + '"'
            }
        }
        return ($out -join $Delimiter)
    }

    $fileTime = (Get-Date).ToString("yyyy-MM-dd_HHmmssfff")
    $baseFileName = [System.IO.Path]::GetFileNameWithoutExtension((Resolve-Path -LiteralPath $CurrentCSVFile).ProviderPath)
    $changesCSVFile = [System.IO.Path]::Combine($OutputFolder, ("Changes_{0}_GeneratedOn_{1}.csv" -f $baseFileName, $fileTime))
    # One Encoding object serves both directions: the read default (a BOM overrides it) and the
    # exact bytes written on export.
    $csvEncoding = Resolve-Encoding -EncodingName $EncodingName

    # 1. Ensure headers from both CSV files match
    $previousHeadersRaw = Get-CsvHeaderFields -Path $PreviousCSVFile -Delimiter $Delimiter -DefaultEncoding $csvEncoding -Side 'Previous'
    $currentHeadersRaw  = Get-CsvHeaderFields -Path $CurrentCSVFile  -Delimiter $Delimiter -DefaultEncoding $csvEncoding -Side 'Current'

    # TextFieldParser returns $null for a file with no data at all, and $null.Count is 0, so the
    # empty-header check below passes silently. The failure then surfaces much later as "You cannot
    # call a method on a null-valued expression" from .Trim() on a piped $null - PowerShell runs a
    # ForEach-Object body once for a piped $null. Diagnose it here, where the cause is still visible.
    if ($null -eq $previousHeadersRaw -or $previousHeadersRaw.Count -eq 0) { throw "Previous CSV file is empty; no header line found." }
    if ($null -eq $currentHeadersRaw  -or $currentHeadersRaw.Count  -eq 0) { throw "Current CSV file is empty; no header line found." }

    # Validate for empty/blank header names
    $emptyPrev = for ($i=0; $i -lt $previousHeadersRaw.Count; $i++) {
        $h = $previousHeadersRaw[$i]
        if ([string]::IsNullOrWhiteSpace($h)) { "Column $($i+1)" }
    }
    if ($emptyPrev) { throw "Empty/blank column name(s) in Previous CSV header at: $($emptyPrev -join ', '). Raw headers: $($previousHeadersRaw -join ', ')" }

    $emptyCurr = for ($i=0; $i -lt $currentHeadersRaw.Count; $i++) {
        $h = $currentHeadersRaw[$i]
        if ([string]::IsNullOrWhiteSpace($h)) { "Column $($i+1)" }
    }
    if ($emptyCurr) { throw "Empty/blank column name(s) in Current CSV header at: $($emptyCurr -join ', '). Raw headers: $($currentHeadersRaw -join ', ')" }

    # Detect duplicates after normalization (per file)
    $prevNormAll = $previousHeadersRaw | ForEach-Object { $_.Trim().ToLowerInvariant() }
    $currNormAll = $currentHeadersRaw  | ForEach-Object { $_.Trim().ToLowerInvariant() }

    $dupPrev = $prevNormAll | Group-Object | Where-Object { $_.Count -gt 1 }
    if ($dupPrev) {
        $details = foreach ($g in $dupPrev) {
            $norm = $g.Name
            $raws = $previousHeadersRaw | Where-Object { $_.Trim().ToLowerInvariant() -eq $norm }
            "{0} => [{1}]" -f $norm, ($raws -join ', ')
        }
        throw "Duplicate column names after normalization in Previous CSV: $($details -join '; ')"
    }

    $dupCurr = $currNormAll | Group-Object | Where-Object { $_.Count -gt 1 }
    if ($dupCurr) {
        $details = foreach ($g in $dupCurr) {
            $norm = $g.Name
            $raws = $currentHeadersRaw | Where-Object { $_.Trim().ToLowerInvariant() -eq $norm }
            "{0} => [{1}]" -f $norm, ($raws -join ', ')
        }
        throw "Duplicate column names after normalization in Current CSV: $($details -join '; ')"
    }
    # Resolve anchor column name (normalized) early for validation
    $anchorNorm = $AnchorColumn.Trim().ToLowerInvariant()

    # Normalized header sets (sorted) for cross-file comparison
    $previousHeadersNorm = $prevNormAll | Sort-Object -ErrorAction Stop
    $currentHeadersNorm  = $currNormAll | Sort-Object -ErrorAction Stop

    # The inner parentheses are required. Without them PowerShell parses the condition as
    # (($previousHeadersNorm -join ',') -eq $currentHeadersNorm) -join ',' which evaluates to the
    # string "False", making -not always $false and the throw below unreachable.
    if (-not (($previousHeadersNorm -join ',') -eq ($currentHeadersNorm -join ','))) {
        throw "Column mismatch detected! Previous CSV columns: $($previousHeadersRaw -join ', ')`nCurrent CSV columns: $($currentHeadersRaw -join ', ')"
    }

    # Map normalized header -> ordinal position per file. Rows are string[] in file order, so a
    # column is addressed by index rather than by property name. The two files may order their
    # columns differently, hence one map each.
    $prevHeaderIdx = @{}
    for ($i = 0; $i -lt $previousHeadersRaw.Count; $i++) {
        $prevHeaderIdx[$previousHeadersRaw[$i].Trim().ToLowerInvariant()] = $i
    }
    $currHeaderIdx = @{}
    for ($i = 0; $i -lt $currentHeadersRaw.Count; $i++) {
        $currHeaderIdx[$currentHeadersRaw[$i].Trim().ToLowerInvariant()] = $i
    }
    # ContainsKey, not truthiness: index 0 is a legitimate position but a falsy value.
    if (-not $prevHeaderIdx.ContainsKey($anchorNorm)) { throw "Anchor column '$AnchorColumn' not found in Previous CSV headers: $($previousHeadersRaw -join ', ')" }
    if (-not $currHeaderIdx.ContainsKey($anchorNorm)) { throw "Anchor column '$AnchorColumn' not found in Current CSV headers: $($currentHeadersRaw -join ', ')" }
    $prevAnchorIdx = $prevHeaderIdx[$anchorNorm]
    $currAnchorIdx = $currHeaderIdx[$anchorNorm]
    Write-Host "Note: Output columns use trimmed and lowercase-normalized header names for consistency."

    # 2. Import Records
    $Previous = Read-CsvRows -Path $PreviousCSVFile -Delimiter $Delimiter -DefaultEncoding $csvEncoding
    $Current  = Read-CsvRows -Path $CurrentCSVFile  -Delimiter $Delimiter -DefaultEncoding $csvEncoding

    # 3. Count Check. Checked here, before the per-row validation below, so an empty file fails
    # with this message instead of just producing an empty lookup silently.
    #"Previous count: $($Previous.Count)" | Write-Verbose
    if ($Previous.Count -eq 0) { throw "No records found in Previous CSV file." }
    #"Current count: $($Current.Count)" | Write-Verbose
    if ($Current.Count -eq 0)  { throw "No records found in Current CSV file." }
    #"Column count: $($previousHeadersRaw.Count)" | Write-Verbose

    # anchor -> row number of its first occurrence. Both tables use $anchorComparer, so the script
    # has a single notion of anchor identity - a default @{} is case-insensitive and would disagree
    # with an Ordinal comparer under -CaseSensitive. Duplicate detection and lookup population are
    # merged into one pass per file, rather than a scan followed by a separate rebuild, since both
    # need to look at the same row.
    $firstRowPrev = [System.Collections.Generic.Dictionary[string,int]]::new($anchorComparer)
    # Only anchors that actually repeat get an entry, holding the row numbers being ignored.
    $duplicateAnchorsPrev = [System.Collections.Generic.Dictionary[string,System.Collections.Generic.List[int]]]::new($anchorComparer)
    $htPrevious = [System.Collections.Generic.Dictionary[string,string[]]]::new([int]$Previous.Count, $anchorComparer)
    $rowNum = 0
    foreach ($row in $Previous) {
        $rowNum++
        # Field count first - a ragged row cannot be safely indexed for its anchor.
        # TextFieldParser reports the fields actually present, which is what lets this check fire.
        # A reader that rebuilds each row from the header erases that evidence.
        if ($row.Length -ne $previousHeadersRaw.Count) {
            throw "Row $rowNum in Previous file has $($row.Length) field(s), expected $($previousHeadersRaw.Count). Fields: $($row -join ' | ')"
        }
        $anchor = $row[$prevAnchorIdx]
        # Anchor Value Validation
        if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in Previous record at row $($rowNum): $($row -join ', ')." }

        # Duplicate Anchor Value Check
        if ($firstRowPrev.ContainsKey($anchor)) {
            if ($RejectDuplicateAnchors) {
                throw "Duplicate anchor '$anchor' in Previous file at row $rowNum (first seen at row $($firstRowPrev[$anchor])). Rejected because -RejectDuplicateAnchors was specified."
            }
            if (-not $duplicateAnchorsPrev.ContainsKey($anchor)) {
                $duplicateAnchorsPrev[$anchor] = [System.Collections.Generic.List[int]]::new()
            }
            $duplicateAnchorsPrev[$anchor].Add($rowNum)   # List.Add is O(1); array += reallocated each time
        } else {
            $firstRowPrev[$anchor] = $rowNum
            $htPrevious.Add($anchor, $row)
        }

        # No blank-row check here by design: the anchor validation above subsumes it, and the
        # parser does not emit records for genuinely empty lines.
    }
    # The anchor gets its own report column already (added first, below) and never needs an old/new
    # pair of its own: two rows only pair up when their anchors already compared equal under the
    # same rule this loop uses, so that comparison can never show a difference. Dropped here, after
    # the column-set and per-file anchor-presence checks above, so neither of those changes.
    $previousHeadersNorm = @($previousHeadersNorm | Where-Object { $_ -ne $anchorNorm })
    $currentHeadersNorm  = @($currentHeadersNorm  | Where-Object { $_ -ne $anchorNorm })

    # Rows are fixed-width object[] in report-column order rather than PSCustomObject. Column names
    # live once in $reportColumns (written as the header line) instead of on every row.
    # object[] rather than string[] so an assigned $null stays $null (a string[] slot coerces it to "").
    $changes = [System.Collections.Generic.List[object[]]]::new()
    # Report columns: AnchorColumn, ChangeType, then old/new pairs for each other column
    $reportColumns = [System.Collections.Generic.List[string]]::new(2 + (2 * $previousHeadersNorm.Count))
    $reportColumns.Add($AnchorColumn)
    $reportColumns.Add("ChangeType")
    # $colIndex maps a column to the offset of its pair: old = i, new = i+1
    $colIndex = @{}
    foreach ($prop in $previousHeadersNorm) {
        $colIndex[$prop] = $reportColumns.Count
        $reportColumns.Add("old $($prop)")
        $reportColumns.Add("new $($prop)")
    }
    $rowWidth = $reportColumns.Count

    # Summary counters
    $adds = 0; $updates = 0; $deletes = 0; $nones = 0

    # anchor -> row number of its first occurrence, for Current (see the Previous loop above for
    # the rationale - same reasoning, same comparer).
    $firstRowCurr = [System.Collections.Generic.Dictionary[string,int]]::new($anchorComparer)
    $duplicateAnchorsCurr = [System.Collections.Generic.Dictionary[string,System.Collections.Generic.List[int]]]::new($anchorComparer)

    # Progress
    $progressId = 1
    $totalCurr = $Current.Count
    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Matching Current against Previous..." -PercentComplete 0

    try {
        # Single pass over $Current, in its real row order, matched against the $htPrevious lookup.
        # A match is compared and removed from $htPrevious as it's consumed, so whatever remains in
        # $htPrevious once this loop ends is exactly the delete set - handled in the pass below.
        $rowNum = 0
        foreach ($currRow in $Current)
        {
            $rowNum++
            # Field count first (see the Previous loop for why).
            if ($currRow.Length -ne $currentHeadersRaw.Count) {
                throw "Row $rowNum in Current file has $($currRow.Length) field(s), expected $($currentHeadersRaw.Count). Fields: $($currRow -join ' | ')"
            }
            $anchor = $currRow[$currAnchorIdx]
            # Anchor Value Validation
            if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in Current record at row $($rowNum): $($currRow -join ', ')." }

            # Duplicate Anchor Value Check. A repeat is recorded and skipped, so only the first
            # occurrence produces a change row - matching the old htCurrent-based behaviour, where a
            # later duplicate was simply never stored.
            if ($firstRowCurr.ContainsKey($anchor)) {
                if ($RejectDuplicateAnchors) {
                    throw "Duplicate anchor '$anchor' in Current file at row $rowNum (first seen at row $($firstRowCurr[$anchor])). Rejected because -RejectDuplicateAnchors was specified."
                }
                if (-not $duplicateAnchorsCurr.ContainsKey($anchor)) {
                    $duplicateAnchorsCurr[$anchor] = [System.Collections.Generic.List[int]]::new()
                }
                $duplicateAnchorsCurr[$anchor].Add($rowNum)
                if (($rowNum % 1000) -eq 0 -or $rowNum -eq $totalCurr) {
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Matching Current against Previous... ($rowNum of $totalCurr)" -PercentComplete ([int](($rowNum/$totalCurr)*100))
                }
                continue
            }
            $firstRowCurr[$anchor] = $rowNum

            $row = [object[]]::new($rowWidth)
            $row[0] = $anchor
            if ($htPrevious.ContainsKey($anchor))
            {
                $prevRow = $htPrevious[$anchor]
                $isUpdate = $false
                #"User exists in both files. $($anchor)" | Write-Verbose
                foreach ($n in $previousHeadersNorm)
                {
                    #"Comparing column: $($n)" | Write-Verbose
                    $prevIdx = $prevHeaderIdx[$n]
                    $currIdx = $currHeaderIdx[$n]

                    $prevValue = $prevRow[$prevIdx]
                    $currValue = $currRow[$currIdx]

                    $valuesDiffer = if ($CaseSensitive) { $prevValue -cne $currValue } else { $prevValue -ine $currValue }

                    if ($valuesDiffer)
                    {
                        #"Values do not match. Column: $($n)   Previous: $prevValue   Current: $currValue" | Write-Verbose
                        # Only changed columns are populated; the rest stay $null and render as empty
                        # unquoted fields, keeping a mostly-unchanged report small.
                        $idx = $colIndex[$n]
                        $row[$idx]     = $prevValue
                        $row[$idx + 1] = $currValue
                        $isUpdate = $true
                    }
                }
                if ($isUpdate)
                {
                    $row[1] = "Update"
                    $updates++
                }
                else
                {
                    $row[1] = "None"
                    $nones++
                }
                # Matched: drop it from $htPrevious so what's left after this loop is the delete set.
                [void]$htPrevious.Remove($anchor)
            }
            else
            {
                #"User add to Current file. $($anchor)" | Write-Verbose
                $row[1] = "Add"
                $adds++
                foreach ($n in $previousHeadersNorm)
                {
                    $currIdx = $currHeaderIdx[$n]
                    $idx = $colIndex[$n]
                    $row[$idx]     = ""
                    $row[$idx + 1] = $currRow[$currIdx]
                }
            }
            $changes.Add($row)
            if (($rowNum % 1000) -eq 0 -or $rowNum -eq $totalCurr) {
                Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Matching Current against Previous... ($rowNum of $totalCurr)" -PercentComplete ([int](($rowNum/$totalCurr)*100))
            }
        }

        # Warn about duplicates but continue processing. Every entry is a duplicate by construction,
        # so there is no count filter; keys enumerate in first-detection order, i.e. by row. Delayed
        # to here (rather than right after each import loop) because Current's duplicate set isn't
        # complete until the loop above finishes.
        foreach ($anchor in $duplicateAnchorsPrev.Keys) {
            $rows = $duplicateAnchorsPrev[$anchor] -join ', '
            Write-Host "WARNING: Duplicate anchor '$anchor' in Previous file. Using row $($firstRowPrev[$anchor]); ignoring row(s): $rows. Pass -RejectDuplicateAnchors to fail the run on a duplicate instead." -ForegroundColor Yellow
        }
        foreach ($anchor in $duplicateAnchorsCurr.Keys) {
            $rows = $duplicateAnchorsCurr[$anchor] -join ', '
            Write-Host "WARNING: Duplicate anchor '$anchor' in Current file. Using row $($firstRowCurr[$anchor]); ignoring row(s): $rows. Pass -RejectDuplicateAnchors to fail the run on a duplicate instead." -ForegroundColor Yellow
        }

        # 4. Whatever is left in $htPrevious was never matched against Current, so it was deleted.
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Finalizing deletions..." -PercentComplete 0
        $toDeleteTotal = $htPrevious.Count
        $iDel = 0
        foreach ($anchor in $htPrevious.Keys)
        {
            $prevRow = $htPrevious[$anchor]
            $row = [object[]]::new($rowWidth)
            $row[0] = $anchor
            $row[1] = "Delete"
            $deletes++
            foreach ($n in $previousHeadersNorm) {
                $prevIdx = $prevHeaderIdx[$n]
                $idx = $colIndex[$n]
                $row[$idx]     = $prevRow[$prevIdx]
                $row[$idx + 1] = ""
            }
            $changes.Add($row)
            $iDel++
            if (($iDel % 1000) -eq 0 -or $iDel -eq $toDeleteTotal) {
                $pct = if ($toDeleteTotal -gt 0) { [int](($iDel/$toDeleteTotal)*100) } else { 100 }
                Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Finalizing deletions... ($iDel of $toDeleteTotal)" -PercentComplete $pct
            }
        }
        if (($adds + $updates + $deletes) -gt 0)
        {
            # StreamWriter rather than Export-Csv: takes an explicit Encoding object for exact BOM
            # control, and writes row-by-row instead of materializing the file.
            $writer = $null
            try {
                $writer = New-Object System.IO.StreamWriter($changesCSVFile, $false, $csvEncoding)
                $writer.NewLine = "`r`n"
                $writer.WriteLine((ConvertTo-CsvLine -Fields $reportColumns -Delimiter $Delimiter))
                foreach ($row in $changes) {
                    $writer.WriteLine((ConvertTo-CsvLine -Fields $row -Delimiter $Delimiter))
                }
                # Explicit Flush() before Dispose() - not because Dispose() skips it, but because a
                # failure here throws normally from this try block. The same failure inside Dispose(),
                # called from finally below, could mask whatever exception the try block was already
                # unwinding from.
                $writer.Flush()
            } catch {
                throw "Cannot write changes CSV to '$changesCSVFile': $($_.Exception.Message)"
            } finally {
                if ($writer) { $writer.Dispose() }
            }
            Write-Host "Changes CSV written to: $changesCSVFile"
        }
        else
        {
            Write-Host "No changes detected; no CSV written"
        }
    }
    finally {
        # Always clear progress
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Completed
    }
    # Summary output ($nones is counted in the compare loop, not recounted here)
    Write-Host ("Summary: Adds={0}, Updates={1}, Deletes={2}, Unchanged={3}" -f $adds, $updates, $deletes, $nones)

    $elapsed = (Get-Date) - $scriptStartTime
    # Floor, not [int]: [int] rounds to nearest, so a 47.7s run reported "1m 47s"
    $elapsedStr = "{0}m {1}s" -f [int][Math]::Floor($elapsed.TotalMinutes), $elapsed.Seconds
    Write-Host "Elapsed: $elapsedStr"
}
catch {
    Write-Error $_
    exit 1
}