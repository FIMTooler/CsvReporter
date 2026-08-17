<#
.SYNOPSIS
Compares two CSV files using a genuine external sort-merge, for inputs too large to hold in memory.

.DESCRIPTION
External sort-merge on disk, for inputs too large to hold in memory:
- Robust header parsing (quoted headers, embedded delimiters).
- Strict anchor presence; fails if the anchor column is missing.
- Each source file is read in chunks of -BatchSize rows; each chunk is sorted in memory and spilled
  to a run file, and the run files are then merged into one sorted file per source. Only one chunk
  is resident at a time.
- The two sorted files are then streamed side by side and compared, and change rows are written as
  they are produced. Neither input file is ever held in memory.
- Detects and warns about duplicate anchor values (uses first occurrence only).
- Case-sensitive or insensitive comparisons per -CaseSensitive.
- Outputs a CSV with ChangeType and old/new values for changed columns.
  - On Update rows only the columns that actually changed are populated. Unchanged columns are
    left as empty unquoted fields, which keeps a mostly-unchanged report small and makes the
    changed cells easy to spot. An explicitly empty value is written as "" instead.
- Output sorted by anchor column, using the same ordinal comparer as key matching and honouring
  -CaseSensitive. Ordinal is not alphabetical: by default 'A-1' sorts before 'a_1' and '_z' after
  both. -CaseSensitive changes the order again, because uppercase precedes lowercase ordinally.
- Prints a one-line summary with counts and elapsed time.

This script trades speed for bounded memory: every row is written to a run file, read back, merged,
and read again to compare. Use it when the input does not fit in memory.

.PARAMETER PreviousCSVFile
Path to the "Previous" CSV file.

.PARAMETER CurrentCSVFile
Path to the "Current" CSV file.

.PARAMETER AnchorColumn
Header name of the key/anchor column used to join rows.

.PARAMETER OutputFolder
Folder where the changes CSV will be written. Run files and the in-progress output file are also
created here and removed when the script finishes, so this folder needs free space for roughly the
combined size of both input files while the script is running. They are kept here rather than in
the system temp folder so they can be found if a run fails.

.PARAMETER BatchSize
Rows per sort chunk, and the flush interval for the output file. At most -BatchSize rows are held
in memory at once, which is what keeps memory flat as the input grows.

Lowering it writes more run files and takes longer. Raising it uses more memory and is faster, up
to the point where a chunk no longer fits comfortably in memory. Lower it if the script runs out of
memory on a very wide or very large file; otherwise the default is a reasonable starting point and
there is little to gain from tuning it. Default 25000.

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

Internal run files are always UTF-8 regardless of this setting, so a lossy value such as 'ascii'
cannot affect the comparison itself - only the encoding of the report that is written out.

.PARAMETER CaseSensitive
Use case-sensitive comparisons when set.

.PARAMETER RejectDuplicateAnchors
Fail the run instead of warning when a duplicate anchor value is found. Default behaviour (this
switch absent) is to warn and continue, using the first occurrence and ignoring the rest. With this
switch, the run throws on the first duplicate it finds and writes no report - useful when the anchor
is meant to be unique, since a duplicate is then a data-quality problem (often a wrong
-AnchorColumn) rather than something to quietly work around. Because this script discovers
duplicates while merging the two sorted files rather than before comparing, which one is "first" can
differ from the other three scripts on the same input - it is whichever side's duplicate the merge
reaches first, not necessarily the one that appears first in the source file.

.INPUTS
None. You cannot pipe objects to this script.

.OUTPUTS
None. Writes a changes CSV to -OutputFolder and summary messages to the console.

.EXAMPLE
.\CompareCSVs_large.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -DelimiterName comma -EncodingName utf8BOM -CaseSensitive

.NOTES
Requires Microsoft.VisualBasic for TextFieldParser header parsing.
Run files and the in-progress output file are removed in a finally block even if errors occur.
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
    [ValidateRange(1, 1000000)]
    [int]$BatchSize = 25000,
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

    # Largest number of run files merged in one pass. Selecting the next record is a linear scan of
    # the open heads, so an unbounded fan-in would make the merge O(rows x runs); above this many
    # runs the merge is done in several passes instead. Only reached when -BatchSize is very small
    # relative to the input.
    $maxFanIn = 32

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
    # One Encoding object serves both directions for the SOURCE and OUTPUT files: the read default
    # (a BOM overrides it) and the exact bytes written on export.
    $csvEncoding = Resolve-Encoding -EncodingName $EncodingName
    $exportEncoding = $csvEncoding
    # Run files are internal and always UTF-8, whatever -EncodingName says. Round-tripping the data
    # through a lossy encoding such as 'ascii' would silently alter the values being compared.
    $runEncoding = New-Object System.Text.UTF8Encoding($true)

    # Every temp file created, so the finally block can remove them all whatever went wrong.
    $tempFiles = [System.Collections.Generic.List[string]]::new()
    $tempSeq = 0
    function New-TempPath {
        param([string]$Tag)
        $script:tempSeq++
        $p = [System.IO.Path]::Combine($OutputFolder, ("{0}.{1}.{2}{3:d4}.tmp" -f $baseFileName, $fileTime, $Tag, $script:tempSeq))
        $script:tempFiles.Add($p)
        return $p
    }

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

    # A run-file record is the source row with its original row number prepended, so every source
    # column sits one position further right. The row number is what makes first-occurrence-wins
    # well defined: it breaks ties between duplicate anchors in both the chunk sort and the merge,
    # and it is the row number the duplicate warning has to report.
    $prevRunAnchorIdx = 1 + $prevAnchorIdx
    $currRunAnchorIdx = 1 + $currAnchorIdx
    $prevRunIdx = @{}
    $currRunIdx = @{}
    foreach ($n in $previousHeadersNorm) {
        $prevRunIdx[$n] = 1 + $prevHeaderIdx[$n]
        $currRunIdx[$n] = 1 + $currHeaderIdx[$n]
    }

    # The anchor gets its own report column already (added first, below) and never needs an old/new
    # pair of its own: two rows only pair up when their anchors already compared equal under the
    # same rule the merge-join uses, so that comparison can never show a difference. Dropped here,
    # after the column-set and per-file anchor-presence checks above, so neither of those changes.
    # $prevRunIdx/$currRunIdx above keep an unused entry for the anchor - harmless, since the compare
    # loop below only ever looks a name up in them by iterating $previousHeadersNorm, which no longer
    # offers the anchor's name to iterate.
    $previousHeadersNorm = @($previousHeadersNorm | Where-Object { $_ -ne $anchorNorm })
    $currentHeadersNorm  = @($currentHeadersNorm  | Where-Object { $_ -ne $anchorNorm })

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

    # Sort one chunk in memory and spill it to a run file. This is the only place rows accumulate,
    # and it holds at most -BatchSize of them.
    function Write-SortedRun {
        param(
            [System.Collections.Generic.List[object[]]]$Chunk,
            [string]$Path,
            [string]$Delimiter,
            [System.Text.Encoding]$Encoding
        )
        # Entry layout is @(anchor, rowNumber, sourceFields). Anchor first so the comparison delegate
        # is the same shape as the one the sibling scripts use.
        $Chunk.Sort([System.Comparison[object[]]]{
            param($a,$b)
            $c = $anchorComparer.Compare([string]$a[0], [string]$b[0])
            if ($c -ne 0) { return $c }
            return ([int]$a[1]).CompareTo([int]$b[1])
        })
        $writer = $null
        try {
            $writer = New-Object System.IO.StreamWriter($Path, $false, $Encoding)
            $writer.NewLine = "`r`n"
            foreach ($e in $Chunk) {
                $src = $e[2]
                $fields = [object[]]::new(1 + $src.Length)
                $fields[0] = [string]$e[1]
                [Array]::Copy($src, 0, $fields, 1, $src.Length)
                $writer.WriteLine((ConvertTo-CsvLine -Fields $fields -Delimiter $Delimiter))
            }
        } catch {
            throw "Cannot write run file '$Path': $($_.Exception.Message)"
        } finally {
            if ($writer) { $writer.Dispose() }
        }
    }

    # Read a source file in -BatchSize chunks, sorting and spilling each one. Validation happens here,
    # on the single pass over the source, so a bad row is rejected before any work is done on it.
    # Returns a hashtable rather than a bare collection: a PowerShell function unrolls a returned
    # list, which would make a one-run result indistinguishable from a bare string.
    function Invoke-SpillSort {
        param(
            [string]$SourcePath,
            [string]$Label,
            [int]$AnchorIdx,
            [int]$ExpectedFields,
            [string]$RunTag,
            [string]$Delimiter,
            [System.Text.Encoding]$SourceEncoding,
            [System.Text.Encoding]$RunEncoding,
            [int]$ChunkSize
        )
        $runs = [System.Collections.Generic.List[string]]::new()
        $chunk = [System.Collections.Generic.List[object[]]]::new($ChunkSize)
        $rowNum = 0
        $parser = $null
        try {
            $parser = New-CsvParser -FullPath $SourcePath -Delimiter $Delimiter -DefaultEncoding $SourceEncoding
            $null = $parser.ReadFields()   # header; validated separately
            while (-not $parser.EndOfData) {
                $row = $parser.ReadFields()
                $rowNum++
                # Field count first - a ragged row cannot be safely indexed for its anchor.
                # TextFieldParser reports the fields actually present, which is what lets this check fire.
                # A reader that rebuilds each row from the header erases that evidence.
                if ($row.Length -ne $ExpectedFields) {
                    throw "Row $rowNum in $Label file has $($row.Length) field(s), expected $ExpectedFields. Fields: $($row -join ' | ')"
                }
                $anchor = $row[$AnchorIdx]
                # Anchor Value Validation
                if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in $Label record at row $($rowNum): $($row -join ', ')." }

                # Built element by element: @(...) would flatten $row into the entry.
                $entry = [object[]]::new(3)
                $entry[0] = $anchor
                $entry[1] = $rowNum
                $entry[2] = $row
                $chunk.Add($entry)

                if ($chunk.Count -ge $ChunkSize) {
                    $path = New-TempPath -Tag $RunTag
                    $runs.Add($path)
                    Write-SortedRun -Chunk $chunk -Path $path -Delimiter $Delimiter -Encoding $RunEncoding
                    $chunk.Clear()
                }
                if (($rowNum % 1000) -eq 0) {
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Sorting $Label... ($rowNum rows, $($runs.Count) run files)"
                }
            }
        } catch [Microsoft.VisualBasic.FileIO.MalformedLineException] {
            throw "Malformed CSV in '$SourcePath' at line $($parser.ErrorLineNumber). Unbalanced quotes are the usual cause. Line reads: $($parser.ErrorLine)"
        } finally {
            if ($parser) { $parser.Close() }
        }
        if ($chunk.Count -gt 0) {
            $path = New-TempPath -Tag $RunTag
            $runs.Add($path)
            Write-SortedRun -Chunk $chunk -Path $path -Delimiter $Delimiter -Encoding $RunEncoding
            $chunk.Clear()
        }
        return @{ Runs = $runs; RowCount = $rowNum }
    }

    # Merge a set of sorted run files into one. Selecting the next record is a linear scan of the
    # open heads; the caller keeps the group small enough for that to be cheap.
    function Merge-RunGroup {
        param(
            [string[]]$InputPaths,
            [string]$OutputPath,
            [int]$AnchorFieldIdx,
            [string]$Delimiter,
            [System.Text.Encoding]$Encoding
        )
        $parsers = [System.Collections.Generic.List[object]]::new()
        $heads = [System.Collections.Generic.List[object]]::new()
        $writer = $null
        try {
            foreach ($p in $InputPaths) {
                $parser = New-CsvParser -FullPath $p -Delimiter $Delimiter -DefaultEncoding $Encoding
                $parsers.Add($parser)
                if ($parser.EndOfData) { $heads.Add($null) } else { $heads.Add($parser.ReadFields()) }
            }
            $writer = New-Object System.IO.StreamWriter($OutputPath, $false, $Encoding)
            $writer.NewLine = "`r`n"
            $n = $parsers.Count
            while ($true) {
                $minI = -1
                for ($i = 0; $i -lt $n; $i++) {
                    $h = $heads[$i]
                    if ($null -eq $h) { continue }
                    if ($minI -lt 0) { $minI = $i; continue }
                    $m = $heads[$minI]
                    $c = $anchorComparer.Compare([string]$h[$AnchorFieldIdx], [string]$m[$AnchorFieldIdx])
                    # Ties broken by original row number, so the first occurrence of a duplicate
                    # anchor always emerges first however the chunks were split.
                    if ($c -lt 0 -or ($c -eq 0 -and [int]$h[0] -lt [int]$m[0])) { $minI = $i }
                }
                if ($minI -lt 0) { break }
                $writer.WriteLine((ConvertTo-CsvLine -Fields $heads[$minI] -Delimiter $Delimiter))
                $parser = $parsers[$minI]
                if ($parser.EndOfData) { $heads[$minI] = $null } else { $heads[$minI] = $parser.ReadFields() }
            }
        } catch [Microsoft.VisualBasic.FileIO.MalformedLineException] {
            throw "Malformed run file during merge: $($_.Exception.Message)"
        } catch {
            throw "Cannot merge run files into '$OutputPath': $($_.Exception.Message)"
        } finally {
            foreach ($parser in $parsers) { if ($parser) { $parser.Close() } }
            if ($writer) { $writer.Dispose() }
        }
    }

    # Reduce a set of run files to a single sorted file, in as many passes as the fan-in limit needs.
    # One run file needs no merge at all, which is the common case for inputs below -BatchSize.
    function Merge-Runs {
        param(
            [System.Collections.Generic.List[string]]$Runs,
            [string]$Label,
            [string]$MergeTag,
            [int]$AnchorFieldIdx,
            [int]$MaxFanIn,
            [string]$Delimiter,
            [System.Text.Encoding]$Encoding
        )
        $current = $Runs
        $pass = 0
        while ($current.Count -gt $MaxFanIn) {
            $pass++
            $next = [System.Collections.Generic.List[string]]::new()
            for ($i = 0; $i -lt $current.Count; $i += $MaxFanIn) {
                $last = [Math]::Min($i + $MaxFanIn, $current.Count) - 1
                $group = [string[]]::new($last - $i + 1)
                for ($j = $i; $j -le $last; $j++) { $group[$j - $i] = $current[$j] }
                $outPath = New-TempPath -Tag $MergeTag
                Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Merging $Label runs (pass $pass, $($current.Count) files remaining)..."
                Merge-RunGroup -InputPaths $group -OutputPath $outPath -AnchorFieldIdx $AnchorFieldIdx -Delimiter $Delimiter -Encoding $Encoding
                $next.Add($outPath)
            }
            $current = $next
        }
        if ($current.Count -eq 1) { return $current[0] }
        $final = New-TempPath -Tag $MergeTag
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Merging $Label runs ($($current.Count) files)..."
        Merge-RunGroup -InputPaths ([string[]]$current) -OutputPath $final -AnchorFieldIdx $AnchorFieldIdx -Delimiter $Delimiter -Encoding $Encoding
        return $final
    }

    # Next record from a sorted run, skipping duplicates of the one just returned. Because the file
    # is sorted, duplicates are adjacent - so this replaces the O(rows) "seen" set the previous
    # version carried, and only anchors that actually repeat are ever stored.
    function Get-NextDistinct {
        param(
            $Parser,
            [ref]$LastAnchor,
            [ref]$LastRowNum,
            $DuplicateTable,
            $FirstRowTable,
            [int]$AnchorFieldIdx,
            [string]$Label
        )
        while (-not $Parser.EndOfData) {
            $rec = $Parser.ReadFields()
            $anchor = $rec[$AnchorFieldIdx]
            if ($null -ne $LastAnchor.Value -and $anchorComparer.Compare([string]$anchor, [string]$LastAnchor.Value) -eq 0) {
                if ($RejectDuplicateAnchors) {
                    throw "Duplicate anchor '$anchor' in $Label file at row $([int]$rec[0]) (first seen at row $($LastRowNum.Value)). Rejected because -RejectDuplicateAnchors was specified."
                }
                if (-not $DuplicateTable.ContainsKey($anchor)) {
                    $DuplicateTable[$anchor] = [System.Collections.Generic.List[int]]::new()
                    $FirstRowTable[$anchor] = $LastRowNum.Value
                }
                $DuplicateTable[$anchor].Add([int]$rec[0])   # List.Add is O(1); array += reallocated each time
                continue
            }
            $LastAnchor.Value = $anchor
            $LastRowNum.Value = [int]$rec[0]
            # Comma operator: returning a bare array would unroll it into the pipeline.
            return ,$rec
        }
        return $null
    }

    # Summary counters
    $adds = 0; $updates = 0; $deletes = 0; $nones = 0

    # Only anchors that actually repeat get an entry, holding the row numbers being ignored.
    $duplicateAnchorsPrev = [System.Collections.Generic.Dictionary[string,System.Collections.Generic.List[int]]]::new($anchorComparer)
    $duplicateAnchorsCurr = [System.Collections.Generic.Dictionary[string,System.Collections.Generic.List[int]]]::new($anchorComparer)
    $firstRowPrev = [System.Collections.Generic.Dictionary[string,int]]::new($anchorComparer)
    $firstRowCurr = [System.Collections.Generic.Dictionary[string,int]]::new($anchorComparer)

    # The output is written as the merge produces it, but the report is only kept if there is at
    # least one real change. Writing to a temp name and renaming at the end keeps both: no
    # accumulation in memory, and no half-written file left behind on a None-only run.
    $pendingOutput = New-TempPath -Tag 'out'

    $progressId = 1
    try {
        # 2. External sort of each source file: chunk, sort, spill, then merge the runs.
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Sorting Previous..."
        $prevPath = Resolve-FullPath -Path $PreviousCSVFile
        $prevInfo = Invoke-SpillSort -SourcePath $prevPath -Label 'Previous' -AnchorIdx $prevAnchorIdx `
            -ExpectedFields $previousHeadersRaw.Count -RunTag 'prevrun' -Delimiter $Delimiter `
            -SourceEncoding $csvEncoding -RunEncoding $runEncoding -ChunkSize $BatchSize
        $prevTotal = $prevInfo.RowCount
        if ($prevTotal -eq 0) { throw "No records found in Previous CSV file." }

        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Sorting Current..."
        $currPath = Resolve-FullPath -Path $CurrentCSVFile
        $currInfo = Invoke-SpillSort -SourcePath $currPath -Label 'Current' -AnchorIdx $currAnchorIdx `
            -ExpectedFields $currentHeadersRaw.Count -RunTag 'currrun' -Delimiter $Delimiter `
            -SourceEncoding $csvEncoding -RunEncoding $runEncoding -ChunkSize $BatchSize
        $currTotal = $currInfo.RowCount
        if ($currTotal -eq 0) { throw "No records found in Current CSV file." }

        $prevSorted = Merge-Runs -Runs $prevInfo.Runs -Label 'Previous' -MergeTag 'prevmerge' `
            -AnchorFieldIdx $prevRunAnchorIdx -MaxFanIn $maxFanIn -Delimiter $Delimiter -Encoding $runEncoding
        $currSorted = Merge-Runs -Runs $currInfo.Runs -Label 'Current' -MergeTag 'currmerge' `
            -AnchorFieldIdx $currRunAnchorIdx -MaxFanIn $maxFanIn -Delimiter $Delimiter -Encoding $runEncoding

        # 3. Merge-join the two sorted files. Both are read one record at a time and change rows are
        # written as they are produced, so neither file is ever resident.
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Comparing..." -PercentComplete 0
        $prevParser = $null; $currParser = $null; $writer = $null
        $prevLastAnchor = $null; $prevLastRowNum = 0
        $currLastAnchor = $null; $currLastRowNum = 0
        $emitted = 0
        $consumed = 0
        $totalRows = [Math]::Max(1, $prevTotal + $currTotal)
        try {
            $prevParser = New-CsvParser -FullPath $prevSorted -Delimiter $Delimiter -DefaultEncoding $runEncoding
            $currParser = New-CsvParser -FullPath $currSorted -Delimiter $Delimiter -DefaultEncoding $runEncoding
            $writer = New-Object System.IO.StreamWriter($pendingOutput, $false, $exportEncoding)
            $writer.NewLine = "`r`n"
            $writer.WriteLine((ConvertTo-CsvLine -Fields $reportColumns -Delimiter $Delimiter))

            $p = Get-NextDistinct -Parser $prevParser -LastAnchor ([ref]$prevLastAnchor) -LastRowNum ([ref]$prevLastRowNum) `
                -DuplicateTable $duplicateAnchorsPrev -FirstRowTable $firstRowPrev -AnchorFieldIdx $prevRunAnchorIdx -Label 'Previous'
            $c = Get-NextDistinct -Parser $currParser -LastAnchor ([ref]$currLastAnchor) -LastRowNum ([ref]$currLastRowNum) `
                -DuplicateTable $duplicateAnchorsCurr -FirstRowTable $firstRowCurr -AnchorFieldIdx $currRunAnchorIdx -Label 'Current'

            while ($null -ne $p -or $null -ne $c)
            {
                # -1 take Previous (a delete), 0 take both (update or none), 1 take Current (an add)
                $side = if ($null -eq $c) { -1 }
                        elseif ($null -eq $p) { 1 }
                        else { $anchorComparer.Compare([string]$p[$prevRunAnchorIdx], [string]$c[$currRunAnchorIdx]) }
                if ($side -lt 0) { $side = -1 } elseif ($side -gt 0) { $side = 1 }

                $row = [object[]]::new($rowWidth)
                if ($side -eq 0)
                {
                    $row[0] = $p[$prevRunAnchorIdx]
                    $isUpdate = $false
                    foreach ($n in $previousHeadersNorm)
                    {
                        $prevValue = $p[$prevRunIdx[$n]]
                        $currValue = $c[$currRunIdx[$n]]
                        $valuesDiffer = if ($CaseSensitive) { $prevValue -cne $currValue } else { $prevValue -ine $currValue }
                        if ($valuesDiffer)
                        {
                            # Only changed columns are populated; the rest stay $null and render as
                            # empty unquoted fields, keeping a mostly-unchanged report small.
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
                }
                elseif ($side -lt 0)
                {
                    $row[0] = $p[$prevRunAnchorIdx]
                    $row[1] = "Delete"
                    $deletes++
                    foreach ($n in $previousHeadersNorm)
                    {
                        $idx = $colIndex[$n]
                        $row[$idx]     = $p[$prevRunIdx[$n]]
                        $row[$idx + 1] = ""
                    }
                }
                else
                {
                    $row[0] = $c[$currRunAnchorIdx]
                    $row[1] = "Add"
                    $adds++
                    foreach ($n in $previousHeadersNorm)
                    {
                        $idx = $colIndex[$n]
                        $row[$idx]     = ""
                        $row[$idx + 1] = $c[$currRunIdx[$n]]
                    }
                }
                $writer.WriteLine((ConvertTo-CsvLine -Fields $row -Delimiter $Delimiter))
                $emitted++

                if ($side -le 0)
                {
                    $consumed++
                    $p = Get-NextDistinct -Parser $prevParser -LastAnchor ([ref]$prevLastAnchor) -LastRowNum ([ref]$prevLastRowNum) `
                        -DuplicateTable $duplicateAnchorsPrev -FirstRowTable $firstRowPrev -AnchorFieldIdx $prevRunAnchorIdx -Label 'Previous'
                }
                if ($side -ge 0)
                {
                    $consumed++
                    $c = Get-NextDistinct -Parser $currParser -LastAnchor ([ref]$currLastAnchor) -LastRowNum ([ref]$currLastRowNum) `
                        -DuplicateTable $duplicateAnchorsCurr -FirstRowTable $firstRowCurr -AnchorFieldIdx $currRunAnchorIdx -Label 'Current'
                }

                # -BatchSize doubles as the output flush interval.
                if (($emitted % $BatchSize) -eq 0) { $writer.Flush() }
                if (($emitted % 1000) -eq 0)
                {
                    $pct = [int](($consumed / $totalRows) * 100)
                    if ($pct -gt 100) { $pct = 100 }
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Comparing... ($emitted change rows)" -PercentComplete $pct
                }
            }
        } catch [Microsoft.VisualBasic.FileIO.MalformedLineException] {
            throw "Malformed sorted run file during compare: $($_.Exception.Message)"
        } finally {
            if ($prevParser) { $prevParser.Close() }
            if ($currParser) { $currParser.Close() }
            if ($writer) { $writer.Dispose() }
        }

        # Warn about duplicates but continue processing. Every entry is a duplicate by construction,
        # so there is no count filter. Duplicates are found in sorted-anchor order here, so the
        # warnings are ordered by first row number to match the other scripts in the family.
        foreach ($anchor in ($duplicateAnchorsPrev.Keys | Sort-Object { $firstRowPrev[$_] }))
        {
            $rows = $duplicateAnchorsPrev[$anchor] -join ', '
            Write-Host "WARNING: Duplicate anchor '$anchor' in Previous file. Using row $($firstRowPrev[$anchor]); ignoring row(s): $rows. Pass -RejectDuplicateAnchors to fail the run on a duplicate instead." -ForegroundColor Yellow
        }
        foreach ($anchor in ($duplicateAnchorsCurr.Keys | Sort-Object { $firstRowCurr[$_] }))
        {
            $rows = $duplicateAnchorsCurr[$anchor] -join ', '
            Write-Host "WARNING: Duplicate anchor '$anchor' in Current file. Using row $($firstRowCurr[$anchor]); ignoring row(s): $rows. Pass -RejectDuplicateAnchors to fail the run on a duplicate instead." -ForegroundColor Yellow
        }

        if (($adds + $updates + $deletes) -gt 0)
        {
            Move-Item -LiteralPath $pendingOutput -Destination $changesCSVFile -Force -ErrorAction Stop
            Write-Host "Changes CSV written to: $changesCSVFile"
        }
        else
        {
            Write-Host "No changes detected; no CSV written"
        }
    }
    finally {
        # Run files, the merged files and any unclaimed output file all go here. Remove-Item on a
        # path that was already renamed away is a no-op.
        foreach ($t in $tempFiles) { Remove-Item -LiteralPath $t -Force -ErrorAction SilentlyContinue }
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