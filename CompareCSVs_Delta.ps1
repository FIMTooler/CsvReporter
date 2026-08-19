<#
.SYNOPSIS
Streams and compares two CSV files, writing every changed record as a whole row - an incremental
change feed for a downstream system that cannot itself produce one.

.DESCRIPTION
Where a standard changes report emits old/new value pairs per column, this script emits the
complete record: an Add or Update row is the whole Current record, a Delete row is the whole
Previous record. There is no per-column detail and no hashing - two records are compared value by
value, not by their formatted text, so a difference only in quoting style never reports as a change.

- Robust header parsing (quoted headers, embedded delimiters).
- Strict anchor presence; fails if the anchor column is missing.
- Builds a lookup of "Previous", then streams "Current". Current is never held in memory, and each
  matched key is removed from the Previous lookup as it is consumed, so the lookup shrinks while
  the comparison runs.
- Detects and warns about duplicate anchor values (uses first occurrence only).
- Case-sensitive or insensitive comparisons per -CaseSensitive.
- Output columns are every column of Current, in Current's physical order. The anchor is an
  ordinary column in its natural position - not hoisted to the front and not duplicated. A
  ChangeType column, naming Add/Update/Delete, is prepended unless -SeparateDeleteFile is used.
- Add and Update rows carry Current's values. Delete rows carry Previous's values, permuted into
  Current's column order - Previous and Current need not share a column order for this to work.
  Unchanged records are counted but never written; that omission is the resource saving the script
  exists for.
- -SeparateDeleteFile splits the output into two schema-identical files - Adds/Updates in one,
  Deletes in the other - neither carrying a ChangeType column, for a consumer whose importer cannot
  tolerate an extra column. Add and Update become indistinguishable in that mode; that is the
  accepted trade for a schema-identical file.
- -AnchorOnlyDeletes reduces a Delete row's non-anchor fields to empty, without changing the header
  or the file layout in either mode.
- Rows are written in the order they are found: Current's row order, then deletions in whatever
  order the lookup enumerates them. Output is not sorted by anchor.
- Always writes an output file, even when it holds only a header row - both files, independently,
  in split mode. This script feeds an automated process rather than being read by a person, so a
  downstream job should never have to distinguish "no file" from "job failed".
- The file mixes two points in time: Add/Update rows are post-change state, Delete rows are the
  last known state before removal. Reading every row as uniform "current state" resurrects deleted
  records.
- Prints a one-line summary with counts, including the unchanged count that is never emitted, and
  elapsed time.

.PARAMETER PreviousCSVFile
Path to the "Previous" CSV file.

.PARAMETER CurrentCSVFile
Path to the "Current" CSV file.

.PARAMETER AnchorColumn
Header name of the anchor column used to join rows.

.PARAMETER OutputFolder
Folder where the delta CSV(s) will be written.

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

.PARAMETER NormalizeHeaderNames
Opt in to trimmed, lowercased output header names, matching the trim+lowercase treatment the rest of
the family applies. Off by default: unlike the old/new column names the rest of the family
generates, this script's output headers are Current's own names, and lowercasing them by default
would silently break a case-sensitive downstream consumer.

This normalizes names this script derives, not names you supplied. Current's header names are always
affected. The ChangeType column name is affected only when it was left at its default - see
-ChangeTypeColumnName.

.PARAMETER AnchorOnlyDeletes
Reduce every non-anchor field on a Delete row to empty. The header is unchanged in both modes - all
columns stay present, only the anchor is populated on those rows.

.PARAMETER Force
Overwrite an existing output file. Without it, the run stops before any parsing begins if the
resolved output path (or, in split mode, either resolved path) already exists - overwriting a delta
that has not yet been consumed by its downstream process would destroy it unrecoverably.

.PARAMETER OutputFileName
Override the auto-derived main output filename. Must be a bare filename: no directory separator, no
path-invalid character, not empty or whitespace. Taken verbatim - a missing ".csv" is not added.

.PARAMETER ChangeTypeColumnName
Header name for the leading Add/Update/Delete column in single-file mode. Default 'ChangeType'.
Cannot be blank, and cannot collide (after trim and lowercase, regardless of -CaseSensitive) with any
column name in Current - the run throws rather than silently renaming either one. Meaningless with
-SeparateDeleteFile, since no such column is written in that mode; PowerShell rejects the combination
at bind time rather than silently ignoring the parameter.

With -NormalizeHeaderNames, this name is normalized only if it was left at its default: the default
belongs to this script, a value you passed is yours and is used exactly as given. That is keyed on
whether the parameter was supplied, not on its value - so under -NormalizeHeaderNames, passing
-ChangeTypeColumnName ChangeType explicitly produces 'ChangeType', while omitting it produces
'changetype'.

.PARAMETER SeparateDeleteFile
Write two schema-identical CSVs instead of one: Adds/Updates in the main file, Deletes in a second
file, neither carrying a ChangeType column. Selects the split-mode parameter set.

.PARAMETER DeleteFileName
Override the auto-derived delete-file filename. Only valid with -SeparateDeleteFile. Same bare-
filename rules as -OutputFileName, and must not resolve to the same name as -OutputFileName.

.INPUTS
None. You cannot pipe objects to this script.

.OUTPUTS
None. Writes one CSV (or two, with -SeparateDeleteFile) to -OutputFolder and summary messages to the
console.

.EXAMPLE
.\CompareCSVs_Delta.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -DelimiterName comma -EncodingName utf8BOM -CaseSensitive

.EXAMPLE
.\CompareCSVs_Delta.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -SeparateDeleteFile

.NOTES
Requires Microsoft.VisualBasic for TextFieldParser header parsing.
Duplicate anchor detection: When duplicates are found, the script warns with yellow text
showing the anchor value and row numbers, then processes only the first occurrence - unless
-RejectDuplicateAnchors is passed, in which case it fails the run on the first one instead.
#>
[CmdletBinding(DefaultParameterSetName='SingleFile')]
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
    [switch]$RejectDuplicateAnchors,
    [switch]$NormalizeHeaderNames,
    [switch]$AnchorOnlyDeletes,
    [switch]$Force,
    [String]$OutputFileName,
    [Parameter(ParameterSetName='SingleFile')]
    [String]$ChangeTypeColumnName = 'ChangeType',
    [Parameter(Mandatory=$true, ParameterSetName='SplitDeletes')]
    [switch]$SeparateDeleteFile,
    [Parameter(ParameterSetName='SplitDeletes')]
    [String]$DeleteFileName
)
try {
    $scriptStartTime = Get-Date
    $isSplit = ($PSCmdlet.ParameterSetName -eq 'SplitDeletes')

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
    # [ValidateNotNullOrEmpty()] is not sufficient here - a value of ' ' passes it. Only checked in
    # single-file mode: the parameter belongs to that set alone, and this is where it becomes a
    # literal output header.
    if (-not $isSplit -and [string]::IsNullOrWhiteSpace($ChangeTypeColumnName)) {
        throw "Parameter -ChangeTypeColumnName cannot be empty or whitespace."
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
    # empty *unquoted* field - the only place that renders is a Delete row's blanked-out columns
    # under -AnchorOnlyDeletes.
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

    # Bare-filename validation for -OutputFileName/-DeleteFileName: no directory separator, no other
    # character the filesystem itself would reject. GetInvalidFileNameChars() already includes both
    # slash characters, so this rejects a path as well as an invalid name in one check.
    function Test-BareFileName {
        param([string]$Value, [string]$ParamName)
        if ([string]::IsNullOrWhiteSpace($Value)) {
            throw "Parameter -$ParamName cannot be empty or whitespace."
        }
        $invalidChars = [System.IO.Path]::GetInvalidFileNameChars()
        if ($Value.IndexOfAny($invalidChars) -ge 0) {
            throw "Parameter -$ParamName '$Value' must be a bare filename: it contains a directory separator or another character the filesystem would reject."
        }
    }

    $fileTime = (Get-Date).ToString("yyyy-MM-dd_HHmmssfff")
    $baseFileName = [System.IO.Path]::GetFileNameWithoutExtension((Resolve-Path -LiteralPath $CurrentCSVFile).ProviderPath)
    # One Encoding object serves both directions: the read default (a BOM overrides it) and the
    # exact bytes written on export.
    $csvEncoding = Resolve-Encoding -EncodingName $EncodingName

    if ($PSBoundParameters.ContainsKey('OutputFileName')) { Test-BareFileName -Value $OutputFileName -ParamName 'OutputFileName' }
    if ($PSBoundParameters.ContainsKey('DeleteFileName')) { Test-BareFileName -Value $DeleteFileName -ParamName 'DeleteFileName' }

    $mainFileName = if ($PSBoundParameters.ContainsKey('OutputFileName')) { $OutputFileName } else { "Delta_{0}_GeneratedOn_{1}.csv" -f $baseFileName, $fileTime }
    $deleteFileName = $null
    if ($isSplit) {
        $deleteFileName = if ($PSBoundParameters.ContainsKey('DeleteFileName')) { $DeleteFileName } else { "Delta_Deletes_{0}_GeneratedOn_{1}.csv" -f $baseFileName, $fileTime }
        if ($mainFileName.Trim() -ieq $deleteFileName.Trim()) {
            throw "-OutputFileName and -DeleteFileName resolve to the same name ('$mainFileName'); they must differ."
        }
    }

    $mainOutputPath = [System.IO.Path]::Combine($OutputFolder, $mainFileName)
    $deleteOutputPath = if ($isSplit) { [System.IO.Path]::Combine($OutputFolder, $deleteFileName) } else { $null }

    # Fail fast, before any parsing: an automated run should not spend minutes comparing large files
    # only to die at the rename step. Move-Item at the rename below stays unforced without -Force,
    # closing the gap if a file appears mid-run - this early check exists for the message and the
    # fail-fast, not because the late one is missing.
    if ((Test-Path -LiteralPath $mainOutputPath) -and -not $Force) {
        throw "Output file already exists: $mainOutputPath. Pass -Force to overwrite it."
    }
    if ($isSplit -and (Test-Path -LiteralPath $deleteOutputPath) -and -not $Force) {
        throw "Output file already exists: $deleteOutputPath. Pass -Force to overwrite it."
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

    # ChangeType name collision: single-file mode only, since split mode never writes this column.
    # Compared after trim+lowercase, independent of -CaseSensitive, matching how duplicate headers are
    # detected above. Never auto-renamed - a silently renamed column is a contract change no
    # downstream process can detect.
    if (-not $isSplit) {
        $changeTypeNorm = $ChangeTypeColumnName.Trim().ToLowerInvariant()
        $collidingColumn = $currentHeadersRaw | Where-Object { $_.Trim().ToLowerInvariant() -eq $changeTypeNorm } | Select-Object -First 1
        if ($collidingColumn) {
            throw "Current CSV column '$collidingColumn' collides with the ChangeType column name '$ChangeTypeColumnName'. Pass a different -ChangeTypeColumnName to resolve this."
        }
    }

    if ($NormalizeHeaderNames) {
        Write-Host "Note: Output columns use trimmed and lowercase-normalized header names for consistency."
    }

    # Output columns: every column of Current, in Current's physical order, trim+lowercased only if
    # -NormalizeHeaderNames. ChangeType is prepended in single-file mode only - it is an ordinary
    # column, not hoisted or duplicated, so the anchor keeps its natural position among these.
    $outputHeaderFields = [string[]]$(if ($NormalizeHeaderNames) { $currentHeadersRaw | ForEach-Object { $_.Trim().ToLowerInvariant() } } else { $currentHeadersRaw })
    # -NormalizeHeaderNames normalizes names this script DERIVES, not names the caller supplied. The
    # default 'ChangeType' is this script's own, so it normalizes along with the rest; a value passed
    # explicitly is the caller's and is used exactly as given. That is the same "what you supplied is
    # yours" rule that keeps Current's header names verbatim by default.
    # Keyed on provenance, not value: under -NormalizeHeaderNames, passing -ChangeTypeColumnName
    # ChangeType explicitly yields 'ChangeType', while omitting it yields 'changetype'.
    $changeTypeHeader = if ($NormalizeHeaderNames -and -not $PSBoundParameters.ContainsKey('ChangeTypeColumnName')) {
        $ChangeTypeColumnName.Trim().ToLowerInvariant()
    } else {
        $ChangeTypeColumnName
    }
    $changeTypeOffset = if ($isSplit) { 0 } else { 1 }
    $outputWidth = $outputHeaderFields.Count + $changeTypeOffset
    $outputColumns = [object[]]::new($outputWidth)
    if (-not $isSplit) { $outputColumns[0] = $changeTypeHeader }
    for ($i = 0; $i -lt $outputHeaderFields.Count; $i++) { $outputColumns[$i + $changeTypeOffset] = $outputHeaderFields[$i] }

    # Previous -> output permutation map: position i (0-based, excluding ChangeType) holds the index
    # into a Previous row for output column i. Built once from the two per-file normalized-header ->
    # ordinal maps above. Used unconditionally by the delete path below - no fast path for the common
    # case where both files already share a column order, since that is an extra branch for no
    # measured gain.
    $prevToOutputMap = [int[]]::new($outputHeaderFields.Count)
    for ($i = 0; $i -lt $currentHeadersRaw.Count; $i++) {
        $norm = $currentHeadersRaw[$i].Trim().ToLowerInvariant()
        $prevToOutputMap[$i] = $prevHeaderIdx[$norm]
    }

    # Non-anchor normalized column names, walked on every Update/None comparison. The anchor is never
    # compared - two rows only pair up because their anchors already compared equal under the same
    # rule this loop uses, so that comparison could never show a difference.
    $compareColumnsNorm = @($currNormAll | Where-Object { $_ -ne $anchorNorm })

    # Pending-name-then-rename: rows are written here as they're produced. Unlike the rest of the
    # family, this script always renames - see "always writes a file" in the header comment above.
    $pendingOutput = [System.IO.Path]::Combine($OutputFolder, ("{0}.{1}.pending.tmp" -f $baseFileName, $fileTime))
    $pendingDeleteOutput = if ($isSplit) { [System.IO.Path]::Combine($OutputFolder, ("{0}.{1}.deletes.pending.tmp" -f $baseFileName, $fileTime)) } else { $null }
    $writer = New-Object System.IO.StreamWriter($pendingOutput, $false, $csvEncoding)
    $writer.NewLine = "`r`n"
    $writer.WriteLine((ConvertTo-CsvLine -Fields $outputColumns -Delimiter $Delimiter))
    $deleteWriter = $null
    if ($isSplit) {
        $deleteWriter = New-Object System.IO.StreamWriter($pendingDeleteOutput, $false, $csvEncoding)
        $deleteWriter.NewLine = "`r`n"
        $deleteWriter.WriteLine((ConvertTo-CsvLine -Fields $outputColumns -Delimiter $Delimiter))
    }
    $emitted = 0

    # Summary counters
    $adds = 0; $updates = 0; $deletes = 0; $nones = 0

    # anchor -> row number of its first occurrence. Both tables use $anchorComparer, so the script
    # has a single notion of anchor identity - a default @{} is case-insensitive and would disagree
    # with an Ordinal comparer under -CaseSensitive.
    $firstRowPrev = [System.Collections.Generic.Dictionary[string,int]]::new($anchorComparer)
    # Only anchors that actually repeat get an entry, holding the row numbers being ignored.
    $duplicateAnchorsPrev = [System.Collections.Generic.Dictionary[string,System.Collections.Generic.List[int]]]::new($anchorComparer)
    $firstRowCurr = [System.Collections.Generic.Dictionary[string,int]]::new($anchorComparer)
    $duplicateAnchorsCurr = [System.Collections.Generic.Dictionary[string,System.Collections.Generic.List[int]]]::new($anchorComparer)
    # Previous is held as a lookup; matched keys are removed while Current streams, so what remains
    # at the end is exactly the set of deletions.
    $previousLookup = [System.Collections.Generic.Dictionary[string,string[]]]::new($anchorComparer)

    $progressId = 1
    try {
        # 2. Stream Previous into the lookup (first occurrence wins, matching the warning text)
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Loading Previous..."
        $prevPath = Resolve-FullPath -Path $PreviousCSVFile
        $parser = $null
        $rowNum = 0
        try {
            $parser = New-CsvParser -FullPath $prevPath -Delimiter $Delimiter -DefaultEncoding $csvEncoding
            $null = $parser.ReadFields()   # header; validated separately
            while (-not $parser.EndOfData) {
                $row = $parser.ReadFields()
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
                    $previousLookup[$anchor] = $row
                }

                if (($rowNum % 1000) -eq 0) {
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Loading Previous... ($rowNum rows)"
                }
            }
        } catch [Microsoft.VisualBasic.FileIO.MalformedLineException] {
            throw "Malformed CSV in '$prevPath' at line $($parser.ErrorLineNumber). Unbalanced quotes are the usual cause. Line reads: $($parser.ErrorLine)"
        } finally {
            if ($parser) { $parser.Close() }
        }
        if ($previousLookup.Count -eq 0) { throw "No records found in Previous CSV file." }

        # 3. Stream Current, comparing against the lookup. Current is never materialized.
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Streaming Current..."
        $currPath = Resolve-FullPath -Path $CurrentCSVFile
        $parser = $null
        $rowNum = 0
        try {
            $parser = New-CsvParser -FullPath $currPath -Delimiter $Delimiter -DefaultEncoding $csvEncoding
            $null = $parser.ReadFields()   # header; validated separately
            while (-not $parser.EndOfData) {
                $currRow = $parser.ReadFields()
                $rowNum++
                # Field count first (see the Previous loop for why).
                if ($currRow.Length -ne $currentHeadersRaw.Count) {
                    throw "Row $rowNum in Current file has $($currRow.Length) field(s), expected $($currentHeadersRaw.Count). Fields: $($currRow -join ' | ')"
                }
                $key = $currRow[$currAnchorIdx]
                # Anchor Value Validation
                if ([string]::IsNullOrWhiteSpace($key)) { throw "Anchor column '$AnchorColumn' is null or empty string in Current record at row $($rowNum): $($currRow -join ', ')." }

                # Duplicate Anchor Value Check. A repeat is recorded and skipped, so only the
                # first occurrence produces a change row - matching the in-memory scripts, where the
                # Current dictionary discards later duplicates.
                if ($firstRowCurr.ContainsKey($key)) {
                    if ($RejectDuplicateAnchors) {
                        throw "Duplicate anchor '$key' in Current file at row $rowNum (first seen at row $($firstRowCurr[$key])). Rejected because -RejectDuplicateAnchors was specified."
                    }
                    if (-not $duplicateAnchorsCurr.ContainsKey($key)) {
                        $duplicateAnchorsCurr[$key] = [System.Collections.Generic.List[int]]::new()
                    }
                    $duplicateAnchorsCurr[$key].Add($rowNum)
                    continue
                }
                $firstRowCurr[$key] = $rowNum

                if (($rowNum % 1000) -eq 0) {
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Streaming Current... ($rowNum rows)"
                }

                if ($previousLookup.ContainsKey($key))
                {
                    $prevRow = $previousLookup[$key]
                    $isUpdate = $false
                    foreach ($n in $compareColumnsNorm)
                    {
                        $prevValue = $prevRow[$prevHeaderIdx[$n]]
                        $currValue = $currRow[$currHeaderIdx[$n]]
                        $valuesDiffer = if ($CaseSensitive) { $prevValue -cne $currValue } else { $prevValue -ine $currValue }
                        if ($valuesDiffer) { $isUpdate = $true; break }   # whole-row output needs no further column, only the verdict
                    }
                    # Matched: drop it from the lookup so what remains is the delete set, and so the
                    # lookup shrinks as the stream progresses.
                    [void]$previousLookup.Remove($key)
                    if ($isUpdate)
                    {
                        $updates++
                        $outRow = [object[]]::new($outputWidth)
                        if (-not $isSplit) { $outRow[0] = "Update" }
                        [Array]::Copy($currRow, 0, $outRow, $changeTypeOffset, $currRow.Length)
                        $writer.WriteLine((ConvertTo-CsvLine -Fields $outRow -Delimiter $Delimiter))
                        $emitted++
                    }
                    else
                    {
                        $nones++
                    }
                }
                else
                {
                    $adds++
                    $outRow = [object[]]::new($outputWidth)
                    if (-not $isSplit) { $outRow[0] = "Add" }
                    [Array]::Copy($currRow, 0, $outRow, $changeTypeOffset, $currRow.Length)
                    $writer.WriteLine((ConvertTo-CsvLine -Fields $outRow -Delimiter $Delimiter))
                    $emitted++
                }
                # No periodic Flush() here: this file is renamed unconditionally at the end and never
                # read while the script runs, so there is nothing a periodic flush would protect -
                # StreamWriter's own internal buffer already bounds how much sits unwritten at any
                # moment. Measured to make no runtime difference either way, so simplicity wins.
            }
        } catch [Microsoft.VisualBasic.FileIO.MalformedLineException] {
            throw "Malformed CSV in '$currPath' at line $($parser.ErrorLineNumber). Unbalanced quotes are the usual cause. Line reads: $($parser.ErrorLine)"
        } finally {
            if ($parser) { $parser.Close() }
        }
        if ($rowNum -eq 0) { throw "No records found in Current CSV file." }

        # Warn about duplicates but continue processing. Every entry is a duplicate by construction,
        # so there is no count filter; keys enumerate in first-detection order, i.e. by row.
        foreach ($anchor in $duplicateAnchorsPrev.Keys) {
            $rows = $duplicateAnchorsPrev[$anchor] -join ', '
            Write-Host "WARNING: Duplicate anchor '$anchor' in Previous file. Using row $($firstRowPrev[$anchor]); ignoring row(s): $rows. Pass -RejectDuplicateAnchors to fail the run on a duplicate instead." -ForegroundColor Yellow
        }
        foreach ($anchor in $duplicateAnchorsCurr.Keys) {
            $rows = $duplicateAnchorsCurr[$anchor] -join ', '
            Write-Host "WARNING: Duplicate anchor '$anchor' in Current file. Using row $($firstRowCurr[$anchor]); ignoring row(s): $rows. Pass -RejectDuplicateAnchors to fail the run on a duplicate instead." -ForegroundColor Yellow
        }

        # 4. Whatever is left in the lookup was never seen in Current, so it was deleted. Its values
        # come from Previous, permuted into Current's column order via $prevToOutputMap.
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Finalizing deletions..."
        $toDeleteTotal = $previousLookup.Count
        $iDel = 0
        $deleteTargetWriter = if ($isSplit) { $deleteWriter } else { $writer }
        foreach ($key in $previousLookup.Keys)
        {
            $prevRow = $previousLookup[$key]
            $deletes++
            $outRow = [object[]]::new($outputWidth)
            if (-not $isSplit) { $outRow[0] = "Delete" }
            if ($AnchorOnlyDeletes)
            {
                $outRow[$changeTypeOffset + $currAnchorIdx] = $prevRow[$prevAnchorIdx]
            }
            else
            {
                for ($i = 0; $i -lt $outputHeaderFields.Count; $i++) {
                    $outRow[$i + $changeTypeOffset] = $prevRow[$prevToOutputMap[$i]]
                }
            }
            $deleteTargetWriter.WriteLine((ConvertTo-CsvLine -Fields $outRow -Delimiter $Delimiter))
            $emitted++
            $iDel++
            if (($iDel % 1000) -eq 0 -or $iDel -eq $toDeleteTotal) {
                $pct = if ($toDeleteTotal -gt 0) { [int](($iDel/$toDeleteTotal)*100) } else { 100 }
                Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Finalizing deletions... ($iDel of $toDeleteTotal)" -PercentComplete $pct
            }
        }

        # No sort, no batch write: rows already went to the pending file(s) as they were produced
        # above. Explicit Flush() before Dispose() - not because Dispose() skips it, but because a
        # failure here throws normally from this try block. The same failure inside Dispose(), called
        # from finally below, could mask whatever exception the try block was already unwinding from.
        $writer.Flush()
        $writer.Dispose()
        $writer = $null
        if ($isSplit) {
            $deleteWriter.Flush()
            $deleteWriter.Dispose()
            $deleteWriter = $null
        }

        # Always rename - this script has no "no changes, no file" path. A downstream job should not
        # have to distinguish "no file" from "job failed", so even a zero-change run produces a
        # header-only CSV.
        # Re-check destinations here, not only at the top of the run. That check ran before parsing;
        # this one catches a file that appeared while the comparison was running. It narrows the
        # window - it cannot close it, since a rename can also fail for reasons no check predicts.
        if (-not $Force) {
            if (Test-Path -LiteralPath $mainOutputPath) {
                throw "Output file already exists: $mainOutputPath. Pass -Force to overwrite it."
            }
            if ($isSplit -and (Test-Path -LiteralPath $deleteOutputPath)) {
                throw "Output file already exists: $deleteOutputPath. Pass -Force to overwrite it."
            }
        }

        if ($isSplit) {
            # A split run produces a PAIR that a provisioning pipeline consumes together. A main file
            # present without its delete file means adds/updates are applied while deprovisioning
            # silently is not - the exact failure this script's "deletes are non-negotiable" design
            # exists to prevent. So a half-completed rename must leave NO output rather than half of
            # it: nothing is unambiguous, half is not. This deliberately differs from the rest of the
            # family, which renames a single file and has no pair to keep consistent.
            #
            # Deletes are renamed FIRST on purpose. If the undo below ever fails too, the residue is
            # an orphaned delete file and no main file, which reads as an incomplete run - the safe
            # direction to fail in.
            if ($Force) {
                Move-Item -LiteralPath $pendingDeleteOutput -Destination $deleteOutputPath -Force -ErrorAction Stop
            } else {
                Move-Item -LiteralPath $pendingDeleteOutput -Destination $deleteOutputPath -ErrorAction Stop
            }
            try {
                if ($Force) {
                    Move-Item -LiteralPath $pendingOutput -Destination $mainOutputPath -Force -ErrorAction Stop
                } else {
                    Move-Item -LiteralPath $pendingOutput -Destination $mainOutputPath -ErrorAction Stop
                }
            } catch {
                # Undo the rename that did succeed, and drop the pending file that did not, so the
                # output folder is left exactly as it was found.
                Remove-Item -LiteralPath $deleteOutputPath -Force -ErrorAction SilentlyContinue
                Remove-Item -LiteralPath $pendingOutput -Force -ErrorAction SilentlyContinue
                throw "Failed to write '$mainOutputPath': $($_.Exception.Message) The delete file that had already been written was removed, so this run left no output - a main file without its matching delete file would silently drop deprovisioning work downstream."
            }
        } else {
            if ($Force) {
                Move-Item -LiteralPath $pendingOutput -Destination $mainOutputPath -Force -ErrorAction Stop
            } else {
                Move-Item -LiteralPath $pendingOutput -Destination $mainOutputPath -ErrorAction Stop
            }
        }
        Write-Host "Delta CSV written to: $mainOutputPath"
        if ($isSplit) {
            Write-Host "Delta deletes CSV written to: $deleteOutputPath"
        }
    }
    finally {
        if ($writer) { $writer.Dispose() }
        if ($deleteWriter) { $deleteWriter.Dispose() }
        # Always clear progress
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Completed
    }
    # Summary output ($nones is counted in the compare loop, not recounted here). Total is every
    # distinct anchor seen across both files - Unchanged is reported despite never being emitted,
    # since it is the number that tells an operator how much work this delta saved.
    $total = $adds + $updates + $deletes + $nones
    Write-Host ("Summary: Adds={0}, Updates={1}, Deletes={2}, Unchanged={3}, Total={4}" -f $adds, $updates, $deletes, $nones, $total)

    $elapsed = (Get-Date) - $scriptStartTime
    # Floor, not [int]: [int] rounds to nearest, so a 47.7s run reported "1m 47s"
    $elapsedStr = "{0}m {1}s" -f [int][Math]::Floor($elapsed.TotalMinutes), $elapsed.Seconds
    Write-Host "Elapsed: $elapsedStr"
}
catch {
    Write-Error $_
    exit 1
}