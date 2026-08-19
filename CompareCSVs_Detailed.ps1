<#
.SYNOPSIS
Compares two CSV files and writes a changes report (Adds, Updates, Deletes) with field-level match tracking.

.DESCRIPTION
Streaming comparison suited for mid/large CSVs, with full field-level match tracking:
- Robust header parsing (quoted headers, embedded delimiters).
- Strict anchor presence; fails if the anchor column is missing.
- Builds a lookup of "Previous", then streams "Current". Current is never held in memory, and each
  matched key is removed from the Previous lookup as it is consumed, so the lookup shrinks while
  the comparison runs. Peak memory is therefore well below the in-memory script for the same input.
- Detects and warns about duplicate anchor values (uses first occurrence only).
- Case-sensitive or insensitive comparisons per -CaseSensitive.
- Outputs a CSV with ChangeType and old/new/match values for all columns.
  - Match column shows True/False for Update/None rows.
  - Match column will be empty for Add/Delete rows (cannot be matched across files).
  - Summary row inserted as first record with per-column mismatch counts ("X of Y FALSE").
- Rows are written in the order they are found: Current's row order, then deletions in whatever
  order the lookup enumerates them. Output is not sorted by anchor.
- Prints a one-line summary with counts and elapsed time.

.PARAMETER PreviousCSVFile
Path to the "Previous" CSV file.

.PARAMETER CurrentCSVFile
Path to the "Current" CSV file.

.PARAMETER AnchorColumn
Header name of the anchor column used to join rows.

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

.PARAMETER ValueTransforms
Hashtable of column-specific value transformations. Format:
  @{
    'ColumnName' = @{
      'oldValue' = 'newValue'
      'Active' = '1'
      'Inactive' = '0'
    }
  }
Column names are normalized (trimmed and lowercase) for consistency with CSV headers,
so you can copy/paste header names directly from your CSV files.
Transformations applied to Previous file values respecting -CaseSensitive flag.
Transformations applied during comparison only; original values stored in output.

.PARAMETER DateFormats
Hashtable for date normalization per column. Format:
  @{
    'ColumnName' = @{
      Previous = 'MM/dd/yyyy'   # format of Previous file values
      Current  = 'yyyy-MM-dd'   # format of Current file values
      Output   = 'yyyy-MM-dd'   # normalized string used for comparison
    }
  }
Previous and Current are required. Output is optional and defaults to 'yyyy-MM-dd'; it only affects
the string the two sides are compared as, never what appears in the report.

Column names are normalized (trim/lower). Parsing uses invariant culture. Empty and whitespace-only
values are skipped rather than parsed, so a blank date is not a warning. A non-empty value that
fails to parse keeps its raw value, emits a warning, and falls back to raw comparison. Original
values are preserved in old/new columns; normalized strings are used for comparison/match.

.PARAMETER IgnoreColumns
Column names to exclude from the comparison entirely. Ignored columns are dropped from the report,
so they produce no old/new/match triplet and do not affect whether a row is an Update.

Names are normalized (trimmed and lowercase) like every other column reference, so header names can
be pasted straight from the CSV.

A name need exist in only ONE of the two files. An ignored column is not compared, so it does not
have to be present on both sides. Because the exclusion is applied before the column-set mismatch
check, -IgnoreColumns also reconciles a column that exists in only one file - in either direction.
Without it, that difference is rejected as a column mismatch.

A name found in NEITHER file is a warning, not an error: nothing matched, so nothing was excluded.
This lets one ignore list be reused across file pairs that do not all carry every column. The
warning names the unmatched columns and lists both header sets, so a typo stays visible.

The anchor column cannot be ignored, and ignoring every non-anchor column is rejected because
nothing would be left to compare. A column cannot appear in -IgnoreColumns and in -ValueTransforms
or -DateFormats at the same time.

.INPUTS
None. You cannot pipe objects to this script.

.OUTPUTS
None. Writes a changes CSV to -OutputFolder and summary messages to the console.

.EXAMPLE
.\CompareCSVs_Detailed.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -DelimiterName comma -EncodingName utf8BOM -CaseSensitive

.EXAMPLE
$transforms = @{
    'status' = @{ 'Active' = '1'; 'Inactive' = '0' }
    'department' = @{ 'HR' = 'Human Resources'; 'IT' = 'Information Technology' }
}
.\CompareCSVs_Detailed.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -ValueTransforms $transforms

.EXAMPLE
# Using prefix (<<) and suffix (>>) modifiers for value manipulation
$transforms = @{
    'productID' = @{ '*' = '>>0' }  # Append '0' to all product IDs before comparison
    'status' = @{ 'Active' = 'Y'; 'Inactive' = 'N'; '*' = 'Unknown' }  # Exact matches + wildcard fallback
}
.\CompareCSVs_Detailed.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn ID -OutputFolder .\out -ValueTransforms $transforms

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
    [switch]$RejectDuplicateAnchors,
    [hashtable]$ValueTransforms = @{},
    [hashtable]$DateFormats = @{},
    [string[]]$IgnoreColumns = @()
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
    # empty *unquoted* field, which is how unset match values render for Add/Delete rows.
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

    function Try-NormalizeDate {
        param(
            [Parameter(Mandatory)][string]$Value,
            [Parameter(Mandatory)][string]$InputFormat,
            [Parameter(Mandatory)][string]$OutputFormat
        )
        try {
            $dt = [DateTime]::ParseExact($Value, $InputFormat, [System.Globalization.CultureInfo]::InvariantCulture)
            return $dt.ToString($OutputFormat, [System.Globalization.CultureInfo]::InvariantCulture)
        } catch {
            return $null
        }
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
    # Normalize IgnoreColumns parameter (trim, lowercase)
    $normalizedIgnoreColumns = @()
    if ($IgnoreColumns -and $IgnoreColumns.Count -gt 0) {
        $normalizedIgnoreColumns = @($IgnoreColumns | ForEach-Object { $_.Trim().ToLowerInvariant() })
    }
    # Resolve anchor column name (normalized) early for validation
    $anchorNorm = $AnchorColumn.Trim().ToLowerInvariant()

    # Normalized header sets (sorted) for cross-file comparison
    $previousHeadersNorm = $prevNormAll | Sort-Object -ErrorAction Stop
    $currentHeadersNorm  = $currNormAll | Sort-Object -ErrorAction Stop

    # Validate IgnoreColumns and filter them out.
    # A column named here is simply not compared, so it need only exist in ONE of the two files:
    # ignoring a Current-only column is as valid as ignoring a Previous-only one, and the filter
    # below removes it from whichever side carries it. Requiring it in Previous was an asymmetry, and
    # it reported "does not exist in CSV headers" for a column that plainly existed in Current.
    # A name matching NEITHER file warns rather than failing, so one ignore list can be reused across
    # file pairs that do not all carry every column. The warning is what keeps a typo visible.
    if ($normalizedIgnoreColumns.Count -gt 0) {
        $unmatchedIgnore = [System.Collections.Generic.List[string]]::new()
        foreach ($ignoreCol in $normalizedIgnoreColumns) {
            if ($ignoreCol -eq $anchorNorm) {
                throw "Anchor column '$anchorNorm' cannot be included in -IgnoreColumns parameter."
            }
            if ($ignoreCol -notin $previousHeadersNorm -and $ignoreCol -notin $currentHeadersNorm) {
                $unmatchedIgnore.Add($ignoreCol)
            }
        }
        if ($unmatchedIgnore.Count -gt 0) {
            Write-Host "WARNING: Column(s) in -IgnoreColumns found in neither file, nothing to exclude: $($unmatchedIgnore -join ', '). Previous columns: $($previousHeadersRaw -join ', '). Current columns: $($currentHeadersRaw -join ', ')" -ForegroundColor Yellow
        }
        $previousHeadersNorm = @($previousHeadersNorm | Where-Object { $_ -notin $normalizedIgnoreColumns })
        $currentHeadersNorm = @($currentHeadersNorm | Where-Object { $_ -notin $normalizedIgnoreColumns })
        # -le 1, not -eq 0. The anchor cannot be ignored (rejected above), so it always survives the
        # filter and the count can never reach 0 - testing for 0 made this check unreachable. The
        # state worth rejecting is the anchor being all that is left, since there is then nothing to
        # compare and every row reports as None.
        if ($previousHeadersNorm.Count -le 1 -or $currentHeadersNorm.Count -le 1) {
            throw "Only the anchor column '$anchorNorm' would remain after applying -IgnoreColumns. At least one other column is required to compare."
        }
    }

    # The inner parentheses are required. Without them PowerShell parses the condition as
    # (($previousHeadersNorm -join ',') -eq $currentHeadersNorm) -join ',' which evaluates to the
    # string "False", making -not always $false and the throw below unreachable.
    if (-not (($previousHeadersNorm -join ',') -eq ($currentHeadersNorm -join ','))) {
        throw "Column mismatch detected! Previous CSV columns: $($previousHeadersRaw -join ', ')`nCurrent CSV columns: $($currentHeadersRaw -join ', ')"
    }

    # Normalize ValueTransforms keys (column names) for consistency
    $normalizedValueTransforms = @{}
    if ($ValueTransforms -and $ValueTransforms.Count -gt 0) {
        # Validate that no transforms are defined for ignored columns
        if ($normalizedIgnoreColumns.Count -gt 0) {
            foreach ($transformColumn in $ValueTransforms.Keys) {
                $transformColNorm = $transformColumn.Trim().ToLowerInvariant()
                if ($transformColNorm -in $normalizedIgnoreColumns) {
                    throw "Column '$transformColumn' in -ValueTransforms cannot be transformed because it is included in -IgnoreColumns."
                }
            }
        }
        foreach ($transformColumn in $ValueTransforms.Keys) {
            $transformColNorm = $transformColumn.Trim().ToLowerInvariant()
            if ($transformColNorm -eq $anchorNorm) {
                throw "Anchor column '$transformColumn' cannot be used in -ValueTransforms; it is the join key and is never compared."
            }
            # Check if column exists in normalized headers
            if ($transformColNorm -notin $previousHeadersNorm) {
                throw "Column '$transformColumn' in -ValueTransforms does not exist in CSV headers. Available columns: $($previousHeadersRaw -join ', ')"
            }
            # Validate transform map is a hashtable
            $transformMap = $ValueTransforms[$transformColumn]
            if ($transformMap -isnot [hashtable]) {
                throw "Transform map for column '$transformColumn' must be a hashtable, got $($transformMap.GetType().Name)"
            }
            if ($transformMap.Count -eq 0) {
                throw "Transform map for column '$transformColumn' is empty"
            }
            # Check for duplicate keys (case-sensitive or insensitive)
            $transformKeys = @($transformMap.Keys)
            if ($CaseSensitive) {
                # Case-sensitive: keys should be unique (hashtable enforces this, but verify)
                $groupedKeys = $transformKeys | Group-Object
                $duplicates = $groupedKeys | Where-Object { $_.Count -gt 1 }
                if ($duplicates) {
                    $details = $duplicates | ForEach-Object { "$($_.Name) (appears $($_.Count) times)" }
                    throw "Duplicate keys (case-sensitive) in transform map for column '$transformColumn': $($details -join ', ')"
                }
            } else {
                # Case-insensitive: check normalized keys for duplicates
                $normalizedKeys = $transformKeys | ForEach-Object { $_.Trim().ToLowerInvariant() }
                $groupedNormKeys = $normalizedKeys | Group-Object
                $duplicates = $groupedNormKeys | Where-Object { $_.Count -gt 1 }
                if ($duplicates) {
                    $details = foreach ($g in $duplicates) {
                        $norm = $g.Name
                        $originals = $transformKeys | Where-Object { $_.Trim().ToLowerInvariant() -eq $norm }
                        "{0} => [{1}]" -f $norm, ($originals -join ', ')
                    }
                    throw "Duplicate keys (case-insensitive) in transform map for column '$transformColumn': $($details -join '; ')"
                }
            }
            # Validate transform map values.
            # An EMPTY value is legal and useful: 'N/A' = '' collapses a Previous-side sentinel so it
            # compares equal to an empty Current cell, which is one of the commonest differences
            # between a legacy extract and its replacement. Whitespace-only is legal for the same
            # reason - it is an ordinary string, not a synonym for empty.
            # $null is still rejected. This validates the CALLER'S hashtable, not CSV data: a field
            # read from a CSV is always a string - '' when empty, never $null - because
            # TextFieldParser returns string[] and a short row is rejected before anything indexes
            # it. A $null here means the operator wrote @{ 'N/A' = $null }, and the modifier check
            # below would then call .StartsWith on nothing.
            foreach ($kvp in $transformMap.GetEnumerator()) {
                if ($null -eq $kvp.Value) {
                    throw "Transform value for key '$($kvp.Key)' in column '$transformColumn' cannot be null."
                }
            }
            # Validate transform strategy consistency: warn if mixing direct replacement with modifiers
            $hasExactDirectReplacement = $false
            $hasWildcardModifier = $false
            $wildcardValue = $null

            foreach ($kvp in $transformMap.GetEnumerator()) {
                if ($kvp.Key -ne '*') {
                    # Check if exact key uses direct replacement (no modifier)
                    if (-not ($kvp.Value.StartsWith('<<') -or $kvp.Value.StartsWith('>>'))) {
                        $hasExactDirectReplacement = $true
                    }
                } else {
                    # Wildcard exists; check if it uses a modifier
                    $wildcardValue = $kvp.Value
                    if ($wildcardValue.StartsWith('<<') -or $wildcardValue.StartsWith('>>')) {
                        $hasWildcardModifier = $true
                    }
                }
            }

            # Warn if mixing strategies
            if ($hasExactDirectReplacement -and $hasWildcardModifier) {
                Write-Host "WARNING: Column '$transformColumn' mixes direct replacement (exact keys) with modifiers (wildcard). This may cause unexpected behavior." -ForegroundColor Yellow
            }

            # Store the normalized transform map with normalized column name key
            $normalizedValueTransforms[$transformColNorm] = $transformMap
        }
    }

    # Normalize DateFormats keys (column names) and validate maps
    $normalizedDateFormats = @{}
    if ($DateFormats -and $DateFormats.Count -gt 0) {
        foreach ($dfCol in $DateFormats.Keys) {
            $dfColNorm = $dfCol.Trim().ToLowerInvariant()
            if ($dfColNorm -eq $anchorNorm) {
                throw "Anchor column '$dfCol' cannot be used in -DateFormats; it is the join key and is never compared."
            }
            if ($dfColNorm -notin $previousHeadersNorm) {
                throw "Column '$dfCol' in -DateFormats does not exist in CSV headers. Available columns: $($previousHeadersRaw -join ', ')"
            }
            if ($normalizedIgnoreColumns -and ($dfColNorm -in $normalizedIgnoreColumns)) {
                throw "Column '$dfCol' in -DateFormats cannot be normalized because it is included in -IgnoreColumns."
            }
            $dfMap = $DateFormats[$dfCol]
            if ($dfMap -isnot [hashtable]) {
                throw "DateFormats entry for '$dfCol' must be a hashtable with Previous/Current/Output keys."
            }
            $prevFmt = $dfMap['Previous']
            $currFmt = $dfMap['Current']
            $outFmt  = $dfMap['Output']
            if ([string]::IsNullOrWhiteSpace($prevFmt) -or [string]::IsNullOrWhiteSpace($currFmt)) {
                throw "DateFormats for '$dfCol' requires non-empty Previous and Current formats."
            }
            if ([string]::IsNullOrWhiteSpace($outFmt)) {
                $outFmt = 'yyyy-MM-dd'
            }
            $normalizedDateFormats[$dfColNorm] = @{
                Previous = $prevFmt
                Current  = $currFmt
                Output   = $outFmt
            }
        }
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

    # The anchor gets its own report column already (added first, below) and never needs an old/new/
    # match set of its own: two rows only pair up when their anchors already compared equal under the
    # same rule this loop uses, so that comparison can never show a difference. Dropped here, after
    # every validation above (column-set, per-file anchor presence, -IgnoreColumns, -ValueTransforms,
    # -DateFormats) has already run against the full header set, so none of those checks changes.
    $previousHeadersNorm = @($previousHeadersNorm | Where-Object { $_ -ne $anchorNorm })
    $currentHeadersNorm  = @($currentHeadersNorm  | Where-Object { $_ -ne $anchorNorm })

    # Rows are fixed-width object[] in report-column order rather than PSCustomObject. Column names
    # live once in $reportColumns (written as the header line) instead of on every row.
    # object[] rather than string[] so an assigned $null stays $null (a string[] slot coerces it to "").
    # Report columns: AnchorColumn, ChangeType, then old/new/match triplets for each column
    $reportColumns = [System.Collections.Generic.List[string]]::new(2 + (3 * $previousHeadersNorm.Count))
    $reportColumns.Add($AnchorColumn)
    $reportColumns.Add("ChangeType")
    # $colIndex maps a column to the offset of its triplet: old = i, new = i+1, match = i+2
    $colIndex = @{}
    foreach ($prop in $previousHeadersNorm) {
        $colIndex[$prop] = $reportColumns.Count
        $reportColumns.Add("old $($prop)")
        $reportColumns.Add("new $($prop)")
        $reportColumns.Add("match $($prop)")
    }
    $rowWidth = $reportColumns.Count

    # The SUMMARY row must be the first data row, but its content (per-column mismatch/transform
    # counts) isn't known until every row has been compared - which conflicts with writing rows as
    # they're produced. Data rows go to this body-spool file as they're produced instead; header and
    # SUMMARY are written directly to the real pending output only once counts are final, with the
    # spool's contents appended after. The spool uses a fixed internal encoding, independent of
    # whatever -EncodingName the caller requested for the real output, and is read back with that
    # same fixed encoding - it never has to round-trip through the caller's chosen encoding, and two
    # independently-BOM'd files are never concatenated at the byte level.
    $spoolEncoding = New-Object System.Text.UTF8Encoding($true)
    $pendingBody = [System.IO.Path]::Combine($OutputFolder, ("{0}.{1}.body.tmp" -f $baseFileName, $fileTime))
    $bodyWriter = New-Object System.IO.StreamWriter($pendingBody, $false, $spoolEncoding)
    $bodyWriter.NewLine = "`r`n"
    $emitted = 0

    # Summary counters
    $adds = 0; $updates = 0; $deletes = 0; $nones = 0
    # Track match/mismatch counts per column for summary row
    $matchCounts = @{}
    # Track applied counts per transform rule per column
    $transformAppliedCounts = @{}
    # Columns where '*' is configured but no explicit '' rule exists. An empty Previous value in one
    # of these is skipped by the wildcard on purpose, silently - so if any is seen, say so once at the
    # end. Only these columns are watched; a column with an explicit '' rule has had its empties
    # handled, and warning about it would be untrue. Value is "an empty was seen", set during compare.
    $wildcardEmptyWatch = @{}
    foreach ($n in $previousHeadersNorm) {
        $matchCounts[$n] = @{ mismatchCount = 0; totalCount = 0 }
        # Initialize transform tracking for columns with transforms
        if ($normalizedValueTransforms -and $normalizedValueTransforms.ContainsKey($n)) {
            $transformAppliedCounts[$n] = @{}
            foreach ($key in $normalizedValueTransforms[$n].Keys) {
                $transformAppliedCounts[$n][$key] = 0
            }
            if ($normalizedValueTransforms[$n].ContainsKey('*') -and -not $normalizedValueTransforms[$n].ContainsKey('')) {
                $wildcardEmptyWatch[$n] = $false
            }
        }
    }

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

                $row = [object[]]::new($rowWidth)
                $row[0] = $key
                $row[1] = "None"
                if ($previousLookup.ContainsKey($key))
                {
                    $prevRow = $previousLookup[$key]
                    $isUpdate = $false
                    #"User exists in both files. $($key)" | Write-Verbose
                    foreach ($n in $previousHeadersNorm)
                    {
                        #"Comparing column: $($n)" | Write-Verbose
                        $prevIdx = $prevHeaderIdx[$n]
                        $currIdx = $currHeaderIdx[$n]

                        $prevValue = $prevRow[$prevIdx]
                        $currValue = $currRow[$currIdx]

                        # Normalize dates (if configured) before value transforms
                        $prevValueForComparison = $prevValue
                        $currValueForComparison = $currValue
                        if ($normalizedDateFormats -and $normalizedDateFormats.ContainsKey($n)) {
                            $df = $normalizedDateFormats[$n]

                            $normalizedPrev = $null
                            if (-not [string]::IsNullOrWhiteSpace($prevValue)) {
                                $normalizedPrev = Try-NormalizeDate -Value $prevValue -InputFormat $df.Previous -OutputFormat $df.Output
                                if (-not $normalizedPrev) { Write-Warning "Date normalize failed (Previous) col '$n' value '$prevValue'" }
                            }

                            $normalizedCurr = $null
                            if (-not [string]::IsNullOrWhiteSpace($currValue)) {
                                $normalizedCurr = Try-NormalizeDate -Value $currValue -InputFormat $df.Current -OutputFormat $df.Output
                                if (-not $normalizedCurr) { Write-Warning "Date normalize failed (Current) col '$n' value '$currValue'" }
                            }

                            if ($normalizedPrev) { $prevValueForComparison = $normalizedPrev }
                            if ($normalizedCurr) { $currValueForComparison = $normalizedCurr }
                        }

                        # Apply value transformation for comparison (if configured) on the (possibly normalized) previous value
                        # Explicit rules match the Previous value EXACTLY, including '' and '   '. The wildcard
                        # applies to anything not matched explicitly EXCEPT '': an empty value is reached only
                        # by an explicit '' rule, so '*' can never sweep up empties and manufacture a
                        # difference against an equally empty Current cell. '   ' is an ordinary string, not a
                        # synonym for empty, so the wildcard does reach it. IsNullOrEmpty, not
                        # IsNullOrWhiteSpace, is what draws that line.
                        # $matched rather than truthiness on $mapKey: '' is a legitimate key and is FALSY, so
                        # "if ($mapKey)" would silently route an empty match into the wildcard branch instead.
                        # Same trap as "if (-not $index)" when index 0 is valid.
                        if ($normalizedValueTransforms -and $normalizedValueTransforms.ContainsKey($n)) {
                            if ([string]::IsNullOrEmpty($prevValueForComparison) -and $wildcardEmptyWatch.ContainsKey($n)) {
                                $wildcardEmptyWatch[$n] = $true
                            }
                            $transformMap = $normalizedValueTransforms[$n]
                            $mapKey  = $null
                            $matched = $false
                            foreach ($candidateKey in $transformMap.Keys) {
                                $isMatch = if ($CaseSensitive) { $candidateKey -ceq $prevValueForComparison } else { $candidateKey -ieq $prevValueForComparison }
                                if ($isMatch) { $mapKey = $candidateKey; $matched = $true; break }
                            }
                            if (-not $matched -and -not [string]::IsNullOrEmpty($prevValueForComparison) -and $transformMap.ContainsKey('*')) {
                                $mapKey  = '*'
                                $matched = $true
                            }

                            if ($matched) {
                                $transformValue = $transformMap[$mapKey]
                                if ($transformAppliedCounts[$n]) { $transformAppliedCounts[$n][$mapKey]++ }
                                if ($transformValue.StartsWith('<<')) {
                                    $prefix = $transformValue.Substring(2)
                                    $prevValueForComparison = $prefix + $prevValueForComparison
                                } elseif ($transformValue.StartsWith('>>')) {
                                    $suffix = $transformValue.Substring(2)
                                    $prevValueForComparison = $prevValueForComparison + $suffix
                                } else {
                                    $prevValueForComparison = $transformValue
                                }
                            }
                        }

                        $valuesDiffer = if ($CaseSensitive) { $prevValueForComparison -cne $currValueForComparison } else { $prevValueForComparison -ine $currValueForComparison }

                        $idx = $colIndex[$n]
                        $row[$idx]     = $prevValue
                        $row[$idx + 1] = $currValue
                        $isMatched = -not $valuesDiffer
                        $row[$idx + 2] = if ($isMatched) { "True" } else { "False" }

                        # Track for summary row
                        $matchCounts[$n].totalCount++
                        if (-not $isMatched) {
                            $matchCounts[$n].mismatchCount++
                        }

                        if ($valuesDiffer)
                        {
                            #"Values do not match. Column: $($n)   Previous: $prevValue   Current: $currValue" | Write-Verbose
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
                    # Matched: drop it from the lookup so what remains is the delete set, and so the
                    # lookup shrinks as the stream progresses.
                    [void]$previousLookup.Remove($key)
                }
                else
                {
                    $row[1] = "Add"
                    $adds++
                    foreach ($n in $previousHeadersNorm)
                    {
                        $currIdx = $currHeaderIdx[$n]
                        $idx = $colIndex[$n]
                        $row[$idx]     = ""
                        $row[$idx + 1] = $currRow[$currIdx]
                        # match slot left $null -> empty unquoted field, matching prior output
                    }
                }
                $bodyWriter.WriteLine((ConvertTo-CsvLine -Fields $row -Delimiter $Delimiter))
                $emitted++
                # No periodic Flush() here: this file is discarded outright on a crash or a no-changes
                # run and never read while the script runs, so there is nothing a periodic flush would
                # protect - StreamWriter's own internal buffer already bounds how much sits unwritten
                # at any moment. Measured to make no runtime difference either way, so simplicity wins.
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

        # 4. Whatever is left in the lookup was never seen in Current, so it was deleted.
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Finalizing deletions..."
        $toDeleteTotal = $previousLookup.Count
        $iDel = 0
        foreach ($key in $previousLookup.Keys)
        {
            $prevRow = $previousLookup[$key]
            $row = [object[]]::new($rowWidth)
            $row[0] = $key
            $row[1] = "Delete"
            $deletes++
            foreach ($n in $previousHeadersNorm)
            {
                $prevIdx = $prevHeaderIdx[$n]
                $idx = $colIndex[$n]
                $row[$idx]     = $prevRow[$prevIdx]
                $row[$idx + 1] = ""
                # match slot left $null -> empty unquoted field, matching prior output
            }
            $bodyWriter.WriteLine((ConvertTo-CsvLine -Fields $row -Delimiter $Delimiter))
            $emitted++
            $iDel++
            if (($iDel % 1000) -eq 0 -or $iDel -eq $toDeleteTotal) {
                $pct = if ($toDeleteTotal -gt 0) { [int](($iDel/$toDeleteTotal)*100) } else { 100 }
                Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Finalizing deletions... ($iDel of $toDeleteTotal)" -PercentComplete $pct
            }
        }
        # Explicit Flush() before Dispose() - not because Dispose() skips it, but because a failure
        # here throws normally from this try block. The same failure inside Dispose(), called from
        # finally below, could mask whatever exception the try block was already unwinding from.
        $bodyWriter.Flush()
        $bodyWriter.Dispose()
        $bodyWriter = $null

        if (($adds + $updates + $deletes) -gt 0)
        {
            # Create summary row
            $summaryRow = [object[]]::new($rowWidth)
            $summaryRow[0] = "SUMMARY"
            $summaryRow[1] = "---"
            foreach ($n in $previousHeadersNorm) {
                $idx = $colIndex[$n]
                $summaryRow[$idx + 1] = ""
                # Format: "X of Y" to avoid Excel date auto-formatting (e.g., "1 of 5")
                $summaryRow[$idx + 2] = "$($matchCounts[$n].mismatchCount) of $($matchCounts[$n].totalCount) FALSE"

                # Build transform summary for "old" column if transforms exist for this column
                if ($transformAppliedCounts -and $transformAppliedCounts[$n]) {
                    $transformLines = @()
                    $maxLinesToShow = 20
                    $appliedRules = @()

                    # Sort rules: most-applied first, then alphabetically, with * always last. The
                    # alphabetical tie-break is required for determinism - Hashtable key order is
                    # unspecified and varies per process, and rules that never fired all tie at 0.
                    $counts = $transformAppliedCounts[$n]
                    $sortedKeys = @($counts.Keys | Where-Object { $_ -ne '*' } |
                        Sort-Object @{Expression={$counts[$_]}; Descending=$true}, @{Expression={$_}}) +
                        @('*' | Where-Object { $counts.ContainsKey($_) })

                    foreach ($ruleKey in $sortedKeys) {
                        $count = $transformAppliedCounts[$n][$ruleKey]
                        # Format count with "+" suffix for large numbers
                        # '->' rather than a U+2192 arrow: this string is written into the summary
                        # row of the CSV, so it has to survive every -EncodingName. Under 'ascii'
                        # the arrow degraded to '?'.
                        if ($ruleKey -eq '*') {
                            $ruleLine = "*->$($normalizedValueTransforms[$n][$ruleKey]) ($count applied)"
                        } else {
                            $ruleLine = "$ruleKey->$($normalizedValueTransforms[$n][$ruleKey]) ($count applied)"
                        }
                        $appliedRules += @{ key = $ruleKey; line = $ruleLine }
                    }

                    # Display first 20 rules
                    if ($appliedRules.Count -le $maxLinesToShow) {
                        $transformLines = @($appliedRules | ForEach-Object { $_.line })
                    } else {
                        $transformLines = @($appliedRules[0..($maxLinesToShow-1)] | ForEach-Object { $_.line })
                        $hiddenCount = $appliedRules.Count - $maxLinesToShow
                        $transformLines += "[...and $hiddenCount more transform rule(s)]"
                    }

                    $summaryRow[$idx] = $transformLines -join "`n"
                } else {
                    $summaryRow[$idx] = ""
                }
            }

            # No sort, no batch write: data rows already went to $pendingBody as they were produced
            # above. Write header + SUMMARY straight to the real pending output, then append the
            # spooled body onto the end of it. The append copies decoded characters through a fixed
            # buffer rather than ReadLine/WriteLine: a quoted field can legitimately contain an
            # embedded newline (e.g. a multi-line "notes" value), and ReadLine treats that embedded
            # character as a record boundary just as readily as a real one - re-terminating it with
            # WriteLine's own NewLine would silently rewrite a bare LF inside quoted content into
            # CRLF. A character-buffer copy never interprets the content, so it cannot do that. It
            # also isn't a byte-level copy (which would need to reconcile two different encodings and
            # BOMs) or a CSV re-parse (which would collapse the null-vs-empty-string distinction on
            # read-back), and it keeps memory bounded to one buffer's worth rather than the spool's
            # full size.
            $pendingOutput = [System.IO.Path]::Combine($OutputFolder, ("{0}.{1}.pending.tmp" -f $baseFileName, $fileTime))
            $writer = New-Object System.IO.StreamWriter($pendingOutput, $false, $csvEncoding)
            $writer.NewLine = "`r`n"
            $writer.WriteLine((ConvertTo-CsvLine -Fields $reportColumns -Delimiter $Delimiter))
            $writer.WriteLine((ConvertTo-CsvLine -Fields $summaryRow -Delimiter $Delimiter))
            $reader = New-Object System.IO.StreamReader($pendingBody, $spoolEncoding)
            try {
                $copyBuffer = [char[]]::new(65536)
                $charsRead = 0
                while (($charsRead = $reader.Read($copyBuffer, 0, $copyBuffer.Length)) -gt 0) {
                    $writer.Write($copyBuffer, 0, $charsRead)
                }
            } finally {
                $reader.Dispose()
            }
            # Explicit Flush() before Dispose() - not because Dispose() skips it, but because a failure
            # here throws normally from this try block. The same failure inside Dispose(), called from
            # finally below, could mask whatever exception the try block was already unwinding from.
            $writer.Flush()
            $writer.Dispose()
            $writer = $null
            Remove-Item -LiteralPath $pendingBody -Force -ErrorAction SilentlyContinue

            Move-Item -LiteralPath $pendingOutput -Destination $changesCSVFile -Force -ErrorAction Stop
            Write-Host "Changes CSV written to: $changesCSVFile"
        }
        else
        {
            Remove-Item -LiteralPath $pendingBody -Force -ErrorAction SilentlyContinue
            Write-Host "No changes detected; no CSV written"
        }
    }
    finally {
        if ($bodyWriter) { $bodyWriter.Dispose() }
        if ($writer) { $writer.Dispose() }
        # Always clear progress
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Completed
    }
    # One line per column where '*' met an empty value it deliberately did not transform. Sorted:
    # Hashtable key order is not stable across processes, and this is console output people diff.
    foreach ($watchedColumn in ($wildcardEmptyWatch.Keys | Sort-Object)) {
        if ($wildcardEmptyWatch[$watchedColumn]) {
            Write-Host "WARNING: Column '$watchedColumn' contains empty values; '*' does not match empty. Add a '' = ... rule to include them." -ForegroundColor Yellow
        }
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