<#
.SYNOPSIS
Compares two CSV files and writes a changes report (Adds, Updates, Deletes) with field-level match tracking.

.DESCRIPTION
In-memory comparison:
- Robust header parsing (quoted headers, embedded delimiters).
- Strict anchor presence; fails if the anchor column is missing.
- Detects and warns about duplicate anchor values (uses first occurrence only).
- Case-sensitive or insensitive comparisons per -CaseSensitive.
- Outputs a CSV with ChangeType and old/new/match values for all columns.
  - Match column shows True/False for Update/None rows.
  - Match column will be empty for Add/Delete rows (cannot be matched across files).
  - Summary row inserted as first record with per-column mismatch counts ("X of Y FALSE").
- Output sorted by anchor column.
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
Input/output encoding. One of: auto, utf8, utf8BOM, unicode, oem, default. Default: utf8BOM.

.PARAMETER CaseSensitive
Use case-sensitive comparisons when set.

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
showing the anchor value and row numbers, then processes only the first occurrence.
#>
[CmdletBinding()]
Param(
    [Parameter(Mandatory=$true)]
    [ValidateScript({Test-Path -Path $_ -PathType Leaf })]
    [String]$PreviousCSVFile,
    [Parameter(Mandatory=$true)]
    [ValidateScript({Test-Path -Path $_ -PathType Leaf })]
    [String]$CurrentCSVFile,
    [Parameter(Mandatory=$true)]
    [ValidateNotNullOrEmpty()]
    [String]$AnchorColumn,
    [Parameter(Mandatory=$true)]
    [ValidateScript({Test-Path -Path $_ -PathType Container })]
    [String]$OutputFolder,
    [ValidateSet('comma','tab','semicolon','pipe')]
    [string]$DelimiterName = 'comma',
    [ValidateSet('auto','utf8','utf8BOM','unicode','oem','default')]
    [string]$EncodingName = 'utf8BOM',
    [switch]$CaseSensitive,
    [hashtable]$ValueTransforms = @{},
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

    # Robust header parsing
    function Get-CsvHeaderFields {
        param([string]$Path, [string]$Delimiter)
        # Resolve to a full, literal path to avoid current-directory surprises
        try {
            $fullPath = (Resolve-Path -LiteralPath $Path -ErrorAction Stop).ProviderPath
        } catch {
            throw "File not found: $Path"
        }
        try {
            $parser = New-Object Microsoft.VisualBasic.FileIO.TextFieldParser($fullPath)
            $parser.TextFieldType = [Microsoft.VisualBasic.FileIO.FieldType]::Delimited
            $parser.SetDelimiters(@($Delimiter))
            $parser.HasFieldsEnclosedInQuotes = $true
            $parser.TrimWhiteSpace = $false
            try { $parser.ReadFields() } finally { $parser.Close() }
        } catch {
            throw "Cannot read CSV header from '$fullPath': $($_.Exception.Message)"
        }
    }

    # Encoding helpers
    function Resolve-ImportCsv {
        param(
            [Parameter(Mandatory)] [string]$LiteralPath,
            [Parameter(Mandatory)] [string]$Delimiter,
            [Parameter(Mandatory)] [string]$EncodingName
        )
        if ($EncodingName -eq 'auto') {
            Import-Csv -LiteralPath $LiteralPath -Delimiter $Delimiter -ErrorAction Stop
        } else {
            $enc = if ($EncodingName -eq 'utf8BOM') { 'utf8' } else { $EncodingName }
            Get-Content -LiteralPath $LiteralPath -Encoding $enc -ErrorAction Stop |
                ConvertFrom-Csv -Delimiter $Delimiter -ErrorAction Stop
        }
    }
    function Get-ExportEncodingName {
        param([Parameter(Mandatory)][string]$EncodingName)
        if ($PSVersionTable.PSVersion.Major -ge 6) {
            if ($EncodingName -eq 'auto') { 'utf8' } else { $EncodingName }
        } else {
            switch ($EncodingName) {
                'utf8BOM' { 'utf8' }
                'auto'    { 'utf8' }
                default   { $EncodingName }
            }
        }
    }

    $fileTime = (Get-Date).ToString("yyyy-MM-dd_HHmmssfff")
    $baseFileName = [System.IO.Path]::GetFileNameWithoutExtension((Resolve-Path -LiteralPath $CurrentCSVFile).ProviderPath)
    $changesCSVFile = [System.IO.Path]::Combine($OutputFolder, ("Changes_{0}_GeneratedOn_{1}.csv" -f $baseFileName, $fileTime))
    $exportEncoding = Get-ExportEncodingName -EncodingName $EncodingName

    # 1. Ensure headers from both CSV files match
    try {
        $previousHeadersRaw = Get-CsvHeaderFields -Path $PreviousCSVFile -Delimiter $Delimiter
    } catch {
        throw "Cannot process Previous CSV file $_"
    }
    try {
        $currentHeadersRaw = Get-CsvHeaderFields -Path $CurrentCSVFile  -Delimiter $Delimiter
    } catch {
        throw "Cannot process Current CSV file $_"
    }

    # Validate for empty/blank header names
    $emptyPrev = for ($i=0; $i -lt $previousHeadersRaw.Count; $i++) {
        $h = $previousHeadersRaw[$i]
        if ([string]::IsNullOrWhiteSpace($h)) { "Column $($i+1)" }
    }
    if ($emptyPrev) { throw "Empty/blank column name(s) in Previous CSV header at: $($emptyPrev -join ', ')" }

    $emptyCurr = for ($i=0; $i -lt $currentHeadersRaw.Count; $i++) {
        $h = $currentHeadersRaw[$i]
        if ([string]::IsNullOrWhiteSpace($h)) { "Column $($i+1)" }
    }
    if ($emptyCurr) { throw "Empty/blank column name(s) in Current CSV header at: $($emptyCurr -join ', ')" }

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

    # Validate IgnoreColumns exist in headers and filter them out
    if ($normalizedIgnoreColumns.Count -gt 0) {
        foreach ($ignoreCol in $normalizedIgnoreColumns) {
            if ($ignoreCol -notin $previousHeadersNorm) {
                throw "Column '$ignoreCol' in -IgnoreColumns does not exist in CSV headers. Available columns: $($previousHeadersRaw -join ', ')"
            }
            if ($ignoreCol -eq $anchorNorm) {
                throw "Anchor column '$anchorNorm' cannot be included in -IgnoreColumns parameter."
            }
        }
        $previousHeadersNorm = @($previousHeadersNorm | Where-Object { $_ -notin $normalizedIgnoreColumns })
        $currentHeadersNorm = @($currentHeadersNorm | Where-Object { $_ -notin $normalizedIgnoreColumns })
        # Validate that at least one column remains after filtering
        if ($previousHeadersNorm.Count -eq 0 or $currentHeadersNorm.Count -eq 0) {
            throw "No columns remain after applying -IgnoreColumns filter. Anchor column cannot be the only remaining column."
        }
    }

    if (-not ($previousHeadersNorm -join ',' -eq $currentHeadersNorm -join ',')) {
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
            # Validate transform map values
            foreach ($kvp in $transformMap.GetEnumerator()) {
                if ([string]::IsNullOrWhiteSpace($kvp.Value)) {
                    throw "Transform value for key '$($kvp.Key)' in column '$transformColumn' cannot be null or empty"
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

    # Map normalized header -> raw header per file
    $prevHeaderMap = @{}
    foreach ($h in $previousHeadersRaw) {
        $n = $h.Trim().ToLowerInvariant()
        $prevHeaderMap[$n] = $h
    }
    $currHeaderMap = @{}
    foreach ($h in $currentHeadersRaw) {
        $n = $h.Trim().ToLowerInvariant()
        $currHeaderMap[$n] = $h
    }
    # Resolve anchor raw names per file (handles case/whitespace differences)
    $prevAnchorRaw = $prevHeaderMap[$anchorNorm]
    $currAnchorRaw = $currHeaderMap[$anchorNorm]
    if (-not $prevAnchorRaw) { throw "Anchor column '$AnchorColumn' not found in Previous CSV headers: $($previousHeadersRaw -join ', ')" }
    if (-not $currAnchorRaw) { throw "Anchor column '$AnchorColumn' not found in Current CSV headers: $($currentHeadersRaw -join ', ')" }
    Write-Host "Note: Output columns use trimmed and lowercase-normalized header names for consistency."

    # 2. Import Records
    $Previous = @(Resolve-ImportCsv -LiteralPath $PreviousCSVFile -Delimiter $Delimiter -EncodingName $EncodingName)
    $Current  = @(Resolve-ImportCsv -LiteralPath $CurrentCSVFile  -Delimiter $Delimiter -EncodingName $EncodingName)

    $anchorSetPrev = New-Object 'System.Collections.Generic.HashSet[string]' ($anchorComparer)
    $duplicateAnchorsPrev = @{}
    $rowNum = 0
    foreach ($row in $Previous) {
        $rowNum++
        $anchor = $row.$prevAnchorRaw
        # 1. Anchor Value Validation
        if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in Previous record at row $($rowNum): $($row)." }

        # 2. Duplicate Anchor Value Check
        if (-not $anchorSetPrev.Add($anchor)) {
            if (-not $duplicateAnchorsPrev.ContainsKey($anchor)) { $duplicateAnchorsPrev[$anchor] = @() }
            $duplicateAnchorsPrev[$anchor] += $rowNum
        } else {
            $duplicateAnchorsPrev[$anchor] = @($rowNum)
        }

        # 3. Consistent Row Length Check
        $actualColumns = @($row.PSObject.Properties).Count
        if ($actualColumns -ne $previousHeadersRaw.Count) { throw "Row $rowNum with anchor '$anchor' in Previous file has $actualColumns columns, expected $($previousHeadersRaw.Count)." }

        # 4. Blank or Malformed Row Check
        $nonEmpty = $row.PSObject.Properties | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) }
        if ($nonEmpty.Count -eq 0) { throw "Blank or malformed row found in Previous file at row $rowNum with anchor '$anchor'." }
    }
    $anchorSetCurr = New-Object 'System.Collections.Generic.HashSet[string]' ($anchorComparer)
    $duplicateAnchorsCurr = @{}
    $rowNum = 0
    foreach ($row in $Current) {
        $rowNum++
        $anchor = $row.$currAnchorRaw
        # 1. Anchor Value Validation
        if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in Current record at row $($rowNum): $($row)." }

        # 2. Duplicate Anchor Value Check
        if (-not $anchorSetCurr.Add($anchor)) {
            if (-not $duplicateAnchorsCurr.ContainsKey($anchor)) { $duplicateAnchorsCurr[$anchor] = @() }
            $duplicateAnchorsCurr[$anchor] += $rowNum
        } else {
            $duplicateAnchorsCurr[$anchor] = @($rowNum)
        }

        # 3. Consistent Row Length Check
        $actualColumns = @($row.PSObject.Properties).Count
        if ($actualColumns -ne $currentHeadersRaw.Count) { throw "Row $rowNum with anchor '$anchor' in Current file has $actualColumns columns, expected $($currentHeadersRaw.Count)." }

        # 4. Blank or Malformed Row Check
        $nonEmpty = $row.PSObject.Properties | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) }
        if ($nonEmpty.Count -eq 0) { throw "Blank or malformed row found in Current file at row $rowNum with anchor '$anchor'." }
    }

    # Warn about duplicates but continue processing
    foreach ($anchor in $duplicateAnchorsPrev.Keys | Where-Object { $duplicateAnchorsPrev[$_].Count -gt 1 }) {
        $rows = $duplicateAnchorsPrev[$anchor] -join ', '
        Write-Host "WARNING: Duplicate anchor '$anchor' found in Previous file. Using first record. Duplicate rows: $rows" -ForegroundColor Yellow
    }
    foreach ($anchor in $duplicateAnchorsCurr.Keys | Where-Object { $duplicateAnchorsCurr[$_].Count -gt 1 }) {
        $rows = $duplicateAnchorsCurr[$anchor] -join ', '
        Write-Host "WARNING: Duplicate anchor '$anchor' found in Current file. Using first record. Duplicate rows: $rows" -ForegroundColor Yellow
    }

    # 5. Count Check (after validation)
    #"Previous count: $($Previous.Count)" | Write-Verbose
    if ($Previous.Count -eq 0) { throw "No records found in Previous CSV file." }
    #"Current count: $($Current.Count)" | Write-Verbose
    if ($Current.Count -eq 0)  { throw "No records found in Current CSV file." }
    #"Column count: $($previousHeadersRaw.Count)" | Write-Verbose

    $htPrevious = [System.Collections.Generic.Dictionary[string,object]]::new([int]$Previous.Count, $anchorComparer)
    for ($i= 0; $i -lt $Previous.Count; $i++)
    {
        if (-not $htPrevious.ContainsKey($Previous[$i].$prevAnchorRaw)) {
            $htPrevious.Add($Previous[$i].$prevAnchorRaw, $Previous[$i])
        }
    }

    $htCurrent = [System.Collections.Generic.Dictionary[string,object]]::new([int]$Current.Count, $anchorComparer)
    for ($i= 0; $i -lt $Current.Count; $i++)
    {
        if (-not $htCurrent.ContainsKey($Current[$i].$currAnchorRaw)) {
            $htCurrent.Add($Current[$i].$currAnchorRaw, $Current[$i])
        }
    }

    $changes = [System.Collections.Generic.List[PSCustomObject]]::new()
    # Report columns: AnchorColumn, ChangeType, then old/new/match triplets for each column
    $reportColumns = [System.Collections.Generic.List[string]]::new(2 + (3 * $previousHeadersNorm.Count))
    $reportColumns.Add($AnchorColumn)
    $reportColumns.Add("ChangeType")
    foreach ($prop in $previousHeadersNorm) {
        $reportColumns.Add("old $($prop)")
        $reportColumns.Add("new $($prop)")
        $reportColumns.Add("match $($prop)")
    }

    # Summary counters
    $adds = 0; $updates = 0; $deletes = 0
    # Track match/mismatch counts per column for summary row
    $matchCounts = @{}
    # Track applied counts per transform rule per column
    $transformAppliedCounts = @{}
    foreach ($n in $previousHeadersNorm) {
        $matchCounts[$n] = @{ mismatchCount = 0; totalCount = 0 }
        # Initialize transform tracking for columns with transforms
        if ($normalizedValueTransforms -and $normalizedValueTransforms.ContainsKey($n)) {
            $transformAppliedCounts[$n] = @{}
            foreach ($key in $normalizedValueTransforms[$n].Keys) {
                $transformAppliedCounts[$n][$key] = 0
            }
        }
    }

    # Progress
    $progressId = 1
    $totalPrev = $htPrevious.Count
    $iPrev = 0
    $totalCurr = $htCurrent.Count
    $iCurr = 0
    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Phase 1/2: Matching Previous vs Current" -PercentComplete 0

    try {
        foreach ($key in $htPrevious.Keys)
        {
            $iPrev++
            $changeObject = @{}
            $changeObject[$AnchorColumn] = $key
            $changeObject["ChangeType"] = "None"
            #"Previous user: $($key)" | Write-Verbose
            if (-not $htCurrent.ContainsKey($key))
            {
                #"User removed in Current file. $($key)" | Write-Verbose
                $changeObject["ChangeType"] = "Delete"
                $deletes++
                foreach ($n in $previousHeadersNorm) {
                    $prevRaw = $prevHeaderMap[$n]
                    $changeObject["old $n"] = $htPrevious[$key].$prevRaw
                    $changeObject["new $n"] = ""
                }
                $changes.Add([PSCustomObject]$changeObject)
                if (($iPrev % 1000) -eq 0 -or $iPrev -eq $totalPrev) {
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Phase 1/2: Matching Previous vs Current ($iPrev of $totalPrev)" -PercentComplete ([int](($iPrev/$totalPrev)*100))
                }
                continue
            }

            $isUpdate = $false
            #"User exists in both files. $($key)" | Write-Verbose
            foreach ($n in $previousHeadersNorm)
            {
                #"Comparing column: $($n)" | Write-Verbose
                $prevRaw = $prevHeaderMap[$n]
                $currRaw = $currHeaderMap[$n]

                $prevValue = $htPrevious[$key].$prevRaw
                $currValue = $htCurrent[$key].$currRaw

                # Apply value transformation for comparison (if configured)
                $prevValueForComparison = $prevValue
                if ($normalizedValueTransforms -and $normalizedValueTransforms.ContainsKey($n) -and -not [string]::IsNullOrWhiteSpace($prevValue)) {
                    $transformMap = $normalizedValueTransforms[$n]
                    $mapKey = if ($CaseSensitive) {
                        $transformMap.Keys | Where-Object { $_ -ceq $prevValue } | Select-Object -First 1
                    } else {
                        $transformMap.Keys | Where-Object { $_ -ieq $prevValue } | Select-Object -First 1
                    }
                    if ($mapKey) {
                        $transformValue = $transformMap[$mapKey]
                    } elseif ($transformMap.ContainsKey('*')) {
                        # Fall back to wildcard if no exact match
                        $mapKey = '*'
                        $transformValue = $transformMap['*']
                    }

                    if ($mapKey) {
                        # Track that this rule was applied
                        if ($transformAppliedCounts[$n]) {
                            $transformAppliedCounts[$n][$mapKey]++
                        }
                        # Handle prefix (<<) and suffix (>>) modifiers for append/prepend operations
                        if ($transformValue.StartsWith('<<')) {
                            # Prepend mode: <<prefix
                            $prefix = $transformValue.Substring(2)
                            $prevValueForComparison = $prefix + $prevValue
                        } elseif ($transformValue.StartsWith('>>')) {
                            # Append mode: >>suffix
                            $suffix = $transformValue.Substring(2)
                            $prevValueForComparison = $prevValue + $suffix
                        } else {
                            # Direct replacement mode
                            $prevValueForComparison = $transformValue
                        }
                    }
                }

                $valuesDiffer = if ($CaseSensitive) { $prevValueForComparison -cne $currValue } else { $prevValueForComparison -ine $currValue }

                $changeObject["old $n"] = $prevValue
                $changeObject["new $n"] = $currValue
                $isMatched = -not $valuesDiffer
                $changeObject["match $n"] = $isMatched

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
                $changeObject["ChangeType"] = "Update"
                $updates++
            }
            else
            {
                $changeObject["ChangeType"] = "None"
            }
            $changes.Add([PSCustomObject]$changeObject)
            if (($iPrev % 1000) -eq 0 -or $iPrev -eq $totalPrev) {
                Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Phase 1/2: Matching Previous vs Current ($iPrev of $totalPrev)" -PercentComplete ([int](($iPrev/$totalPrev)*100))
            }
        }

        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Phase 2/2: Scanning Current for Additions" -PercentComplete 50
        foreach ($key in $htCurrent.Keys)
        {
            $iCurr++
            if (-not $htPrevious.ContainsKey($key))
            {
                #"User add to Current file. $($key)" | Write-Verbose
                $changeObject = @{}
                $changeObject[$AnchorColumn] = $key
                $changeObject["ChangeType"] = "Add"
                $adds++

                foreach ($n in $previousHeadersNorm)
                {
                    $currRaw = $currHeaderMap[$n]
                    $changeObject["old $n"] = ""
                    $changeObject["new $n"] = $htCurrent[$key].$currRaw
                }
                $changes.Add([PSCustomObject]$changeObject)
            }
            if (($iCurr % 1000) -eq 0 -or $iCurr -eq $totalCurr) {
                $pct = [Math]::Min(100, 50 + [int](($iCurr/$totalCurr)*50))
                Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Phase 2/2: Scanning Current for Additions ($iCurr of $totalCurr)" -PercentComplete $pct
            }
        }
        if (($adds + $updates + $deletes) -gt 0)
        {
            # Sort changes by anchor
            $sortedChanges = $changes | Sort-Object { $_.$AnchorColumn } -CaseSensitive:$CaseSensitive

            # Create summary row
            $summaryRow = @{}
            $summaryRow[$AnchorColumn] = "SUMMARY"
            $summaryRow["ChangeType"] = "---"
            foreach ($n in $previousHeadersNorm) {
                $summaryRow["new $n"] = ""
                # Format: "X of Y" to avoid Excel date auto-formatting (e.g., "1 of 5")
                $summaryRow["match $n"] = "$($matchCounts[$n].mismatchCount) of $($matchCounts[$n].totalCount) FALSE"

                # Build transform summary for "old" column if transforms exist for this column
                if ($transformAppliedCounts -and $transformAppliedCounts[$n]) {
                    $transformLines = @()
                    $maxLinesToShow = 20
                    $appliedRules = @()

                    # Sort rules: exact keys first (excluding *), then * at end
                    $sortedKeys = @($transformAppliedCounts[$n].Keys | Where-Object { $_ -ne '*' }) + @('*' | Where-Object { $transformAppliedCounts[$n].ContainsKey($_) })

                    foreach ($ruleKey in $sortedKeys) {
                        $count = $transformAppliedCounts[$n][$ruleKey]
                        # Format count with "+" suffix for large numbers
                        if ($ruleKey -eq '*') {
                            $ruleLine = "*→$($normalizedValueTransforms[$n][$ruleKey]) ($count applied)"
                        } else {
                            $ruleLine = "$ruleKey→$($normalizedValueTransforms[$n][$ruleKey]) ($count applied)"
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

                    $summaryRow["old $n"] = $transformLines -join "`n"
                } else {
                    $summaryRow["old $n"] = ""
                }
            }

            # Insert summary as first row
            $changesWithSummary = [System.Collections.Generic.List[PSCustomObject]]::new($sortedChanges.Count + 1)
            $changesWithSummary.Add([PSCustomObject]$summaryRow)
            foreach ($change in $sortedChanges) {
                $changesWithSummary.Add($change)
            }

            try {
                $changesWithSummary | Select-Object -Property $reportColumns | Export-Csv -LiteralPath $changesCSVFile -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -ErrorAction Stop
                Write-Host "Changes CSV written to: $changesCSVFile"
            } catch {
                throw "Cannot write changes CSV to '$changesCSVFile': $($_.Exception.Message)"
            }
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
    # Summary output
    $nones = ($changes | Where-Object { $_.ChangeType -eq "None" }).Count
    Write-Host ("Summary: Adds={0}, Updates={1}, Deletes={2}, Unchanged={3}" -f $adds, $updates, $deletes, $nones)

    $elapsed = (Get-Date) - $scriptStartTime
    $elapsedStr = "{0}m {1}s" -f [int]$elapsed.TotalMinutes, $elapsed.Seconds
    Write-Host "Elapsed: $elapsedStr"
}
catch {
    Write-Error $_
    exit 1
}