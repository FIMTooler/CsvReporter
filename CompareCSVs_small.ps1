﻿<# 
.SYNOPSIS
Compares two CSV files and writes a changes report (Adds, Updates, Deletes).

.DESCRIPTION
In-memory comparison:
- Robust header parsing (quoted headers, embedded delimiters).
- Strict anchor presence; fails if the anchor column is missing.
- Case-sensitive or insensitive comparisons per -CaseSensitive.
- Outputs a CSV with ChangeType and old/new values for changed columns.
- Prints a one-line summary with counts.

.PARAMETER PreviousCSVFile
Path to the “Previous” CSV file.

.PARAMETER CurrentCSVFile
Path to the “Current” CSV file.

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

.INPUTS
None. You cannot pipe objects to this script.

.OUTPUTS
None. Writes a changes CSV to -OutputFolder and summary messages to the console.

.EXAMPLE
.\CompareCSVs_advanced.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -DelimiterName comma -EncodingName utf8BOM -CaseSensitive

.NOTES
Requires Microsoft.VisualBasic for TextFieldParser header parsing.
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
    [switch]$CaseSensitive
)
try {
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
        $parser = New-Object Microsoft.VisualBasic.FileIO.TextFieldParser($fullPath)
        $parser.TextFieldType = [Microsoft.VisualBasic.FileIO.FieldType]::Delimited
        $parser.SetDelimiters(@($Delimiter))
        $parser.HasFieldsEnclosedInQuotes = $true
        $parser.TrimWhiteSpace = $false
        try { $parser.ReadFields() } finally { $parser.Close() }
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
    $previousHeadersRaw  = Get-CsvHeaderFields -Path $PreviousCSVFile -Delimiter $Delimiter
    $currentHeadersRaw   = Get-CsvHeaderFields -Path $CurrentCSVFile  -Delimiter $Delimiter

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
    
    # Normalized header sets (sorted) for cross-file comparison
    $previousHeadersNorm = $prevNormAll | Sort-Object -ErrorAction Stop
    $currentHeadersNorm  = $currNormAll | Sort-Object -ErrorAction Stop

    if (-not ($previousHeadersNorm -join ',' -eq $currentHeadersNorm -join ',')) {
        throw "Column mismatch detected! Previous CSV columns: $($previousHeadersRaw -join ', ')`nCurrent CSV columns: $($currentHeadersRaw -join ', ')"
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
    $anchorNorm    = $AnchorColumn.Trim().ToLowerInvariant()
    $prevAnchorRaw = $prevHeaderMap[$anchorNorm]
    $currAnchorRaw = $currHeaderMap[$anchorNorm]
    if (-not $prevAnchorRaw) { throw "Anchor column '$AnchorColumn' not found in Previous CSV headers: $($previousHeadersRaw -join ', ')" }
    if (-not $currAnchorRaw) { throw "Anchor column '$AnchorColumn' not found in Current CSV headers: $($currentHeadersRaw -join ', ')" }

    # 2. Import Records
    $Previous = @(Resolve-ImportCsv -LiteralPath $PreviousCSVFile -Delimiter $Delimiter -EncodingName $EncodingName)
    $Current  = @(Resolve-ImportCsv -LiteralPath $CurrentCSVFile  -Delimiter $Delimiter -EncodingName $EncodingName)


    $anchorSetPrev = New-Object 'System.Collections.Generic.HashSet[string]' ($anchorComparer)
    $rowNum = 0
    foreach ($row in $Previous) {
        $rowNum++
        $anchor = $row.$prevAnchorRaw
        # 1. Anchor Value Validation
        if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in Previous record at row $($rowNum): $($row)." }

        # 2. Duplicate Anchor Value Check
        if (-not $anchorSetPrev.Add($anchor)) { throw "Duplicate anchor value '$anchor' found in Previous file at row $rowNum." }

        # 3. Consistent Row Length Check
        $actualColumns = @($row.PSObject.Properties).Count
        if ($actualColumns -ne $previousHeadersRaw.Count) { throw "Row $rowNum with anchor '$anchor' in Previous file has $actualColumns columns, expected $($previousHeadersRaw.Count)." }

        # 4. Blank or Malformed Row Check
        $nonEmpty = $row.PSObject.Properties | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) }
        if ($nonEmpty.Count -eq 0) { throw "Blank or malformed row found in Previous file  at row $rowNumwith anchor '$anchor'." }
    }
    $anchorSetCurr = New-Object 'System.Collections.Generic.HashSet[string]' ($anchorComparer)
    $rowNum = 0
    foreach ($row in $Current) {
        $rowNum++
        $anchor = $row.$currAnchorRaw
        # 1. Anchor Value Validation
        if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in Current record at row $($rowNum): $($row)." }

        # 2. Duplicate Anchor Value Check
        if (-not $anchorSetCurr.Add($anchor)) { throw "Duplicate anchor value '$anchor' found in Current file at row $rowNum." }

        # 3. Consistent Row Length Check
        $actualColumns = @($row.PSObject.Properties).Count
        if ($actualColumns -ne $currentHeadersRaw.Count) { throw "Row $rowNum with anchor '$anchor' in Current file has $actualColumns columns, expected $($currentHeadersRaw.Count)." }

        # 4. Blank or Malformed Row Check
        $nonEmpty = $row.PSObject.Properties | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) }
        if ($nonEmpty.Count -eq 0) { throw "Blank or malformed row found in Current file at row $rowNum with anchor '$anchor'." }
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
        $htPrevious.Add($Previous[$i].$prevAnchorRaw, $Previous[$i])
    }

    $htCurrent = [System.Collections.Generic.Dictionary[string,object]]::new([int]$Current.Count, $anchorComparer)
    for ($i= 0; $i -lt $Current.Count; $i++)
    {
        $htCurrent[$Current[$i].$currAnchorRaw] = $Current[$i]
    }

    $changes = [System.Collections.Generic.List[PSCustomObject]]::new()
    $reportColumns = [System.Collections.Generic.List[string]]::new(2 + (2 * $previousHeadersNorm.Count))
    $reportColumns.Add($AnchorColumn)
    $reportColumns.Add("ChangeType")
    foreach ($prop in $previousHeadersNorm) {
        $reportColumns.Add("old $($prop)")
        $reportColumns.Add("new $($prop)")
    }

    # Summary counters
    $adds = 0; $updates = 0; $deletes = 0

    # Progress
    $progressId = 1
    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Phase 1/2: Matching Previous vs Current" -PercentComplete 0

    try {
        foreach ($key in $htPrevious.Keys)
        {
            if (-not $totalPrev) { $totalPrev = $htPrevious.Count; $iPrev = 0 }
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

                $valuesDiffer = if ($CaseSensitive) { $prevValue -cne $currValue } else { $prevValue -ine $currValue }
                if ($valuesDiffer)
                {
                    #"Values do not match. Column: $($n)   Previous: $prevValue   Current: $currValue" | Write-Verbose
                    $changeObject["old $n"] = $prevValue
                    $changeObject["new $n"] = $currValue
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
            if (-not $totalCurr) { $totalCurr = $htCurrent.Count; $iCurr = 0 }
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
        if ($changes.Count -gt 0)
        {
            $changes | Select-Object -Property $reportColumns |
                Export-Csv -LiteralPath $changesCSVFile -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -ErrorAction Stop
        }
        if (($adds + $updates + $deletes) -eq 0)
        {
            Write-Host "No changes detected"
        }
    }
    finally {
        # Always clear progress
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Completed
    }
    # Summary output
    Write-Host ("Summary: Adds={0}, Updates={1}, Deletes={2}" -f $adds, $updates, $deletes)
}
catch {
    Write-Error $_
    exit 1    
}