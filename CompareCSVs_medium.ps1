<# 
.SYNOPSIS
Streams and compares two CSV files, writing a batched changes report.

.DESCRIPTION
Streaming comparison suited for mid/large CSVs:
- Robust header parsing and strict anchor validation.
- Builds a lookup of “Previous”, then streams “Current”.
- Batches output to reduce memory (see -BatchSize).
- Case-sensitive or insensitive comparisons per -CaseSensitive.
- Prints a one-line summary with counts and writes a CSV of changes.

.PARAMETER PreviousCSVFile
Path to the “Previous” CSV file.

.PARAMETER CurrentCSVFile
Path to the “Current” CSV file.

.PARAMETER AnchorColumn
Header name of the key/anchor column used to join rows.

.PARAMETER OutputFolder
Folder where the changes CSV will be written.

.PARAMETER BatchSize
Number of change records to buffer before appending to output.

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
    [ValidateRange(1, 100000)]
    [int]$BatchSize = 1000,
    [ValidateSet('comma','tab','semicolon','pipe')]
    [string]$DelimiterName = 'comma',
    [ValidateSet('auto','utf8','utf8BOM','unicode','oem','default')]
    [string]$EncodingName = 'utf8BOM',
    [switch]$CaseSensitive
)
try {
    # Resolve delimiter from name
    switch ($DelimiterName) {
        'comma'     { $Delimiter = ',' }
        'tab'       { $Delimiter = "`t" }
        'semicolon' { $Delimiter = ';' }
        'pipe'      { $Delimiter = '|' }
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

    # Prepare output file paths
    $fileTime = (Get-Date).ToString("yyyy-MM-dd_HHmmssfff")
    $baseFileName = [System.IO.Path]::GetFileNameWithoutExtension((Resolve-Path -LiteralPath $CurrentCSVFile).ProviderPath)
    $changesCSVFile = [System.IO.Path]::Combine($OutputFolder, ("Changes_{0}_GeneratedOn_{1}.csv" -f $baseFileName, $fileTime))
    $exportEncoding = Get-ExportEncodingName -EncodingName $EncodingName

    # Ensure headers from both CSV files match
    $previousHeadersRaw = Get-CsvHeaderFields -Path $PreviousCSVFile -Delimiter $Delimiter
    $currentHeadersRaw  = Get-CsvHeaderFields -Path $CurrentCSVFile  -Delimiter $Delimiter

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

    if (-not ($previousHeadersNorm -join ',' -eq $currentHeadersNorm -join ','))
    {
        throw "Column mismatch detected! Previous CSV columns: $($previousHeadersRaw -join ', ')`nCurrent CSV columns: $($currentHeadersRaw -join ', ')"
    }

    # Map: normalized header -> raw header (per file)
    $prevHeaderMap = @{}
    foreach ($h in $previousHeadersRaw)
    {
        $n = $h.Trim().ToLowerInvariant()
        $prevHeaderMap[$n] = $h
    }
    $currHeaderMap = @{}
    foreach ($h in $currentHeadersRaw)
    {
        $n = $h.Trim().ToLowerInvariant()
        $currHeaderMap[$n] = $h
    }
    # Resolve anchor raw names per file
    $anchorNorm   = $AnchorColumn.Trim().ToLowerInvariant()
    $prevAnchorRaw = $prevHeaderMap[$anchorNorm]
    $currAnchorRaw = $currHeaderMap[$anchorNorm]
    if (-not $prevAnchorRaw) { throw "Anchor column '$AnchorColumn' not found in Previous CSV headers: $($previousHeadersRaw -join ', ')" }
    if (-not $currAnchorRaw) { throw "Anchor column '$AnchorColumn' not found in Current CSV headers: $($currentHeadersRaw -join ', ')" }

    # Begin processing files
    $anchorSetPrev = New-Object 'System.Collections.Generic.HashSet[string]' ($anchorComparer)
    $previousLookup = [System.Collections.Generic.Dictionary[string,object]]::new($anchorComparer)
    $progressId = 1

    try {
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Loading Previous..." 
        $prevRowIndex = 0
        Resolve-ImportCsv -LiteralPath $PreviousCSVFile -Delimiter $Delimiter -EncodingName $EncodingName | ForEach-Object {
            $prevRowIndex++
            $anchor = $_.$prevAnchorRaw
            # 1. Anchor Value Validation
            if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in Previous record (row $prevRowIndex): $($_)" }

            # 2. Duplicate Anchor Value Check
            if (-not $anchorSetPrev.Add($anchor)) { throw "Duplicate anchor value '$anchor' found in Previous file (row $prevRowIndex)." }

            # 3. Consistent Row Length Check
            $actualColumns = @($_.PSObject.Properties).Count
            if ($actualColumns -ne $previousHeadersRaw.Count) { throw "Row with anchor '$anchor' in Previous file has $actualColumns columns, expected $($previousHeadersRaw.Count)." }

            # 4. Blank or Malformed Row Check
            $nonEmpty = $_.PSObject.Properties | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) }
            if ($nonEmpty.Count -eq 0) { throw "Blank or malformed row found in Previous file with anchor '$anchor'." }
            $previousLookup[$anchor] = $_

            if (($prevRowIndex % 1000) -eq 0) {
                Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Loading Previous... ($prevRowIndex rows)"
            }
        }
        #"Previous count: $($previousLookup.Count)" | Write-Verbose
        if ($previousLookup.Count -eq 0) { throw "No records found in Previous CSV file." }

        # Get column information
        #"Column count: $($previousHeadersRaw.Count)" | Write-Verbose

        $reportColumns = [System.Collections.Generic.List[string]]::new(2 + (2 * $previousHeadersNorm.Count))
        $reportColumns.Add($AnchorColumn)
        $reportColumns.Add("ChangeType")
        foreach ($prop in $previousHeadersNorm)
        {
            $reportColumns.Add("old $($prop)")
            $reportColumns.Add("new $($prop)")
        }
        # Summary counters
        $adds = 0; $updates = 0; $deletes = 0

        $anchorSetCurr = New-Object 'System.Collections.Generic.HashSet[string]' ($anchorComparer)
        $changeBuffer = [System.Collections.Generic.List[object]]::new($BatchSize)
        $currentRowCount = 0
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Streaming Current..."
        Resolve-ImportCsv -LiteralPath $CurrentCSVFile -Delimiter $Delimiter -EncodingName $EncodingName | ForEach-Object {
            $currentRowCount++
            # 1. Anchor Value Validation
            $anchor = $_.$currAnchorRaw
            if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in Current record (row $currentRowCount): $($_)" }

            # 2. Duplicate Anchor Value Check
            if (-not $anchorSetCurr.Add($anchor)) { throw "Duplicate anchor value '$anchor' found in Current file (row $currentRowCount)." }
            # 3. Consistent Row Length Check
            $actualColumns = @($_.PSObject.Properties).Count
            if ($actualColumns -ne $currentHeadersRaw.Count) { throw "Row with anchor '$anchor' in Current file has $actualColumns columns, expected $($currentHeadersRaw.Count)." }

            # 4. Blank or Malformed Row Check
            $nonEmpty = $_.PSObject.Properties | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) }
            if ($nonEmpty.Count -eq 0) { throw "Blank or malformed row found in Current file with anchor '$anchor'." }

            if (($currentRowCount % 1000) -eq 0) {
                Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Streaming Current... ($currentRowCount rows)"
            }

            $key = $anchor
            #"Current user: $($key)" | Write-Verbose
            $changeObject = @{}
            $changeObject[$AnchorColumn] = $key
            $changeObject["ChangeType"] = "None"
            if ($previousLookup.ContainsKey($key))
            {
                # Compare fields and output differences
                #"User exists in both files. $($key)" | Write-Verbose
                foreach ($n in $previousHeadersNorm)
                {
                    #"Comparing column: $($n)" | Write-Verbose
                    $prevRaw = $prevHeaderMap[$n]
                    $currRaw = $currHeaderMap[$n]

                    $prevValue = $previousLookup[$key].$prevRaw
                    $currValue = $_.$currRaw
                    $valuesDiffer = if ($CaseSensitive) { $prevValue -cne $currValue } else { $prevValue -ine $currValue }
                    if ($valuesDiffer)
                    {
                        #"Values do not match. Column: $($n)   Previous: $($prevValue)  Current: $($currValue)" | Write-Verbose
                        $changeObject["old $n"] = $prevValue
                        $changeObject["new $n"] = $currValue
                        $changeObject["ChangeType"] = "Update"
                    }
                }
                if ($changeObject["ChangeType"] -eq "Update") { $updates++ }
                [void]$previousLookup.Remove($key) # Mark as matched
            }
            else
            {
                # New record (addition)
                #"User add to Current file. $($key)" | Write-Verbose
                $changeObject["ChangeType"] = "Add"
                $adds++

                foreach ($n in $previousHeadersNorm)
                {
                    $currRaw = $currHeaderMap[$n]
                    #"Values of new record. Column: $($n)   Current: $($currRaw)" | Write-Verbose
                    $changeObject["old $n"] = ""
                    $changeObject["new $n"] = $_.$currRaw
                }
            }
            $changeBuffer.Add([PSCustomObject]$changeObject)
            if ($changeBuffer.Count -ge $BatchSize) {
                $changeBuffer | Select-Object -Property $reportColumns |
                Export-Csv -LiteralPath $changesCSVFile -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -Append -ErrorAction Stop
                $changeBuffer.Clear()
            }
        }
        if ($currentRowCount -eq 0) { throw "No records found in Current CSV file." }

        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Finalizing deletions..."
        $toDeleteTotal = $previousLookup.Count
        $iDel = 0
        foreach($key in $previousLookup.Keys)
        {
            #"User removed in Current file. $($key)" | Write-Verbose
            $changeObject = @{}
            $changeObject[$AnchorColumn] = $key
            $changeObject["ChangeType"] = "Delete"
            $deletes++
            foreach ($n in $previousHeadersNorm)
            {
                #"Values of removed record. Column: $($n)   Previous: $($prevValue)" | Write-Verbose
                $prevRaw = $prevHeaderMap[$n]
                $prevValue = $previousLookup[$key].$prevRaw
                $changeObject["old $n"] = $prevValue
                $changeObject["new $n"] = ""
            }
            $changeBuffer.Add([PSCustomObject]$changeObject)
            if ($changeBuffer.Count -ge $BatchSize) {
                $changeBuffer | Select-Object -Property $reportColumns |
                Export-Csv -LiteralPath $changesCSVFile -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -Append -ErrorAction Stop
                $changeBuffer.Clear()
            }
            $iDel++
            if (($iDel % 1000) -eq 0 -or $iDel -eq $toDeleteTotal) {
                $pct = if ($toDeleteTotal -gt 0) { [int](($iDel/$toDeleteTotal)*100) } else { 100 }
                Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Finalizing deletions... ($iDel of $toDeleteTotal)" -PercentComplete $pct
            }
        }
        if ($changeBuffer.Count -gt 0) {
            $changeBuffer | Select-Object -Property $reportColumns |
                Export-Csv -LiteralPath $changesCSVFile -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -Append -ErrorAction Stop
        }
        # No changes notice
        if (($adds + $updates + $deletes) -eq 0) {
            Write-Host "No changes detected"
        }
    }
    finally {
        # Always clear progress, even if an error occurs
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Completed
    }
    # Summary output
    Write-Host "Summary: Adds=$adds, Updates=$updates, Deletes=$deletes"
}
catch {
    Write-Error $_
    exit 1
}