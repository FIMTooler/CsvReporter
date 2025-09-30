<# 
.SYNOPSIS
Sort-merge compares two CSV files using temporary sorted files.

.DESCRIPTION
Externalized sort-merge on disk to bound memory usage:
- Robust header parsing and strict anchor validation.
- Sorts both CSVs by the anchor and merges them.
- Temp sorted files are created next to the source CSVs and always cleaned up.
- Case-sensitive or insensitive comparisons per -CaseSensitive.
- Batches output and prints a one-line summary with counts.

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
Temporary files are removed in a finally block even if errors occur.
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

    # Map: normalized header -> raw header for each file
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

    # Resolve anchor column raw names per file
    $anchorNorm   = $AnchorColumn.Trim().ToLowerInvariant()
    $prevAnchorRaw = $prevHeaderMap[$anchorNorm]
    $currAnchorRaw = $currHeaderMap[$anchorNorm]
    if (-not $prevAnchorRaw) { throw "Anchor column '$AnchorColumn' not found in Previous CSV headers: $($previousHeadersRaw -join ', ')" }
    if (-not $currAnchorRaw) { throw "Anchor column '$AnchorColumn' not found in Current CSV headers: $($currentHeadersRaw -join ', ')" }

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

    # Begin processing files
    $previousCSVTempSorted = "$PreviousCSVFile.$fileTime.sorted"
    $currentCSVTempSorted = "$CurrentCSVFile.$fileTime.sorted"

    $progressId = 1
    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Sorting Previous..." -PercentComplete 0

    try
    {
        # Totals for progress (counted during sort pipelines)
        $prevTotal = 0
        $anchorSetPrev = New-Object 'System.Collections.Generic.HashSet[string]' ($anchorComparer)
        $prevRowIndex = 0
        Resolve-ImportCsv -LiteralPath $PreviousCSVFile -Delimiter $Delimiter -EncodingName $EncodingName |
            ForEach-Object {
                $prevRowIndex++
                # 1. Anchor Value Validation
                $anchor = $_.$prevAnchorRaw
                if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in Previous record (row $prevRowIndex): $($_)." }
                # 2. Duplicate Anchor Value Check
                if (-not $anchorSetPrev.Add($anchor)) { throw "Duplicate anchor value '$anchor' found in Previous file (row $prevRowIndex)." }
                # 3. Consistent Row Length Check
                $actualColumns = @($_.PSObject.Properties).Count
                if ($actualColumns -ne $previousHeadersRaw.Count) { throw "Row $prevRowIndex with anchor '$anchor' in Previous file has $actualColumns columns, expected $($previousHeadersRaw.Count)." }
                # 4. Blank or Malformed Row Check
                $nonEmpty = $_.PSObject.Properties | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) }
                if ($nonEmpty.Count -eq 0) { throw "Blank or malformed row found in Previous file at row $prevRowIndex with anchor '$anchor'." }
                $prevTotal++
                $_
            } |
            Sort-Object { $_.$prevAnchorRaw }  -CaseSensitive:$CaseSensitive -ErrorAction Stop |
            Export-Csv -LiteralPath $previousCSVTempSorted -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -ErrorAction Stop

        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Sorting Current..." -PercentComplete 20

        $currTotal = 0
        $anchorSetCurr = New-Object 'System.Collections.Generic.HashSet[string]' ($anchorComparer)
        $currRowIndex = 0
        Resolve-ImportCsv -LiteralPath $CurrentCSVFile -Delimiter $Delimiter -EncodingName $EncodingName |
            ForEach-Object {
                $currRowIndex++
                # 1. Anchor Value Validation
                $anchor = $_.$currAnchorRaw
                if ([string]::IsNullOrWhiteSpace($anchor)) { throw "Anchor column '$AnchorColumn' is null or empty string in Current record (row $currRowIndex): $($_)." }
                # 2. Duplicate Anchor Value Check
                if (-not $anchorSetCurr.Add($anchor)) { throw "Duplicate anchor value '$anchor' found in Current file (row $currRowIndex)." }
                # 3. Consistent Row Length Check
                $actualColumns = @($_.PSObject.Properties).Count
                if ($actualColumns -ne $currentHeadersRaw.Count) { throw "Row $currRowIndex with anchor '$anchor' in Current file has $actualColumns columns, expected $($currentHeadersRaw.Count)." }
                # 4. Blank or Malformed Row Check
                $nonEmpty = $_.PSObject.Properties | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Value) }
                if ($nonEmpty.Count -eq 0) { throw "Blank or malformed row found in Current file at row $currRowIndex with anchor '$anchor'." }
                $currTotal++
                $_
            } |
            Sort-Object { $_.$currAnchorRaw } -CaseSensitive:$CaseSensitive  -ErrorAction Stop |
            Export-Csv -LiteralPath $currentCSVTempSorted -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -ErrorAction Stop

        Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Merging..." -PercentComplete 40
        $processedPrev = 0; $processedCurr = 0

        $prevEnum = (Resolve-ImportCsv -LiteralPath $previousCSVTempSorted -Delimiter $Delimiter -EncodingName $EncodingName).GetEnumerator()
        $currEnum = (Resolve-ImportCsv -LiteralPath $currentCSVTempSorted  -Delimiter $Delimiter -EncodingName $EncodingName).GetEnumerator()

        $prevHasNext = $prevEnum.MoveNext()
        if (-not $prevHasNext) { throw "No records found in Previous CSV file." }
        $currHasNext = $currEnum.MoveNext()
        if (-not $currHasNext) { throw "No records found in Current CSV file." }

        $changeBuffer = [System.Collections.Generic.List[object]]::new($BatchSize)

        while ($prevHasNext -or $currHasNext)
        {
            $change = @{}
            $prevRow = $null; $currRow = $null
            if ($prevHasNext) { $prevRow = $prevEnum.Current }
            if ($currHasNext) { $currRow = $currEnum.Current }

            if (-not $prevHasNext)
            {
                # New record (addition)
                $currKey = $currRow.$currAnchorRaw
                $change["ChangeType"] = "Add"
                $adds++
                $change[$AnchorColumn] = $currKey
                #"User add to Current file. $($currKey)" | Write-Verbose

                foreach ($n in $previousHeadersNorm)
                {
                    $currRaw = $currHeaderMap[$n]
                    $currValue = $currRow.$currRaw
                    #"Values of new record. Column: $($n)   Value: $($currValue)" | Write-Verbose
                    $change["old $n"] = ""
                    $change["new $n"] = $currValue
                }
                $changeBuffer.Add([PSCustomObject]$change)
                if ($changeBuffer.Count -ge $BatchSize) {
                    $changeBuffer | Select-Object -Property $reportColumns |
                    Export-Csv -LiteralPath $changesCSVFile -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -Append -ErrorAction Stop
                    $changeBuffer.Clear()
                }
                $processedCurr++
                $currHasNext = $currEnum.MoveNext()
                if ( (($processedPrev + $processedCurr) % 2000) -eq 0 -or (-not $prevHasNext -and -not $currHasNext) ) {
                    $total = [math]::Max(1, $prevTotal + $currTotal)
                    $pct = [int]((($processedPrev + $processedCurr) / $total) * 60) + 40
                    if ($pct -gt 100) { $pct = 100 }
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Merging... ($($processedPrev + $processedCurr) of $total rows)" -PercentComplete $pct
                }
                continue
            }
            if (-not $currHasNext)
            {
                # ...process deletion...
                $prevKey = $prevRow.$prevAnchorRaw
                #"User removed in Current file. $($prevKey)" | Write-Verbose
                $change[$AnchorColumn] = $prevKey
                $change["ChangeType"] = "Delete"
                $deletes++
                foreach ($n in $previousHeadersNorm)
                {
                    $prevRaw = $prevHeaderMap[$n]
                    $prevValue = $prevRow.$prevRaw
                    #"Values of removed record. Column: $($n)   Previous: $($prevValue)" | Write-Verbose
                    $change["old $n"] = $prevValue
                    $change["new $n"] = ""
                }
                $changeBuffer.Add([PSCustomObject]$change)
                if ($changeBuffer.Count -ge $BatchSize) {
                    $changeBuffer | Select-Object -Property $reportColumns |
                        Export-Csv -LiteralPath $changesCSVFile -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -Append -ErrorAction Stop
                    $changeBuffer.Clear()
                }
                $processedPrev++
                $prevHasNext = $prevEnum.MoveNext()
                if ( (($processedPrev + $processedCurr) % 2000) -eq 0 -or (-not $prevHasNext -and -not $currHasNext) ) {
                    $total = [math]::Max(1, $prevTotal + $currTotal)
                    $pct = [int]((($processedPrev + $processedCurr) / $total) * 60) + 40
                    if ($pct -gt 100) { $pct = 100 }
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Merging... ($($processedPrev + $processedCurr) of $total rows)" -PercentComplete $pct
                }
                continue
            }

            $prevKey = $prevRow.$prevAnchorRaw
            $currKey = $currRow.$currAnchorRaw
            #"Previous user: $($prevKey)" | Write-Verbose
            #"Current user: $($currKey)" | Write-Verbose

            $keysEqual = if ($CaseSensitive) { $prevKey -ceq $currKey } else { $prevKey -ieq $currKey }
            if ($keysEqual)
            {
                # Compare fields for update/none
                #"User exists in both files. $($prevKey)" | Write-Verbose
                $change[$AnchorColumn] = $prevKey
                $isUpdate = $false
                foreach ($n in $previousHeadersNorm)
                {
                    #"Comparing column: $($n)" | Write-Verbose
                    $prevRaw = $prevHeaderMap[$n]
                    $currRaw = $currHeaderMap[$n]
                    $prevValue = $prevRow.$prevRaw
                    $currValue = $currRow.$currRaw
                    $valuesEqual = if ($CaseSensitive) { $prevValue -ceq $currValue } else { $prevValue -ieq $currValue }
                    if ($valuesEqual)
                    {
                        #"Values match. Column: $($n)   Previous: $($prevValue)   Current: $($currValue)" | Write-Verbose
                    }
                    else
                    {
                        #"Values do not match. Column: $($n)   Previous: $($prevValue)   Current: $($currValue)" | Write-Verbose
                        $change["old $n"] = $prevValue
                        $change["new $n"] = $currValue
                        $isUpdate = $true
                    }
                }
                if ($isUpdate)
                {
                    $change["ChangeType"] = "Update"
                    $updates++
                }
                else
                {
                    $change["ChangeType"] = "None"
                }
                $changeObject = [PSCustomObject]$change
                $processedPrev++; $processedCurr++
                $prevHasNext = $prevEnum.MoveNext()
                $currHasNext = $currEnum.MoveNext()
                if ( (($processedPrev + $processedCurr) % 2000) -eq 0 -or (-not $prevHasNext -and -not $currHasNext) ) {
                    $total = [math]::Max(1, $prevTotal + $currTotal)
                    $pct = [int]((($processedPrev + $processedCurr) / $total) * 60) + 40
                    if ($pct -gt 100) { $pct = 100 }
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Merging... ($($processedPrev + $processedCurr) of $total rows)" -PercentComplete $pct
                }
            }
            elseif ( ($CaseSensitive -and ($prevKey -clt $currKey)) -or (-not $CaseSensitive -and ($prevKey -ilt $currKey)) )
            {
                # Record deleted
                #"User removed in Current file. $($prevKey)" | Write-Verbose
                $change[$AnchorColumn] = $prevKey
                $change["ChangeType"] = "Delete"
                $deletes++
                foreach ($n in $previousHeadersNorm)
                {
                    $prevRaw = $prevHeaderMap[$n]
                    $prevValue = $prevRow.$prevRaw
                    #"Values of removed record. Column: $($n)   Previous: $($prevValue)" | Write-Verbose
                    $change["old $n"] = $prevValue
                    $change["new $n"] = ""
                }
                $changeObject = [PSCustomObject]$change
                $processedPrev++
                $prevHasNext = $prevEnum.MoveNext()
                if ( (($processedPrev + $processedCurr) % 2000) -eq 0 -or (-not $prevHasNext -and -not $currHasNext) ) {
                    $total = [math]::Max(1, $prevTotal + $currTotal)
                    $pct = [int]((($processedPrev + $processedCurr) / $total) * 60) + 40
                    if ($pct -gt 100) { $pct = 100 }
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Merging... ($($processedPrev + $processedCurr) of $total rows)" -PercentComplete $pct
                }
            }
            else
            {
                # New record (addition)
                $currKey = $currRow.$currAnchorRaw
                $change["ChangeType"] = "Add"
                $adds++
                $change[$AnchorColumn] = $currKey
                #"User add to Current file. $($currKey)" | Write-Verbose
                foreach ($n in $previousHeadersNorm)
                {
                    $currRaw = $currHeaderMap[$n]
                    $currValue = $currRow.$currRaw
                    #"Values of new record. Column: $($n)   Value: $($currValue)" | Write-Verbose
                    $change["old $n"] = ""
                    $change["new $n"] = $currValue
                }
                $changeObject = [PSCustomObject]$change
                $processedCurr++
                $currHasNext = $currEnum.MoveNext()
                if ( (($processedPrev + $processedCurr) % 2000) -eq 0 -or (-not $prevHasNext -and -not $currHasNext) ) {
                    $total = [math]::Max(1, $prevTotal + $currTotal)
                    $pct = [int]((($processedPrev + $processedCurr) / $total) * 60) + 40
                    if ($pct -gt 100) { $pct = 100 }
                    Write-Progress -Id $progressId -Activity "Compare CSVs" -Status "Merging... ($($processedPrev + $processedCurr) of $total rows)" -PercentComplete $pct
                }
            }
            $changeBuffer.Add($changeObject)
            if ($changeBuffer.Count -ge $BatchSize)
            {
                $changeBuffer | Select-Object -Property $reportColumns |
                    Export-Csv -LiteralPath $changesCSVFile -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -Append -ErrorAction Stop
                $changeBuffer.Clear()
            }
        }
        if ($changeBuffer.Count -gt 0)
        {
            $changeBuffer | Select-Object -Property $reportColumns |
                Export-Csv -LiteralPath $changesCSVFile -Delimiter $Delimiter -NoTypeInformation -Encoding $exportEncoding -Append -ErrorAction Stop
        }
        # No changes notice
        if (($adds + $updates + $deletes) -eq 0) {
            Write-Host "No changes detected"
        }
    }
    finally
    {
        Remove-Item -LiteralPath $previousCSVTempSorted,$currentCSVTempSorted -Force -ErrorAction SilentlyContinue
        Write-Progress -Id $progressId -Activity "Compare CSVs" -Completed
    }
    # Summary output
    Write-Host "Summary: Adds=$adds, Updates=$updates, Deletes=$deletes"
}
catch {
    Write-Error $_
    exit 1
}