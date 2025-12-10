# CsvReporter (HashTable, Streaming, Sort-Merge, Detailed Validation)

PowerShell scripts to compare two CSV files and produce a changes report. Choose by data size and validation depth. All provide robust header validation, configurable delimiter and encoding, case-sensitive/insensitive comparisons, and progress reporting.

Scripts
- CompareCSVs_small.ps1 — In-memory comparison (fastest for small/medium files; standard output)
- CompareCSVs_medium.ps1 — Streaming/batched comparison (handles larger files; bounded writes; standard output)
- CompareCSVs_large.ps1 — External sort-merge using temporary sorted files (bounded memory for very large files; standard output)
- CompareCSVs_Detailed.ps1 — In-memory comparison with field-level match tracking and summary statistics (validation/audit scenarios)
- CompareCSVs_DetailedV2.ps1 — In-memory comparison with field-level match tracking, value transforms, and column filtering (advanced validation/audit scenarios)


Key features
- Robust header parsing (quoted headers, embedded delimiters) via Microsoft.VisualBasic.TextFieldParser
- Validation:
  - Empty/blank header detection (with 1-based positions)
  - Duplicate header detection after normalization (Trim + ToLowerInvariant)
  - Required anchor column present in both files
  - Duplicate anchor values within each file
  - Consistent row length per file
  - Blank/malformed data row detection
- Configurable delimiter: comma, tab, semicolon, pipe
- Configurable encoding for input and output
- Case-sensitive or insensitive comparisons
- Progress bars (Write-Progress) with phase updates; bars always clear even on errors
- Output filenames include millisecond-precision timestamps for uniqueness

Requirements
- Windows PowerShell 5.1 or PowerShell 7+ on Windows
- Microsoft.VisualBasic available (the scripts load it for TextFieldParser)

Parameters (common)
- -PreviousCSVFile: Path to the “Previous” CSV
- -CurrentCSVFile: Path to the “Current” CSV
- -AnchorColumn: Header name of the key column used to join rows
- -OutputFolder: Folder where the changes CSV will be written
- -DelimiterName: One of comma, tab, semicolon, pipe
- -EncodingName: One of auto, utf8, utf8BOM, unicode, oem, default
  - auto: let Import-Csv detect BOM; export uses UTF-8 on PS 6+ (UTF-8 with BOM behavior on PS 5.1 mappings handled internally)
  - utf8BOM: UTF-8 (Excel-friendly; default)
  - utf8: UTF-8 (no BOM on PS 6+, BOM on PS 5.1)
  - unicode: UTF-16 LE
  - oem, default: legacy code pages as exposed by PowerShell
- -CaseSensitive: Use case-sensitive comparisons

Parameters (script-specific)
- CompareCSVs_medium.ps1: -BatchSize (default 1000)
- CompareCSVs_large.ps1: -BatchSize (default 1000)
- CompareCSVs_DetailedV2.ps1: -ValueTransforms (hashtable of column transformations), -IgnoreColumns (array of column names to skip)

Delimiters
- comma => ,
- tab => `t
- semicolon => ;
- pipe => |

Output
- A CSV written to -OutputFolder named:
  - Changes_{CurrentFileBaseName}_GeneratedOn_{yyyy-MM-dd_HHmmssfff}.csv
- Standard output (Small, Medium, Large):
  - Columns: AnchorColumn, ChangeType, and for each header: "old header" and "new header"
  - Includes all rows (Add, Update, Delete, None)
- Detailed output (Detailed):
  - Columns: AnchorColumn, ChangeType, and for each header: "old header", "new header", "match header"
  - Includes all rows (Add, Update, Delete, None) with per-field match indicators (True/False)
  - Summary row inserted as first record showing mismatch counts per column ("X of Y FALSE")
  - Output sorted by anchor column
- ChangeType meanings (all scripts):
  - Add: Exists only in Current
  - Delete: Exists only in Previous
  - Update: Exists in both with at least one differing value
  - None: Exists in both with no value changes
- Match column (Detailed only):
  - True: Values match between Previous and Current
  - False: Values differ between Previous and Current
  - Empty: Not applicable (Add/Delete rows cannot be matched)

Usage examples
- Standard output, in-memory (small/medium):
  - .\CompareCSVs_small.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv -AnchorColumn EmployeeID -OutputFolder .\out -DelimiterName comma -EncodingName utf8BOM
- Standard output, streaming (larger files):
  - .\CompareCSVs_medium.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv -AnchorColumn EmployeeID -OutputFolder .\out -BatchSize 2000 -DelimiterName comma -EncodingName auto
- Standard output, sort-merge (very large files):
  - .\CompareCSVs_large.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv -AnchorColumn EmployeeID -OutputFolder .\out -BatchSize 5000 -DelimiterName comma -EncodingName utf8
- Detailed validation (field-level match tracking, audit trails):
  - .\CompareCSVs_Detailed.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv -AnchorColumn EmployeeID -OutputFolder .\out -DelimiterName comma -EncodingName utf8BOM
- Detailed validation with value transforms (normalize data during comparison):
  - .\CompareCSVs_DetailedV2.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv -AnchorColumn EmployeeID -OutputFolder .\out -ValueTransforms @{'Salary'='<<0.00'} -DelimiterName comma -EncodingName utf8BOM

Choosing a script
- CompareCSVs_small.ps1: Entire files in memory. Fastest and simplest for small/medium datasets. Use when you need a quick list of changes.
- CompareCSVs_medium.ps1: Loads "Previous" into a dictionary; streams "Current" and writes in batches. Good for larger inputs where memory is a constraint.
- CompareCSVs_large.ps1: Sorts both files by anchor on disk, then merges. Best for very large inputs; temp files are always cleaned up. Use when standard output meets your needs but data is too large for memory.
- CompareCSVs_Detailed.ps1: In-memory with field-level match tracking. Use for validation scenarios where you need to identify exactly which fields differ and by how much. Ideal for pre-cutover validation (old process vs new process), audit trails, and Excel-based analysis with filtering. Not suitable for very large datasets due to memory requirements and output size.
- CompareCSVs_DetailedV2.ps1: Enhanced version of Detailed with -ValueTransforms (column-specific value mapping) and -IgnoreColumns (skip columns from comparison). Use when you need to normalize data during comparison or exclude specific columns from analysis. Same memory/output limitations as Detailed.

Progress and diagnostics
- Progress bars show phases such as loading, streaming, sorting, and merging, with periodic updates.
- Clear error messages for header and row validation issues.
- A summary line with Adds/Updates/Deletes/Unchanged counts is printed at completion.
- Elapsed time (minutes and seconds) is printed at completion.
- "No changes detected; no CSV written" is printed if all records are unchanged (ChangeType=None).

Notes
- Paths are handled with -LiteralPath to avoid wildcard surprises.
- Temp file names in Large include millisecond timestamps and are removed in a finally block, even on errors.

Contributing
- File issues and PRs with minimal repro CSVs where possible.
- Keep behavior consistent across scripts.

License
- MIT (or your preferred license)
