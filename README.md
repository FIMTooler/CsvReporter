# CsvReporter (HashTable, Streaming, Sort-Merge)

PowerShell scripts to compare two CSV files and produce a detailed changes report (Adds, Updates, Deletes). They provide robust header validation, configurable delimiter and encoding, case-sensitive/insensitive comparisons, and progress reporting.

Scripts
- CompareCSVs_small.ps1 — In-memory comparison (fastest for small/medium files)
- CompareCSVs_medium.ps1 — Streaming/batched comparison (handles larger files; bounded writes)
- CompareCSVs_large.ps1 — External sort-merge using temporary sorted files (bounded memory for very large files)

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

Delimiters
- comma => ,
- tab => `t
- semicolon => ;
- pipe => |

Output
- A CSV written to -OutputFolder named:
  - Changes_{CurrentFileBaseName}_GeneratedOn_{yyyy-MM-dd_HHmmssfff}.csv
- Columns:
  - AnchorColumn, ChangeType, and for each header: “old header” and “new header”
- ChangeType meanings:
  - Add: Exists only in Current
  - Delete: Exists only in Previous
  - Update: Exists in both with at least one differing value
  - None: Exists in both with no value changes

Usage examples
- In-memory (small/medium):
  - .\CompareCSVs_small.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv -AnchorColumn EmployeeID -OutputFolder .\out -DelimiterName comma -EncodingName utf8BOM
- Streaming (larger files):
  - .\CompareCSVs_medium.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv -AnchorColumn EmployeeID -OutputFolder .\out -BatchSize 2000 -DelimiterName comma -EncodingName auto
- Sort-merge (very large files):
  - .\CompareCSVs_large.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv -AnchorColumn EmployeeID -OutputFolder .\out -BatchSize 5000 -DelimiterName comma -EncodingName utf8

Choosing a script
- CompareCSVs_small.ps1: Entire files in memory. Fastest and simplest for small/medium datasets.
- CompareCSVs_medium.ps1: Loads “Previous” into a dictionary; streams “Current” and writes in batches. Good for larger inputs.
- CompareCSVs_large.ps1: Sorts both files by anchor on disk, then merges. Best for very large inputs; temp files are always cleaned up.

Progress and diagnostics
- Progress bars show phases such as loading, streaming, sorting, and merging, with periodic updates.
- Clear error messages for header and row validation issues.
- A summary line with Adds/Updates/Deletes is printed at completion. “No changes detected” is printed if applicable.

Notes
- Paths are handled with -LiteralPath to avoid wildcard surprises.
- Temp file names in large include millisecond timestamps and are removed in a finally block, even on errors.

Contributing
- File issues and PRs with minimal repro CSVs where possible.
- Keep behavior consistent across scripts.

License
- MIT (or your preferred license)
