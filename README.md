# CsvReporter

PowerShell scripts that compare two CSV files on an anchor column and write a changes report listing
what was added, updated, deleted, or left unchanged.

Each of the five scripts is a single self-contained file — copy one wherever you need it and run it.
There is no module to install and nothing shared between them. They differ in **how much of the
input they hold in memory** and in **what shape of report they produce**; everything else behaves the
same.

## Requirements

- Windows PowerShell 5.1 or PowerShell 7+ on Windows
- Microsoft.VisualBasic (the scripts load it for `TextFieldParser`)

Every script produces identical bytes on both PowerShell versions.

## Quick start

```powershell
.\CompareCSVs_small.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out
```

Writes `out\Changes_curr_GeneratedOn_2026-01-31_143022817.csv` and prints a summary line. `small` is
the one to try first; [pick properly](#which-script-should-i-use) once you know your file size.

### If the script won't run

A `.ps1` downloaded from GitHub is tagged by Windows as coming from the internet, and the default
execution policy refuses to load it:

```
File ...\CompareCSVs_small.ps1 cannot be loaded. The file ...\CompareCSVs_small.ps1 is not
digitally signed. You cannot run this script on the current system.
```

Nothing is wrong with the download — this happens to every unsigned `.ps1`. Either clear the tag,
once per file:

```powershell
Unblock-File .\CompareCSVs_small.ps1
```

or run it without altering anything on the machine:

```powershell
powershell -ExecutionPolicy Bypass -File .\CompareCSVs_small.ps1 -PreviousCSVFile .\prev.csv `
  -CurrentCSVFile .\curr.csv -AnchorColumn EmployeeID -OutputFolder .\out
```

`Unblock-File` removes the download tag from that one file. `-ExecutionPolicy Bypass` applies to that
one process. Neither changes your machine's execution policy.

## Which script should I use?

Two questions.

### 1. Do you need to see which fields changed, which records changed, or a feed for another system?

- **Which fields changed** — a **detailed** report: `Detailed`. Half again as many columns, plus a
  summary row, value transforms, date normalisation and column filtering. Adds a `match` column per
  field, fills in every column whether it changed or not, and inserts a summary row counting
  mismatches:

  | EmployeeID | ChangeType | old dept | new dept | match dept | old title | new title | match title |
  |---|---|---|---|---|---|---|---|
  | SUMMARY | --- | | | 0 of 1 FALSE | | | 1 of 1 FALSE |
  | E1042 | Update | Ops | Ops | True | Analyst | Senior Analyst | False |

- **Only which records changed, and only what's different on them**, for a person to read — a
  **standard** report: `small`, `medium` or `large`:

  | EmployeeID | ChangeType | old dept | new dept | old title | new title |
  |---|---|---|---|---|---|
  | E1042 | Update | | | Analyst | Senior Analyst |

- **Only which records changed, in full, as a feed for another system** — `Delta`. No per-column
  detail: an Add or Update row is the complete new record, a Delete row is the complete old one, and
  unchanged records are dropped rather than reported. Built for handing a downstream system an
  incremental import it cannot otherwise produce, not for a person to read:

  | ChangeType | EmployeeID | dept | title |
  |---|---|---|---|
  | Update | E1042 | Ops | Senior Analyst |

  See [`CompareCSVs_Delta.ps1`](#comparecsvs_deltaps1--whole-row-delta-feed) below for the full
  shape, including Add/Delete rows and the split-file mode.

Two source columns become 6 report columns in standard and 8 in detailed; the gap widens with every
column you add. Full column rules are under [Output](#output).

All three standard-report scripts agree on **content** for the same input, and are identical when
sorted by anchor. See [Output](#output) for row order.

### 2. How big are your files?

Memory is driven by cell count, not by size on disk:

**rows x columns = cells** — so 5,000 rows x 50 columns = 250,000 cells

A 20 MB export of 80 narrow columns therefore costs more than a 40 MB one of 12 wide columns.

Peak memory on PowerShell 7, above the ~70 MB PowerShell itself uses, with a rough time estimate
alongside it. One test machine, rounded and padded — a gauge for "a minute or an hour," not a
guarantee:

| cells | rows | `Delta` | `small` | `medium` | `large` (25K batch) | `Detailed` |
|---|---|---|---|---|---|---|
| 250,000 | 5,000 | 80 MB / ~4 s | 110 MB / 1.5x | 80 MB / 1.2x | 90 MB / 2.5x | 90 MB / 2.5x |
| 1.25 million | 25,000 | 170 MB / ~10 s | 290 MB / 1.5x | 170 MB / 1.2x | 200 MB / 4.5x | 170 MB / 4x |
| 2.5 million | 50,000 | 270 MB / ~17 s | 510 MB / 1.5x | 270 MB / 1.2x | 200 MB / 5.5x | 270 MB / 4.5x |
| 5 million | 100,000 | 480 MB / ~31 s | 950 MB / 1.5x | 480 MB / 1.2x | 200 MB / 6x | 480 MB / 5x |
| 7.5 million | 150,000 | 680 MB / ~45 s | 1.4 GB / 1.5x | 680 MB / 1.2x | 210 MB / 6x | 680 MB / 5x |
| 10 million | 200,000 | 900 MB / ~60 s | 1.9 GB / 1.5x | 900 MB / 1.2x | 220 MB / 6x | 900 MB / 5x |
| 30 million | 600,000 | 2.5 GB / ~178 s | 5.4 GB / 1.5x | 2.5 GB / 1.2x | 230 MB / 6.5x | 2.5 GB / 5x |

- `Delta` is the fastest at every size measured — its column gives the real seconds; every other
  column is a multiple of it.
- `large` costs more than `medium` below 2.5 million cells; past that, `medium` is cheaper.
- `large`'s memory barely moves across every size here — it trades time for a memory ceiling, and
  that trade gets steeper as files grow.

**Among the standard reports, use `medium` until its column exceeds the memory you can spare, then
switch to `large`.**

**The `medium` column assumes most values match — but it barely changes when they don't.** At 5
million cells, `medium` used 478 MB less than `small` on a typical file and 476 MB less when every
value had changed. `small` and `large` don't vary with how much changed either.

**Switching script is a filename change.** All five take the same required parameters; `large` adds
an optional `-BatchSize`, `Detailed` three optional extras, and `Delta` several more of its own — none
of those are needed to run.

**`large` trades memory for disk.** It sorts on disk instead of in memory, so it also needs scratch
space in `-OutputFolder` — roughly 2-3x the combined size of both CSVs.

**Halving the length of your values does not halve the memory.** Every field is a separate string
carrying its own overhead, so the column count matters far more than how long the values are.

## Common parameters

All five scripts accept these.

| parameter | meaning |
|---|---|
| `-PreviousCSVFile` | path to the "before" CSV (required) |
| `-CurrentCSVFile` | path to the "after" CSV (required) |
| `-AnchorColumn` | header name of the anchor column used to join rows (required) |
| `-OutputFolder` | existing folder the report is written to (required) |
| `-DelimiterName` | `comma` (default), `tab`, `semicolon`, `pipe` |
| `-EncodingName` | see below (default `utf8BOM`) |
| `-CaseSensitive` | compare keys and values case-sensitively |
| `-RejectDuplicateAnchors` | fail the run instead of warning on a duplicate anchor value |

### -EncodingName

One of `auto`, `ascii`, `ansi`, `default`, `oem`, `unicode`, `utf8BOM`, `utf8NoBOM`.

**Reading:** a byte-order mark in the input always wins. This setting only decides how a file with
**no** BOM is decoded, and a BOM-less file is assumed to be UTF-8. `auto` and `utf8BOM` are therefore
equivalent on read.

If your input is a legacy Windows export with no BOM, pass `-EncodingName ansi`. Decoded as UTF-8,
accented characters become replacement characters — and since these scripts compare values, that
surfaces as spurious differences, not an error.

**Writing:** the report is written in the encoding named here. Keep the default `utf8BOM` if the
report will be opened in Excel, which otherwise reads UTF-8 as ANSI. `default` is an alias for
`ansi`.

## Output

Covers `small`, `medium`, `large` and `Detailed`. `Delta`'s naming and column rules are its own —
see [`CompareCSVs_Delta.ps1`](#comparecsvs_deltaps1--whole-row-delta-feed) below.

The report is written to `-OutputFolder` as
`Changes_{CurrentFileBaseName}_GeneratedOn_{yyyy-MM-dd_HHmmssfff}.csv`.

**`large` sorts rows by the anchor column** using **ordinal** ordering, honouring `-CaseSensitive`.
Ordinal is not alphabetical: by default `A-1` sorts before `a_1`, and `_z` after both.
`-CaseSensitive` changes the order again, since uppercase precedes lowercase ordinally.

**`small`, `medium`, and `Detailed` do not sort rows.** Output order is whatever order the rows are
found in: `Current`'s row order for Adds/Updates/Nones, followed by Deletes in whatever order they
happen to be enumerated in. If row order matters to what consumes the report, sort by the anchor
column after the fact, or use `large` instead.

If every record is unchanged, the script prints `No changes detected; no CSV written` and writes no
file.

### ChangeType

| value | meaning |
|---|---|
| `Add` | exists only in Current |
| `Delete` | exists only in Previous |
| `Update` | exists in both, at least one value differs |
| `None` | exists in both, no values differ |

### Column names and order

Three things to know before building anything on top of the report.

**Names are normalised.** Header names are trimmed and lowercased, so a source column `Zebra` becomes
`old zebra` / `new zebra`, and ` Mid Name ` becomes `old mid name`. Every run prints a note saying so.

**Columns are alphabetical, not in source order — and the anchor isn't part of that ordering.** A
file with headers `EmployeeID, Zebra, apple, Mid Name` produces columns in the order `apple`,
`mid name`, `zebra` — the anchor is never among them, since it already has its own column, first,
under the name you passed to `-AnchorColumn` and in its original case:

```
"EmployeeID","ChangeType","old apple","new apple","old mid name","new mid name","old zebra","new zebra"
```

Don't address report fields by a position derived from the input file. The anchor gets no `old`/`new`
pair of its own: two rows only pair up when their anchors already matched, so it could never show a
difference.

**Column order follows the machine's locale, not ordinal rules.** Rows are ordered ordinally and are
stable everywhere; columns are ordered by the current culture's collation — a Czech locale sorts `ch`
after `h`. All four scripts do this identically, so they agree with each other on one machine — but
don't byte-compare a report produced on one machine against one produced on another.

### Standard report — `small`, `medium`, `large`

Columns are the anchor, `ChangeType`, then `old <column>` and `new <column>` for every other column.
The anchor itself gets no `old`/`new` pair — see "Column names and order" above for why.

**On Update rows only the columns that actually changed are populated.** Unchanged columns are left
blank, which keeps a mostly-unchanged report small and makes the changed cells easy to find.

```
"EmployeeID","ChangeType","old dept","new dept","old phone","new phone","old title","new title"
"E1001","Update","Ops","Finance",,,,
"E1002","Update",,,"555-0101","",,
"E1003","Update",,,,,"","Analyst"
"E1004","None",,,,,,
"E1005","Add","","Sales","","555-0199","","Rep"
"E1006","Delete","Legal","","555-0177","","Counsel",""
```

Reading an `Update` row, one column pair at a time:

| `old` | `new` | meaning |
|---|---|---|
| blank | blank | column did not change |
| value | blank | value was **cleared** |
| blank | value | value was **added** |
| value | value | value **changed** |

The pair is always unambiguous, because a column is populated only when it changed — and a change
from empty to empty is not a change.

In the raw file the two kinds of blank differ: `,,` is an unchanged column, `,"",` is an explicitly
empty value. **Excel shows both as an empty cell**, so read the pair rather than the single cell. The
distinction is there for anything consuming the CSV programmatically.

### Detailed report — `Detailed`

Columns are the anchor, `ChangeType`, then `old <column>`, `new <column>` and `match <column>` for
every other column. The anchor itself gets no `old`/`new`/`match` set — see "Column names and order"
above for why. Every other column is populated on every row.

- `match` is `True` or `False` on Update and None rows, and empty on Add and Delete rows, which have
  nothing to compare against.
- A `SUMMARY` row is inserted as the first record, giving per-column mismatch counts as
  `X of Y FALSE`. Its `ChangeType` cell reads `---`, so it is easy to filter out.
- When `-ValueTransforms` is used, the SUMMARY row's `old` cell lists **every configured transform
  rule with the number of times it was applied** — an inventory of what you supplied, not of what
  fired, so a rule that never matched still appears, as `(0 applied)`. Rules are ordered most-applied
  first, then alphabetically, with the `*` wildcard last:

  ```
  Employee->EMP (5 applied)
  Contingent Worker->CWK (2 applied)
  Intern->INT (0 applied)
  ```

  Those lines share **one cell**, separated by bare line feeds — Excel shows them stacked in a single
  cell. The list caps at 20 rules; beyond that it is truncated and a final line reads
  `[...and N more transform rule(s)]`.

## Script-specific parameters

### `CompareCSVs_large.ps1` — `-BatchSize`

Rows per sort chunk, default 25,000, accepted range **1 to 1000000**. A value outside that is
rejected before the run starts. At most this many rows are held in memory at once.

**The default costs about 207 MB above what PowerShell itself uses** — roughly 275-285 MB total. If
that fits, leave it alone. It also keeps `large` single-pass, with no extra merge pass over the data,
up to **800,000 rows**.

**Go lower if memory is genuinely tight**, or the row is very wide. A smaller chunk still works; it
just pays an extra full pass over the data once the file exceeds `rows / 32` (below), which costs
time and temporary disk, not correctness. Measured on a 200,000-row file, `-BatchSize 1000` took
**40% longer and used 52% more temporary disk** than `6250` did.

**Go higher only past 800,000 rows, and only to stay single-pass: set it to about `rows / 32`.** That
is where `large` merges its temporary files in one pass; higher buys nothing — on the same
200,000-row file, raising the chunk from 25,000 to 100,000 more than tripled memory (3.4x) and ran
slightly *slower*. A chunk big enough to hold the whole file defeats the script: `large` then uses as
much memory as an in-memory comparison and still writes every row to disk.

Chunk memory depends on the chunk size and the width of a row, **not** on how big the file is — a
given `-BatchSize` costs the same on a 100,000-row file as on a 200,000-row one. Measured on 50
columns of 20-30 characters, about 1.2 KB per row, both PowerShell versions, 2026-08-21:

| `-BatchSize` | PS7 | PS5.1 |
|---|---|---|
| 1,000 | 72 MB | 126 MB |
| 6,250 | 98 MB | 98 MB |
| 12,500 | 125 MB | 148 MB |
| 25,000 (default) | 207 MB | 247 MB |
| 50,000 | 408 MB | 357 MB |
| 100,000 | 714 MB | 604 MB |
| 200,000 | 1.5 GB | 1.0 GB |

PS5.1 runs higher than PS7 at small chunk sizes and lower at large ones, crossing over somewhere near
50,000 — worth knowing if you're tuning this on a machine that only has PowerShell 5.1.

Narrower data costs less, but not proportionally — about 49 MB is fixed overhead that doesn't shrink
with column count:

| columns | memory at `-BatchSize 25,000`, PS7 |
|---|---|
| 50 (20-30 chars) | 207 MB |
| 30 (15-25 chars) | 148 MB |
| 10 (~10 chars) | 82 MB |

Check `rows / 32` against the `-BatchSize` table before going higher — a 1,000,000-row file wants
31,250, roughly 255 MB at this width.

`large` also needs free space in `-OutputFolder` while it runs — **about 2.2x the combined size of
both inputs** single-pass, roughly **3.3x** below `rows / 32`, since each merge pass leaves its
inputs on disk until the run ends. Temporary files are removed when it finishes, including after an
error.

### `CompareCSVs_Detailed.ps1` — `-ValueTransforms`, `-DateFormats`, `-IgnoreColumns`

All three are optional and all match column names case-insensitively after trimming, so header names
can be pasted straight from the CSV.

#### `-ValueTransforms`

Rewrites Previous values before comparison, so cosmetic differences do not show up as changes. The
report still shows the original values.

```powershell
$transforms = @{
    'worker_type'   = @{ 'Employee' = 'EMP'; 'Contingent Worker' = 'CWK' }
    'profit_center' = @{ '*' = '>>0' }
}
.\CompareCSVs_Detailed.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -ValueTransforms $transforms
```

Rule forms:

- `'OldValue' = 'NewValue'` — direct replacement
- `'OldValue' = '<<prefix'` — prepend, e.g. `'5' = '<<00'` compares as `005`
- `'OldValue' = '>>suffix'` — append, e.g. `'USA' = '>>0'` compares as `USA0`
- `'*' = ...` — fallback applied to any value not matched by an explicit rule, **except empty**

#### Empty values

Two systems rarely agree on how to represent "nothing" — a legacy extract writes `N/A`, `NULL` or
`-` where its replacement leaves the cell blank, or the reverse. Left alone, every one of those rows
reports as an Update when nothing changed. Both directions are expressible:

```powershell
$transforms = @{
    'termination_date' = @{ 'N/A' = ''    }   # Previous sentinel compares as empty
    'status'           = @{ ''    = 'N/A' }   # Previous blank compares as the sentinel
}
```

An empty rule key matches an empty Previous value exactly, and an empty rule value is legal — so
`'*' = ''` is valid too, collapsing every non-empty value in a column to blank. That is useful when
the old system populated a field the new one abandoned: the column still appears in the report,
which `-IgnoreColumns` would not allow.

**`'*'` does not match empty.** Only an explicit `'' = …` rule reaches blank values. Transforms
rewrite the Previous value only — Current is never touched — so a wildcard that swept up blanks would
turn an empty cell into something the equally empty Current cell no longer matches, manufacturing a
difference on every blank rather than reconciling one.

**Whitespace is not empty.** `'   '` is an ordinary value — it can be targeted explicitly
(`'   ' = 'X'`), and `'*'` does reach it.

If a column has a `'*'` rule, contains empty values, and has no `''` rule, the run says so:

```
WARNING: Column 'status' contains empty values; '*' does not match empty. Add a '' = ... rule to include them.
```

One line per column, whether one cell is empty or ten thousand.

#### `-DateFormats`

Normalizes dates before comparison, for when the two files write the same date differently. Without
it, `01/15/2020` and `2020-01-15` compare as a difference.

```powershell
$dates = @{
    'hire_date' = @{
        Previous = 'MM/dd/yyyy'    # how Previous writes it
        Current  = 'yyyy-MM-dd'    # how Current writes it
        Output   = 'yyyy-MM-dd'    # normalized form used for the comparison
    }
}
.\CompareCSVs_Detailed.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -DateFormats $dates
```

`Previous` and `Current` are required. `Output` is optional and defaults to `yyyy-MM-dd` — it only
affects the string the two sides are compared as, never what appears in the report.

Parsing uses the invariant culture. Empty and whitespace-only values are skipped, so a blank date is
not a warning. A value that will not parse keeps its raw value, emits a warning, and falls back to
comparing the raw text.

#### `-IgnoreColumns`

Drops columns from the comparison entirely.

```powershell
.\CompareCSVs_Detailed.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -IgnoreColumns @('LastModifiedDate','ProcessingNotes')
```

Ignored columns are **removed from the report**, not merely skipped — they produce no old/new/match
triplet and cannot make a row an Update.

A name need exist in only **one** of the two files, since an ignored column is not compared — which
also means ignoring the odd column out reconciles two files whose column sets would otherwise be
rejected as mismatched.

A name found in **neither** file is a warning, not an error, so one ignore list can be reused across
file pairs that don't all carry every column. The warning names the unmatched columns and prints
both header sets, so a typo stays visible.

The anchor cannot be ignored, and it cannot be targeted by `-ValueTransforms` or `-DateFormats` — two
rows only pair up when their anchors already matched, so a transform on it would have nothing to do.
Ignoring every non-anchor column is rejected, and a column cannot be both ignored and transformed.

### `CompareCSVs_Delta.ps1` — whole-row delta feed

Where the other four scripts report *which fields* changed, `Delta` reports *which records* changed,
in full — see ["Which script should I use?"](#which-script-should-i-use) above for when to reach for
it.

**Two output modes.**

- **Single-file (default).** One CSV. Column 0 is `ChangeType` (`Add`, `Update` or `Delete` —
  `None` is counted but never written); columns 1..N are every column of Current, **in Current's own
  physical order**. The anchor is an ordinary column in its natural position, not hoisted to the
  front and not duplicated. Add and Update rows carry Current's values; Delete rows carry Previous's
  values, permuted into Current's column order — Previous and Current do not need to share a column
  order for this to work.

  | ChangeType | EmployeeID | dept | title |
  |---|---|---|---|
  | Update | E1042 | Ops | Senior Analyst |
  | Add | E1055 | Sales | Rep |
  | Delete | E1003 | Legal | Counsel |

- **Split (`-SeparateDeleteFile`).** Two CSVs, both with Current's columns in Current's order,
  **neither carrying a `ChangeType` column**: Adds and Updates go to the main file, Deletes to a
  second file. For a consumer whose importer cannot tolerate an extra column — the trade is that Add
  and Update become indistinguishable in that file.

**The `ChangeType` column** defaults to that name; override it with `-ChangeTypeColumnName`. It
cannot be blank, and the run throws — naming the offending column and the fix — if it collides with a
column already in Current. It is meaningless with `-SeparateDeleteFile`, which writes no such column,
and PowerShell rejects that combination at bind time.

**`-NormalizeHeaderNames`** opts in to the family's trim+lowercase header treatment. Off by default:
this script's output headers are Current's own names verbatim, and lowercasing them would silently
break a case-sensitive downstream consumer.

**`-AnchorOnlyDeletes`** reduces a Delete row's non-anchor fields to empty. The header is unchanged in
both modes — all columns stay present, only the anchor is populated on those rows. In single-file
mode the `ChangeType` cell still reads `Delete`, so there is no ambiguity with a record whose fields
happened to already be empty.

**`-Force` and overwriting.** Without it, a run stops *before any parsing begins* if the output path
already exists (both paths, independently, in split mode). The auto-derived timestamped names make a
collision near-impossible, but `-OutputFileName`/`-DeleteFileName` remove that protection — and
overwriting a delta a downstream process has not yet consumed destroys it unrecoverably.

**Filenames** are auto-derived as `Delta_{CurrentFileBaseName}_GeneratedOn_{yyyy-MM-dd_HHmmssfff}.csv`,
plus `Delta_Deletes_...` in split mode — the `Delta_` prefix keeps delta output distinguishable from
a standard report in the same folder. Override with `-OutputFileName`/`-DeleteFileName`: bare
filenames only, and in split mode they must differ from each other.

**Always writes a file — the one place this script breaks from the rest of the family.** The other
four print `No changes detected; no CSV written` and write nothing. `Delta` always writes, even a
header-only CSV, because a downstream job should not have to tell "no file" from "job failed".

**Row order is unsorted**, the same as `small`, `medium` and `Detailed`: Current's row order for
Adds/Updates, then Deletes in whatever order they happen to be enumerated in — not stable across
processes, for the reasons under ["Column names and order"](#column-names-and-order) above.

**The file mixes two points in time.** Add and Update rows are Current's post-change state; Delete
rows are Previous's last-known state before removal. Reading every row uniformly as "current state"
would resurrect the deleted records — route Delete rows to whatever deprovisioning process they
need, separately from Add/Update rows.

## Validation

Input problems are reported rather than silently absorbed. All five scripts:

- reject empty or blank header names, naming the 1-based column position
- reject duplicate header names after trimming and lowercasing
- reject a run where the two files do not have the same set of columns
- reject a missing anchor column, or a row whose anchor is empty or whitespace
- reject a row whose field count does not match the header, reporting the row number and its fields
- reject malformed quoting, reporting the line number and the offending line
- warn on duplicate anchor values, naming the row kept and the rows ignored, then continue using the
  **first** occurrence

Quoted fields containing embedded newlines, delimiters, or doubled quotes are handled correctly.

**`-RejectDuplicateAnchors` fails the run on the first duplicate instead**, writing no report and
exiting `1`. Worth using when the anchor is meant to be unique — a duplicate usually means a
data-quality problem, often a wrong `-AnchorColumn`.

## Progress and diagnostics

- `Write-Progress` bars show the current phase; they always clear, including on error.
- Prints `Note: Output columns use trimmed and lowercase-normalized header names for consistency.`
  before writing — always for `small`/`medium`/`large`/`Detailed`, only with `-NormalizeHeaderNames`
  for `Delta`. See "Column names and order" above.
- A summary line with Adds/Updates/Deletes/Unchanged counts and elapsed time is printed at the end.
- Output filenames carry millisecond-precision timestamps, so repeated runs do not overwrite.

### Exit codes

`0` on success, `1` on failure. Nothing else is returned.

Two cases that might look like failures but exit `0`: a run that finds **no changes** and therefore
writes no file, and a run that **warns about duplicate anchors** and continues using the first
occurrence. Both are successful comparisons — check for the output file if you need to know whether
one was written.

`1` covers the scripts' own rejections — malformed input, mismatched column sets, a missing anchor
column — and also PowerShell's own parameter-binding failures, such as omitting a mandatory
parameter or passing `-BatchSize 0`. A wrapper does not need to tell the two apart.

## Related tools

`Compare-CsvData` solves the same problem in a different shape: instead of writing a report file, it
hands the changed rows straight back to your own script, ready to pipe into whatever comes next. It's
a function you dot-source and call, not a script you run on its own. See
[Compare-CsvData/README.md](Compare-CsvData/README.md).

## License

MIT — see [LICENSE](LICENSE).
