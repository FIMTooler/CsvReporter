# CsvReporter

PowerShell scripts that compare two CSV files on a key column and write a changes report listing
what was added, updated, deleted, or left unchanged.

There are five scripts. (Previously five — `Detailed` and `DetailedV2` merged into one script under
the `Detailed` name, 2026-08-09. If you have `CompareCSVs_DetailedV2.ps1` saved somewhere,
`CompareCSVs_Detailed.ps1` now does everything it did.) They differ in **how much of the input they
hold in memory** and in **what shape of report they produce** — per-column detail, or a whole-row
change feed. Pick by those axes; everything else behaves the same.

Each is a single self-contained file — copy one wherever you need it and run it. There is no module
to install and nothing shared between them.

## Which script should I use?

Two questions.

### 1. Do you need to see which fields changed, which records changed, or a feed for another system?

- **Which fields changed** — a **detailed** report: `Detailed`. Half again as many columns, plus a
  summary row, value transforms, date normalisation and column filtering.
- **Only which records changed**, for a person to read — a **standard** report: `small`, `medium` or
  `large`.
- **Only which records changed, in full, as a feed for another system** — `Delta`. No per-column
  detail: an Add or Update row is the complete new record, a Delete row is the complete old one, and
  unchanged records are dropped rather than reported. Built for handing a downstream system an
  incremental import it cannot otherwise produce, not for a person to read. See
  [`CompareCSVs_Delta.ps1`](#comparecsvs_deltaps1--whole-row-delta-feed) below.

Samples of the standard and detailed shapes are at the end of this section; `Delta`'s is in its own
section below.

### 2. How big are your files?

Memory is driven by cell count, not by size on disk:

**rows x columns = cells** — so 5,000 rows x 30 columns = 150,000 cells

A 20 MB export of 80 narrow columns therefore costs more than a 40 MB one of 12 wide columns.

Peak memory on PowerShell 7, on top of the ~70 MB PowerShell itself uses:

| cells | for example | `small` | `medium` | `large` |
|---|---|---|---|---|
| 150,000 | 5,000 rows x 30 columns | 75 MB | 70 MB | 65 MB |
| 1 million | 25,000 x 40 | 250 MB | 150 MB | ~70 MB |
| 3 million | 100,000 x 30 | 600 MB | 370 MB | ~70 MB |
| 10 million | 200,000 x 50 | 1.9 GB | 1.1 GB | ~70 MB |
| 30 million | 600,000 x 50 | 5.5 GB | 3.2 GB | ~70 MB |

**Use `medium` until its column exceeds the memory you can spare, then switch to `large`.** Below
about 150,000 cells all three are within 10 MB of each other and the choice makes no difference.

**The `medium` column above assumes a typical comparison, where most values match.** `medium` saves
memory by discarding a value once it is confirmed unchanged — the more of your data actually differs
between the two files, the less there is to discard, and the closer `medium`'s memory moves toward
`small`'s. Measured at 5 million cells (100,000 rows x 50 columns): a typical comparison showed
`medium` using 398 MB less than `small`; a file where every single value had changed showed only a
59 MB gap. If you're comparing two files that are mostly unrelated rather than two snapshots of mostly
the same data, expect `medium`'s advantage to shrink toward that worst case. `small`'s own figures
never move with how much changes, since it holds every value regardless of match status. `large`'s
figures don't move either, for a different reason — its memory is set by `-BatchSize` before any
comparison happens, not by how much of the data differs.

Measured up to 10 million cells; the larger figures follow the same per-cell model. `Detailed` shares
`medium`'s architecture, not `small`'s — neither file is held in full — so treat `medium`'s column as
its floor, plus a third field per column on top. Measured at 100,000 rows x 50 columns: `medium`
471 MB, `Detailed` 481 MB — close, not equal, since `Detailed` writes every column on every row
rather than discarding unchanged values the way `medium` does.

**Switching script is a filename change.** All four take the same required parameters; `large` adds
an optional `-BatchSize` and `Detailed` three optional extras, and none of those are needed to run.

**`large`'s memory does not grow with input at all** — it sorts on disk instead of in memory. It
costs 2.5-3x the runtime and needs scratch space in `-OutputFolder`, roughly 2-3x the combined size
of both CSVs. Reach for it when the others would need more memory than the machine can spare.

**Halving the length of your values does not halve the memory.** Every field is a separate string
carrying its own overhead, so the column count matters far more than how long the values are.

On Windows PowerShell 5.1 all three sit near 150 MB whatever the input until the data outgrows that,
so below about 25,000 rows the choice makes little difference there.

### What the two report shapes look like

For a source file with `dept` and `title` columns. **Standard** gives `old`/`new` per column, and on
an `Update` row fills in only what actually changed:

```
"EmployeeID","ChangeType","old dept","new dept","old title","new title"
"E1042","Update",,,"Analyst","Senior Analyst"
```

**Detailed** adds a `match` column per field, fills in every column whether it changed or not, and
inserts a summary row counting mismatches:

```
"EmployeeID","ChangeType","old dept","new dept","match dept","old title","new title","match title"
"SUMMARY","---","","","0 of 1 FALSE","","","1 of 1 FALSE"
"E1042","Update","Ops","Ops","True","Analyst","Senior Analyst","False"
```

Two source columns become 6 report columns in standard and 8 in detailed; the gap widens with every
column you add. Full column rules are under [Output](#output).

All three standard-report scripts agree on **content** for the same input — same rows, same
Add/Update/Delete/None calls, same populated cells. Only `large` sorts rows by anchor; `small` and
`medium` do not, so their row order can differ from `large`'s (and from each other's) even when the
content is identical. Switching between them changes resource use and, for `small`/`medium`, row
order — never which changes are reported.

## Requirements

- Windows PowerShell 5.1 or PowerShell 7+ on Windows
- Microsoft.VisualBasic (the scripts load it for `TextFieldParser`)

Every script produces identical bytes on both PowerShell versions.

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

## Quick start

```powershell
.\CompareCSVs_small.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out
```

Writes `out\Changes_curr_GeneratedOn_2026-01-31_143022817.csv` and prints a summary line.

## Common parameters

All five scripts accept these.

| parameter | meaning |
|---|---|
| `-PreviousCSVFile` | path to the "before" CSV (required) |
| `-CurrentCSVFile` | path to the "after" CSV (required) |
| `-AnchorColumn` | header name of the key column used to join rows (required) |
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

If your input is a legacy Windows export with no BOM, pass `-EncodingName ansi`. Decoding such a file
as UTF-8 turns accented characters into replacement characters, and because these scripts compare
values, that surfaces as spurious differences rather than as an error.

**Writing:** the report is written in the encoding named here. Keep the default `utf8BOM` if the
report will be opened in Excel, which otherwise reads UTF-8 as ANSI. `default` is an alias for
`ansi`.

Every option produces the same bytes on PowerShell 5.1 and 7+.

## Script-specific parameters

### `CompareCSVs_large.ps1` — `-BatchSize`

Rows per sort chunk, default 25,000, accepted range **1 to 1000000**. A value outside that is
rejected before the run starts. At most this many rows are held in memory at once.

**The default costs about 215 MB above what PowerShell itself uses** — roughly 280-290 MB total,
comfortably inside what a general-purpose PowerShell script can expect to use on most systems. Below
that there is nothing to tune. It also keeps `large` single-pass — no extra merge pass over the data
— up to **800,000 rows**, past what most CSV comparisons need.

**Go lower if memory is genuinely tight**, or the row is very wide. A smaller chunk still works; it
just pays an extra full pass over the data once the file exceeds `rows / 32` (below), which costs
time and temporary disk, not correctness. Measured on a 200,000-row file, `-BatchSize 1000` took
**49% longer and used 50% more temporary disk** than `6250` did.

**Go higher only past 800,000 rows, if you want to stay single-pass: set it to about `rows / 32`.**
That is where `large` can merge its temporary files in a single pass; going any higher buys nothing —
on the same 200,000-row file, raising the chunk from 25,000 to 100,000 quadrupled memory and ran very
slightly *slower*. A chunk large enough to hold the whole file defeats the point of the script
entirely — `large` then holds as much as an in-memory comparison while still writing every row to
disk and reading it back.

Chunk memory depends on the chunk size and the width of a row, **not** on how big the file is: a
given `-BatchSize` costs the same on a 100,000-row file as on a 200,000-row one. Rough figures,
measured on 50 columns of 20-30 characters, about 1.2 KB per row:

| `-BatchSize` | memory |
|---|---|
| 6,000 | ~100 MB |
| 25,000 | ~215 MB (default) |
| 40,000 | ~300 MB |
| 80,000 | ~600 MB |
| 125,000 | ~900 MB |

Chunk memory also depends on the width of your data — the table above assumes 50 columns of 20-30
character values. Narrower data costs less, but not proportionally: about 49 MB of the total is
fixed overhead (loading the CSV parser, starting the script) that doesn't shrink with column count.

| columns | memory at `-BatchSize 25,000` |
|---|---|
| 50 (20-30 chars) | 214 MB |
| 30 (15-25 chars) | 148 MB |
| 10 (~10 chars) | 82 MB |

Use this to gauge direction and rough scale for other widths, not an exact prediction.

Use the `-BatchSize` table above to check whether `rows / 32` fits your budget — a 1,000,000-row
file wants 31,250, or roughly 260 MB at this same 50-column width. If it does not fit, a smaller
chunk still works; it just pays the extra pass.

`large` also needs free space in `-OutputFolder` while it runs — **about 2.2x the combined size of
both inputs** when it is single-pass, rising to roughly **3.3x** below `rows / 32`, because each
merge pass leaves its inputs on disk until the run ends. Temporary files are removed when it
finishes, including after an error.

### `CompareCSVs_Detailed.ps1` — `-ValueTransforms`, `-DateFormats`, `-IgnoreColumns`

All three are optional and all match column names case-insensitively after trimming, so header names
can be pasted straight from the CSV.

**`-ValueTransforms`** rewrites Previous values before comparison, so cosmetic differences do not
show up as changes. The report still shows the original values.

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

### Empty values

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

**`'*'` does not match empty.** Only an explicit `'' = …` rule reaches blank values. This is
deliberate, and it is the one exception you have to remember. Transforms rewrite the Previous value
only — Current is never touched — so a wildcard that swept up blanks would turn an empty cell into
something the equally empty Current cell no longer matches, manufacturing a difference on every blank
rather than reconciling one. Keeping them separate also stays flexible: a `''` rule can do whatever
`'*'` would have done, but a `'*'` that consumed empties could never be split back out.

**Whitespace is not empty.** `'   '` is an ordinary value — it can be targeted explicitly
(`'   ' = 'X'`), and `'*'` does reach it.

You don't have to remember any of that in the moment. If a column has a `'*'` rule, contains empty
values, and has no `''` rule, the run says so:

```
WARNING: Column 'status' contains empty values; '*' does not match empty. Add a '' = ... rule to include them.
```

One line per column, whether one cell is empty or ten thousand.

**`-DateFormats`** normalizes dates before comparison, for when the two files write the same date
differently. Without it, `01/15/2020` and `2020-01-15` compare as a difference.

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

Parsing uses the invariant culture. Empty and whitespace-only values are skipped rather than parsed,
so a blank date is not a warning. A non-empty value that will not parse keeps its raw value, emits a
warning, and falls back to comparing the raw text. The report always shows the original values, not
the normalized ones.

**`-IgnoreColumns`** drops columns from the comparison entirely.

```powershell
.\CompareCSVs_Detailed.ps1 -PreviousCSVFile .\prev.csv -CurrentCSVFile .\curr.csv `
  -AnchorColumn EmployeeID -OutputFolder .\out -IgnoreColumns @('LastModifiedDate','ProcessingNotes')
```

Ignored columns are **removed from the report**, not merely skipped — they produce no old/new/match
triplet and cannot make a row an Update.

A name need exist in only **one** of the two files, since an ignored column is not compared. That has
a useful consequence: the two files must normally have identical column sets, and any difference is
rejected outright — but ignoring the odd column out reconciles it, in either direction.

A name found in **neither** file is a warning rather than an error, so one ignore list can be reused
across file pairs that don't all carry every column. The warning names the unmatched columns and
prints both header sets, so a typo stays visible.

The anchor column cannot be ignored, ignoring every non-anchor column is rejected — nothing would be
left to compare — and a column cannot be both ignored and transformed. The anchor also cannot be
targeted by `-ValueTransforms` or `-DateFormats`: it is the join key, so two rows only ever pair up
because their anchors already compared equal, and a transform on it would never have anything to do.

### `CompareCSVs_Delta.ps1` — whole-row delta feed

Where the other four scripts report *which fields* changed, `Delta` reports *which records* changed,
in full — see ["Which script should I use?"](#which-script-should-i-use) above for when to reach for
it. This section covers what it does once chosen.

**Two output modes.**

- **Single-file (default).** One CSV. Column 0 is `ChangeType` (`Add`, `Update` or `Delete` —
  `None` is counted but never written); columns 1..N are every column of Current, **in Current's own
  physical order**. The anchor is an ordinary column in its natural position, not hoisted to the
  front and not duplicated. Add and Update rows carry Current's values; Delete rows carry Previous's
  values, permuted into Current's column order — Previous and Current do not need to share a column
  order for this to work.

  ```
  "ChangeType","EmployeeID","dept","title"
  "Update","E1042","Ops","Senior Analyst"
  "Add","E1055","Sales","Rep"
  "Delete","E1003","Legal","Counsel"
  ```

- **Split (`-SeparateDeleteFile`).** Two CSVs, both with Current's columns in Current's order,
  **neither carrying a `ChangeType` column**: Adds and Updates go to the main file, Deletes to a
  second file. For a consumer whose importer cannot tolerate an extra column — the trade is that Add
  and Update become indistinguishable in that file.

**The `ChangeType` column** defaults to that name; override it with `-ChangeTypeColumnName`. It
cannot be blank, and the run throws if it collides (after trim and lowercase, regardless of
`-CaseSensitive`) with any column already in Current — naming the offending column and
`-ChangeTypeColumnName` as the fix. It is never auto-renamed. The parameter is meaningless with
`-SeparateDeleteFile`, since no such column is written in that mode; PowerShell rejects the
combination at bind time rather than silently ignoring it.

**`-NormalizeHeaderNames`** opts in to the family's trim+lowercase header treatment. Off by default —
unlike the generated `old`/`new` names the rest of the family writes, this script's output headers
are Current's own names verbatim, and lowercasing them by default would silently break a
case-sensitive downstream consumer.

**`-AnchorOnlyDeletes`** reduces a Delete row's non-anchor fields to empty. The header is unchanged in
both modes — all columns stay present, only the anchor is populated on those rows. In single-file
mode the `ChangeType` cell still reads `Delete`, so there is no ambiguity with a record whose fields
happened to already be empty.

**`-Force` and overwriting.** Without it, a run stops *before any parsing begins* if the resolved
output path already exists (both paths, independently, in split mode) — naming the path and `-Force`
as the way to allow it. This matters most with `-OutputFileName`/`-DeleteFileName`: the auto-derived,
timestamped names make a collision near-impossible, but an explicit override removes that
protection, and overwriting a delta a downstream process has not yet consumed destroys it
unrecoverably.

**Filenames** are auto-derived as `Delta_{CurrentFileBaseName}_GeneratedOn_{yyyy-MM-dd_HHmmssfff}.csv`
and, in split mode, `Delta_Deletes_{CurrentFileBaseName}_GeneratedOn_{yyyy-MM-dd_HHmmssfff}.csv` — the
`Delta_` prefix, rather than the family's `Changes_`, keeps delta output distinguishable from a
standard report when both land in the same folder. Override with `-OutputFileName`/`-DeleteFileName`,
which must be bare filenames (no directory separator, no other filesystem-invalid character, not
empty) and, in split mode, must not resolve to the same name as each other.

**Always writes a file — the one place this script breaks from the rest of the family.** The other
four print `No changes detected; no CSV written` and write nothing when nothing changed. `Delta`
always writes (both files, independently, in split mode), even when that means a header-only CSV,
because it feeds an automated process rather than being read by a person — a downstream job should
not have to distinguish "no file" from "job failed".

**Row order is unsorted**, the same as `small`, `medium` and `Detailed`: Current's row order for
Adds/Updates, then Deletes in whatever order they happen to be enumerated in — not stable across
processes, for the reasons under ["Column names and order"](#column-names-and-order) below.

**The file mixes two points in time.** Add and Update rows are Current's post-change state; Delete
rows are Previous's last-known state before removal. Reading every row uniformly as "current state"
would resurrect the deleted records — route Delete rows to whatever deprovisioning process they
need, separately from Add/Update rows.

## Output

The report is written to `-OutputFolder` as
`Changes_{CurrentFileBaseName}_GeneratedOn_{yyyy-MM-dd_HHmmssfff}.csv`.

**`large` sorts rows by the anchor column** using **ordinal** ordering, honouring `-CaseSensitive`.
Ordinal is not the same as alphabetical: by default `A-1` sorts before `a_1`, and `_z` sorts after
both. Adding `-CaseSensitive` changes the order again, because uppercase precedes lowercase ordinally.

**`small`, `medium`, and `Detailed` do not sort rows.** Output order is whatever order the rows are
found in: `Current`'s row order for Adds/Updates/Nones, followed by Deletes in whatever order they
happen to be enumerated in. If row order matters to what consumes the report, sort by the anchor
column after the fact, or use `large` instead.

If every record is unchanged, the script prints `No changes detected; no CSV written` and writes no
file.

### Column names and order

Two things to know before building anything on top of the report.

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
pair of its own — two rows only ever pair up because their anchors already compared equal, so that
comparison could never show a difference.

Column order is also the one part of the report that follows the machine's locale rather than
ordinal rules. Report *rows* are ordered ordinally and are stable everywhere, but report *columns*
are ordered by the current culture's collation — a Czech locale sorts `ch` after `h`, for example.
All four scripts do this identically, so they still agree with each other on one machine; but do not
byte-compare a report produced on one machine against one produced on another.

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
  rule with the number of times it was applied** — including rules that never
  matched, which appear as `(0 applied)`. It is an inventory of the rules you supplied, not a list of
  the ones that fired, so a `(0 applied)` line means "this rule never matched", not "this rule
  failed". Rules are ordered most-applied first, then alphabetically, with the `*` wildcard last:

  ```
  Employee->EMP (5 applied)
  Contingent Worker->CWK (2 applied)
  Intern->INT (0 applied)
  ```

  Those lines share **one cell**, separated by bare line feeds — Excel shows them stacked in a single
  cell. The list caps at 20 rules; beyond that it is truncated and a final line reads
  `[...and N more transform rule(s)]`.

### ChangeType

| value | meaning |
|---|---|
| `Add` | exists only in Current |
| `Delete` | exists only in Previous |
| `Update` | exists in both, at least one value differs |
| `None` | exists in both, no values differ |

## Validation

Input problems are reported rather than silently absorbed. All five scripts:

- reject empty or blank header names, naming the 1-based column position
- reject duplicate header names after trimming and lowercasing
- reject a run where the two files do not have the same set of columns
- reject a missing anchor column, or a row whose anchor is empty or whitespace
- reject a row whose field count does not match the header, reporting the row number and its fields
- reject malformed quoting, reporting the line number and the offending line
- warn on duplicate anchor values, naming the row kept and the rows ignored, then continue using the
  **first** occurrence - pass `-RejectDuplicateAnchors` to fail the run on the first duplicate found
  instead, which writes no report and exits `1`. Useful when the anchor is meant to be unique, since
  a duplicate is usually a data-quality problem (often a wrong `-AnchorColumn`) rather than something
  to quietly work around.

Quoted fields containing embedded newlines, delimiters, or doubled quotes are handled correctly.

## Progress and diagnostics

- `Write-Progress` bars show the current phase; they always clear, including on error.
- Every run prints `Note: Output columns use trimmed and lowercase-normalized header names for
  consistency.` before the report is written — see "Column names and order" above for what that
  means in practice.
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

## License

MIT — see [LICENSE](LICENSE).
