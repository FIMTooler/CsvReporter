# Compare-CsvData

An embeddable PowerShell function, not a standalone script. Dot-source it into your own automation
and call `Compare-CsvData` directly — there is nothing to run on its own.

It compares two CSV exports on an anchor column and hands back only the rows that changed, each one
the whole row, with a `ChangeType` of `Add`, `Update` or `Delete`. Rows that did not change are
counted but never returned.

Add and Update rows carry today's values. Delete rows carry the previous day's, because there is no
current version of that row. One return value therefore holds two points in time — do not treat every
row as current state.

## Quick start

```powershell
. .\Compare-CsvData.ps1

$changes = Compare-CsvData -PreviousCsvPath .\yesterday.csv -CurrentCsvPath .\today.csv `
                            -AnchorColumn EmployeeID -Encoding UTF8 -DelimiterName comma

$changes | Export-Csv -LiteralPath .\delta.csv -NoTypeInformation -Encoding UTF8
```

The rows come back ready to export — no reshaping needed. Add `-IncludeSummary` if you also want the
summary: how many Adds, Updates and Deletes, how many rows were unchanged, and how many rows were in
each file. See [Output](#output) below for both shapes.

## Parameters

| parameter | meaning |
|---|---|
| `-PreviousCsvPath` | path to the previous day's CSV (required) |
| `-CurrentCsvPath` | path to today's CSV (required) |
| `-AnchorColumn` | header name that identifies a row and joins the two files on; matched exactly, letter case included (required) |
| `-Encoding` | must be `UTF8` — the only value Windows PowerShell 5.1 and PowerShell 7 read identically (required) |
| `-DelimiterName` | `comma`, `tab`, `semicolon` or `pipe`; must match what wrote the files (required) |
| `-ExpectedColumns` | the column names your transform produces, anchor included; both files must match this set exactly |
| `-ChangeTypeColumnName` | name of the leading verdict column; default `ChangeType` |
| `-CaseSensitive` | compares field **values** case-sensitively; does not reach anchor matching |
| `-IncludeSummary` | changes the return shape — see [Output](#output) |

### -ExpectedColumns

Optional, but it is the only thing that catches a column missing from **both** files: `Export-Csv`
builds its header from the first object alone, so a property absent there vanishes from every day's
file alike, and comparing the two files to each other cannot detect that. Give the source column
names, anchor included; order is irrelevant, and the verdict column is never listed. Empty,
whitespace-only and duplicate entries are rejected before either file is opened, and a mismatch
against either file names the side that diverged.

### -ChangeTypeColumnName and -CaseSensitive

`-ChangeTypeColumnName` cannot be blank, and cannot collide with a source column — the run throws
naming the collision rather than silently renaming either one, checked ignoring letter case and
surrounding whitespace.

`-CaseSensitive` only reaches field values. Anchors always match case-insensitively and trimmed, so
`E1042`, `e1042` and ` E1042 ` are one row; splitting them on a case or whitespace difference would
emit a Delete plus an Add instead of recognizing the same record. Trimming is identity-only — an
emitted row keeps the file's own value — and field values are never trimmed, so a padded value is
still a real difference.

## What your calling code must do

1. **Write both files with `-Encoding UTF8`, and pass `-Encoding UTF8` here.** The two must match. If
   your own `Export-Csv` leaves `-Encoding` off entirely, Windows PowerShell 5.1 writes accented
   characters as question marks — the data is destroyed before this function ever sees the file.
2. **Give every row the same properties.** `Export-Csv` builds the header from the first object only;
   a property missing there vanishes from the file with no warning, the same way every time, so
   comparing two days cannot catch it. Pass `-ExpectedColumns` and this function will.
3. **Use the same delimiter on both sides**, named via `-DelimiterName`. Avoid `-UseCulture` on your
   own `Export-Csv` — its separator is a comma on an en-US machine and something else elsewhere, so it
   passes local testing and fails on a differently configured one.
4. **Pass `-NoTypeInformation` on every `Export-Csv`** in your script, including the delta file this
   function's output feeds. Without it, 5.1 writes a `#TYPE ...` line that PowerShell reads past but
   Excel and other importers do not.
5. **The anchor column must be present, filled in, and unique**, and its name must match the header
   exactly, letter case included.
6. **Keep column order and row order fixed.** A plain hashtable does not preserve the order you wrote
   its fields in, and orders it differently on 5.1 than on 7. Use `[ordered]` or a `[pscustomobject]`
   literal instead.
7. **Treat a column-name change as a deploy step.** Adding, removing, renaming or re-capitalising a
   field makes the next run stop, because the stored previous-day file still has the old shape — that
   is deliberate, since otherwise a schema change would make every row look changed. Moving to a new
   shape means updating the field name in your transform, `-AnchorColumn` and `-ExpectedColumns` if
   either names the changed field, and replacing the stored previous-day file, together.

## What it refuses to do

These all stop the run with a message naming the cause:

- either file is empty, or has a header and no rows
- the two files do not have the same columns (or don't match `-ExpectedColumns`, when supplied)
- a column name is blank — PowerShell renames it to `H1`, and that name would reach the output
- the anchor column is missing, misspelled, or a different letter case than the header
- a row has a blank anchor, or two rows share one
- a source column collides with the verdict column name
- `-ExpectedColumns` has a blank entry, or names the same column twice

## Two things that might surprise you

**Row order is not guaranteed.** Sort the returned rows yourself if order matters to whatever reads
them next.

**Anchors are matched ignoring letter case and surrounding whitespace**, so `E1042`, `e1042` and
` E1042 ` are all the same row. Field values are not treated that way: a department of `Ops ` against
`Ops` is a real difference and is reported as one.

## Output

Without `-IncludeSummary` (the default), the changed rows themselves, one object each, ready to pipe
straight to `Export-Csv`:

```powershell
$changes = Compare-CsvData -PreviousCsvPath $yesterday -CurrentCsvPath $today `
                            -AnchorColumn EmployeeID -Encoding UTF8 -DelimiterName comma
$changes | Export-Csv -LiteralPath $out -NoTypeInformation -Encoding UTF8
```

With `-IncludeSummary`, a hashtable of two keys instead — `Changes` (the same rows) and `Summary`
(`Adds`, `Updates`, `Deletes`, `Unchanged`, `Total`, `PreviousCount`, `CurrentCount`):

```powershell
$r = Compare-CsvData -PreviousCsvPath $yesterday -CurrentCsvPath $today `
                      -AnchorColumn EmployeeID -Encoding UTF8 -DelimiterName comma -IncludeSummary
$r.Changes | Export-Csv -LiteralPath $out -NoTypeInformation -Encoding UTF8
"Unchanged today: $($r.Summary.Unchanged)"
```

`Unchanged`, `PreviousCount` and `CurrentCount` cannot be recovered from the rows afterwards, so reach
for `-IncludeSummary` if you need any of them. No changes returns an empty array, not `$null`; what to
write in that case is yours to decide — a day with no changes writes a file with no header line unless
you handle it explicitly:

```powershell
if ($changes.Count -gt 0) {
    $changes | Export-Csv -LiteralPath .\delta.csv -NoTypeInformation -Encoding UTF8
} else {
    '"ChangeType","EmployeeID","GivenName","dept","title"' |
        Set-Content -LiteralPath .\delta.csv -Encoding UTF8
}
```

One thing to avoid: `Export-Csv -InputObject $changes`. It looks equivalent to piping and is much
faster, because it writes the array as a single object instead of one line per row — two lines
written instead of the thousands you meant to write. Always pipe the rows through.
