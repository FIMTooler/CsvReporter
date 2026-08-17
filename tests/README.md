# Tests

Verification suite for the five `CompareCSVs_*.ps1` scripts at the repo root.

```powershell
.\tests\Invoke-CompareVerification.ps1                 # agreement across scripts and PS versions
.\tests\Invoke-CompareVerification.ps1 -Mode Malformed # bad input and mismatched column sets
.\tests\Invoke-CompareVerification.ps1 -Mode Memory    # peak working set (see below)
.\tests\Invoke-CompareVerification.ps1 -Mode Core      # Detailed's transform/date/ignore features
```

Every mode prints `PASS`/`FAIL` per check, ends with a `RESULT:` line, and **exits non-zero if any
check failed**, so it can be run unattended.

Paths are repo-relative — the harness finds the scripts at the repo root and its fixtures in
`tests/fixtures/`. No arguments are needed.

> **`-Scripts` is an array.** Invoke the harness directly, as above. Running it through
> `pwsh -File ... -Scripts small,medium` binds the whole list as a single string, so no script
> matches and every check fails with `NOFILE`.

## What each mode checks

**Agreement** runs every script against every behavioural fixture on **both** Windows PowerShell 5.1
and PowerShell 7, and asserts:

- each script produces identical bytes on both PowerShell versions
- `small == medium == large` (they share the standard output shape) — all three are **content-equality
  checks** (`Test-ContentEqual`): `large` is the only one of the three that still sorts by anchor
  value, so its row order can differ from `small`'s/`medium`'s even when the content is identical
- every script's output matches a recorded baseline for its fixture and shape (see **Golden
  baselines** below) — the checks above catch one script disagreeing with its peers, but are
  structurally blind to all drifting together; the baseline check is not. Same content-equality
  approach applies to the baseline check for every script - none of them assume byte-identity anymore
- **`Delta` has its own two checks, since it shares no sibling's output shape:** it always writes a
  file, even on a zero-change run, unlike every other script (`Delta` never `NOFILE` is a failure
  specific to it); and its `ChangeType` classification (Add/Update/Delete) is cross-checked against
  `medium`'s own explicit `ChangeType` column, anchor by anchor - Delta's only independent oracle,
  since its baseline check below only proves agreement with its own past self

`small != Detailed != Delta` is expected — different output shapes, not a failure.

A script that produces *no* file counts as a failure, not as agreement (except `Delta`, which is
never expected to produce no file at all — see above). Two scripts that both produced nothing have
failed identically, which is not the same as agreeing.

### Golden baselines and `-UpdateBaseline`

`tests/baselines/` holds one recorded file per behavioural fixture per output shape - 18 files,
`<fixture>_standard.csv`, `<fixture>_detailed.csv` and `<fixture>_delta.csv` - compared by content
(`Test-ContentEqual` — see "Order-tolerant comparison" below), not filename, since the report
filename carries a millisecond timestamp the content never does, and not raw bytes, since none of
`small`/`medium`/`large`/`Detailed`/`Delta` guarantee row order anymore. A missing baseline is a loud
failure, not a silent skip.

Baselines are read-only on every normal invocation. `-UpdateBaseline` regenerates them from that
run's own output instead:

```powershell
.\tests\Invoke-CompareVerification.ps1 -UpdateBaseline
```

- Prints the old-versus-new diff, line by line, before overwriting any file that already differs.
- **Refuses outright — writes nothing — if any check other than the baseline comparison itself
  failed on the same run**, so a baseline can never be re-recorded from a state the rest of the
  suite already considers broken. Proved directly: a scratch copy of the scripts with one output
  trivially altered produced `REFUSED: 4 other check(s) failed above; no baseline written.`, and
  `tests/baselines/` was confirmed byte-for-byte unchanged.

These baselines are *recorded*, not *derived* — they capture what the scripts produce today, which
could in principle enshrine an existing defect. What supports trusting the initial 2026-08-09
baseline anyway: it was recorded from scripts that already agreed with each other across both
PowerShell versions on every fixture, so a defect would have had to be present identically in all
five to survive into it.

### Order-tolerant comparison (everywhere except `large`)

`small`, `medium`, `Detailed` and `Delta` don't sort their output (`Delta` never did); `large` is the
only script in the family
that still does. So every content-comparison check in this harness - `small == medium`,
`small == large`, and every script's own baseline check - uses order-tolerant comparison, not a plain
`Get-FileHash` byte-compare. `Get-ComparableLines` strips the fixed header line(s) — 1 for the
standard/`Delta` shape, 2 for `Detailed`'s (header + SUMMARY, which isn't a sortable data row) — sorts
the remaining rows as plain text, and `Test-ContentEqual` compares the two resulting arrays
index-by-index, printing the first differing line on failure. Not `Get-FileHash` (order can
legitimately differ) and not `Compare-Object` (see "Notes" below) — a manual array comparison, always
run rather than a hash-then-diff-on-failure split, since these fixtures are a handful of lines either
way. Every script's own PS7-vs-PS5.1 check is the one comparison still on `Get-FileHash` — that's the
same script comparing its own output to itself, where row order is deterministic regardless of
sorting, so byte-identity remains the right check there.

`Delta`'s `ChangeType` cross-check against `medium` (see above) is a different kind of comparison
again - not a line-level diff at all, but an anchor-keyed dictionary comparison
(`Get-AnchorChangeTypeMap`/`Test-ChangeTypeAgreement`), since it's checking a classification verdict
per record, not matching whole report rows.

**Malformed** feeds each bad input to every script, on **both** PowerShell versions, and asserts the
expected rejection fires. Covers ragged rows, malformed quoting, duplicate anchors, and mismatched
column sets.

Both versions because this mode asserts on error *text*, and the two frame it differently:
PowerShell 7 prefixes the record with `Exception:`, 5.1 prefixes it with the script path and splits
it across several output objects. A pattern anchored to either version's framing passes on one and
fails on the other. Verified 2026-08-06: searching for `Exception:` passes every PS7 throw case and
fails every 5.1 one.

**Memory** compares peak working set on a large generated fixture, on **PowerShell 7 only**. It
asserts that `CompareCSVs_large.ps1` peaks lower than `CompareCSVs_small.ps1` — that being the only
reason the external-sort version exists. Run `New-LargeFixture.ps1` first; it writes ~244 MB (100,000
rows x 50 columns, values padded to 20-30 characters — G3, 2026-08-09) to
`tests/fixtures/large-generated/`, which is gitignored rather than committed.

7-only is deliberate: the assertion compares two scripts inside one runtime, so a second version
re-confirms an architectural property rather than testing anything new, and would need its own
baseline process. Profiling all four across both runtimes is a separate exercise. Do still run this
after any change affecting how much data a script holds — `large` ceasing to peak below `small` is a
signal about the change, not a performance curiosity.

**Core** exercises `CompareCSVs_Detailed.ps1`'s `-ValueTransforms`, `-DateFormats` and
`-IgnoreColumns` against the `newlines/` fixture, on **both** PowerShell versions, implemented
2026-08-10. No new fixture: `newlines/` already carries the `Status` and
`HireDate` columns those cases were written against, and `Dept` (present in every fixture) covers
`-IgnoreColumns`. Only those three parameters are exercised under this mode; other planned cases —
rejection paths, non-default delimiters and encodings, `-CaseSensitive`, `-BatchSize` — remain
unimplemented.

Hashtable-valued parameters (`-ValueTransforms`, `-DateFormats`) cannot cross a `-File` process
boundary as command-line text — PowerShell renders an object argument by its `ToString()`
(`System.Collections.Hashtable`), not as a real object on the far side. Those two cases route through
a small driver `.ps1`, generated per run into `_work/`, that builds the hashtable in-process and
calls `Detailed` via splatting — one driver per PowerShell version, keeping the same
fresh-child-process discipline as every other mode. `-IgnoreColumns` is a `[string[]]` and crosses the
boundary fine as plain text, so that case runs directly.

### How much a green run is worth

Weigh the modes by how many assertions each actually makes, because "all checks passed" says nothing
about how much was checked:

| mode | assertions | runtimes covered | runtime |
|---|---|---|---|
| Agreement | 85 | 5.1 and 7 | seconds |
| Malformed | 91 | 5.1 and 7 | seconds |
| Memory | **1** | 7 only | minutes — 15 runs over a 100K-row fixture |
| Core | 17 | 5.1 and 7 | seconds |

A green Memory run confirms one inequality. It is not a broad statement about performance.

## Fixtures

All fixtures are UTF-8 with BOM and use `ID` as the anchor column. They are small on purpose and each
one exists to catch something specific.

Two deliberate exceptions, both providing coverage nothing else does — **do not "normalise" them**:

- `newlines/` uses **LF line terminators throughout**, where every other fixture uses CRLF. That
  exercises the parser against a non-Windows line ending as well as against the embedded newline it
  is named for.
- `newlines/prev.csv` contains a **non-ASCII character** (`é`, U+00E9), giving multi-byte UTF-8
  coverage through the read/compare/write path.

| folder | what it proves |
|---|---|
| `sparse/` | All four change types with sparse population. Update rows populate only changed columns, so this catches regressions in the `,,` (unchanged) versus `""` (explicitly empty) distinction. |
| `newlines/` | A quoted field containing an embedded newline. Reproduces the original reader bug: phantom rows and fields shifted into the wrong columns. Also carries status and date values suitable for testing `-ValueTransforms` and `-DateFormats` on `Detailed`. |
| `symmetric/` | A genuinely changed field (`Dept: IT -> FINANCE`) on a record containing an embedded newline. Under the old reader **both** files corrupted identically, so the change was reported as `None` — the worst failure mode, because nothing looks wrong. |
| `collation/` | Anchors `_z A-1 a_1 b-1 B-10 b-2`, chosen so ordinal and culture collation **disagree**. Anchors like `1,2,3` sort identically under both and cannot detect a collation change. |
| `duplicates/` | A repeated anchor on **each** side whose copies hold different values, so first-occurrence-wins and last-occurrence-wins produce visibly different output. |
| `column-order/` | Previous (`ID,Alpha,Beta,Gamma`) and Current (`Gamma,ID,Alpha,Beta`) order columns differently, and the anchor sits at column 0 in one and column 1 in the other. The only fixture exercising a Previous-to-Current column permutation and an anchor not at column 0 - added for `Delta` (G9), which permutes Delete rows' values into Current's column order. |
| `malformed/` | `good.csv` plus `short_row`, `long_row`, `bad_quotes`, `dup_anchor`, `header_only` (header-only file, byte-identical header to `good.csv`'s, added for G9 - exercises the empty-Previous/empty-Current throw without the column-mismatch check masking it). Each must be rejected or warned about, never silently accepted. |
| `mismatched-columns/` | Column sets that differ three ways: extra column in Current, extra in Previous, and a renamed column at the same count. All three must be rejected. Used by `-Mode Malformed`, not by Agreement — there every script correctly writes no file, which Agreement would score as agreement. |

Fixtures are committed rather than generated so they stop being re-derived, and because several
encode findings that were not obvious. Add to them rather than replacing them.

## Known limits of this suite

Worth knowing before trusting a green run:

- **Partially closed by golden baselines (Group D, 2026-08-09).** Agreement mode used to test only
  agreement between scripts, not correctness against an expected result — altering a fixture changed
  what all five scripts produced equally, so they still agreed and the suite still passed. The
  baseline check closes exactly that gap: verified by deliberately corrupting `sparse/curr.csv`, which
  now fails all 5 baseline-match checks for that fixture while every sibling/version check still
  passes. What is still not covered: a baseline only catches drift from what was recorded on
  2026-08-09, and recorded is not the same as independently verified: the baselines capture what the
  scripts produced on that date, not what an independent oracle proved correct.
- **Coverage of rejection paths is thin.** The scripts reject a great many bad inputs; this suite
  exercises a small fraction of them. Non-default `-DelimiterName`, `-EncodingName`, `-CaseSensitive`
  and `-BatchSize` are not exercised at all.
- **Memory runs on PowerShell 7 only.** Agreement and Malformed loop both versions; Memory does not,
  by design (see above). So no memory figure in this repo has been measured on 5.1, and none should
  be quoted as if it had.

Further cases aimed squarely at those gaps are specified but not yet built.

## Notes

- Comparison uses `Get-FileHash` for the remaining byte-identity checks (PS7-vs-PS5.1 per script).
  **Never `Compare-Object` on file contents** — it wraps every element in a `PSObject`, measured at
  ~770-930 bytes per byte compared, and took a process past 10 GB on 13 MB files. The order-tolerant
  checks (see above) don't use `Compare-Object` either, for the same reason, even though these
  fixtures are far below the scale where that cost would actually bite — no reason to reach for the
  one idiom this codebase specifically avoids.
- `-Mode Memory` discards a warm-up run per script and takes best-of-N, interleaved. A cold first
  run measured 84 s against 26 s for the same code, so a single unwarmed run means nothing.
  Subtract the `pwsh` baseline the run prints for itself — it is machine-specific, so use the
  measured value rather than one quoted in prose.
- `_work/` is harness scratch, recreated on every run and not committed.
