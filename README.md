# `get_expansion_ratio.sh` — ZFS Old/New Layout Audit

Audit how much of your ZFS data still lives on the **old RAIDZ layout** after a RAIDZ expansion, estimate the **space you’d save** by rewriting it to the **new layout**, and watch scanning progress with timestamped updates.

The script is **read-only**. It parses `zdb` output to classify blocks as **OLD** vs **NEW** relative to a TXG (transaction group) boundary and summarizes bytes and percentages per dataset and overall. It also estimates physical space for “all data old” vs “all data new” and highlights potential savings.

---

## What it does

- Parses `zdb -ddddd <dataset>` to classify blocks as OLD vs NEW using a TXG boundary you supply (`-t` or `--time`).
- Auto-detects RAIDZ parity/widths from `zpool status` and, if not provided, tries to infer old→new widths from `zpool history -il`.
- **Before scanning** each dataset, prints an estimate banner: **Old N,P** and **New N,P** with estimated *physical* size if all data were old vs new, plus **potential savings**.
- Live progress with timestamps: `Total=<processed>/<denominator>` `OLD=<bytes>` `NEW=<bytes>`
- CSV-ish line per dataset and an overall summary with computed percentages and savings.

> **Safe:** The script never modifies your pool. It only reads metadata using `zdb`, `zfs`, and `zpool`.

---

## How “OLD vs NEW” is decided

Each block has a *birth TXG*. You provide the boundary via:

- `-t <TXG>` — explicit, or
- `--time "YYYY-MM-DD HH:MM:SS"` — the script maps the timestamp to the next uberblock TXG via `zdb -u` or `zdb -l`.

Blocks with `birthTXG < TXG` → **OLD**; otherwise **NEW**.

---

## Denominator modes for progress

- **logical** (default): `usedbydataset + usedbysnapshots`. Useful to visualize parity/overhead; progress may exceed 100%.
- **physical**: pre-pass sums ASIZE; *Total* becomes a true 0–100% progress for this job.

> Prefer **physical** if you want a strict progress bar. The pre-pass adds time but improves clarity.

---

## Requirements

- Linux with ZFS utilities: `zfs`, `zdb`, `zpool`
- `bash`, `awk`, `date`
- Root/sudo recommended (for `zdb` and `zpool history`)

---

## Quick start

**Single dataset with a known TXG**
```bash
sudo bash ./get_expansion_ratio.sh -p Data -t 5020371 Data/Storage/Photos
```

**All datasets (recursive), map wall-clock time to TXG, and use physical denominator**
```bash
sudo bash ./get_expansion_ratio.sh -p Data --time "2025-08-01 00:00:00" -r --denom physical
```

**Override expansion widths & parity explicitly**
```bash
sudo bash ./get_expansion_ratio.sh -p Data -t 5020371 \
  --old_drive_count 7 --new_drive_count 10 --parity 2 \
  Data/Storage/Photos
```

---

## Full option reference

| Option | Description |
|---|---|
| `-p, --pool <name>` | Pool name (required). |
| `-t, --txg <TXG>` | Boundary TXG (expansion point). Use either this or `--time`. |
| `--time "YYYY-MM-DD HH:MM:SS"` | Map timestamp to TXG using uberblocks. |
| `-r, --recursive` | Scan all datasets under the pool (ignores explicit dataset args). |
| `-o, --output <file>` | Log file path (defaults to timestamped file). |
| `--denom logical\|physical` | Denominator for progress `Total` (default `logical`). |
| `--old_drive_count <N_old>` | Force old RAIDZ width (N). |
| `--new_drive_count <N_new>` | Force new RAIDZ width (N). |
| `--parity <P>` | Force parity columns (1, 2, or 3). |
| `--progress / --no-progress` | Enable/disable progress lines (default enabled). |
| `--progress-seconds <N>` | Emit progress at least every N seconds (default 10). |
| `--progress-gib <M>` | Emit progress after M GiB additional processed (default 1; 0 to disable byte trigger). |
| `--progress-to-log` | Duplicate progress lines to the log file. |
| `--check-lines <N>` | Check progress every N parsed lines (default 4096). |
| `--debug` | Shell tracing for debugging. |
| `--self-test` | Print tool paths and sample ZFS output to diagnose issues. |
| *(trailing args)* | Dataset names to scan (omit with `-r` to scan all). |

> If `--old_drive_count`, `--new_drive_count`, or `--parity` are omitted, the script tries **history**, then **status**, and finally assumes a **single‑drive expansion** (N_new = N_cur, N_old = N_cur−1; P from status or 2).

---

## “Kitchen‑sink” example (every option)

> Use either `-t` **or** `--time` (both shown below for illustration; comment one out).

```bash
sudo bash ./get_expansion_ratio.sh \
  -p Data \
  -t 5020371 \
  # --time "2025-08-01 00:00:00" \
  -r \
  -o /var/log/zfs-oldnew-$(date +%Y%m%d-%H%M%S).log \
  --denom physical \
  --old_drive_count 7 \
  --new_drive_count 10 \
  --parity 2 \
  --progress \
  --progress-seconds 5 \
  --progress-gib 2 \
  --progress-to-log \
  --check-lines 2048 \
  --debug \
  --self-test
```

> Tip: to target only a specific dataset, drop `-r` and append a dataset (e.g., `Data/Storage/Photos`) to scan only that subtree.

---

## Sample output (annotated)

```
ZFS Old/New Layout Audit
Started:       2025-08-31T06:00:00Z
Pool:          Data
Expansion time: 2025-08-01 00:00:00  -> TXG: 5020371
Output log:    zfs-oldlayout-report-Data-txg5020371-20250831-060000.log
Recursion:     disabled
Explicit datasets provided (1): Data/Storage/Photos
Detected RAIDZ parities: 2
Detected widths (N):     7 10
Assumed Old/New/Parity:  N_old=7, N_new=10, P=2  (source=history)
Progress:      enabled
  - progress seconds: 10
  - progress GiB:     1
  - check lines:      4096
  - denom mode:       physical
  - progress to log:  no
  - debug tracing:    off

Dataset, OLD_layout_bytes, NEW_layout_bytes, OLD%, NEW%
[dataset] Data/Storage/Photos size: used=565 GiB, usedbydataset=401 GiB, snaps=0 B, children=0 B (denom=physical pre-pass)
[estimate] Data/Storage/Photos Old N,P: N=7,P=2; est_physical=~560 GiB | New N,P: N=10,P=2; est_physical=~501 GiB | potential savings: ~59 GiB (~10.5%)
[progress] Data/Storage/Photos  Total=112 GiB/560 GiB  OLD=112 GiB  NEW=0 B
...
Data/Storage/Photos, 606140006400, 0, 100.00, 0.00
  - Data/Storage/Photos    Total=564 GiB/560 GiB   OLD=564 GiB   NEW=0 B

Overall:
  OLD-layout total: 564 GiB
  NEW-layout total: 0 B
  Processed total:  564 GiB
  Denominator sum:  560 GiB   # sum of denominators per dataset
  OLD %: 100.00%
  NEW %: 0.00%
  Estimate if ALL data were on OLD width (N=7,P=2): ~560 GiB
  Estimate if ALL data were on NEW width (N=10,P=2): ~501 GiB
  Potential savings by rewriting to NEW width: ~59 GiB (~10.5%)
Finished: 2025-08-31T06:28:38Z
```

---

## Troubleshooting

- **awk: syntax error** near `BEGIN { in = ... }`: fixed in current version (uses `inside` instead of reserved `in`).
- **Silent exit** before header: run with `--self-test` and/or `--debug`. Confirm `zdb`, `zfs`, `zpool` are in PATH and run as root.
- **--time** didn’t map to TXG: script falls back among multiple methods (`zdb -u`, label scans via `zdb -l`). Use `-t` to specify the TXG directly if needed.
- Totals > denominator: expected with **logical** mode. Use **physical** if you want 0–100% progress.
