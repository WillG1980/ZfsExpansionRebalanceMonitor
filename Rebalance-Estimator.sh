#!/usr/bin/env bash
# get_expansion_ratio.sh — OLD vs NEW layout bytes relative to an expansion TXG
# Adds:
#   --old_drive_count N_old, --new_drive_count N_new, --parity P
# Auto inference order (if flags omitted):
#   1) Parse zpool history for raidz expansion columns and parity
#   2) Use zpool status min/max widths + detected parity
#   3) Fallback: assume single-drive expansion (N_old = N_cur-1, N_new=N_cur), P from status or 2
# Keeps:
#   --time mapping, timestamped progress, “Enumerating datasets”, dataset size line,
#   Total=<processed>/<denominator>, --denom logical|physical, layout detection/estimates.

set -uo pipefail  # no `-e`: we log and continue

ts(){ date -u '+%Y-%m-%dT%H:%M:%SZ'; }
log(){ printf "[%s] %s\n" "$(ts)" "$1" >&2; }
log2(){ log "$1"; $PROGRESS_TO_LOG && [[ -n "${LOGFILE:-}" ]] && printf "[%s] %s\n" "$(ts)" "$1" >> "$LOGFILE"; }
die(){ log "ERROR: $1"; exit 2; }
human(){ local b=$1 u=(B KiB MiB GiB TiB PiB) i=0; while (( b>=1024 && i<${#u[@]}-1 )); do b=$((b/1024)); ((i++)); done; printf "%d %s" "$b" "${u[$i]}"; }
bytes_from_gib(){ echo $(( $1 * 1024 * 1024 * 1024 )); }
round_mul_parity(){ awk -v L="$1" -v N="$2" -v P="$3" 'BEGIN{ if(N<=P||P<=0||L<=0){print 0;exit} x=L*N/(N-P); printf "%d\n", int(x+0.5) }'; }

trap 'ec=$?; log "[exit] status=$ec"' EXIT

POOL=""; TXG=""; WHEN=""
RECURSE=false; LOGFILE=""
PROGRESS=true; PROG_SECONDS=10; PROG_GIB=1; PROGRESS_TO_LOG=false; CHECK_LINES=4096; DEBUG=false; SELFTEST=false
DENOM_MODE="logical"   # logical | physical
# NEW: user-provided overrides
USER_OLDN=""; USER_NEWN=""; USER_P=""

declare -a REM_ARGS=() DS_LIST=()

# ---------- time -> TXG ----------
try_uberblocks_from_pool(){ { zdb -u "$1" 2>/dev/null || true; } | awk '/txg =/{t=$3}/timestamp =/{s=$3;gsub(/[^0-9]/,"",s); if(t!=""&&s!="") print t,s}'; }
leaf_devices(){ { zdb -C "$1" 2>/dev/null || true; } | awk 'tolower($0) ~ /path:/ { for(i=1;i<=NF;i++) if($i=="path:" && (i+1)<=NF){print $(i+1); break} }'; }
try_uberblocks_from_leaves(){
  local pool="$1"
  while IFS= read -r dev; do
    [[ -n "$dev" && -e "$dev" ]] || continue
    { zdb -l "$dev" 2>/dev/null || true; } | awk '/txg =/{t=$3}/timestamp =/{s=$3;gsub(/[^0-9]/,"",s); if(t!=""&&s!="") print t,s}'
  done < <(leaf_devices "$pool")
}
pick_txg_for_time(){ local target="$1"; local best="" max=""; while read -r txg ts; do [[ -n "$txg" && -n "$ts" ]] || continue; max="$txg"; if [[ -z "$best" && "$ts" -ge "$target" ]]; then best="$txg"; break; fi; done; [[ -n "$best" ]] && echo "$best" || echo "$max"; }
guess_txg_from_time(){
  local pool="$1" stamp="$2" tgt; tgt="$(date -d "$stamp" +%s)" || { echo ""; return 0; }
  local txg=""; txg="$(try_uberblocks_from_pool "$pool" | pick_txg_for_time "$tgt")"
  if [[ -n "$txg" ]]; then echo "$txg"; return 0; fi
  try_uberblocks_from_leaves "$pool" | pick_txg_for_time "$tgt"
}

# ---------- detect RAIDZ groups (P,N) from status ----------
detect_raidz_groups() {
  # Emits lines: "P N" for each raidz vdev in the pool
  zpool status "$POOL" 2>/dev/null \
  | awk '
      BEGIN { inside=0; p=0; n=0 }
      /^[[:space:]]*raidz[123]-[0-9]+/ {
        if (inside==1) { print p, n; n=0 }
        if (match($1, /raidz([123])-/, m)) { p = m[1]+0 } else { p=0 }
        inside=1; next
      }
      (inside==1) {
        # stop when a new vdev stanza starts or we leave the vdev block
        if ($0 ~ /^[[:space:]]*raidz[123]-[0-9]+/ ||
            $0 ~ /^[[:space:]]*mirror-/ ||
            $0 ~ /^[^[:space:]]/ ||
            $0 ~ /^[[:space:]]*(logs|spares|special|cache)\b/) {
          print p, n; n=0; inside=0
          if ($0 ~ /^[[:space:]]*raidz[123]-[0-9]+/) {
            if (match($1, /raidz([123])-/, m)) { p = m[1]+0; inside=1; n=0 }
          }
          next
        }
        # count child device lines (indented, non-blank)
        if ($0 ~ /^[[:space:]]+[A-Za-z0-9_\/\.\-:]+/) n++
      }
      END { if (inside==1) print p, n }
  '
}
summarize_raidz_groups() {
  RAIDZ_PARITIES=""; RAIDZ_WIDTHS=""
  local line P N; declare -A seenP=() seenN=()
  while read -r line; do
    [[ -z "$line" ]] && continue
    read -r P N <<<"$line"
    ((N>0)) || continue
    seenP["$P"]=1; seenN["$N"]=1
  done < <(detect_raidz_groups || true)
  for k in "${!seenP[@]}"; do RAIDZ_PARITIES+="${RAIDZ_PARITIES:+ }$k"; done
  for k in "${!seenN[@]}"; do RAIDZ_WIDTHS+="${RAIDZ_WIDTHS:+ }$k"; done
  MIN_N="$(echo "$RAIDZ_WIDTHS" | tr ' ' '\n' | awk 'NF{print $0}' | sort -n | head -1)"
  MAX_N="$(echo "$RAIDZ_WIDTHS" | tr ' ' '\n' | awk 'NF{print $0}' | sort -n | tail -1)"
  MAX_P="$(echo "$RAIDZ_PARITIES" | tr ' ' '\n' | awk 'NF{print $0}' | sort -n | tail -1)"
}

# ---------- infer from history ----------
# Try to extract parity and any "columns X->Y" hints from zpool history
infer_from_history() {
  local pool="$1"
  # Outputs three fields: P_guess OLDN_guess NEWN_guess (any may be empty)
  zpool history -il "$pool" 2>/dev/null \
  | awk '
      BEGIN{P=""; old=""; new=""}
      {
        low=tolower($0)
        if (match(low,/raidz([123])/,m)) P=m[1]
        # Look for "columns" or "cols" or "width" transitions like "7->10" or "7 to 10"
        if (match(low,/([0-9]+)[[:space:]]*->[[:space:]]*([0-9]+)/,c)) {
          if (old=="" || c[1]<old) old=c[1]
          if (new=="" || c[2]>new) new=c[2]
        } else if (match(low,/(columns|cols|width)[^0-9]*([0-9]+)[^0-9]+([0-9]+)/,d)) {
          if (old=="" || d[2]<old) old=d[2]
          if (new=="" || d[3]>new) new=d[3]
        } else if (match(low,/from[[:space:]]+([0-9]+)[^0-9]+to[[:space:]]+([0-9]+)/,e)) {
          if (old=="" || e[1]<old) old=e[1]
          if (new=="" || e[2]>new) new=e[2]
        }
      }
      END{printf "%s %s %s\n", P, old, new}
  '
}

# ---------- sizes ----------
dataset_size_fetch(){ zfs list -Hp -o used,usedbydataset,usedbysnapshots,usedbychildren "$1" 2>/dev/null | awk '{print $1, $2, $3, $4}' || echo "0 0 0 0"; }
dataset_phys_asize_sum(){
  zdb -ddddd "$1" 2>/dev/null \
  | awk 'BEGIN{IGNORECASE=1; s=0}
         function hx(x){ return (x ~ /^[0-9a-fA-F]+$/) ? strtonum("0x"x) : 0 }
         { if (match($0,/([0-9a-fA-F]+):([0-9a-fA-F]+):([0-9a-fA-F]+)/,m)) s += hx(m[3]); }
         END{ printf "%d\n", s }' \
  || echo "0"
}

dataset_size_line(){
  local ds="$1"; local all ds_only snaps kids
  read -r all ds_only snaps kids <<<"$(dataset_size_fetch "$ds")"
  local denom_logical=$(( ${ds_only:-0} + ${snaps:-0} ))
  if [[ "$DENOM_MODE" == "physical" ]]; then
    log2 "[dataset] $ds size: used=$(human "${all:-0}"), usedbydataset=$(human "${ds_only:-0}"), snaps=$(human "${snaps:-0}"), children=$(human "${kids:-0}") (denom=physical pre-pass)"
    local denom_phys; denom_phys="$(dataset_phys_asize_sum "$ds")"
    echo "$denom_phys" " ${ds_only:-0}" " ${snaps:-0}"
  else
    log2 "[dataset] $ds size: used=$(human "${all:-0}"), usedbydataset=$(human "${ds_only:-0}"), snaps=$(human "${snaps:-0}"), children=$(human "${kids:-0}") (denom=logical)"
    echo "$denom_logical" " ${ds_only:-0}" " ${snaps:-0}"
  fi
}

# ---------- args ----------
parse_args() {
  log "[init] starting get_expansion_ratio (args: $*)"
  while (( $# )); do
    case "$1" in
      -p|--pool)      POOL="${2:-}"; shift 2;;
      -t|--txg)       TXG="${2:-}";  shift 2;;
      --time)         WHEN="${2:-}"; shift 2;;
      -r|--recursive) RECURSE=true;  shift;;
      -o|--output)    LOGFILE="${2:-}"; shift 2;;
      --denom)        DENOM_MODE="${2:-logical}"; shift 2;;
      --old_drive_count) USER_OLDN="${2:-}"; shift 2;;
      --new_drive_count) USER_NEWN="${2:-}"; shift 2;;
      --parity)       USER_P="${2:-}"; shift 2;;
      --progress)     PROGRESS=true; shift;;
      --no-progress)  PROGRESS=false; shift;;
      --progress-seconds) PROG_SECONDS="${2:-10}"; shift 2;;
      --progress-gib)     PROG_GIB="${2:-1}"; shift 2;;
      --progress-to-log)  PROGRESS_TO_LOG=true; shift;;
      --check-lines)      CHECK_LINES="${2:-4096}"; shift 2;;
      --debug)        DEBUG=true; shift;;
      --self-test)    SELFTEST=true; shift;;
      -h|--help)
        cat <<'H'
Usage:
  get_expansion_ratio.sh -p <pool> (-t <TXG> | --time "YYYY-MM-DD HH:MM:SS") [-r] [-o logfile]
                         [--denom logical|physical]
                         [--old_drive_count N_old] [--new_drive_count N_new] [--parity P]
                         [--progress|--no-progress] [--progress-seconds N] [--progress-gib M]
                         [--progress-to-log] [--check-lines N] [--debug] [--self-test] [<dataset>...]

If N/P flags are omitted:
  - Try to infer P and N_old->N_new from `zpool history -il`.
  - Else use `zpool status` (min/max N across raidz vdevs; max P).
  - Else **fallback** assumes a single-drive expansion: N_new = N_cur, N_old = N_cur - 1, P from status or 2.

H
        exit 0;;
      --) shift; break;;
      -*) die "Unknown option: $1";;
      *)  REM_ARGS+=("$1"); shift;;
    esac
  done

  [[ -n "$POOL" ]] || die "Specify -p <pool>"
  if [[ -z "$TXG" && -n "$WHEN" ]]; then
    TXG="$(guess_txg_from_time "$POOL" "$WHEN" || true)"
    [[ -n "$TXG" ]] || die "Could not map --time '$WHEN' to a TXG"
    log "[info] --time '$WHEN' mapped to TXG $TXG"
  fi
  [[ -n "$TXG"  ]] || die "Specify -t <TXG> or --time \"YYYY-MM-DD HH:MM:SS\""
  [[ -n "$LOGFILE" ]] || LOGFILE="zfs-oldlayout-report-${POOL}-txg${TXG}-$(date +%Y%m%d-%H%M%S).log"
  $DEBUG && set -x
}

datasets_to_scan(){ echo "Enumerating datasets to scan" >&2; if $RECURSE || [[ ${#REM_ARGS[@]} -eq 0 ]]; then zfs list -r -H -o name "$POOL"; else printf "%s\n" "${REM_ARGS[@]}"; fi; }

# ---------- choose Old/New N,P (user -> history -> status -> fallback) ----------
CHOSEN_OLDN=""; CHOSEN_NEWN=""; CHOSEN_P=""; CHOSEN_SRC=""

choose_layout() {
  summarize_raidz_groups
  local HIST_P="" HIST_OLDN="" HIST_NEWN=""
  if [[ -z "$USER_OLDN$USER_NEWN$USER_P" ]]; then
    read -r HIST_P HIST_OLDN HIST_NEWN <<<"$(infer_from_history "$POOL" || true)"
  fi

  # 1) user overrides
  if [[ -n "$USER_P" || -n "$USER_OLDN" || -n "$USER_NEWN" ]]; then
    CHOSEN_P="${USER_P:-${HIST_P:-${MAX_P:-}}}"
    CHOSEN_OLDN="${USER_OLDN:-${HIST_OLDN:-${MIN_N:-}}}"
    CHOSEN_NEWN="${USER_NEWN:-${HIST_NEWN:-${MAX_N:-}}}"
    CHOSEN_SRC="user"
  # 2) history
  elif [[ -n "$HIST_P$HIST_OLDN$HIST_NEWN" ]]; then
    CHOSEN_P="${HIST_P:-${MAX_P:-}}"
    CHOSEN_OLDN="${HIST_OLDN:-${MIN_N:-}}"
    CHOSEN_NEWN="${HIST_NEWN:-${MAX_N:-}}"
    CHOSEN_SRC="history"
  # 3) status
  elif [[ -n "${MIN_N:-}" || -n "${MAX_N:-}" || -n "${MAX_P:-}" ]]; then
    CHOSEN_P="${MAX_P:-2}"
    CHOSEN_OLDN="${MIN_N:-${MAX_N:-0}}"
    CHOSEN_NEWN="${MAX_N:-${MIN_N:-0}}"
    CHOSEN_SRC="status"
  fi

  # 4) fallback: single-drive expansion (if still ambiguous)
  # Use current width (MAX_N if present) as new; old = new - 1
  if [[ -z "$CHOSEN_NEWN" || "$CHOSEN_NEWN" == "0" ]]; then
    CHOSEN_NEWN="${MAX_N:-0}"
  fi
  if [[ -z "$CHOSEN_OLDN" || "$CHOSEN_OLDN" == "0" ]]; then
    if [[ -n "$CHOSEN_NEWN" && "$CHOSEN_NEWN" != "0" ]]; then
      CHOSEN_OLDN="$(( CHOSEN_NEWN - 1 ))"
    fi
    CHOSEN_SRC="${CHOSEN_SRC:-fallback-single-drive}"
  fi
  if [[ -z "$CHOSEN_P" || "$CHOSEN_P" == "0" ]]; then
    CHOSEN_P="${MAX_P:-2}"
    [[ -z "$CHOSEN_SRC" ]] && CHOSEN_SRC="fallback-single-drive"
  fi

  # sanity: ensure > parity
  if (( CHOSEN_OLDN <= CHOSEN_P )); then CHOSEN_OLDN=$(( CHOSEN_P + 1 )); fi
  if (( CHOSEN_NEWN <= CHOSEN_P )); then CHOSEN_NEWN=$(( CHOSEN_P + 1 )); fi

  log "[layout] chosen Old N=${CHOSEN_OLDN}, New N=${CHOSEN_NEWN}, P=${CHOSEN_P} (source=${CHOSEN_SRC})"
}

# ---------- scanner ----------
scan_dataset() {
  local ds="$1" threshold_txg="$2"
  local old=0 new=0 skipped=0 lines=0
  local last_ts="$(date +%s)" now last_bytes=0 bytes_trigger=0
  (( PROG_GIB > 0 )) && bytes_trigger="$(bytes_from_gib "$PROG_GIB")"

  # Sizes and denom
  local denom_bytes ds_only snaps
  read -r denom_bytes ds_only snaps <<<"$(dataset_size_line "$ds")"
  local logical_bytes=$(( ${ds_only:-0} + ${snaps:-0} ))

  # Per-dataset estimate header using chosen Old/New N,P
  if (( logical_bytes > 0 )); then
    local old_est new_est diff pct=""
    old_est="$(round_mul_parity "$logical_bytes" "$CHOSEN_OLDN" "$CHOSEN_P")"
    new_est="$(round_mul_parity "$logical_bytes" "$CHOSEN_NEWN" "$CHOSEN_P")"
    diff=$(( old_est - new_est )); (( diff < 0 )) && diff=0
    if (( old_est > 0 )); then pct=$(awk -v d="$diff" -v o="$old_est" 'BEGIN{printf "~%.2f%%", (d*100)/o}'); fi
    if (( CHOSEN_OLDN == CHOSEN_NEWN )); then
      log2 "[estimate] $ds Old N,P: N=${CHOSEN_OLDN},P=${CHOSEN_P}; est_physical=$(human "$old_est") | New N,P: N=${CHOSEN_NEWN},P=${CHOSEN_P}; est_physical=$(human "$new_est") | potential savings: ~0 (same width)"
    else
      log2 "[estimate] $ds Old N,P: N=${CHOSEN_OLDN},P=${CHOSEN_P}; est_physical=$(human "$old_est") | New N,P: N=${CHOSEN_NEWN},P=${CHOSEN_P}; est_physical=$(human "$new_est") | potential savings: $(human "$diff") ($pct)"
    fi
  else
    log2 "[estimate] $ds Unable to compute estimates (logical size=0)"
  fi

  $PROGRESS && log2 "[progress] scanning $ds ..."

  while read -r size_hex birth; do
    ((lines++))
    if [[ -z "$size_hex" || -z "$birth" ]]; then ((skipped++)); continue; fi
    local sz=$((16#$size_hex))
    if (( birth < threshold_txg )); then old=$((old+sz)); else new=$((new+sz)); fi

    if $PROGRESS && (( lines % CHECK_LINES == 0 )); then
      now="$(date +%s)"; local total=$((old+new))
      local time_ok=$(( now - last_ts >= PROG_SECONDS ? 1 : 0 ))
      local bytes_ok=0; (( PROG_GIB > 0 && total - last_bytes >= bytes_trigger )) && bytes_ok=1
      if (( time_ok==1 || bytes_ok==1 )); then
        log2 "[progress] $ds  Total=$(human "$total")/$(human "${denom_bytes:-0}")  OLD=$(human "$old")  NEW=$(human "$new")"
        last_ts="$now"; last_bytes="$total"
      fi
    fi
  done < <(
    zdb -ddddd "$ds" 2>/dev/null \
      | awk 'BEGIN{IGNORECASE=1}
             { if (match($0,/([0-9a-fA-F]+):([0-9a-fA-F]+):([0-9a-fA-F]+)/,m) &&
                   match($0,/B=([0-9]+)\//,b)) { printf "%s %s\n", m[3], b[1] } }' \
      || true
  )

  local processed=$((old+new))
  $PROGRESS && log2 "[progress] done $ds  Total=$(human "$processed")/$(human "${denom_bytes:-0}")  OLD=$(human "$old")  NEW=$(human "$new")"
  printf "%d %d %d %d %d\n" "$old" "$new" "$processed" "$denom_bytes" "$logical_bytes"
}

# ---------- self-test ----------
self_test(){
  log "[self-test] tools…"; for cmd in zdb zfs awk date; do command -v "$cmd" >/dev/null 2>&1 && log "  found $(command -v "$cmd")" || log "  MISSING '$cmd'"; done
  log "[self-test] zfs list -H -o name $POOL | head"; zfs list -H -o name "$POOL" 2>&1 | head -n 10 >&2 || log "  'zfs list' failed"
  local any_ds; any_ds="$(zfs list -r -H -o name "$POOL" 2>/dev/null | head -n1 || true)"
  if [[ -n "$any_ds" ]]; then log "[self-test] zdb -ddddd $any_ds | head (40 lines)"; zdb -ddddd "$any_ds" 2>&1 | head -n 40 >&2 || log "  zdb failed"; else log "  no datasets under '$POOL'"; fi
}

main() {
  parse_args "$@"
  choose_layout  # sets CHOSEN_OLDN/NEWN/P and CHOSEN_SRC
  summarize_raidz_groups

  {
    echo "ZFS Old/New Layout Audit"
    echo "Started:       $(ts)"
    echo "Pool:          $POOL"
    if [[ -n "$WHEN" ]]; then echo "Expansion time: $WHEN  -> TXG: $TXG"; else echo "Expansion TXG:  $TXG"; fi
    echo "Output log:    $LOGFILE"
    echo "Recursion:     $($RECURSE && echo enabled || echo disabled)"
    if (( ${#REM_ARGS[@]} > 0 )); then echo "Explicit datasets provided (${#REM_ARGS[@]}): ${REM_ARGS[*]}"; $RECURSE && echo "Note: explicit datasets are ignored when recursion is enabled."; else echo "Explicit datasets: none (defaults to all under pool)"; fi
    echo "Detected RAIDZ parities: ${RAIDZ_PARITIES:-none}"
    echo "Detected widths (N):     ${RAIDZ_WIDTHS:-none}"
    echo "Assumed Old/New/Parity:  N_old=${CHOSEN_OLDN}, N_new=${CHOSEN_NEWN}, P=${CHOSEN_P}  (source=${CHOSEN_SRC})"
    echo "Progress:      $($PROGRESS && echo enabled || echo disabled)"
    echo "  - progress seconds: $PROG_SECONDS"
    echo "  - progress GiB:     $PROG_GIB"
    echo "  - check lines:      $CHECK_LINES"
    echo "  - denom mode:       $DENOM_MODE"
    echo "  - progress to log:  $($PROGRESS_TO_LOG && echo yes || echo no)"
    echo "  - debug tracing:    $($DEBUG && echo on || echo off)"
    echo
    echo "Dataset, OLD_layout_bytes, NEW_layout_bytes, OLD%, NEW%"
  } | tee "$LOGFILE"

  $SELFTEST && self_test

  mapfile -t DS_LIST < <(datasets_to_scan || true)
  if (( ${#DS_LIST[@]} == 0 )); then log "[progress] no datasets found under pool '$POOL'"; echo "Finished: $(ts)" | tee -a "$LOGFILE"; exit 0; fi
  log2 "[progress] will scan ${#DS_LIST[@]} dataset(s)"

  local total_old=0 total_new=0 total_processed=0 total_denomsum=0 total_logical=0
  for ds in "${DS_LIST[@]}"; do
    [[ "$ds" == *@* ]] && continue
    if read -r old new processed denom logical <<<"$(scan_dataset "$ds" "$TXG" || true)"; then :; else log2 "[warn] scan failed for $ds (skipping)"; continue; fi
    local ds_total=$((old+new)) old_pct="0.00" new_pct="0.00"
    if (( ds_total > 0 )); then
      old_pct=$(awk -v o="$old" -v t="$ds_total" 'BEGIN{printf "%.2f", (o*100)/t}')
      new_pct=$(awk -v n="$new" -v t="$ds_total" 'BEGIN{printf "%.2f", (n*100)/t}')
    fi
    { printf "%s, %d, %d, %s, %s\n" "$ds" "$old" "$new" "$old_pct" "$new_pct"
      echo "  - $ds    Total=$(human "$processed")/$(human "${denom:-0}")   OLD=$(human "$old")   NEW=$(human "$new")"; } | tee -a "$LOGFILE"
    total_old=$((total_old+old)); total_new=$((total_new+new)); total_processed=$((total_processed+processed)); total_denomsum=$((total_denomsum+denom)); total_logical=$((total_logical+logical))
  done

  local grand=$((total_old+total_new))
  local overall_old_pct="" overall_new_pct=""
  if (( grand > 0 )); then
    overall_old_pct=$(awk -v o="$total_old" -v t="$grand" 'BEGIN{printf "%.2f%%", (o*100)/t}')
    overall_new_pct=$(awk -v n="$total_new" -v t="$grand" 'BEGIN{printf "%.2f%%", (n*100)/t}')
  fi

  # Overall estimated physical if all old vs all new (using chosen N/P)
  local overall_est_old=0 overall_est_new=0 overall_savings=0 overall_pct=""
  if (( total_logical > 0 )); then
    overall_est_old="$(round_mul_parity "$total_logical" "$CHOSEN_OLDN" "$CHOSEN_P")"
    overall_est_new="$(round_mul_parity "$total_logical" "$CHOSEN_NEWN" "$CHOSEN_P")"
    overall_savings=$(( overall_est_old - overall_est_new )); (( overall_savings < 0 )) && overall_savings=0
    if (( overall_est_old > 0 )); then overall_pct=$(awk -v d="$overall_savings" -v o="$overall_est_old" 'BEGIN{printf "~%.2f%%", (d*100)/o}'); fi
  fi

  { echo; echo "Overall:"
    echo "  OLD-layout total: $(human "$total_old")"
    echo "  NEW-layout total: $(human "$total_new")"
    echo "  Processed total:  $(human "$total_processed")"
    echo "  Denominator sum:  $(human "$total_denomsum")   # sum of denominators per dataset"
    [[ -n "$overall_old_pct" ]] && echo "  OLD %: $overall_old_pct"
    [[ -n "$overall_new_pct" ]] && echo "  NEW %: $overall_new_pct"
    if (( overall_est_old > 0 )); then
      echo "  Estimate if ALL data were on OLD width (N=${CHOSEN_OLDN},P=${CHOSEN_P}): $(human "$overall_est_old")"
      echo "  Estimate if ALL data were on NEW width (N=${CHOSEN_NEWN},P=${CHOSEN_P}): $(human "$overall_est_new")"
      echo "  Potential savings by rewriting to NEW width: $(human "$overall_savings") ${overall_pct:+($overall_pct)}"
    else
      echo "  (Savings estimate unavailable: missing logical total or N/P)"
    fi
    echo "Finished: $(ts)"; } | tee -a "$LOGFILE"

  echo; echo "Log saved to: $LOGFILE"
}

main "$@"
