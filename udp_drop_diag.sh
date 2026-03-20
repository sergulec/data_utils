#!/usr/bin/env bash
# udp_drop_diag.sh
#
# Gather diagnostics for intermittent UDP packet drops reported by a consumer.
# Safe for production: read-only commands only.
#
# Usage:
#   ./udp_drop_diag.sh -i eth0 -p 12345
#   ./udp_drop_diag.sh -i eth0 -n feed_receiver --port 18000 --interval 10

set -euo pipefail

INTERVAL=5
IFACE=""
PID=""
PROC_NAME=""
UDP_PORT=""
OUTDIR=""

usage() {
  cat <<'EOF'
Usage: udp_drop_diag.sh -i <iface> [-p <pid> | -n <process_name>] [--port <udp_port>] [--interval <seconds>] [-o <outdir>]

Required:
  -i, --iface <iface>          Network interface (e.g. eth0, enp3s0)

Optional:
  -p, --pid <pid>              Target process PID
  -n, --name <process_name>    Target process name / pattern (first pgrep match used)
      --port <udp_port>        UDP port used by the consumer
      --interval <seconds>     Seconds between sample 1 and sample 2 (default: 5)
  -o, --outdir <dir>           Output directory (default: ./udp_drop_diag_<ts>)
  -h, --help                   Show help
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -i|--iface) IFACE="$2"; shift 2 ;;
      -p|--pid) PID="$2"; shift 2 ;;
      -n|--name) PROC_NAME="$2"; shift 2 ;;
      --port) UDP_PORT="$2"; shift 2 ;;
      --interval) INTERVAL="$2"; shift 2 ;;
      -o|--outdir) OUTDIR="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) echo "ERROR: unknown arg '$1'" >&2; usage; exit 1 ;;
    esac
  done
}

run_sh() {
  local outfile="$1"; shift
  {
    echo "### CMD: $*"
    echo "### TS: $(date -Is)"
    echo
    bash -lc "$*" 2>&1 || true
  } > "$outfile"
}

log() {
  echo "[$(date +%H:%M:%S)] $*"
}

require_iface() {
  if [[ -z "$IFACE" ]]; then
    echo "ERROR: --iface is required" >&2
    usage
    exit 1
  fi
  if [[ ! -d "/sys/class/net/$IFACE" ]]; then
    echo "ERROR: interface '$IFACE' does not exist" >&2
    exit 1
  fi
}

resolve_pid() {
  if [[ -n "$PID" ]]; then
    [[ "$PID" =~ ^[0-9]+$ ]] || { echo "ERROR: PID must be numeric" >&2; exit 1; }
    return
  fi
  if [[ -n "$PROC_NAME" ]]; then
    PID="$(pgrep -f "$PROC_NAME" | head -n1 || true)"
  fi
}

extract_netstat_udp_value() {
  local pattern="$1"
  local file="$2"
  awk -v pat="$pattern" '
    BEGIN{IGNORECASE=1}
    $0 ~ pat {
      for(i=1;i<=NF;i++){
        if($i ~ /^[0-9]+$/){ print $i; exit }
      }
    }
  ' "$file" | head -n1
}

softnet_sum_drops() {
  local file="$1"
  awk '
    {
      val = strtonum("0x"$2)
      s += val
    }
    END{ print s+0 }
  ' "$file"
}

sample_collect() {
  local label="$1"
  local dir="$OUTDIR/$label"
  mkdir -p "$dir"

  log "Collecting $label"

  run_sh "$dir/date.txt" "date -Is"
  run_sh "$dir/uptime.txt" "uptime"
  run_sh "$dir/ip_link.txt" "ip -s link show dev $IFACE"
  run_sh "$dir/ethtool_link.txt" "ethtool $IFACE"
  run_sh "$dir/ethtool_stats.txt" "ethtool -S $IFACE"
  run_sh "$dir/ethtool_ring.txt" "ethtool -g $IFACE"
  run_sh "$dir/ethtool_channels.txt" "ethtool -l $IFACE"
  run_sh "$dir/ethtool_coalesce.txt" "ethtool -c $IFACE"
  run_sh "$dir/interrupts_iface.txt" "grep -iE '$IFACE|mlx|ixgbe|i40e|ice|ena|virtio|igb|e1000' /proc/interrupts || true"
  run_sh "$dir/netstat_su.txt" "netstat -su"
  run_sh "$dir/ss_udp_summary.txt" "ss -u -s"
  if [[ -n "$UDP_PORT" ]]; then
    run_sh "$dir/ss_udp_port.txt" "ss -uapni | grep -E '[:.]$UDP_PORT\\b|:$UDP_PORT ' || true"
  else
    run_sh "$dir/ss_udp_all.txt" "ss -uapni | head -n 200"
  fi
  run_sh "$dir/softnet_stat.txt" "cat /proc/net/softnet_stat"
  run_sh "$dir/sysctl_sockbuf.txt" "sysctl net.core.rmem_default net.core.rmem_max net.core.netdev_max_backlog net.ipv4.udp_mem net.ipv4.udp_rmem_min"
  run_sh "$dir/mpstat_all.txt" "mpstat -P ALL 1 3"
  run_sh "$dir/vmstat.txt" "vmstat 1 3"
  run_sh "$dir/top_head.txt" "top -b -n 1 | head -n 40"
  run_sh "$dir/proc_cmdline.txt" "cat /proc/cmdline"

  if [[ -n "$PID" ]]; then
    run_sh "$dir/process_cmdline.txt" "tr '\\0' ' ' < /proc/$PID/cmdline; echo"
    run_sh "$dir/process_status.txt" "cat /proc/$PID/status"
    run_sh "$dir/process_limits.txt" "cat /proc/$PID/limits"
    run_sh "$dir/process_taskset.txt" "taskset -cp $PID"
    run_sh "$dir/process_ps.txt" "ps -o pid,ppid,psr,pcpu,pmem,stat,comm,args -p $PID"
    run_sh "$dir/process_threads.txt" "ps -eLo pid,tid,psr,pcpu,stat,comm | awk '\$1 == $PID'"
    run_sh "$dir/process_pidstat_cpu.txt" "pidstat -u -w -t -p $PID 1 3"
    run_sh "$dir/process_pidstat_mem.txt" "pidstat -r -p $PID 1 3"
  else
    echo "No PID selected" > "$dir/process_none.txt"
  fi

  run_sh "$dir/dmesg_net.txt" "dmesg --color=never | egrep -i '$IFACE|mlx|ixgbe|i40e|ice|ena|bnx|virtio|UDP|NETDEV|softnet|drop|overrun|missed|rx' | tail -n 200 || true"
}

write_summary() {
  local s1="$OUTDIR/sample1"
  local s2="$OUTDIR/sample2"
  local summary="$OUTDIR/SUMMARY.txt"

  rb1="$(extract_netstat_udp_value 'receive buffer errors' "$s1/netstat_su.txt" || true)"
  rb2="$(extract_netstat_udp_value 'receive buffer errors' "$s2/netstat_su.txt" || true)"
  pe1="$(extract_netstat_udp_value 'packet receive errors' "$s1/netstat_su.txt" || true)"
  pe2="$(extract_netstat_udp_value 'packet receive errors' "$s2/netstat_su.txt" || true)"
  rb1="${rb1:-0}"; rb2="${rb2:-0}"
  pe1="${pe1:-0}"; pe2="${pe2:-0}"

  sd1="$(softnet_sum_drops "$s1/softnet_stat.txt" || echo 0)"
  sd2="$(softnet_sum_drops "$s2/softnet_stat.txt" || echo 0)"

  {
    echo "UDP Drop Diagnostic Summary"
    echo "Generated: $(date -Is)"
    echo "Interface: $IFACE"
    [[ -n "$PID" ]] && echo "PID: $PID"
    [[ -n "$UDP_PORT" ]] && echo "UDP port: $UDP_PORT"
    echo "Interval: ${INTERVAL}s"
    echo

    echo "Process affinity / CPU placement"
    if [[ -n "$PID" ]]; then
      sed -n '1,120p' "$s1/process_taskset.txt"
      echo
      sed -n '1,120p' "$s1/process_ps.txt"
      echo
      sed -n '1,200p' "$s1/process_threads.txt"
      echo
    else
      echo "No PID selected."
      echo
    fi

    echo "NIC interrupt placement"
    sed -n '1,200p' "$s1/interrupts_iface.txt"
    echo

    echo "Kernel UDP counters"
    echo "receive buffer errors: $rb1 -> $rb2 (delta $((rb2-rb1)))"
    echo "packet receive errors: $pe1 -> $pe2 (delta $((pe2-pe1)))"
    echo

    echo "softnet backlog drops"
    echo "softnet dropped sum: $sd1 -> $sd2 (delta $((sd2-sd1)))"
    echo

    echo "Interpretation"
    echo "- receive buffer errors rising => UDP socket buffer overflow / app not draining fast enough."
    echo "- packet receive errors rising => kernel/network receive path issue."
    echo "- softnet drops rising => softirq/backlog issue, CPU pressure, or burst traffic."
    echo "- NIC counters rising (see ethtool -S / ip -s link) => drop before socket layer."
    echo "- If none of the above move but app reports gaps => app-level sequencing / queue handoff / parsing issue."
    echo

    echo "Recommended files to inspect"
    echo "- $s1/ethtool_stats.txt"
    echo "- $s1/interrupts_iface.txt"
    echo "- $s1/process_threads.txt"
    echo "- $s1/process_pidstat_cpu.txt"
    echo "- $s1/process_pidstat_mem.txt"
    echo "- $s1/sysctl_sockbuf.txt"
    echo "- $s1/mpstat_all.txt"
    echo
  } > "$summary"
}

main() {
  parse_args "$@"
  require_iface
  resolve_pid

  if [[ -z "$OUTDIR" ]]; then
    OUTDIR="./udp_drop_diag_$(date +%Y%m%d_%H%M%S)"
  fi
  mkdir -p "$OUTDIR"

  log "Output directory: $OUTDIR"
  [[ -n "$PID" ]] && log "Using PID: $PID"

  sample_collect sample1
  log "Sleeping ${INTERVAL}s"
  sleep "$INTERVAL"
  sample_collect sample2
  write_summary

  log "Done"
  log "Summary: $OUTDIR/SUMMARY.txt"
}

main "$@"
