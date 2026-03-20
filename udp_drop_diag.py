
#!/usr/bin/env python3
from __future__ import annotations
import argparse
import csv
import json
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

DEFAULT_ISOLATED_THREAD_REGEX = r"udp_recv"

def run(cmd: str) -> str:
    try:
        p = subprocess.run(cmd, shell=True, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        return p.stdout
    except Exception as e:
        return f"ERROR: {e}\n"

def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", errors="replace")

def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def resolve_pid(name, pid):
    if pid is not None:
        return pid
    if not name:
        return None
    out = run(f"pgrep -f {shlex.quote(name)} | head -n1").strip()
    return int(out) if out.isdigit() else None

def collect(sample_dir: Path, iface: str, pid, port, group, src_ip, do_tcpdump: bool, tcpdump_count: int):
    cmds = {
        "date.txt": "date -Is",
        "uptime.txt": "uptime",
        "ip_link.txt": f"ip -s link show dev {shlex.quote(iface)}",
        "ethtool_stats.txt": f"ethtool -S {shlex.quote(iface)}",
        "ethtool_ring.txt": f"ethtool -g {shlex.quote(iface)}",
        "ethtool_channels.txt": f"ethtool -l {shlex.quote(iface)}",
        "ethtool_coalesce.txt": f"ethtool -c {shlex.quote(iface)}",
        "interrupts_iface.txt": f"grep -iE '{re.escape(iface)}|mlx|ixgbe|i40e|ice|ena|virtio|igb|e1000' /proc/interrupts || true",
        "netstat_su.txt": "netstat -su || true",
        "ss_udp_summary.txt": "ss -u -s || true",
        "softnet_stat.txt": "cat /proc/net/softnet_stat",
        "sysctl_sockbuf.txt": "sysctl net.core.rmem_default net.core.rmem_max net.core.netdev_max_backlog net.ipv4.udp_mem net.ipv4.udp_rmem_min 2>&1 || true",
        "mpstat_all.txt": "mpstat -P ALL 1 3 || true",
        "vmstat.txt": "vmstat 1 3 || true",
        "ip_maddr.txt": f"ip maddr show dev {shlex.quote(iface)} || true",
        "top_head.txt": "top -b -n 1 | head -n 40 || true",
    }

    if port is not None:
        cmds["ss_udp_port.txt"] = f"ss -uapni | grep -E '[:.]{port}\\b|:{port} ' || true"
    if group and port is not None:
        cmds["ss_udp_endpoint.txt"] = f"ss -uapni | grep -F '{group}' | grep -E '[:.]{port}\\b|:{port} ' || true"
    elif group:
        cmds["ss_udp_group.txt"] = f"ss -uapni | grep -F '{group}' || true"
    if src_ip:
        cmds["ss_udp_srcip.txt"] = f"ss -uapni | grep -F '{src_ip}' || true"

    if pid is not None:
        cmds["process_taskset.txt"] = f"taskset -cp {pid} || true"
        cmds["process_ps.txt"] = f"ps -o pid,ppid,psr,pcpu,pmem,stat,comm,args -p {pid} || true"
        cmds["process_threads.txt"] = f"ps -eLo pid,tid,psr,pcpu,stat,comm | awk '$1 == {pid}' || true"
        cmds["process_pidstat_cpu.txt"] = f"pidstat -u -w -t -p {pid} 1 3 || true"
        cmds["process_pidstat_mem.txt"] = f"pidstat -r -p {pid} 1 3 || true"

    if do_tcpdump:
        filt = "udp"
        if group:
            filt += f" and host {shlex.quote(group)}"
        if src_ip:
            filt += f" and host {shlex.quote(src_ip)}"
        if port is not None:
            filt += f" and port {int(port)}"
        cmds["tcpdump_snapshot.txt"] = f"tcpdump -ni {shlex.quote(iface)} -c {int(tcpdump_count)} {filt} 2>&1 || true"

    for name, cmd in cmds.items():
        write(sample_dir / name, f"### CMD: {cmd}\n### TS: {datetime.now().isoformat()}\n\n" + run(cmd))

def parse_udp(text: str):
    low = text.lower()
    def get(patterns):
        for line in low.splitlines():
            for p in patterns:
                if p in line:
                    m = re.search(r"(\d+)", line)
                    if m:
                        return int(m.group(1))
        return 0
    return {
        "receive_buffer_errors": get(["receive buffer errors", "rcvbuferrors"]),
        "packet_receive_errors": get(["packet receive errors"]),
        "unknown_port": get(["packets to unknown port received"]),
    }

def parse_softnet(text: str):
    total = 0
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            try:
                total += int(parts[1], 16)
            except Exception:
                pass
    return total

def parse_ip_link(text: str):
    out = {"rx_errors": 0, "rx_dropped": 0}
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if "RX:" in line and i + 1 < len(lines):
            vals = lines[i + 1].split()
            if len(vals) >= 4:
                try:
                    out["rx_errors"] = int(vals[2]); out["rx_dropped"] = int(vals[3])
                except Exception:
                    pass
    return out

def parse_ethtool(text: str):
    stats = {}
    keys = ["rx_dropped","rx_missed_errors","rx_no_buffer_count","rx_errors","rx_missed","rx_discards","alloc_rx_page_failed","alloc_rx_buff_failed","rx_out_of_buffer"]
    for line in text.splitlines():
        if ":" not in line:
            continue
        n, v = line.split(":", 1)
        n = n.strip(); v = v.strip()
        if any(k in n for k in keys):
            m = re.search(r"(-?\d+)$", v)
            if m:
                stats[n] = int(m.group(1))
    return stats

def parse_taskset(text: str):
    m = re.search(r"affinity list:\s*(.+)", text)
    return m.group(1).strip() if m else None

def parse_threads(text: str):
    threads = []
    for line in text.splitlines():
        if re.match(r"\s*\d+\s+\d+\s+\d+\s+", line):
            parts = line.split()
            if len(parts) >= 6:
                try:
                    threads.append({
                        "pid": int(parts[0]),
                        "tid": int(parts[1]),
                        "psr": int(parts[2]),
                        "pcpu": float(parts[3]),
                        "stat": parts[4],
                        "comm": parts[5],
                    })
                except Exception:
                    pass
    return threads

def parse_interrupt_cpus(text: str):
    out = set()
    for line in text.splitlines():
        parts = line.split()
        if not parts or ":" not in parts[0]:
            continue
        counts = []
        for p in parts[1:]:
            if p.isdigit():
                counts.append(int(p))
            else:
                break
        if counts:
            mx = max(counts)
            if mx > 0:
                for idx, val in enumerate(counts):
                    if val == mx:
                        out.add(idx)
    return sorted(out)

def parse_sysctl(text: str):
    vals = {}
    for line in text.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            k = k.strip(); v = v.strip()
            vals[k] = int(v) if re.match(r"^-?\d+$", v) else None
    return vals

def parse_ss_lines(text: str):
    return len([l for l in text.splitlines() if l.strip() and not l.startswith("###")])

def delta(a, b):
    keys = set(a) | set(b)
    return {k: b.get(k, 0) - a.get(k, 0) for k in keys}

def load_baseline(path: Path | None):
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}

def save_baseline(path: Path | None, record):
    if not path:
        return
    if path.exists():
        try:
            data = json.loads(path.read_text())
        except Exception:
            data = {"runs": []}
    else:
        data = {"runs": []}
    data.setdefault("runs", []).append(record)
    data["last"] = record
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))

def get_baseline_rate(baseline, key):
    try:
        return float(baseline["last"]["rates"][key])
    except Exception:
        return None

def append_csv_history(path: Path | None, row: dict):
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(row.keys())
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        w.writerow(row)

def analyze(outdir: Path, iface: str, pid, port, group, src_ip, interval: int, baseline, isolated_thread_regex: str):
    s1 = outdir / "sample1"
    s2 = outdir / "sample2"

    net1 = parse_udp((s1/"netstat_su.txt").read_text(errors="replace"))
    net2 = parse_udp((s2/"netstat_su.txt").read_text(errors="replace"))
    netd = delta(net1, net2)

    soft1 = parse_softnet((s1/"softnet_stat.txt").read_text(errors="replace"))
    soft2 = parse_softnet((s2/"softnet_stat.txt").read_text(errors="replace"))
    softd = soft2 - soft1

    ip1 = parse_ip_link((s1/"ip_link.txt").read_text(errors="replace"))
    ip2 = parse_ip_link((s2/"ip_link.txt").read_text(errors="replace"))
    ipd = delta(ip1, ip2)

    eth1 = parse_ethtool((s1/"ethtool_stats.txt").read_text(errors="replace"))
    eth2 = parse_ethtool((s2/"ethtool_stats.txt").read_text(errors="replace"))
    ethd = delta(eth1, eth2)
    ethd_pos = {k:v for k,v in ethd.items() if v > 0}

    sysctls = parse_sysctl((s1/"sysctl_sockbuf.txt").read_text(errors="replace"))
    taskset = parse_taskset((s1/"process_taskset.txt").read_text(errors="replace")) if pid else None
    threads = parse_threads((s1/"process_threads.txt").read_text(errors="replace")) if pid else []
    intr_cpus = parse_interrupt_cpus((s1/"interrupts_iface.txt").read_text(errors="replace"))

    ss_text = ""
    for name in ["ss_udp_endpoint.txt", "ss_udp_group.txt", "ss_udp_port.txt", "ss_udp_srcip.txt", "ss_udp_summary.txt"]:
        p = s1 / name
        if p.exists():
            ss_text += p.read_text(errors="replace") + "\n"
    matching_socket_lines = parse_ss_lines(ss_text)

    rates = {
        "receive_buffer_errors_per_sec": netd.get("receive_buffer_errors", 0) / max(interval, 1),
        "packet_receive_errors_per_sec": netd.get("packet_receive_errors", 0) / max(interval, 1),
        "softnet_drops_per_sec": softd / max(interval, 1),
        "rx_dropped_per_sec": ipd.get("rx_dropped", 0) / max(interval, 1),
    }

    findings, causes, steps, significance = [], [], [], []

    if matching_socket_lines > 0:
        findings.append(f"Endpoint/socket filters matched {matching_socket_lines} socket line(s).")
    else:
        findings.append("Specified endpoint was not clearly visible in socket inspection output.")
        steps.append("Verify group/port/process inputs and permissions for socket inspection.")

    isolated_threads = []
    nonisolated_threads = []
    if threads and isolated_thread_regex:
        pat = re.compile(isolated_thread_regex)
        for t in threads:
            if pat.search(t["comm"]):
                isolated_threads.append(t)
            else:
                nonisolated_threads.append(t)
        if isolated_threads:
            isolated_cpus = sorted({t["psr"] for t in isolated_threads})
            thread_desc = ", ".join([f"{t['comm']}[tid={t['tid']}]@CPU{t['psr']}" for t in isolated_threads[:8]])
            findings.append(f"Threads matching isolation regex '{isolated_thread_regex}' found: {thread_desc}")
            if len(isolated_cpus) > 1:
                findings.append(f"Isolated-thread candidates are spread across multiple CPUs: {isolated_cpus}.")
                causes.append(("threads that should be isolated are not staying on one CPU", 85))
                steps.append("Ensure udp_recv-like threads are pinned/affined to a single intended CPU.")
        else:
            findings.append(f"No threads matched isolation regex '{isolated_thread_regex}'.")
            steps.append("Confirm thread names match the regex or override it with --isolated-thread-regex.")

    if netd.get("receive_buffer_errors", 0) > 0:
        findings.append(f"UDP receive buffer errors increased by {netd['receive_buffer_errors']} ({rates['receive_buffer_errors_per_sec']:.2f}/s).")
        causes.append(("socket buffer overflow or application not draining packets fast enough", 95))
        steps.append("Inspect receiver-thread work after recv(), including parsing, logging, and queue handoff.")
        steps.append("Verify SO_RCVBUF and compare against net.core.rmem_max.")
    if netd.get("packet_receive_errors", 0) > 0:
        findings.append(f"Kernel packet receive errors increased by {netd['packet_receive_errors']} ({rates['packet_receive_errors_per_sec']:.2f}/s).")
        causes.append(("kernel/network receive path issue", 85))
    if softd > 0:
        findings.append(f"softnet backlog drops increased by {softd} ({rates['softnet_drops_per_sec']:.2f}/s).")
        causes.append(("softirq/backlog overload under burst traffic", 90))
        steps.append("Check IRQ placement, softirq load, and net.core.netdev_max_backlog.")
    if ethd_pos:
        findings.append("NIC/driver drop-related counters increased: " + ", ".join([f"{k}={v}" for k,v in sorted(ethd_pos.items())]))
        causes.append(("NIC ring/driver/hardware receive-side drop before socket layer", 92))
        steps.append("Inspect RX ring sizing with ethtool -g and compare to burst size.")
    if ipd.get("rx_dropped", 0) > 0 or ipd.get("rx_errors", 0) > 0:
        findings.append(f"Interface RX counters increased: dropped={ipd.get('rx_dropped',0)}, errors={ipd.get('rx_errors',0)}.")
        causes.append(("host/interface dropping packets before userspace", 88))

    if pid and taskset:
        findings.append(f"Process affinity list: {taskset}")
        if isolated_threads:
            iso_cpus = sorted({t["psr"] for t in isolated_threads})
            if intr_cpus and iso_cpus and not any(cpu in intr_cpus for cpu in iso_cpus):
                findings.append(f"NIC interrupts appear busiest on CPU(s) {intr_cpus}, while isolated receiver thread CPU(s) are {iso_cpus}.")
                causes.append(("IRQ / receiver CPU misalignment", 80))
                steps.append("Align IRQ affinity with the receiver CPU or queue model.")
        elif intr_cpus:
            all_thread_cpus = sorted({t["psr"] for t in threads})
            if all_thread_cpus and all_thread_cpus[0] not in intr_cpus:
                findings.append(f"NIC interrupts appear busiest on CPU(s) {intr_cpus}, while process thread CPU(s) include {all_thread_cpus}.")
                causes.append(("IRQ / application CPU misalignment", 70))

    if sysctls.get("net.core.rmem_max") is not None and sysctls["net.core.rmem_max"] < 8 * 1024 * 1024:
        findings.append(f"net.core.rmem_max looks relatively small at {sysctls['net.core.rmem_max']} bytes.")
        causes.append(("system receive buffer cap may be too small for bursty UDP", 60))

    for key, current in rates.items():
        base = get_baseline_rate(baseline, key)
        if base is None:
            significance.append(f"No baseline available yet for {key}.")
        elif base == 0 and current > 0:
            significance.append(f"{key} is non-zero now but was zero in the last baseline run.")
        elif base > 0:
            ratio = current / base
            if ratio >= 5:
                significance.append(f"{key} is {ratio:.1f}x above the last baseline rate.")
            elif current > 0 and ratio <= 0.2:
                significance.append(f"{key} is below the last baseline rate ({current:.2f}/s vs {base:.2f}/s).")

    if not findings:
        findings.append("No decisive host-side kernel/NIC counters moved during the sampling window.")
        causes.append(("application-level parsing, sequencing, queue handoff, or upstream issue outside the host", 70))
        steps.append("Correlate application sequence gaps with host-side packet capture for this endpoint.")

    dedup = {}
    for c, s in causes:
        dedup[c] = max(dedup.get(c, 0), s)
    causes_sorted = sorted(dedup.items(), key=lambda x: x[1], reverse=True)

    lines = []
    lines.append("UDP Drop Diagnostic Summary v3")
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append(f"Interface: {iface}")
    if pid is not None:
        lines.append(f"PID: {pid}")
    if group:
        lines.append(f"Group/Destination IP: {group}")
    if port is not None:
        lines.append(f"UDP port: {port}")
    if src_ip:
        lines.append(f"Source IP filter: {src_ip}")
    lines.append(f"Isolated-thread regex: {isolated_thread_regex}")
    lines.append("")
    lines.append("Key findings:")
    for f in findings:
        lines.append(f"- {f}")
    lines.append("")
    lines.append("Most likely issue(s):")
    for c, s in causes_sorted[:5]:
        lines.append(f"- {c} (confidence: {s}%)")
    lines.append("")
    lines.append("Rates and deltas:")
    lines.append(f"- UDP receive buffer errors: {net1.get('receive_buffer_errors',0)} -> {net2.get('receive_buffer_errors',0)} (delta {netd.get('receive_buffer_errors',0)}, {rates['receive_buffer_errors_per_sec']:.2f}/s)")
    lines.append(f"- UDP packet receive errors: {net1.get('packet_receive_errors',0)} -> {net2.get('packet_receive_errors',0)} (delta {netd.get('packet_receive_errors',0)}, {rates['packet_receive_errors_per_sec']:.2f}/s)")
    lines.append(f"- softnet dropped sum: {soft1} -> {soft2} (delta {softd}, {rates['softnet_drops_per_sec']:.2f}/s)")
    lines.append(f"- Interface RX dropped/errors delta: {ipd.get('rx_dropped',0)}/{ipd.get('rx_errors',0)}")
    if ethd_pos:
        lines.append("- NIC counter deltas: " + ", ".join([f"{k}={v}" for k,v in sorted(ethd_pos.items())]))
    lines.append("")
    lines.append("Baseline significance:")
    for s in significance:
        lines.append(f"- {s}")
    lines.append("")
    lines.append("Interpretation guidance:")
    lines.append("- Threads matching the isolation regex are expected to remain isolated; non-matching threads are allowed to float.")
    lines.append("- Incrementing receiver-error counters are not automatically abnormal; rate and correlation matter more than absolute values.")
    lines.append("- On multi-feed hosts, host-wide counters can rise even if only one feed is affected or if another feed is the true source of pressure.")
    lines.append("")
    lines.append("Recommended next steps:")
    seen = set()
    for step in steps:
        if step not in seen:
            lines.append(f"- {step}")
            seen.add(step)

    analysis = {
        "findings": findings,
        "likely_causes": causes_sorted,
        "rates": rates,
        "counter_deltas": {"udp": netd, "softnet_dropped_delta": softd, "ip_link": ipd, "ethtool": ethd_pos},
        "process_affinity": taskset,
        "threads": threads,
        "isolated_thread_regex": isolated_thread_regex,
        "isolated_threads": isolated_threads,
        "nonisolated_threads": nonisolated_threads,
        "interrupt_cpu_candidates": intr_cpus,
        "sysctls": sysctls,
        "matching_socket_lines": matching_socket_lines,
        "baseline_significance": significance,
        "next_steps": list(seen),
    }
    return "\n".join(lines) + "\n", analysis

def main():
    ap = argparse.ArgumentParser(description="UDP drop diagnostic collector + interpreter v3")
    ap.add_argument("-i", "--iface", required=True, help="Network interface")
    ap.add_argument("-p", "--pid", type=int, help="Target process PID")
    ap.add_argument("-n", "--name", help="Target process name / pattern")
    ap.add_argument("--group", help="Destination IP / multicast group")
    ap.add_argument("--port", type=int, help="UDP port")
    ap.add_argument("--src-ip", help="Expected source IP")
    ap.add_argument("--interval", type=int, default=5, help="Seconds between samples")
    ap.add_argument("-o", "--outdir", help="Output directory")
    ap.add_argument("--baseline-file", help="JSON file for saving/comparing baseline rates")
    ap.add_argument("--csv-history", help="Append per-run rate metrics to a CSV file")
    ap.add_argument("--isolated-thread-regex", default=DEFAULT_ISOLATED_THREAD_REGEX, help=f"Regex for threads that should remain isolated (default: {DEFAULT_ISOLATED_THREAD_REGEX})")
    ap.add_argument("--tcpdump", action="store_true", help="Capture a small tcpdump snapshot for the specified endpoint")
    ap.add_argument("--tcpdump-count", type=int, default=200, help="Packet count for optional tcpdump snapshot")
    args = ap.parse_args()

    if not Path(f"/sys/class/net/{args.iface}").exists():
        print(f"ERROR: interface {args.iface!r} not found", file=sys.stderr)
        return 1

    pid = resolve_pid(args.name, args.pid)
    outdir = Path(args.outdir or f"./udp_drop_diag_v3_{ts()}").resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"[+] Output directory: {outdir}")
    if pid is not None:
        print(f"[+] Using PID: {pid}")
    elif args.name:
        print(f"[!] No PID found for process pattern: {args.name}")

    print("[+] Collecting sample1...")
    collect(outdir / "sample1", args.iface, pid, args.port, args.group, args.src_ip, args.tcpdump, args.tcpdump_count)
    print(f"[+] Sleeping {args.interval}s...")
    time.sleep(args.interval)
    print("[+] Collecting sample2...")
    collect(outdir / "sample2", args.iface, pid, args.port, args.group, args.src_ip, args.tcpdump, args.tcpdump_count)

    baseline_path = Path(args.baseline_file).resolve() if args.baseline_file else None
    baseline = load_baseline(baseline_path)
    summary, analysis = analyze(outdir, args.iface, pid, args.port, args.group, args.src_ip, args.interval, baseline, args.isolated_thread_regex)

    write(outdir / "SUMMARY.txt", summary)
    write(outdir / "analysis.json", json.dumps(analysis, indent=2))

    record = {
        "ts": datetime.now().isoformat(),
        "iface": args.iface,
        "pid": pid,
        "group": args.group or "",
        "port": args.port if args.port is not None else "",
        "src_ip": args.src_ip or "",
        "rates": analysis["rates"],
        "counter_deltas": analysis["counter_deltas"],
    }
    save_baseline(baseline_path, record)

    csv_path = Path(args.csv_history).resolve() if args.csv_history else None
    append_csv_history(csv_path, {
        "ts": record["ts"],
        "iface": args.iface,
        "pid": pid or "",
        "group": args.group or "",
        "port": args.port if args.port is not None else "",
        "src_ip": args.src_ip or "",
        "receive_buffer_errors_per_sec": analysis["rates"]["receive_buffer_errors_per_sec"],
        "packet_receive_errors_per_sec": analysis["rates"]["packet_receive_errors_per_sec"],
        "softnet_drops_per_sec": analysis["rates"]["softnet_drops_per_sec"],
        "rx_dropped_per_sec": analysis["rates"]["rx_dropped_per_sec"],
        "matching_socket_lines": analysis["matching_socket_lines"],
    })

    print(f"[+] Summary written to {outdir / 'SUMMARY.txt'}")
    if baseline_path:
        print(f"[+] Baseline updated at {baseline_path}")
    if csv_path:
        print(f"[+] CSV history appended to {csv_path}")
    print(summary)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
