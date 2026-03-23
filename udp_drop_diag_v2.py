#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, json, re, shlex, subprocess, sys, time
from datetime import datetime
from pathlib import Path

DEFAULT_ISOLATED_THREAD_REGEX = r"udp_recv"
EXANIC_TOOLS = ["exanic-config","exanic-capture","exanic-clock-sync","exanic-experf","exanic-fwupdate","exanic-fwversion","exanic-port-config"]

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

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

def detect_driver(iface: str) -> str:
    out = run(f"ethtool -i {shlex.quote(iface)} 2>/dev/null")
    m = re.search(r"^driver:\s*(\S+)", out, re.M)
    return m.group(1) if m else ""

def infer_rx_path(iface: str, requested: str, driver: str) -> str:
    if requested != "auto":
        return requested
    if iface in {"eth0", "eth1"}:
        return "kernel"
    d = driver.lower()
    if "exanic" in d:
        return "exanic"
    if any(x in d for x in ["sfc", "solarflare", "xilinx"]):
        return "onload"
    return "kernel"

def collect_vendor_tools(sample_dir: Path, rx_path: str, iface: str, do_capture: bool, capture_count: int, pid):
    cmds = {}
    if rx_path == "exanic":
        cmds["exanic_tools_present.txt"] = "bash -lc 'for c in " + " ".join(EXANIC_TOOLS) + "; do printf \"%s: \" \"$c\"; command -v \"$c\" || true; done'"
        cmds["exanic_config.txt"] = "exanic-config 2>&1 || true"
        if pid is not None:
            cmds["exanic_pid_lsof.txt"] = f"lsof -p {pid} 2>/dev/null | egrep -i 'exanic|/dev/' || true"
        if do_capture:
            cmds["exanic_capture_snapshot.txt"] = f"bash -lc 'if command -v exanic-capture >/dev/null 2>&1; then exanic-capture {shlex.quote(iface)} 2>&1 | head -n {int(capture_count)}; else echo exanic-capture not found; fi'"
    for name, cmd in cmds.items():
        log(f"Collecting vendor probe: {name}")
        write(sample_dir / name, f"### CMD: {cmd}\n### TS: {datetime.now().isoformat()}\n\n" + run(cmd))

def collect(sample_dir: Path, iface: str, pid, port, group, src_ip, do_capture: bool, capture_count: int):
    cmds = {
        "date.txt": "date -Is",
        "uptime.txt": "uptime",
        "driver_info.txt": f"ethtool -i {shlex.quote(iface)} || true",
        "ip_link.txt": f"ip -s link show dev {shlex.quote(iface)}",
        "ethtool_stats.txt": f"ethtool -S {shlex.quote(iface)}",
        "ethtool_ring.txt": f"ethtool -g {shlex.quote(iface)}",
        "interrupts_iface.txt": f"grep -iE '{re.escape(iface)}|exanic|sfc|ixgbe|i40e|ice|ena|mlx|virtio' /proc/interrupts || true",
        "netstat_su.txt": "netstat -su || true",
        "ss_udp_summary.txt": "ss -u -s || true",
        "softnet_stat.txt": "cat /proc/net/softnet_stat || true",
        "sysctl_sockbuf.txt": "sysctl net.core.rmem_default net.core.rmem_max net.core.netdev_max_backlog net.ipv4.udp_mem net.ipv4.udp_rmem_min 2>&1 || true",
    }
    if port is not None:
        cmds["ss_udp_port.txt"] = f"ss -uapni | grep -E '[:.]{port}\\b|:{port} ' || true"
    if group and port is not None:
        cmds["ss_udp_endpoint.txt"] = f"ss -uapni | grep -F '{group}' | grep -E '[:.]{port}\\b|:{port} ' || true"
    if pid is not None:
        cmds["process_taskset.txt"] = f"taskset -cp {pid} || true"
        cmds["process_threads.txt"] = f"ps -eLo pid,tid,psr,pcpu,stat,comm | awk '$1 == {pid}' || true"
    for name, cmd in cmds.items():
        log(f"Collecting {name}")
        write(sample_dir / name, f"### CMD: {cmd}\n### TS: {datetime.now().isoformat()}\n\n" + run(cmd))

def parse_udp(text: str):
    low = text.lower()
    def get(pats):
        for line in low.splitlines():
            for p in pats:
                if p in line:
                    m = re.search(r"(\d+)", line)
                    if m:
                        return int(m.group(1))
        return 0
    return {
        "receive_buffer_errors": get(["receive buffer errors", "rcvbuferrors"]),
        "packet_receive_errors": get(["packet receive errors"]),
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
            vals = lines[i+1].split()
            if len(vals) >= 4:
                try:
                    out["rx_errors"] = int(vals[2]); out["rx_dropped"] = int(vals[3])
                except Exception:
                    pass
    return out

def parse_ethtool(text: str):
    stats = {}
    for line in text.splitlines():
        if ":" in line:
            n, v = line.split(":", 1)
            n = n.strip(); v = v.strip()
            if any(k in n for k in ["rx_dropped","rx_missed_errors","rx_no_buffer_count","rx_errors","rx_missed","rx_discards"]):
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
                    threads.append({"tid": int(parts[1]), "psr": int(parts[2]), "comm": parts[5]})
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
            for idx, val in enumerate(counts):
                if val == mx and mx > 0:
                    out.add(idx)
    return sorted(out)

def parse_ss_lines(text: str):
    return len([l for l in text.splitlines() if l.strip() and not l.startswith("###")])

def delta(a, b):
    keys = set(a) | set(b)
    return {k: b.get(k, 0) - a.get(k, 0) for k in keys}

def parse_exanic_validation(sample_dir: Path):
    result = {"tools_present": [], "capture_used": False, "app_uses_exanic": False}
    p = sample_dir / "exanic_tools_present.txt"
    if p.exists():
        txt = p.read_text(errors="replace")
        for line in txt.splitlines():
            if ":" in line:
                name, val = line.split(":", 1)
                if val.strip():
                    result["tools_present"].append(name.strip())
    result["capture_used"] = (sample_dir / "exanic_capture_snapshot.txt").exists()
    p = sample_dir / "exanic_pid_lsof.txt"
    if p.exists():
        txt = p.read_text(errors="replace").lower()
        if "exanic" in txt or "/dev/" in txt:
            result["app_uses_exanic"] = True
    return result

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
    data = {"runs": []}
    if path.exists():
        try:
            data = json.loads(path.read_text())
        except Exception:
            pass
    data.setdefault("runs", []).append(record)
    data["last"] = record
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))

def get_baseline_rate(baseline, key):
    try:
        return float(baseline["last"]["rates"][key])
    except Exception:
        return None

def analyze(outdir: Path, iface: str, pid, interval: int, baseline, isolated_thread_regex: str, rx_path: str, driver: str):
    s1 = outdir / "sample1"; s2 = outdir / "sample2"
    net1 = parse_udp((s1/"netstat_su.txt").read_text(errors="replace")); net2 = parse_udp((s2/"netstat_su.txt").read_text(errors="replace")); netd = delta(net1, net2)
    soft1 = parse_softnet((s1/"softnet_stat.txt").read_text(errors="replace")); soft2 = parse_softnet((s2/"softnet_stat.txt").read_text(errors="replace")); softd = soft2 - soft1
    ip1 = parse_ip_link((s1/"ip_link.txt").read_text(errors="replace")); ip2 = parse_ip_link((s2/"ip_link.txt").read_text(errors="replace")); ipd = delta(ip1, ip2)
    eth1 = parse_ethtool((s1/"ethtool_stats.txt").read_text(errors="replace")); eth2 = parse_ethtool((s2/"ethtool_stats.txt").read_text(errors="replace")); ethd = delta(eth1, eth2)
    ethd_pos = {k:v for k,v in ethd.items() if v > 0}
    taskset = parse_taskset((s1/"process_taskset.txt").read_text(errors="replace")) if pid else None
    threads = parse_threads((s1/"process_threads.txt").read_text(errors="replace")) if pid else []
    intr_cpus = parse_interrupt_cpus((s1/"interrupts_iface.txt").read_text(errors="replace"))
    ss_text = ""
    for name in ["ss_udp_endpoint.txt","ss_udp_port.txt","ss_udp_summary.txt"]:
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
    findings.append(f"Detected driver: {driver or 'unknown'}")
    findings.append(f"Receive path mode: {rx_path}")
    if rx_path == "exanic":
        exv = parse_exanic_validation(s1)
        findings.append(f"ExaNIC tools present: {', '.join(exv['tools_present']) if exv['tools_present'] else 'none detected'}")
        findings.append("exanic-capture was used in this run." if exv["capture_used"] else "exanic-capture was NOT used in this run; it is only used when --tcpdump is specified.")
        findings.append("PID-level validation suggests app uses ExaNIC device paths." if exv["app_uses_exanic"] else "Could not confirm from lsof that app uses ExaNIC device paths.")
    if matching_socket_lines > 0:
        findings.append(f"Endpoint/socket filters matched {matching_socket_lines} socket line(s).")
    else:
        findings.append("Specified endpoint was not clearly visible in socket inspection output.")
    isolated_threads = []
    if threads and isolated_thread_regex:
        pat = re.compile(isolated_thread_regex)
        isolated_threads = [t for t in threads if pat.search(t["comm"])]
        if isolated_threads:
            desc = ", ".join([f"{t['comm']}[tid={t['tid']}]@CPU{t['psr']}" for t in isolated_threads[:8]])
            findings.append(f"Threads matching isolation regex '{isolated_thread_regex}': {desc}")
            iso_cpus = sorted({t["psr"] for t in isolated_threads})
            if len(iso_cpus) > 1:
                causes.append(("threads that should be isolated are not staying on one CPU", 85))
        else:
            findings.append(f"No threads matched isolation regex '{isolated_thread_regex}'.")
    if netd.get("receive_buffer_errors", 0) > 0:
        findings.append(f"UDP receive buffer errors increased by {netd['receive_buffer_errors']} ({rates['receive_buffer_errors_per_sec']:.2f}/s)")
        causes.append(("socket buffer overflow or application not draining packets fast enough", 95 if rx_path=="kernel" else 70))
    if softd > 0:
        findings.append(f"softnet backlog drops increased by {softd} ({rates['softnet_drops_per_sec']:.2f}/s)")
        causes.append(("softirq/backlog overload under burst traffic", 90 if rx_path=="kernel" else 65))
    if ethd_pos:
        findings.append("NIC/driver drop-related counters increased: " + ", ".join([f"{k}={v}" for k,v in sorted(ethd_pos.items())]))
        causes.append(("NIC ring/driver/hardware receive-side drop before socket layer", 92))
    if pid and taskset:
        findings.append(f"Process affinity list from taskset: {taskset}")
        steps.append("Use taskset as runtime verification even with isolcpus; isolcpus does not prove the process/threads are affined correctly.")
        if isolated_threads:
            iso_cpus = sorted({t['psr'] for t in isolated_threads})
            if intr_cpus and iso_cpus and not any(cpu in intr_cpus for cpu in iso_cpus):
                findings.append(f"NIC interrupts appear busiest on CPU(s) {intr_cpus}, while isolated receiver thread CPU(s) are {iso_cpus}")
                causes.append(("IRQ / receiver CPU misalignment", 80))
    for key, current in rates.items():
        base = get_baseline_rate(baseline, key)
        if base is None:
            significance.append(f"No baseline available yet for {key}")
        elif base == 0 and current > 0:
            significance.append(f"{key} is non-zero now but was zero in the last baseline run")
        elif base > 0:
            ratio = current / base
            if ratio >= 5:
                significance.append(f"{key} is {ratio:.1f}x above the last baseline rate")
    dedup = {}
    for c, s in causes:
        dedup[c] = max(dedup.get(c, 0), s)
    causes_sorted = sorted(dedup.items(), key=lambda x: x[1], reverse=True)
    lines = []
    lines.append("UDP Drop Diagnostic Summary v5.1")
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append(f"Interface: {iface}")
    lines.append(f"Detected driver: {driver or 'unknown'}")
    lines.append(f"Receive path mode: {rx_path}")
    lines.append(f"Isolated-thread regex: {isolated_thread_regex}")
    lines.append("")
    lines.append("Key findings:")
    for f in findings: lines.append(f"- {f}")
    lines.append("")
    lines.append("Most likely issue(s):")
    for c, s in causes_sorted[:5]: lines.append(f"- {c} (confidence: {s}%)")
    lines.append("")
    lines.append("Baseline significance:")
    for s in significance: lines.append(f"- {s}")
    lines.append("")
    lines.append("Interpretation guidance:")
    lines.append("- exanic-capture is NOT used automatically for --rx-path exanic; it is only used when --tcpdump is specified.")
    lines.append("- taskset remains relevant as a runtime verification tool; isolcpus reduces scheduler noise but does not by itself prove the process/threads are affined correctly.")
    analysis = {"findings": findings, "likely_causes": causes_sorted, "rates": rates, "counter_deltas": {"udp": netd, "softnet_dropped_delta": softd, "ip_link": ipd, "ethtool": ethd_pos}, "driver": driver, "rx_path": rx_path}
    return "\n".join(lines) + "\n", analysis

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

def main():
    ap = argparse.ArgumentParser(description="UDP drop diagnostic collector + interpreter v5.1")
    ap.add_argument("-i", "--iface", required=True)
    ap.add_argument("-p", "--pid", type=int)
    ap.add_argument("-n", "--name")
    ap.add_argument("--group")
    ap.add_argument("--port", type=int)
    ap.add_argument("--src-ip")
    ap.add_argument("--interval", type=int, default=5)
    ap.add_argument("-o", "--outdir")
    ap.add_argument("--baseline-file")
    ap.add_argument("--csv-history")
    ap.add_argument("--isolated-thread-regex", default=DEFAULT_ISOLATED_THREAD_REGEX)
    ap.add_argument("--tcpdump", action="store_true")
    ap.add_argument("--tcpdump-count", type=int, default=200)
    ap.add_argument("--rx-path", choices=["auto","kernel","onload","exanic"], default="auto")
    args = ap.parse_args()

    if not Path(f"/sys/class/net/{args.iface}").exists():
        print(f"ERROR: interface {args.iface!r} not found", file=sys.stderr)
        return 1
    pid = resolve_pid(args.name, args.pid)
    driver = detect_driver(args.iface)
    rx_path = infer_rx_path(args.iface, args.rx_path, driver)
    outdir = Path(args.outdir or f"./udp_drop_diag_v5_1_{ts()}").resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    log(f"Output directory: {outdir}")
    log(f"Detected driver: {driver or 'unknown'}")
    log(f"Receive path mode: {rx_path}")
    if pid is not None:
        log(f"Using PID: {pid}")
    if rx_path == "exanic":
        log("For exanic mode, exanic-capture is only invoked when --tcpdump is specified.")
    collect(outdir / "sample1", args.iface, pid, args.port, args.group, args.src_ip, args.tcpdump, args.tcpdump_count)
    log(f"Sleeping {args.interval}s")
    time.sleep(args.interval)
    collect(outdir / "sample2", args.iface, pid, args.port, args.group, args.src_ip, args.tcpdump, args.tcpdump_count)

    baseline_path = Path(args.baseline_file).resolve() if args.baseline_file else None
    baseline = load_baseline(baseline_path)
    summary, analysis = analyze(outdir, args.iface, pid, args.interval, baseline, args.isolated_thread_regex, rx_path, driver)
    write(outdir / "SUMMARY.txt", summary)
    write(outdir / "analysis.json", json.dumps(analysis, indent=2))

    record = {"ts": datetime.now().isoformat(), "iface": args.iface, "pid": pid, "driver": driver, "rx_path": rx_path, "rates": analysis["rates"], "counter_deltas": analysis["counter_deltas"]}
    save_baseline(baseline_path, record)

    csv_path = Path(args.csv_history).resolve() if args.csv_history else None
    if csv_path:
        append_csv_history(csv_path, {"ts": record["ts"], "iface": args.iface, "pid": pid or "", "driver": driver, "rx_path": rx_path, **analysis["rates"]})

    log(f"Summary written to {outdir / 'SUMMARY.txt'}")
    print(summary)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
