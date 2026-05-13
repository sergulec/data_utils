#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
udp_drop_diag_v6.py

UDP packet-drop diagnostic collector + interpreter.

This version keeps the existing collector/analyzer shape and adds traffic-mode
awareness for unicast vs multicast without removing the existing kernel/onload/
ExaNIC receive-path checks.

No third-party Python packages are required.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

VERSION = "6.0"
DEFAULT_ISOLATED_THREAD_REGEX = r"udp_recv"

EXANIC_TOOLS = [
    "exanic-config",
    "exanic-capture",
    "exanic-clock-sync",
    "exanic-expire",
    "exanic-fwupdate",
    "exanic-fwversion",
    "exanic-port-config",
]

# Counters that commonly indicate receive-side loss or pressure.  The exact
# names vary by NIC/driver, so matching is intentionally broad.
ETHTOOL_DROP_PATTERNS = (
    "rx_dropped",
    "rx_discards",
    "rx_missed",
    "rx_no_buffer",
    "rx_nobuf",
    "rx_overrun",
    "rx_fifo",
    "rx_errors",
    "rx_crc_errors",
    "rx_length_errors",
    "rx_queue_drops",
    "rx_out_of_buffer",
    "rx_alloc_fail",
    "rx_lost",
    "rx_drops",
)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def run(cmd: str, timeout: int = 45) -> str:
    """Run a shell command and return stdout+stderr.  Best-effort only."""
    try:
        p = subprocess.run(
            cmd,
            shell=True,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
        )
        return p.stdout or ""
    except subprocess.TimeoutExpired as exc:
        out = exc.stdout or ""
        err = exc.stderr or ""
        if isinstance(out, bytes):
            out = out.decode(errors="replace")
        if isinstance(err, bytes):
            err = err.decode(errors="replace")
        return f"{out}{err}\n[TIMEOUT after {timeout}s]\n"
    except Exception as exc:
        return f"[COMMAND ERROR] {exc}\n"


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text or "", encoding="utf-8", errors="replace")


def append_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", errors="replace") as f:
        f.write(text)


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def q(s: Any) -> str:
    return shlex.quote(str(s))


def bash_lc(script: str) -> str:
    return "bash -lc " + q(script)


def resolve_pid(name: Optional[str], pid: Optional[int]) -> Optional[int]:
    if pid is not None:
        return pid
    if not name:
        return None
    out = run(f"pgrep -f {q(name)} | head -n1", timeout=10).strip()
    try:
        return int(out) if out else None
    except Exception:
        return None


def detect_driver(iface: str) -> str:
    out = run(f"ethtool -i {q(iface)} 2>/dev/null", timeout=15)
    m = re.search(r"^driver:\s*(\S+)", out, flags=re.MULTILINE)
    return m.group(1) if m else ""


def infer_rx_path(iface: str, requested: str, driver: str) -> str:
    """Preserve existing receive-path inference behavior."""
    if requested != "auto":
        return requested
    if iface in ("eth0", "eth1"):
        return "kernel"
    if driver == "exanic":
        return "exanic"
    if any(x in driver.lower() for x in ("sfc", "solarflare", "mlx")):
        return "onload"
    return "kernel"


def is_multicast_ip(value: Optional[str]) -> bool:
    if not value:
        return False
    try:
        return ip_address(value).is_multicast
    except Exception:
        return False


def validate_optional_ip(label: str, value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    try:
        ip_address(value)
    except Exception:
        raise argparse.ArgumentTypeError(f"{label} must be an IPv4 or IPv6 address, got {value!r}")
    return value


def infer_traffic_mode(
    requested: str,
    group: Optional[str],
    dst_ip: Optional[str],
    peer_ip: Optional[str],
) -> str:
    if requested != "auto":
        return requested
    if group or is_multicast_ip(dst_ip):
        return "multicast"
    if peer_ip or dst_ip:
        return "unicast"
    return "unicast"


def build_tcpdump_filter(
    traffic_mode: str,
    port: Optional[int],
    group: Optional[str],
    src_ip: Optional[str],
    dst_ip: Optional[str],
    peer_ip: Optional[str],
) -> str:
    """Build a safe, targeted tcpdump filter string."""
    parts: List[str] = ["udp"]

    if traffic_mode == "multicast":
        if src_ip:
            parts.append(f"and src host {src_ip}")
        if group:
            parts.append(f"and dst host {group}")
        elif dst_ip:
            parts.append(f"and dst host {dst_ip}")
    else:
        # For unicast, --peer-ip is the broadest useful constraint.  --src-ip
        # and --dst-ip are honored when supplied for direction-specific tests.
        if peer_ip:
            parts.append(f"and host {peer_ip}")
        if src_ip:
            parts.append(f"and src host {src_ip}")
        if dst_ip:
            parts.append(f"and dst host {dst_ip}")

    if port is not None:
        parts.append(f"and port {int(port)}")

    return " ".join(parts)


def command_exists(name: str) -> bool:
    return bool(run(f"command -v {q(name)}", timeout=5).strip())


def collect_vendor_tools(
    sample_dir: Path,
    rx_path: str,
    iface: str,
    do_capture: bool,
    capture_count: int,
    pid: Optional[int],
) -> None:
    """Collect ExaNIC-specific data without forcing ExaNIC behavior."""
    cmds: Dict[str, str] = {}
    if rx_path == "exanic":
        tools = " ".join(q(t) for t in EXANIC_TOOLS)
        cmds["exanic_tools_present.txt"] = bash_lc(
            f'for c in {tools}; do printf "%s " "$c"; command -v "$c" || echo NOT_FOUND; done'
        )
        cmds["exanic_config.txt"] = "exanic-config 2>&1 || true"
        cmds["exanic_port_config.txt"] = "exanic-port-config 2>&1 || true"
        if pid is not None:
            cmds["exanic_pid_lsof.txt"] = (
                f"lsof -p {int(pid)} 2>/dev/null | egrep -i 'exanic|/dev/' || true"
            )
        if do_capture:
            # Preserve prior behavior: exanic-capture is only attempted when
            # the user explicitly asks for capture via --tcpdump.
            cmds["exanic_capture_snapshot.txt"] = bash_lc(
                f"if command -v exanic-capture >/dev/null 2>&1; then "
                f"timeout 5 exanic-capture {q(iface)} 2>&1 | head -n {int(capture_count)}; "
                f"else echo exanic-capture not found; fi"
            )

    for name, cmd in cmds.items():
        log(f"Collecting vendor probe: {name}")
        write(sample_dir / name, run(cmd))


def collect(
    sample_dir: Path,
    iface: str,
    pid: Optional[int],
    port: Optional[int],
    group: Optional[str],
    src_ip: Optional[str],
    dst_ip: Optional[str],
    peer_ip: Optional[str],
    traffic_mode: str,
    rx_path: str,
    do_capture: bool,
    capture_count: int,
) -> None:
    sample_dir.mkdir(parents=True, exist_ok=True)

    cmds: Dict[str, str] = {
        "date.txt": "date -Is",
        "hostname.txt": "hostname || true",
        "uname.txt": "uname -a || true",
        "uptime.txt": "uptime || true",
        "driver_info.txt": f"ethtool -i {q(iface)} 2>&1 || true",
        "ip_link.txt": f"ip -s link show dev {q(iface)} 2>&1 || true",
        "ip_addr.txt": f"ip addr show dev {q(iface)} 2>&1 || true",
        "ip_maddr.txt": f"ip maddr show dev {q(iface)} 2>&1 || true",
        "ethtool_stats.txt": f"ethtool -S {q(iface)} 2>&1 || true",
        "ethtool_ring.txt": f"ethtool -g {q(iface)} 2>&1 || true",
        "ethtool_channels.txt": f"ethtool -l {q(iface)} 2>&1 || true",
        "ethtool_coalesce.txt": f"ethtool -c {q(iface)} 2>&1 || true",
        "interrupts.txt": "cat /proc/interrupts 2>&1 || true",
        "netstat_su.txt": "netstat -su 2>&1 || ss -s 2>&1 || true",
        "ss_udp_summary.txt": "ss -uapn 2>&1 || ss -u -n 2>&1 || true",
        "softnet_stat.txt": "cat /proc/net/softnet_stat 2>&1 || true",
        "proc_cmdline.txt": "cat /proc/cmdline 2>&1 || true",
        "sysctl_sockbuf.txt": (
            "sysctl net.core.rmem_default net.core.rmem_max "
            "net.core.netdev_max_backlog net.ipv4.udp_mem "
            "net.ipv4.udp_rmem_min 2>&1 || true"
        ),
    }

    if port is not None:
        # Keep an explicit port-specific ss output because it is fast to inspect.
        cmds["ss_udp_port.txt"] = (
            f"ss -uapn 2>&1 | egrep -E '[:.]({int(port)})([[:space:]]|$)' || true"
        )

    if traffic_mode == "multicast":
        if group:
            cmds["ss_udp_group.txt"] = f"ss -uapn 2>&1 | grep -F {q(group)} || true"
        if group and port is not None:
            cmds["ss_udp_group_port.txt"] = (
                f"ss -uapn 2>&1 | egrep -E '{re.escape(group)}|[:.]({int(port)})([[:space:]]|$)' || true"
            )
    else:
        endpoint_grep_terms = [x for x in (peer_ip, src_ip, dst_ip) if x]
        if endpoint_grep_terms:
            grep_expr = "|".join(re.escape(x) for x in endpoint_grep_terms)
            cmds["ss_udp_unicast_endpoint.txt"] = (
                f"ss -uapn 2>&1 | egrep -E {q(grep_expr)} || true"
            )

    if pid is not None:
        cmds["process_taskset.txt"] = f"taskset -cp {int(pid)} 2>&1 || true"
        cmds["process_threads.txt"] = bash_lc(
            f"ps -Leo pid,tid,psr,pcpu,stat,comm | awk '$1 == {int(pid)} {{print}}' || true"
        )
        cmds["process_lsof_udp.txt"] = (
            f"lsof -nP -a -p {int(pid)} -iUDP 2>&1 || true"
        )
        cmds["process_cmdline.txt"] = f"tr '\\0' ' ' < /proc/{int(pid)}/cmdline 2>&1 || true"

    if do_capture:
        filt = build_tcpdump_filter(traffic_mode, port, group, src_ip, dst_ip, peer_ip)
        # tcpdump filter is not shell-quoted as one token; quote only iface and
        # pass the filter after -- so tcpdump receives the expression as words.
        cmds["tcpdump_snapshot.txt"] = (
            f"timeout 6 tcpdump -nn -tt -i {q(iface)} -c {int(capture_count)} {filt} 2>&1 || true"
        )
        write(sample_dir / "tcpdump_filter.txt", filt + "\n")

    for name, cmd in cmds.items():
        log(f"Collecting {sample_dir.name}/{name}")
        write(sample_dir / name, run(cmd))

    collect_vendor_tools(sample_dir, rx_path, iface, do_capture, capture_count, pid)


def parse_key_value_ints(text: str) -> Dict[str, int]:
    stats: Dict[str, int] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip().split()[0] if value.strip() else ""
        try:
            stats[key] = int(value, 0)
        except Exception:
            continue
    return stats


def parse_ethtool(text: str) -> Dict[str, int]:
    return parse_key_value_ints(text)


def parse_udp(text: str) -> Dict[str, int]:
    """Parse netstat -su / ss -s style UDP counters."""
    out: Dict[str, int] = {}
    patterns = {
        "packets_received": [r"packets received", r"packet received"],
        "packets_sent": [r"packets sent", r"packet sent"],
        "packet_receive_errors": [r"packet receive errors?", r"rcverrors"],
        "receive_buffer_errors": [r"receive buffer errors?", r"rcvbuferrors"],
        "send_buffer_errors": [r"send buffer errors?", r"sndbuferrors"],
        "unknown_port_received": [r"packets to unknown port received", r"no ports"],
    }

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for key, pats in patterns.items():
        for line in lines:
            low = line.lower()
            if any(re.search(p, low) for p in pats):
                m = re.search(r"(-?\d+)", line)
                if m:
                    try:
                        out[key] = int(m.group(1))
                    except Exception:
                        pass
                break
    return out


def parse_softnet(text: str) -> Dict[str, int]:
    """Parse /proc/net/softnet_stat.

    Field 0 is processed, field 1 is dropped, field 2 is time_squeeze.
    Values are hex on Linux.
    """
    totals = {"processed": 0, "dropped": 0, "time_squeeze": 0}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            totals["processed"] += int(parts[0], 16)
            totals["dropped"] += int(parts[1], 16)
            totals["time_squeeze"] += int(parts[2], 16)
        except Exception:
            continue
    return totals


def parse_ip_link(text: str) -> Dict[str, int]:
    """Parse `ip -s link show dev IFACE` RX/TX counters."""
    stats: Dict[str, int] = {}
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if re.search(r"\bRX:\s+bytes\s+packets\s+errors\s+dropped\s+missed\s+mcast", line):
            if i + 1 < len(lines):
                parts = lines[i + 1].split()
                if len(parts) >= 6:
                    keys = ["rx_bytes", "rx_packets", "rx_errors", "rx_dropped", "rx_missed", "rx_mcast"]
                    for key, value in zip(keys, parts[:6]):
                        try:
                            stats[key] = int(value, 0)
                        except Exception:
                            pass
        if re.search(r"\bTX:\s+bytes\s+packets\s+errors\s+dropped", line):
            if i + 1 < len(lines):
                parts = lines[i + 1].split()
                if len(parts) >= 4:
                    keys = ["tx_bytes", "tx_packets", "tx_errors", "tx_dropped"]
                    for key, value in zip(keys, parts[:4]):
                        try:
                            stats[key] = int(value, 0)
                        except Exception:
                            pass
    return stats


def parse_taskset(text: str) -> str:
    m = re.search(r"affinity list:\s*([^\n]+)", text)
    return m.group(1).strip() if m else ""


def expand_cpu_list(cpu_list: str) -> List[int]:
    cpus: List[int] = []
    if not cpu_list:
        return cpus
    for token in re.split(r"[,\s]+", cpu_list.strip()):
        token = token.strip()
        if not token:
            continue
        # isolcpus can include flags such as managed_irq,domain,1-3.
        if not re.search(r"\d", token):
            continue
        m = re.search(r"(\d+)(?:-(\d+))?", token)
        if not m:
            continue
        start = int(m.group(1))
        end = int(m.group(2)) if m.group(2) else start
        cpus.extend(range(start, end + 1))
    return sorted(set(cpus))


def parse_isolcpus(cmdline: str) -> List[int]:
    m = re.search(r"(?:^|\s)isolcpus=([^\s]+)", cmdline)
    if not m:
        return []
    return expand_cpu_list(m.group(1))


def parse_threads(text: str) -> List[Dict[str, Any]]:
    """Parse `ps -Leo pid,tid,psr,pcpu,stat,comm` rows filtered to a PID."""
    rows: List[Dict[str, Any]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.lower().startswith("pid "):
            continue
        parts = line.split(None, 5)
        if len(parts) < 6:
            continue
        try:
            rows.append(
                {
                    "pid": int(parts[0]),
                    "tid": int(parts[1]),
                    "psr": int(parts[2]),
                    "pcpu": float(parts[3]),
                    "stat": parts[4],
                    "comm": parts[5],
                }
            )
        except Exception:
            continue
    return rows


def parse_interrupt_cpus(text: str, iface: str) -> Dict[int, int]:
    counts: Dict[int, int] = {}
    if not text:
        return counts
    for raw in text.splitlines():
        if iface not in raw:
            continue
        if ":" not in raw:
            continue
        _, rest = raw.split(":", 1)
        nums: List[int] = []
        for token in rest.split():
            if re.fullmatch(r"\d+", token):
                nums.append(int(token))
            else:
                break
        for idx, value in enumerate(nums):
            counts[idx] = counts.get(idx, 0) + value
    return counts


def parse_ss_lines(
    text: str,
    port: Optional[int] = None,
    group: Optional[str] = None,
    src_ip: Optional[str] = None,
    dst_ip: Optional[str] = None,
    peer_ip: Optional[str] = None,
) -> List[str]:
    targets = [x for x in (group, src_ip, dst_ip, peer_ip) if x]
    out: List[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.lower().startswith(("state", "netid")):
            continue
        matched = False
        if port is not None and re.search(rf"[:.]({int(port)})(\b|\s|$)", line):
            matched = True
        if any(t in line for t in targets):
            matched = True
        if not targets and port is None and "udp" in line.lower():
            matched = True
        if matched:
            out.append(line)
    return out


def parse_ss_pids(lines: Sequence[str]) -> List[int]:
    pids = set()
    for line in lines:
        for m in re.finditer(r"pid=(\d+)", line):
            try:
                pids.add(int(m.group(1)))
            except Exception:
                pass
    return sorted(pids)


def parse_exanic_validation(sample_dir: Path) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "tools_present": {},
        "capture_ok": False,
        "app_uses_exanic": False,
        "config_seen": False,
    }

    tools_file = sample_dir / "exanic_tools_present.txt"
    if tools_file.exists():
        for line in read_text(tools_file).splitlines():
            parts = line.split(None, 1)
            if not parts:
                continue
            tool = parts[0]
            value = parts[1] if len(parts) > 1 else ""
            result["tools_present"][tool] = bool(value and "NOT_FOUND" not in value)

    config = read_text(sample_dir / "exanic_config.txt")
    result["config_seen"] = bool(config.strip()) and "not found" not in config.lower()

    lsof = read_text(sample_dir / "exanic_pid_lsof.txt").lower()
    result["app_uses_exanic"] = "exanic" in lsof or "/dev/" in lsof

    capture = read_text(sample_dir / "exanic_capture_snapshot.txt")
    if capture:
        low = capture.lower()
        result["capture_ok"] = bool(capture.strip()) and not any(
            bad in low for bad in ("not found", "command not found", "no such file")
        )

    return result


def delta(before: Dict[str, int], after: Dict[str, int]) -> Dict[str, int]:
    keys = set(before) | set(after)
    return {k: int(after.get(k, 0)) - int(before.get(k, 0)) for k in keys}


def positive_delta_items(d: Dict[str, int], patterns: Iterable[str]) -> Dict[str, int]:
    selected: Dict[str, int] = {}
    lower_patterns = tuple(p.lower() for p in patterns)
    for key, value in d.items():
        low_key = key.lower()
        if value > 0 and any(p in low_key for p in lower_patterns):
            selected[key] = value
    return selected


def rate(value: int, interval: int) -> float:
    return float(value) / max(float(interval), 1.0)


def dedupe_causes(causes: Sequence[Tuple[str, int]]) -> List[Tuple[str, int]]:
    best: Dict[str, int] = {}
    for text, conf in causes:
        best[text] = max(best.get(text, 0), int(conf))
    return sorted(best.items(), key=lambda x: x[1], reverse=True)


def load_baseline(path: Optional[Path]) -> Dict[str, Any]:
    if not path:
        return {}
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}


def save_baseline(path: Optional[Path], analysis: Dict[str, Any]) -> None:
    if not path:
        return
    payload = {
        "created_at": datetime.now().isoformat(),
        "version": VERSION,
        "rates": analysis.get("rates", {}),
        "counter_deltas": analysis.get("counter_deltas", {}),
        "context": {
            "iface": analysis.get("iface"),
            "driver": analysis.get("driver"),
            "rx_path": analysis.get("rx_path"),
            "traffic_mode": analysis.get("traffic_mode"),
        },
    }
    write(path, json.dumps(payload, indent=2, sort_keys=True))


def append_csv_history(path: Optional[Path], row: Dict[str, Any]) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ts",
        "iface",
        "pid",
        "driver",
        "rx_path",
        "traffic_mode",
        "port",
        "peer_ip",
        "src_ip",
        "dst_ip",
        "group",
        "receive_buffer_errors_per_sec",
        "packet_receive_errors_per_sec",
        "softnet_drops_per_sec",
        "softnet_squeezed_per_sec",
        "ip_rx_dropped_per_sec",
        "ip_rx_errors_per_sec",
        "top_cause",
        "top_confidence",
    ]
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def append_ss_history(path: Optional[Path], stamp: str, lines: Sequence[str]) -> None:
    if not path:
        return
    block = [f"\n===== {stamp} ====="]
    block.extend(lines if lines else ["<no matching ss lines>"])
    block.append("")
    append_text(path, "\n".join(block))


def analyze(
    outdir: Path,
    iface: str,
    pid: Optional[int],
    interval: int,
    baseline: Dict[str, Any],
    isolated_thread_regex: str,
    rx_path: str,
    driver: str,
    traffic_mode: str,
    group: Optional[str],
    src_ip: Optional[str],
    dst_ip: Optional[str],
    peer_ip: Optional[str],
    port: Optional[int],
    tcpdump_requested: bool,
) -> Dict[str, Any]:
    s1 = outdir / "sample1"
    s2 = outdir / "sample2"

    net1 = parse_udp(read_text(s1 / "netstat_su.txt"))
    net2 = parse_udp(read_text(s2 / "netstat_su.txt"))
    soft1 = parse_softnet(read_text(s1 / "softnet_stat.txt"))
    soft2 = parse_softnet(read_text(s2 / "softnet_stat.txt"))
    ip1 = parse_ip_link(read_text(s1 / "ip_link.txt"))
    ip2 = parse_ip_link(read_text(s2 / "ip_link.txt"))
    eth1 = parse_ethtool(read_text(s1 / "ethtool_stats.txt"))
    eth2 = parse_ethtool(read_text(s2 / "ethtool_stats.txt"))

    netd = delta(net1, net2)
    softd = delta(soft1, soft2)
    ipd = delta(ip1, ip2)
    ethd = delta(eth1, eth2)
    ethd_pos = positive_delta_items(ethd, ETHTOOL_DROP_PATTERNS)

    ss_text = read_text(s2 / "ss_udp_summary.txt")
    matching_socket_lines = parse_ss_lines(ss_text, port, group, src_ip, dst_ip, peer_ip)
    socket_pids = parse_ss_pids(matching_socket_lines)

    taskset = parse_taskset(read_text(s2 / "process_taskset.txt"))
    taskset_cpus = expand_cpu_list(taskset)
    threads = parse_threads(read_text(s2 / "process_threads.txt"))
    isolated_cpus = parse_isolcpus(read_text(s2 / "proc_cmdline.txt"))

    int1 = parse_interrupt_cpus(read_text(s1 / "interrupts.txt"), iface)
    int2 = parse_interrupt_cpus(read_text(s2 / "interrupts.txt"), iface)
    int_delta = {cpu: int2.get(cpu, 0) - int1.get(cpu, 0) for cpu in set(int1) | set(int2)}
    active_irq_cpus = sorted(cpu for cpu, count in int_delta.items() if count > 0)

    rates: Dict[str, float] = {
        "receive_buffer_errors_per_sec": rate(netd.get("receive_buffer_errors", 0), interval),
        "packet_receive_errors_per_sec": rate(netd.get("packet_receive_errors", 0), interval),
        "softnet_drops_per_sec": rate(softd.get("dropped", 0), interval),
        "softnet_squeezed_per_sec": rate(softd.get("time_squeeze", 0), interval),
        "ip_rx_dropped_per_sec": rate(ipd.get("rx_dropped", 0), interval),
        "ip_rx_errors_per_sec": rate(ipd.get("rx_errors", 0), interval),
    }
    for key, value in ethd_pos.items():
        rates[f"ethtool_{key}_per_sec"] = rate(value, interval)

    findings: List[str] = []
    causes: List[Tuple[str, int]] = []
    significance: List[str] = []

    findings.append(f"Detected driver: {driver or 'unknown'}")
    findings.append(f"Receive path mode: {rx_path}")
    findings.append(f"Traffic mode: {traffic_mode}")

    endpoint_bits = []
    if port is not None:
        endpoint_bits.append(f"port={port}")
    if peer_ip:
        endpoint_bits.append(f"peer_ip={peer_ip}")
    if src_ip:
        endpoint_bits.append(f"src_ip={src_ip}")
    if dst_ip:
        endpoint_bits.append(f"dst_ip={dst_ip}")
    if group:
        endpoint_bits.append(f"group={group}")
    if endpoint_bits:
        findings.append("Endpoint filter: " + ", ".join(endpoint_bits))

    if matching_socket_lines:
        findings.append(f"Matching UDP socket lines visible in ss output: {len(matching_socket_lines)}")
        if socket_pids:
            findings.append(f"Socket owner PID(s) visible from ss: {', '.join(str(p) for p in socket_pids)}")
    else:
        if port is not None or group or peer_ip or src_ip or dst_ip:
            findings.append("No matching UDP socket was clearly visible in ss output.")
            causes.append(("Expected UDP socket/endpoint not visible from ss; verify bind address, port, namespace, and process ownership", 70))

    if traffic_mode == "multicast":
        maddr_text = read_text(s2 / "ip_maddr.txt")
        if group:
            if group in maddr_text:
                findings.append(f"Multicast group {group} appears in ip maddr output for {iface}.")
            else:
                findings.append(f"Multicast group {group} was not found in ip maddr output for {iface}.")
                causes.append(("Expected multicast group membership not visible on the interface", 80))
        findings.append("Multicast interpretation enabled: group membership and fanout assumptions are included.")
    else:
        findings.append("Unicast interpretation enabled: group/IGMP assumptions are not used.")
        if socket_pids and pid is not None and pid not in socket_pids:
            causes.append(("A UDP socket matched the endpoint, but not the requested PID; verify the consuming process", 75))
        if len(socket_pids) > 1:
            causes.append(("Multiple PIDs appear to match the unicast UDP endpoint; check for competing readers or stale sockets", 65))

    if rx_path == "exanic":
        env = parse_exanic_validation(s1)
        present = sorted([k for k, v in env.get("tools_present", {}).items() if v])
        missing = sorted([k for k, v in env.get("tools_present", {}).items() if not v])
        findings.append(
            "EXANIC tools present: " + (", ".join(present) if present else "none detected")
        )
        if missing:
            findings.append("EXANIC tools missing: " + ", ".join(missing))
        if env.get("app_uses_exanic"):
            findings.append("PID-level validation suggests the app may be using ExaNIC device paths.")
        else:
            findings.append("PID-level validation did not confirm ExaNIC device-path usage.")
        if tcpdump_requested:
            findings.append(
                "Capture was requested; ExaNIC capture output was collected when exanic-capture was available."
            )
        else:
            findings.append(
                "ExaNIC capture was not invoked automatically; use --tcpdump to request capture."
            )

    if isolated_thread_regex and threads:
        try:
            pat = re.compile(isolated_thread_regex)
            isolated_threads = [t for t in threads if pat.search(str(t.get("comm", "")))]
        except re.error:
            isolated_threads = []
            findings.append(f"Invalid isolated-thread regex: {isolated_thread_regex!r}")
        if isolated_threads:
            psrs = sorted({int(t["psr"]) for t in isolated_threads})
            findings.append(
                f"Threads matching isolated regex {isolated_thread_regex!r}: "
                f"{len(isolated_threads)} thread(s) on CPU(s) {psrs}"
            )
            if len(psrs) > 1:
                causes.append(("Receive threads that should be isolated are moving across CPUs", 85))
            if isolated_cpus and any(cpu not in isolated_cpus for cpu in psrs):
                causes.append(("Receive thread CPU placement does not match isolcpus set", 80))
            if taskset_cpus and any(cpu not in taskset_cpus for cpu in psrs):
                causes.append(("Receive thread is running outside the process taskset affinity mask", 75))
        else:
            findings.append(f"No process threads matched isolated regex {isolated_thread_regex!r}.")

    if taskset:
        findings.append(f"Process taskset affinity list: {taskset}")
    if isolated_cpus:
        findings.append(f"Kernel isolcpus list: {isolated_cpus}")
    if active_irq_cpus:
        findings.append(f"Interface IRQ activity observed on CPU(s): {active_irq_cpus}")
        if isolated_cpus and traffic_mode == "unicast":
            overlap = sorted(set(active_irq_cpus) & set(isolated_cpus))
            if not overlap:
                causes.append(("NIC IRQ activity is not landing on isolated receiver CPU(s)", 65))

    if netd.get("receive_buffer_errors", 0) > 0:
        count = netd.get("receive_buffer_errors", 0)
        findings.append(f"UDP receive buffer errors increased by {count} during the interval.")
        if traffic_mode == "unicast":
            causes.append(("Socket buffer overrun or unicast application not draining packets fast enough", 95))
        else:
            causes.append(("Socket buffer overrun or multicast application not draining packets fast enough", 90))

    if netd.get("packet_receive_errors", 0) > 0:
        count = netd.get("packet_receive_errors", 0)
        findings.append(f"UDP packet receive errors increased by {count} during the interval.")
        causes.append(("Kernel UDP receive error counter increased", 80))

    if softd.get("dropped", 0) > 0:
        count = softd.get("dropped", 0)
        findings.append(f"softnet dropped increased by {count} during the interval.")
        causes.append(("Kernel softnet/backlog drops before socket delivery", 90))

    if softd.get("time_squeeze", 0) > 0:
        count = softd.get("time_squeeze", 0)
        findings.append(f"softnet time_squeeze increased by {count} during the interval.")
        causes.append(("CPU budget pressure in network softirq processing", 75))

    if ipd.get("rx_dropped", 0) > 0:
        findings.append(f"ip-link RX dropped increased by {ipd.get('rx_dropped', 0)}.")
        causes.append(("Driver/interface RX drops visible at ip-link layer", 80))

    if ipd.get("rx_errors", 0) > 0:
        findings.append(f"ip-link RX errors increased by {ipd.get('rx_errors', 0)}.")
        causes.append(("Interface RX errors increased", 75))

    if ethd_pos:
        pretty = ", ".join(f"{k}+{v}" for k, v in sorted(ethd_pos.items()))
        findings.append(f"ethtool RX/drop-related counters increased: {pretty}")
        # Confidence depends on counter family.
        if any("no_buffer" in k.lower() or "nobuf" in k.lower() or "missed" in k.lower() for k in ethd_pos):
            causes.append(("NIC/driver buffer pressure or ring exhaustion before socket layer", 90))
        else:
            causes.append(("NIC/driver RX/drop-related counters increased", 75))

    baseline_rates = baseline.get("rates", {}) if isinstance(baseline, dict) else {}
    for key, current in sorted(rates.items()):
        try:
            base = float(baseline_rates.get(key, 0.0))
        except Exception:
            base = 0.0
        if current <= 0:
            continue
        if base <= 0:
            significance.append(f"{key} is non-zero now but was zero or absent in the baseline")
        else:
            ratio = current / base
            if ratio >= 3.0:
                significance.append(f"{key} is {ratio:.1f}x above the baseline rate")

    causes_sorted = dedupe_causes(causes)
    if not causes_sorted:
        causes_sorted = [("No definitive drop source found in sampled counters; increase interval or capture during the event", 40)]

    counter_deltas = {
        "udp": netd,
        "softnet": softd,
        "ip_link": ipd,
        "ethtool_drop_related": ethd_pos,
        "interrupt_cpu_delta": {str(k): v for k, v in sorted(int_delta.items()) if v},
    }

    return {
        "version": VERSION,
        "generated_at": datetime.now().isoformat(),
        "outdir": str(outdir),
        "iface": iface,
        "pid": pid,
        "driver": driver,
        "rx_path": rx_path,
        "traffic_mode": traffic_mode,
        "endpoint": {
            "port": port,
            "group": group,
            "src_ip": src_ip,
            "dst_ip": dst_ip,
            "peer_ip": peer_ip,
        },
        "rates": rates,
        "counter_deltas": counter_deltas,
        "taskset": taskset,
        "isolcpus": isolated_cpus,
        "threads": threads,
        "matching_socket_lines": matching_socket_lines,
        "socket_pids": socket_pids,
        "active_irq_cpus": active_irq_cpus,
        "findings": findings,
        "likely_causes": causes_sorted,
        "significance": significance,
    }


def write_summary(outdir: Path, analysis: Dict[str, Any]) -> str:
    lines: List[str] = []
    endpoint = analysis.get("endpoint", {}) or {}

    lines.append(f"UDP Drop Diagnostic Summary v{VERSION}")
    lines.append("=" * 40)
    lines.append(f"Generated: {analysis.get('generated_at')}")
    lines.append(f"Output directory: {analysis.get('outdir')}")
    lines.append(f"Interface: {analysis.get('iface')}")
    lines.append(f"PID: {analysis.get('pid') or 'unknown'}")
    lines.append(f"Detected driver: {analysis.get('driver') or 'unknown'}")
    lines.append(f"Receive path mode: {analysis.get('rx_path')}")
    lines.append(f"Traffic mode: {analysis.get('traffic_mode')}")

    endpoint_parts = [f"{k}={v}" for k, v in endpoint.items() if v not in (None, "")]
    if endpoint_parts:
        lines.append("Endpoint: " + ", ".join(endpoint_parts))

    lines.append("")
    lines.append("Key findings")
    lines.append("------------")
    for f in analysis.get("findings", []):
        lines.append(f"- {f}")

    lines.append("")
    lines.append("Most likely issue(s)")
    lines.append("--------------------")
    for cause, confidence in analysis.get("likely_causes", []):
        lines.append(f"- {cause} (confidence: {confidence}%)")

    if analysis.get("significance"):
        lines.append("")
        lines.append("Baseline significance")
        lines.append("---------------------")
        for item in analysis.get("significance", []):
            lines.append(f"- {item}")

    lines.append("")
    lines.append("Counter deltas")
    lines.append("--------------")
    counter_deltas = analysis.get("counter_deltas", {}) or {}
    for group, values in counter_deltas.items():
        if not values:
            continue
        lines.append(f"{group}:")
        for key, value in sorted(values.items(), key=lambda kv: str(kv[0])):
            lines.append(f"  - {key}: {value}")

    lines.append("")
    lines.append("Interpretation guidance")
    lines.append("-----------------------")
    lines.append("- The same collector is used for unicast and multicast; traffic mode changes only endpoint validation and interpretation weighting.")
    lines.append("- In unicast mode, IGMP/group-membership assumptions are not used; socket ownership, peer/port matching, app drain rate, rmem, IRQ/RSS, and softnet pressure are weighted more strongly.")
    lines.append("- In multicast mode, group membership and multicast endpoint matching are included, but taskset/isolcpus still only prove configuration hints, not actual runtime placement.")
    lines.append("- ExaNIC capture is not invoked automatically; it is only attempted when --tcpdump is supplied.")
    lines.append("- taskset remains useful as runtime verification; isolcpus reduces scheduler noise but does not by itself prove receiver threads are pinned correctly.")
    lines.append("- Findings are based on deltas between sample1 and sample2, so run during the actual loss window or increase --interval if counters are too quiet.")

    summary = "\n".join(lines) + "\n"
    write(outdir / "SUMMARY.txt", summary)
    return summary


def positive_int(value: str) -> int:
    try:
        n = int(value)
    except Exception:
        raise argparse.ArgumentTypeError("must be an integer")
    if n <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return n


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description=f"UDP drop diagnostic collector + interpreter v{VERSION}"
    )
    ap.add_argument("--iface", required=True, help="Network interface to inspect, for example eth2 or ens5f0")
    ap.add_argument("--pid", type=int, help="PID of receiving process")
    ap.add_argument("--name", help="Process-name pattern used with pgrep -f when --pid is not supplied")
    ap.add_argument("--outdir", help="Output directory; default is ./udp_drop_diag_v6_<timestamp>")

    # Existing endpoint options retained.
    ap.add_argument("--group", help="Expected multicast group address")
    ap.add_argument("--src-ip", help="Expected packet source IP")
    ap.add_argument("--port", type=int, help="Expected UDP port")

    # New unicast-aware endpoint options.
    ap.add_argument("--dst-ip", help="Expected destination/local IP, useful for unicast mode")
    ap.add_argument("--peer-ip", help="Expected remote peer IP, useful for unicast mode")
    ap.add_argument(
        "--traffic-mode",
        choices=["auto", "unicast", "multicast"],
        default="auto",
        help="Endpoint interpretation mode. auto treats --group or multicast --dst-ip as multicast; otherwise unicast.",
    )

    ap.add_argument("--interval", type=positive_int, default=5, help="Seconds between sample1 and sample2")
    ap.add_argument("--baseline-file", help="JSON baseline file to compare against")
    ap.add_argument("--ss-history", help="Append matching ss socket lines to this file")
    ap.add_argument("--csv-history", help="Append one-row rate/cause history to this CSV file")
    ap.add_argument("--isolated-thread-regex", default=DEFAULT_ISOLATED_THREAD_REGEX)
    ap.add_argument("--tcpdump", action="store_true", help="Collect a short tcpdump/exanic-capture snapshot")
    ap.add_argument("--tcpdump-count", type=positive_int, default=200)
    ap.add_argument(
        "--rx-path",
        choices=["auto", "kernel", "onload", "exanic"],
        default="auto",
        help="Receive path interpretation mode",
    )
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = build_parser()
    args = ap.parse_args(argv)

    try:
        args.group = validate_optional_ip("--group", args.group)
        args.src_ip = validate_optional_ip("--src-ip", args.src_ip)
        args.dst_ip = validate_optional_ip("--dst-ip", args.dst_ip)
        args.peer_ip = validate_optional_ip("--peer-ip", args.peer_ip)
    except argparse.ArgumentTypeError as exc:
        ap.error(str(exc))

    iface_path = Path(f"/sys/class/net/{args.iface}")
    if not iface_path.exists():
        print(f"ERROR: interface {args.iface!r} not found", file=sys.stderr)
        return 2

    pid = resolve_pid(args.name, args.pid)
    driver = detect_driver(args.iface)
    rx_path = infer_rx_path(args.iface, args.rx_path, driver)
    traffic_mode = infer_traffic_mode(args.traffic_mode, args.group, args.dst_ip, args.peer_ip)

    outdir = Path(args.outdir or f"./udp_drop_diag_v6_{now_tag()}").resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    log(f"Output directory: {outdir}")
    log(f"Detected driver: {driver or 'unknown'}")
    log(f"Receive path mode: {rx_path}")
    log(f"Traffic mode: {traffic_mode}")
    if pid is not None:
        log(f"Resolved PID: {pid}")

    meta = {
        "version": VERSION,
        "started_at": datetime.now().isoformat(),
        "argv": sys.argv if argv is None else list(argv),
        "iface": args.iface,
        "pid": pid,
        "name": args.name,
        "driver": driver,
        "rx_path": rx_path,
        "traffic_mode": traffic_mode,
        "endpoint": {
            "port": args.port,
            "group": args.group,
            "src_ip": args.src_ip,
            "dst_ip": args.dst_ip,
            "peer_ip": args.peer_ip,
        },
        "interval": args.interval,
        "tcpdump": args.tcpdump,
        "tcpdump_count": args.tcpdump_count,
    }
    write(outdir / "run_metadata.json", json.dumps(meta, indent=2, sort_keys=True))

    collect(
        outdir / "sample1",
        args.iface,
        pid,
        args.port,
        args.group,
        args.src_ip,
        args.dst_ip,
        args.peer_ip,
        traffic_mode,
        rx_path,
        args.tcpdump,
        args.tcpdump_count,
    )

    log(f"Sleeping {args.interval}s between samples")
    time.sleep(args.interval)

    collect(
        outdir / "sample2",
        args.iface,
        pid,
        args.port,
        args.group,
        args.src_ip,
        args.dst_ip,
        args.peer_ip,
        traffic_mode,
        rx_path,
        args.tcpdump,
        args.tcpdump_count,
    )

    baseline_path = Path(args.baseline_file).resolve() if args.baseline_file else None
    baseline = load_baseline(baseline_path)

    analysis = analyze(
        outdir=outdir,
        iface=args.iface,
        pid=pid,
        interval=args.interval,
        baseline=baseline,
        isolated_thread_regex=args.isolated_thread_regex,
        rx_path=rx_path,
        driver=driver,
        traffic_mode=traffic_mode,
        group=args.group,
        src_ip=args.src_ip,
        dst_ip=args.dst_ip,
        peer_ip=args.peer_ip,
        port=args.port,
        tcpdump_requested=args.tcpdump,
    )

    write(outdir / "analysis.json", json.dumps(analysis, indent=2, sort_keys=True))
    save_baseline(outdir / "baseline_current.json", analysis)
    summary = write_summary(outdir, analysis)

    ss_hist = Path(args.ss_history).resolve() if args.ss_history else None
    append_ss_history(ss_hist, analysis["generated_at"], analysis.get("matching_socket_lines", []))

    top_cause, top_conf = analysis.get("likely_causes", [("", "")])[0]
    csv_path = Path(args.csv_history).resolve() if args.csv_history else None
    rates = analysis.get("rates", {}) or {}
    append_csv_history(
        csv_path,
        {
            "ts": analysis.get("generated_at"),
            "iface": args.iface,
            "pid": pid or "",
            "driver": driver,
            "rx_path": rx_path,
            "traffic_mode": traffic_mode,
            "port": args.port or "",
            "peer_ip": args.peer_ip or "",
            "src_ip": args.src_ip or "",
            "dst_ip": args.dst_ip or "",
            "group": args.group or "",
            "receive_buffer_errors_per_sec": rates.get("receive_buffer_errors_per_sec", 0.0),
            "packet_receive_errors_per_sec": rates.get("packet_receive_errors_per_sec", 0.0),
            "softnet_drops_per_sec": rates.get("softnet_drops_per_sec", 0.0),
            "softnet_squeezed_per_sec": rates.get("softnet_squeezed_per_sec", 0.0),
            "ip_rx_dropped_per_sec": rates.get("ip_rx_dropped_per_sec", 0.0),
            "ip_rx_errors_per_sec": rates.get("ip_rx_errors_per_sec", 0.0),
            "top_cause": top_cause,
            "top_confidence": top_conf,
        },
    )

    log(f"Summary written to {outdir / 'SUMMARY.txt'}")
    print("\n" + summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
