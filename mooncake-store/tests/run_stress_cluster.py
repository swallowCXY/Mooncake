#!/usr/bin/env python3
"""
Cluster orchestration for real_client_stress_workload.py (nerdctl + SSH).

Subcommands:
  run   Start master + clients, optional orchestrated preload/read sync, merge STATS_JSON.
  kill  Remove mc-client-node* and mc-master on listed hosts.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# Path inside the stress-test container image (override with MOONCAKE_STRESS_PY).
DEFAULT_PY_SCRIPT_CONTAINER = os.environ.get(
    "MOONCAKE_STRESS_PY", "/vllm-workspace/Mooncake/mooncake-store/tests/real_client_stress_workload.py"
)
DEFAULT_IMAGE = os.environ.get("IMAGE", "localhost/mooncake-p2p-test:v380_new")
MASTER_CONTAINER = "mc-master"
PRELOAD_DONE_RE = re.compile(r"=== Phase preload_done node_id=(\d+) keys=(\d+) ===")
CONTROL_ENDPOINT_RE = re.compile(r"CONTROL_ENDPOINT:([0-9.]+:\d+)")


def sh_quote(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"


def local_ips() -> List[str]:
    try:
        out = subprocess.check_output(["hostname", "-I"], text=True, stderr=subprocess.DEVNULL)
        return out.split()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []


def is_local_host(host: str) -> bool:
    if host in ("127.0.0.1", "localhost"):
        return True
    return host in local_ips()


def ssh_base(user: str, password: Optional[str]) -> List[str]:
    if password:
        return ["sshpass", "-p", password, "ssh", "-o", "StrictHostKeyChecking=no"]
    return ["ssh", "-o", "StrictHostKeyChecking=no"]


def run_shell(
    cmd: str,
    *,
    host: Optional[str] = None,
    ssh_user: str = "root",
    ssh_password: Optional[str] = None,
    check: bool = False,
) -> subprocess.CompletedProcess:
    if host and not is_local_host(host):
        full = " ".join(ssh_base(ssh_user, ssh_password)) + f" {sh_quote(ssh_user + '@' + host)} " + sh_quote(cmd)
        return subprocess.run(full, shell=True, capture_output=True, text=True, check=check)
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)


def get_logs(container: str, host: Optional[str], ssh_user: str, ssh_password: Optional[str]) -> str:
    cmd = f"nerdctl logs {container} 2>&1"
    r = run_shell(cmd, host=host, ssh_user=ssh_user, ssh_password=ssh_password)
    return r.stdout or r.stderr or ""


def send_read(host: str, endpoint: str, timeout: float = 10.0) -> bool:
    try:
        h, p = endpoint.rsplit(":", 1)
        # On remote node, listener binds 0.0.0.0; connect via the host's address we SSH to.
        if h in ("0.0.0.0", "::"):
            h = host
        with socket.create_connection((h, int(p)), timeout=timeout) as s:
            s.sendall(b"READ\n")
        return True
    except OSError as e:
        print(f"[WARN] send READ to {host} {endpoint}: {e}", file=sys.stderr)
        return False


def parse_client_ips(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def per_node_rpc_ports(client_ips: List[str], base_port: int) -> List[int]:
    seen: Dict[str, int] = {}
    ports = []
    for h in client_ips:
        c = seen.get(h, 0)
        ports.append(base_port + c)
        seen[h] = c + 1
    return ports


def control_port_for(rpc_port: int, node_id: int) -> int:
    return rpc_port + 10_000 + node_id


def build_python_cmd(
    args: argparse.Namespace,
    node_id: int,
    num_nodes: int,
    rpc_port: int,
    control_port: int,
) -> str:
    script = args.python_script
    local_host = args.client_hosts[node_id - 1]
    # Mooncake v0.3.8: setup_real local hostname must include port (e.g. 10.0.0.11:12345),
    # and we may run multiple clients on the same machine -> use per-node rpc_port.
    if ":" not in local_host:
        local_host = f"{local_host}:{rpc_port}"
    common = [
        "python3",
        "-u",
        sh_quote(script),
        f"--workload_mode={args.workload_mode}",
        f"--protocol={args.protocol}",
        f"--master_address={args.master_address}",
        f"--local_hostname={local_host}",
        f"--metadata_connection_string={args.metadata_connection_string}",
        f"--client_mode={args.client_mode}",
        f"--num_nodes={num_nodes}",
        f"--node_id={node_id}",
        f"--key_count={args.key_count}",
        f"--num_threads={args.num_threads}",
        f"--test_operation_nums={args.test_operation_nums}",
        f"--value_size={args.value_size}",
        f"--random_seed={args.random_seed}",
        f"--remote_read_ratio={args.remote_read_ratio}",
        f"--write_ratio={args.write_ratio}",
        f"--op_sequence_max_rounds={args.op_sequence_max_rounds}",
        f"--rpc_thread_num={args.rpc_thread_num}",
        f"--client_rpc_port={rpc_port}",
        f"--post_test_wait_seconds={args.post_test_wait_seconds}",
        f"--cw_max_threads={args.cw_max_threads}",
        f"--cw_success_rate_threshold={args.cw_success_rate_threshold}",
        f"--cw_thread_scale_base={args.cw_thread_scale_base}",
        f"--cw_target_fill_ratio={args.cw_target_fill_ratio}",
        f"--cw_evict_data_ratio={args.cw_evict_data_ratio}",
        f"--cw_base_memory_bytes={args.cw_base_memory_bytes}",
        f"--cw_verify_sample_limit={args.cw_verify_sample_limit}",
        f"--global_segment_size={args.global_segment_size}",
        f"--local_buffer_size={args.local_buffer_size}",
    ]
    if args.device_names:
        common.append(f"--device_names={args.device_names}")
    if args.use_orchestrated and args.workload_mode == "preload_then_read":
        common.append("--run_mode=orchestrated")
        common.append(f"--control_listen=0.0.0.0:{control_port}")
    else:
        common.append("--run_mode=once")
        if args.start_timestamp_ms > 0:
            common.append(f"--start_timestamp_ms={args.start_timestamp_ms}")
    return " ".join(common)


def start_master(args: argparse.Namespace) -> None:
    master = args.master_ip

    # Check if concurrent_write_with_evict workload is used
    is_evict_mode = getattr(args, "workload_mode", "") == "concurrent_write_with_evict"

    # Centralized or p2p 
    if args.client_mode == "p2p":
        mooncake_args = (
            f"/usr/local/bin/mooncake_master --deployment_mode=P2P "
            f"--rpc_address=0.0.0.0 --rpc_port={args.master_rpc_port} "
            f"--rpc_thread_num={args.rpc_thread_num} "
        )
        if is_evict_mode:
            mooncake_args += (
                " --eviction_high_watermark_ratio=0.8"
                " --allow_evict_soft_pinned_objects=true"
            )
        else:
            mooncake_args += (
                " --default_kv_lease_ttl=600000 --default_kv_soft_pin_ttl=3600000"
            )
    else:
        mooncake_args = (
            f"/usr/local/bin/mooncake_master "
            f"--rpc_address=0.0.0.0 --rpc_port={args.master_rpc_port} "
            f"--rpc_thread_num={args.rpc_thread_num} "
            f"--enable_http_metadata_server=true --http_metadata_server_host=0.0.0.0 "
            f"--http_metadata_server_port={args.master_http_port} "
        )
        if is_evict_mode:
            mooncake_args += (
                " --eviction_high_watermark_ratio=0.8"
                " --allow_evict_soft_pinned_objects=true"
            )
        else:
            mooncake_args += (
                " --default_kv_lease_ttl=600000 --default_kv_soft_pin_ttl=3600000"
            )
    inner = (
        f"nerdctl rm -f {MASTER_CONTAINER} 2>/dev/null || true; "
        f"nerdctl run -d --name {MASTER_CONTAINER} --network host {sh_quote(args.image)} "
        f"{mooncake_args}"
    )
    print(f"[INFO] Starting master on {master}...")
    r = run_shell(inner, host=master if not is_local_host(master) else None, ssh_user=args.ssh_user, ssh_password=args.ssh_password)
    if r.returncode != 0:
        print(r.stderr or r.stdout, file=sys.stderr)
        raise SystemExit(f"start master failed: {r.returncode}")
    time.sleep(5)


def start_client_container(
    args: argparse.Namespace,
    host: str,
    node_id: int,
    rpc_port: int,
    control_port: int,
) -> None:
    name = f"mc-client-node{node_id}"
    py_cmd = build_python_cmd(args, node_id, len(args.client_hosts), rpc_port, control_port)
    py_shell = py_cmd + " && sleep infinity"
    inner = (
        f"nerdctl rm -f {name} 2>/dev/null || true; "
        f"nerdctl run -d --name {name} --network host "
        f"-e MOONCAKE_TIERED_CONFIG={sh_quote(args.tiered_backend_config)} "
        f"-e PYTHONPATH=/tmp/mooncake_038_clean:$PYTHONPATH "
        f"{sh_quote(args.image)} sh -c {sh_quote(py_shell)}"
    )
    print(f"[INFO] Starting client {name} on {host} (rpc={rpc_port})...")
    r = run_shell(inner, host=host if not is_local_host(host) else None, ssh_user=args.ssh_user, ssh_password=args.ssh_password)
    if r.returncode != 0:
        print(r.stderr or r.stdout, file=sys.stderr)
        raise SystemExit(f"start client {name} failed: {r.returncode}")


def wait_preload_done(
    args: argparse.Namespace,
    endpoints_out: Dict[int, str],
    poll_interval: float = 2.0,
    timeout: float = 3600.0,
) -> bool:
    deadline = time.time() + timeout
    num = len(args.client_hosts)
    while time.time() < deadline:
        done_nodes = set()
        for i, host in enumerate(args.client_hosts):
            nid = i + 1
            log = get_logs(f"mc-client-node{nid}", None if is_local_host(host) else host, args.ssh_user, args.ssh_password)
            for m in PRELOAD_DONE_RE.finditer(log):
                done_nodes.add(int(m.group(1)))
            ce = CONTROL_ENDPOINT_RE.search(log)
            if ce:
                endpoints_out[nid] = ce.group(1).strip()
        if len(done_nodes) >= num:
            print(f"[INFO] All {num} clients reported preload_done.")
            return True
        print(f"[INFO] preload_done: {len(done_nodes)}/{num} (waiting...)")
        time.sleep(poll_interval)
    print("[ERROR] Timeout waiting for preload_done", file=sys.stderr)
    return False


def wait_test_completed(
    args: argparse.Namespace,
    poll_interval: float = 5.0,
    timeout: float = 7200.0,
) -> bool:
    deadline = time.time() + timeout
    num = len(args.client_hosts)
    while time.time() < deadline:
        n = 0
        for i, host in enumerate(args.client_hosts):
            nid = i + 1
            log = get_logs(f"mc-client-node{nid}", None if is_local_host(host) else host, args.ssh_user, args.ssh_password)
            if "=== Test Completed ===" in log:
                n += 1
        print(f"[INFO] Test completed: {n}/{num}")
        if n >= num:
            return True
        time.sleep(poll_interval)
    print("[ERROR] Timeout waiting for Test Completed", file=sys.stderr)
    return False


def collect_stats(args: argparse.Namespace) -> List[dict]:
    merged = []
    tag = "STATS_JSON:"
    for i, host in enumerate(args.client_hosts):
        nid = i + 1
        log = get_logs(f"mc-client-node{nid}", None if is_local_host(host) else host, args.ssh_user, args.ssh_password)
        for line in log.splitlines():
            p = line.find(tag)
            if p < 0:
                continue
            raw = line[p + len(tag) :].strip()
            try:
                merged.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
    return merged


def cmd_run(ns: argparse.Namespace) -> int:
    ns.client_hosts = parse_client_ips(ns.client_ips)
    if not ns.client_hosts:
        print("No client IPs", file=sys.stderr)
        return 1
    if (
        not ns.use_orchestrated
        and ns.workload_mode == "preload_then_read"
        and ns.start_delay_sec > 0
        and ns.start_timestamp_ms == 0
    ):
        ns.start_timestamp_ms = int(time.time() * 1000) + ns.start_delay_sec * 1000
        print(f"[INFO] Computed start_timestamp_ms={ns.start_delay_sec}s ahead -> {ns.start_timestamp_ms}")
    ns.master_address = f"{ns.master_ip}:{ns.master_rpc_port}"
    if ns.client_mode == "p2p":
        ns.metadata_connection_string = "P2PHANDSHAKE"
    else:
        ns.metadata_connection_string = f"http://{ns.master_ip}:{ns.master_http_port}/metadata"

    rpc_ports = per_node_rpc_ports(ns.client_hosts, ns.client_rpc_port)
    ns._rpc_ports = rpc_ports

    start_master(ns)
    for i, host in enumerate(ns.client_hosts):
        nid = i + 1
        cp = control_port_for(rpc_ports[i], nid)
        start_client_container(ns, host, nid, rpc_ports[i], cp)

    endpoints: Dict[int, str] = {}
    if ns.use_orchestrated and ns.workload_mode == "preload_then_read":
        if not wait_preload_done(ns, endpoints):
            return 1
        for i, host in enumerate(ns.client_hosts):
            nid = i + 1
            ep = endpoints.get(nid)
            if not ep:
                # Fallback: derive from rpc port
                h, p = "0.0.0.0", str(control_port_for(rpc_ports[i], nid))
                ep = f"{h}:{p}"
            print(f"[INFO] Sending READ to node {nid} at {host} (control {ep})")
            send_read(host, ep)
        time.sleep(1)

    if ns.wait_completion:
        if not wait_test_completed(ns):
            return 1
        stats = collect_stats(ns)
        if stats:
            out = Path(ns.merge_output)
            out.write_text(json.dumps({"nodes": stats}, indent=2), encoding="utf-8")
            print(f"[INFO] Wrote merged stats to {out}")
        for i, host in enumerate(ns.client_hosts):
            nid = i + 1
            print(f"\n--- Client {nid} ({host}) ---")
            log = get_logs(f"mc-client-node{nid}", None if is_local_host(host) else host, ns.ssh_user, ns.ssh_password)
            lines = log.splitlines()
            print("\n".join(lines[-50:]))
        if ns.cleanup:
            cmd_kill(ns)
    return 0


def cmd_kill(ns: argparse.Namespace) -> int:
    hosts = parse_client_ips(ns.client_ips) if ns.client_ips else []
    if not hosts:
        print("No client IPs for kill", file=sys.stderr)
        return 1
    num = len(hosts)
    for i, host in enumerate(hosts):
        nid = i + 1
        name = f"mc-client-node{nid}"
        c = f"nerdctl rm -f {name} 2>/dev/null || true"
        print(f"[INFO] kill {name} on {host}")
        run_shell(c, host=host if not is_local_host(host) else None, ssh_user=ns.ssh_user, ssh_password=ns.ssh_password)
    mk = ns.master_ip
    print(f"[INFO] kill {MASTER_CONTAINER} on {mk}")
    run_shell(
        f"nerdctl rm -f {MASTER_CONTAINER} 2>/dev/null || true",
        host=mk if not is_local_host(mk) else None,
        ssh_user=ns.ssh_user,
        ssh_password=ns.ssh_password,
    )
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Mooncake stress cluster orchestrator")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("run", help="Deploy and run stress test")
    pr.add_argument("--ssh-user", default=os.environ.get("SSH_USER", "root"))
    pr.add_argument("--ssh-password", default=os.environ.get("SSH_PASSWORD") or None)
    pr.add_argument("--mode", choices=("p2p", "centralized"), default=os.environ.get("DEPLOYMENT_MODE", "centralized"))
    pr.add_argument("--master-ip", default=os.environ.get("MASTER_IP", "192.168.200.15"))
    pr.add_argument("--client-ips", default=os.environ.get("CLIENT_IPS", "192.168.200.15,192.168.200.25"))
    pr.add_argument("--image", default=os.environ.get("IMAGE", DEFAULT_IMAGE))
    pr.add_argument("--workload-mode", default=os.environ.get("WORKLOAD_MODE", "op_sequence"))
    pr.add_argument("--protocol", default=os.environ.get("PROTOCOL", "tcp"))
    pr.add_argument("--key-count", type=int, default=int(os.environ.get("KEY_COUNT", "1000")))
    pr.add_argument("--num-threads", type=int, default=int(os.environ.get("NUM_THREADS", "4")))
    pr.add_argument("--test-operation-nums", type=int, default=int(os.environ.get("TEST_OPERATION_NUMS", "500")))
    pr.add_argument("--value-size", type=int, default=int(os.environ.get("VALUE_SIZE", "1048576")))
    pr.add_argument("--random-seed", type=int, default=int(os.environ.get("RANDOM_SEED", "12345")))
    pr.add_argument("--remote-read-ratio", type=float, default=float(os.environ.get("REMOTE_READ_RATIO", "0.5")))
    pr.add_argument("--write-ratio", type=float, default=float(os.environ.get("WRITE_RATIO", "0.3")))
    pr.add_argument("--op-sequence-max-rounds", type=int, default=int(os.environ.get("OP_SEQUENCE_MAX_ROUNDS", "8")))
    pr.add_argument("--post-test-wait", type=int, default=int(os.environ.get("POST_TEST_WAIT_SECONDS", "40")))
    pr.add_argument("--master-rpc-port", type=int, default=int(os.environ.get("MASTER_RPC_PORT", "50053")))
    pr.add_argument("--master-http-port", type=int, default=int(os.environ.get("MASTER_HTTP_PORT", "8080")))
    pr.add_argument("--rpc-thread-num", type=int, default=int(os.environ.get("RPC_THREAD_NUM", "16")))
    pr.add_argument("--client-rpc-port", type=int, default=int(os.environ.get("CLIENT_RPC_PORT", "12345")))
    pr.add_argument("--tiered-backend-config", default=os.environ.get("TIERED_BACKEND_CONFIG", '{"tiers":[{"type":"DRAM","capacity":1073741824,"priority":10}]}'))
    pr.add_argument("--cw-max-threads", type=int, default=int(os.environ.get("CW_MAX_THREADS", "256")))
    pr.add_argument("--cw-success-rate-threshold", type=float, default=float(os.environ.get("CW_SUCCESS_RATE_THRESHOLD", "0.99")))
    pr.add_argument("--cw-thread-scale-base", type=int, default=int(os.environ.get("CW_THREAD_SCALE_BASE", "2")))
    pr.add_argument("--cw-target-fill-ratio", type=float, default=float(os.environ.get("CW_TARGET_FILL_RATIO", "0.15")))
    pr.add_argument("--cw-evict-data-ratio", type=float, default=float(os.environ.get("CW_EVICT_DATA_RATIO", "3.0")))
    pr.add_argument("--cw-base-memory-bytes", type=int, default=int(os.environ.get("CW_BASE_MEMORY_BYTES", "1073741824")))
    pr.add_argument("--cw-verify-sample-limit", type=int, default=int(os.environ.get("CW_VERIFY_SAMPLE_LIMIT", "0")))
    pr.add_argument("--global-segment-size", type=int, default=int(os.environ.get("GLOBAL_SEGMENT_SIZE", "2147483648")))
    pr.add_argument("--local-buffer-size", type=int, default=int(os.environ.get("LOCAL_BUFFER_SIZE", str(16 * 1024 * 1024))))
    pr.add_argument("--device-names", default=os.environ.get("DEVICE_NAMES", ""))
    pr.add_argument(
        "--python-script",
        default=os.environ.get("MOONCAKE_STRESS_PY", DEFAULT_PY_SCRIPT_CONTAINER),
        help="Path to real_client_stress_workload.py inside the container image",
    )
    pr.add_argument("--start-timestamp-ms", type=int, default=int(os.environ.get("START_TIMESTAMP_MS", "0")))
    pr.add_argument(
        "--start-delay-sec",
        type=int,
        default=int(os.environ.get("START_DELAY_SEC", "0")),
        help="If >0 and preload_then_read once mode: set start_timestamp_ms to now+delay (seconds)",
    )
    pr.add_argument(
        "--use-orchestrated",
        action="store_true",
        default=os.environ.get("USE_ORCHESTRATED", "").lower() in ("1", "true", "yes"),
        help="preload_then_read: wait for preload_done then send READ (no wall-clock start_ts)",
    )
    pr.add_argument("--no-wait", action="store_true", help="Do not wait for completion")
    pr.add_argument("--no-cleanup", action="store_true", help="Do not remove containers after run")
    pr.add_argument("--merge-output", default="merged_stress_report.json")
    pr.set_defaults(func=cmd_run)

    pk = sub.add_parser("kill", help="Remove client and master containers")
    pk.add_argument("--ssh-user", default=os.environ.get("SSH_USER", "root"))
    pk.add_argument("--ssh-password", default=os.environ.get("SSH_PASSWORD") or None)
    pk.add_argument("--master-ip", default=os.environ.get("MASTER_IP", "192.168.200.15"))
    pk.add_argument("--client-ips", default=os.environ.get("CLIENT_IPS", "192.168.200.15,192.168.200.25"))
    pk.set_defaults(func=cmd_kill)

    args = p.parse_args()
    if args.cmd == "run":
        args.client_mode = "p2p" if args.mode == "p2p" else "centralized"
        args.post_test_wait_seconds = args.post_test_wait
        args.wait_completion = not args.no_wait
        args.cleanup = not args.no_cleanup
        if args.use_orchestrated and args.workload_mode != "preload_then_read":
            print("[WARN] --use-orchestrated only affects preload_then_read; ignoring for other modes", file=sys.stderr)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
