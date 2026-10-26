#!/usr/bin/env python3
"""Run the real-browser suites against the workspace's disposable test server."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import tomllib

ROOT = Path(__file__).resolve().parents[1]


def bindgen_version():
    packages = tomllib.loads((ROOT / "Cargo.lock").read_text())["package"]
    versions = {p["version"] for p in packages if p["name"] == "wasm-bindgen"}
    if len(versions) != 1:
        raise RuntimeError("Expected exactly one locked wasm-bindgen version")
    return versions.pop()


def listening(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print-bindgen-version", action="store_true")
    args = parser.parse_args()
    version = bindgen_version()
    if args.print_bindgen_version:
        print(version)
        return
    for tool in ("wasm-bindgen", "wasm-bindgen-test-runner"):
        actual = subprocess.check_output([tool, "--version"], text=True).strip().split()[-1]
        if actual != version:
            raise RuntimeError(f"{tool} {actual} does not match Cargo.lock {version}")
    env = os.environ.copy()
    driver = env.get("CHROMEDRIVER") or shutil.which("chromedriver")
    if not driver and env.get("CHROMEWEBDRIVER"):
        driver = str(Path(env["CHROMEWEBDRIVER"]) / "chromedriver")
    if not driver or not Path(driver).is_file():
        raise RuntimeError("Install Chrome and a matching chromedriver; set CHROMEDRIVER if needed")
    env["CHROMEDRIVER"] = driver
    env["CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER"] = "wasm-bindgen-test-runner"
    env["WASM_BINDGEN_USE_BROWSER"] = "1"
    env.pop("NO_HEADLESS", None)
    ports = (32123, 32124)
    if any(listening(port) for port in ports):
        raise RuntimeError("Browser-test ports 32123/32124 must be unused")
    subprocess.run(["cargo", "build", "--locked", "-p", "websock-browser-test-server"],
                   cwd=ROOT, check=True)
    # cargo metadata also honors an externally configured target directory.
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], cwd=ROOT))
    binary = Path(metadata["target_directory"]) / "debug" / "websock-browser-test-server"
    if os.name == "nt":
        binary = binary.with_suffix(".exe")
    with tempfile.TemporaryFile(mode="w+") as log:
        server = subprocess.Popen([str(binary)], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 30
            while not all(listening(port) for port in ports):
                if server.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("Browser test server failed to start")
                time.sleep(0.1)
            subprocess.run([
                "cargo", "test", "--locked", "--target", "wasm32-unknown-unknown",
                "-p", "websock-wasm", "-p", "websock-wasm-mux",
            ], cwd=ROOT, env=env, check=True, timeout=300)
        except BaseException:
            log.seek(0)
            print(log.read())
            raise
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()


if __name__ == "__main__":
    main()
