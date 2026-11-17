"""Check release metadata; publish Rust crates only with --publish-rust."""

import argparse
import json
import math
import re
import subprocess
import sys
import time
from collections import deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REPOSITORY = "https://github.com/maartyman/incroxigraph"
ROOT = Path(__file__).resolve().parents[2]


def rust_version_is_published(name, version):
    request = Request(
        f"https://crates.io/api/v1/crates/{name}/{version}",
        headers={"User-Agent": f"incroxigraph-release ({REPOSITORY})"},
    )
    try:
        with urlopen(request, timeout=30) as response:
            published = json.load(response).get("version", {})
    except HTTPError as error:
        error.close()
        if error.code == 404:
            return False
        raise ValueError(
            f"Cannot check {name} {version}: crates.io returned HTTP {error.code}. "
            "Publishing stopped; no version was assumed missing."
        ) from error
    except URLError as error:
        raise ValueError(f"Cannot check {name} {version}: {error.reason}") from error
    if published.get("crate") != name or published.get("num") != version:
        raise ValueError(f"Unexpected crates.io response for {name} {version}")
    if published.get("yanked"):
        raise ValueError(
            f"{name} {version} is yanked; resolve this before publishing dependants"
        )
    return True


def publish_rust_crate(name):
    command = ["cargo", "publish", "--locked", "--package", name]
    while True:
        output = deque(maxlen=30)
        with subprocess.Popen(
            command, cwd=ROOT, stderr=subprocess.PIPE, text=True
        ) as process:
            assert process.stderr is not None
            for line in process.stderr:
                sys.stderr.write(line)
                sys.stderr.flush()
                output.append(line)
            status = process.wait()
        if status == 0:
            return
        error = "".join(output)
        retry = re.search(r"Please try again after ([^\r\n]+? GMT)", error)
        if "429 Too Many Requests" not in error or retry is None:
            raise subprocess.CalledProcessError(status, command)
        try:
            retry_at = parsedate_to_datetime(retry[1])
        except (ValueError, TypeError):
            raise subprocess.CalledProcessError(status, command) from None
        delay = max(
            30, math.ceil((retry_at - datetime.now(timezone.utc)).total_seconds()) + 2
        )
        print(
            f"crates.io rate limit: waiting {delay} seconds before retrying {name} "
            f"(server retry time: {retry[1]}). You can interrupt and resume later.",
            flush=True,
        )
        time.sleep(delay)


def rust_release_order(packages, version):
    by_name = {package["name"]: package for package in packages}
    if "incroxigraph" not in by_name:
        raise ValueError(
            "Rust publishing is not ready: rename the oxigraph package to incroxigraph "
            "and namespace its local dependencies before enabling Rust releases."
        )
    ordered = []
    visited = set()
    active = set()

    def visit(name):
        if name in visited:
            return
        if name in active:
            raise ValueError(f"Cyclic Rust dependency: {name}")
        package = by_name[name]
        if name != "incroxigraph" and not name.startswith("incroxigraph-"):
            raise ValueError(f"Refusing to publish upstream crate name: {name}")
        if package["version"] != version or package["publish"] == []:
            raise ValueError(f"{name} must be publishable at version {version}")
        if package["repository"] != REPOSITORY:
            raise ValueError(f"{name} must identify the Incroxigraph repository")
        active.add(name)
        for dependency in package["dependencies"]:
            if dependency["kind"] != "dev" and dependency["name"] in by_name:
                visit(dependency["name"])
        active.remove(name)
        visited.add(name)
        ordered.append(name)

    visit("incroxigraph")
    return ordered


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="Expected Git tag, e.g. v0.0.1")
    parser.add_argument(
        "--rust", action="store_true", help="Also require publishable Rust metadata"
    )
    parser.add_argument(
        "--publish-rust",
        action="store_true",
        help="Upload Rust crates in dependency order",
    )
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    command = ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"]
    if args.offline:
        command.append("--offline")
    metadata = json.loads(subprocess.check_output(command, cwd=ROOT))
    workspace = set(metadata["workspace_members"])
    packages = [
        package for package in metadata["packages"] if package["id"] in workspace
    ]
    bindings = {
        package["name"]: package
        for package in packages
        if package["name"] in {"pyincroxigraph", "incroxigraph-js"}
    }
    if set(bindings) != {"pyincroxigraph", "incroxigraph-js"}:
        raise ValueError(
            "Both pyincroxigraph and incroxigraph-js must be workspace members"
        )
    version = bindings["pyincroxigraph"]["version"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError(
            "This release workflow expects a regular version such as 0.0.1"
        )
    for name, package in bindings.items():
        if package["version"] != version or package["repository"] != REPOSITORY:
            raise ValueError(
                f"{name} must use version {version} and repository {REPOSITORY}"
            )
    if args.tag is not None and args.tag != f"v{version}":
        raise ValueError(f"Tag {args.tag!r} does not match package version v{version}")
    print(f"Python and npm metadata match version {version}", flush=True)
    if args.rust or args.publish_rust:
        order = rust_release_order(packages, version)
        print("Rust release order: " + ", ".join(order), flush=True)
        if args.publish_rust:
            if args.offline:
                raise ValueError("Publishing requires network access; remove --offline")
            for name in order:
                if rust_version_is_published(name, version):
                    print(f"Skipping {name} {version}: already published", flush=True)
                    continue
                try:
                    publish_rust_crate(name)
                except subprocess.CalledProcessError:
                    raise SystemExit(
                        f"Publishing stopped at {name}. Resolve the Cargo error above, "
                        "or wait until the registry's retry time, then rerun this command. "
                        "Already published versions will be skipped."
                    ) from None


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        raise SystemExit(str(error)) from error
    except KeyboardInterrupt:
        raise SystemExit(
            "Publishing interrupted. Rerun the command to resume; "
            "already published versions will be skipped."
        ) from None
