"""Prepare and restore a self-contained Windows CPython 3.13 package bundle."""

import argparse
import email
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUNDLE = ROOT / ".offline" / "windows-x64-cp313"


class BundleError(RuntimeError):
    """Safe, actionable failure without including credentials."""


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def run(arguments, *, env=None):
    subprocess.run([str(arg) for arg in arguments], cwd=ROOT, env=env, check=True)


def source_hashes():
    return {
        name: digest(ROOT / name)
        for name in (
            "pyproject.toml",
            "uv.lock",
            ".python-version",
            "tools/build-requirements.lock",
        )
    }


def wheel_requirements(directory: Path) -> str:
    rows = {}
    for wheel in sorted(directory.glob("*.whl")):
        with zipfile.ZipFile(wheel) as archive:
            metadata_files = [
                name
                for name in archive.namelist()
                if name.endswith(".dist-info/METADATA")
            ]
            if len(metadata_files) != 1:
                raise BundleError(f"Invalid wheel metadata: {wheel.name}")
            metadata = email.message_from_bytes(archive.read(metadata_files[0]))
        name = metadata["Name"].lower().replace("_", "-")
        if name in rows:
            raise BundleError(
                f"More than one wheel for {name}; use a fresh bundle directory"
            )
        rows[name] = f"{name}=={metadata['Version']} --hash=sha256:{digest(wheel)}"
    if not rows:
        raise BundleError("Wheel directory is empty")
    return "\n".join(rows[name] for name in sorted(rows)) + "\n"


def validate_bundle(bundle: Path, *, check_source=True):
    manifest_path = bundle / "manifest.json"
    if not manifest_path.is_file():
        raise BundleError("Bundle incomplete: manifest.json is missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != 1 or manifest.get("platform") != "windows-x64":
        raise BundleError("Unsupported offline bundle")
    if check_source and manifest["source_hashes"] != source_hashes():
        raise BundleError(
            "Bundle does not match project configuration; prepare a new bundle"
        )
    for relative, expected in manifest["files"].items():
        path = (bundle / relative).resolve()
        if not path.is_relative_to(bundle.resolve()):
            raise BundleError("Invalid path in bundle manifest")
        if not path.is_file() or digest(path) != expected:
            raise BundleError(f"Missing or corrupted bundle file: {relative}")
    print(f"PASS: verified {len(manifest['files'])} bundle files", flush=True)
    return manifest


def prepare(bundle: Path, uv: Path):
    if sys.platform != "win32" or platform.machine().lower() not in ("amd64", "x86_64"):
        raise BundleError("Prepare this bundle on Windows x64")
    pinned_python = (ROOT / ".python-version").read_text().strip()
    if platform.python_version() != pinned_python:
        raise BundleError(f"Run preparation with Python {pinned_python}")
    if (bundle / "manifest.json").exists():
        raise BundleError(
            "Complete bundle already exists; choose a new --bundle path to preserve it"
        )
    if not uv.is_file():
        raise BundleError("uv executable not found; pass --uv with its full path")
    base_python = Path(sys.base_prefix) / "python.exe"
    state = {
        name: digest(ROOT / name)
        for name in ("pyproject.toml", "uv.lock", ".python-version")
    }
    if bundle.exists():
        marker = bundle / "preparing.json"
        if not marker.is_file() or json.loads(marker.read_text()) != state:
            raise BundleError(
                "Existing directory is not a matching partial bundle; choose a new path"
            )
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "preparing.json").write_text(json.dumps(state), encoding="utf-8")
    wheelhouse = bundle / "wheels"
    wheelhouse.mkdir(exist_ok=True)
    runtime = bundle / "runtime-requirements.txt"
    constraints = bundle / "runtime-constraints.txt"
    run(
        [
            uv,
            "export",
            "--quiet",
            "--locked",
            "--all-groups",
            "--no-emit-project",
            "--output-file",
            runtime,
        ]
    )
    run(
        [
            uv,
            "export",
            "--quiet",
            "--locked",
            "--all-groups",
            "--no-emit-project",
            "--no-hashes",
            "--output-file",
            constraints,
        ]
    )
    pip = [
        base_python,
        "-m",
        "pip",
        "--disable-pip-version-check",
        "download",
        "--index-url",
        "https://pypi.org/simple",
        "--only-binary=:all:",
        "--timeout",
        "15",
        "--retries",
        "1",
    ]
    run([*pip, "--no-deps", "--require-hashes", "-r", runtime, "--dest", wheelhouse])
    build_dir = bundle / "build-wheels"
    build_dir.mkdir(exist_ok=True)
    build_lock = ROOT / "tools/build-requirements.lock"
    if not build_lock.is_file():
        raise BundleError("Missing tools/build-requirements.lock; restore it from Git")
    run([*pip, "--no-deps", "--require-hashes", "-r", build_lock, "--dest", build_dir])
    for wheel in build_dir.glob("*.whl"):
        destination = wheelhouse / wheel.name
        if destination.exists() and digest(destination) != digest(wheel):
            raise BundleError(f"Conflicting wheel: {wheel.name}")
        shutil.copy2(wheel, destination)
    (bundle / "requirements.txt").write_text(
        wheel_requirements(wheelhouse), encoding="utf-8"
    )
    shutil.copy2(uv, bundle / "uv.exe")
    # Snapshot of uv-managed standalone Python, including pip, not the project venv.
    shutil.copytree(
        Path(sys.base_prefix),
        bundle / "python",
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    manifest = {
        "schema": 1,
        "platform": "windows-x64",
        "python": pinned_python,
        "source_hashes": source_hashes(),
        "files": {
            str(path.relative_to(bundle)).replace("\\", "/"): digest(path)
            for path in sorted(bundle.rglob("*"))
            if path.is_file()
        },
    }
    (bundle / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    validate_bundle(bundle)
    print(f"Bundle ready: {bundle}; wheels: {len(list(wheelhouse.glob('*.whl')))}")


def restore(bundle: Path, target: Path):
    validate_bundle(bundle)
    if sys.platform != "win32":
        raise BundleError("This bundle restores Windows x64 environments only")
    target = target.resolve()
    if not target.is_relative_to(ROOT.resolve()) or target == ROOT.resolve():
        raise BundleError("Target must be a new directory inside the project")
    if target.is_relative_to(bundle.resolve()) or bundle.resolve().is_relative_to(
        target
    ):
        raise BundleError("Target and bundle must be separate directories")
    if target.exists():
        raise BundleError(
            "Target already exists; choose a new --target, no files were deleted"
        )
    uv = bundle / "uv.exe"
    python = bundle / "python/python.exe"
    environment = os.environ.copy()
    environment.update(
        UV_OFFLINE="true",
        UV_PYTHON_DOWNLOADS="never",
        PYTHONDONTWRITEBYTECODE="1",
        PIP_NO_INDEX="1",
    )
    # Deliberately empty per-run cache, never the user's populated cache.
    with tempfile.TemporaryDirectory(prefix="orbita-offline-cache-") as cache:
        environment["UV_CACHE_DIR"] = cache
        common = ["--offline", "--no-cache", "--no-config", "--no-python-downloads"]
        packages = ["--no-index", "--find-links", bundle / "wheels"]
        run([uv, "venv", *common, "--python", python, target], env=environment)
        target_python = target / "Scripts/python.exe"
        run(
            [
                uv,
                "pip",
                "sync",
                *common,
                *packages,
                "--python",
                target_python,
                "--require-hashes",
                bundle / "requirements.txt",
            ],
            env=environment,
        )
        run(
            [
                uv,
                "pip",
                "install",
                *common,
                *packages,
                "--python",
                target_python,
                "--no-build-isolation",
                "--no-deps",
                "--editable",
                ROOT,
            ],
            env=environment,
        )
        run(
            [uv, "pip", "check", "--offline", "--no-config", "--python", target_python],
            env=environment,
        )
        run([target_python, "-m", "orbita.cli", "--mode", "offline"], env=environment)
    print(
        f"PASS: environment restored without package network access or shared cache: {target}"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "verify", "restore"))
    parser.add_argument("--bundle", type=Path, default=DEFAULT_BUNDLE)
    parser.add_argument("--target", type=Path, default=ROOT / ".venv")
    parser.add_argument("--uv", type=Path, default=Path(shutil.which("uv") or "uv.exe"))
    args = parser.parse_args()
    try:
        if args.action == "prepare":
            prepare(args.bundle.resolve(), args.uv.resolve())
        elif args.action == "restore":
            restore(args.bundle.resolve(), args.target)
        else:
            validate_bundle(args.bundle.resolve())
    except (BundleError, OSError, subprocess.CalledProcessError) as error:
        if isinstance(error, BundleError):
            raise SystemExit(str(error)) from None
        raise SystemExit(
            f"FAIL: {type(error).__name__}; no existing environment was deleted"
        ) from None


if __name__ == "__main__":
    main()
