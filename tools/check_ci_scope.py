"""Select full CI unless the entire Git change is acceptance reports."""

import argparse
import os
import re
import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]


def reports_only(paths):
    return bool(paths) and all(
        re.fullmatch(rb"docs/acceptance/[^/]+\.md", path) for path in paths
    )


def check_reports(root):
    errors = []
    for path in sorted((root / "docs/acceptance").glob("*.md")):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            errors.append(f"{path.name}: expected repository file")
            continue
        content = path.read_text(encoding="utf-8")
        if not content.startswith("# "):
            errors.append(f"{path.name}: missing report title")
        for target in re.findall(r"\]\(([^()\s]+)\)", content):
            url = urlsplit(target)
            if url.scheme or url.netloc or not url.path:
                continue
            destination = (path.parent / unquote(url.path)).resolve()
            if not destination.is_relative_to(root) or not destination.exists():
                errors.append(f"{path.name}: broken local link {target}")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("event", choices=["pull_request", "push"])
    parser.add_argument("base")
    parser.add_argument("head")
    args = parser.parse_args()
    full = True
    if all(
        re.fullmatch(r"[0-9a-f]{40}", sha) and sha != "0" * 40
        for sha in (args.base, args.head)
    ):
        separator = "..." if args.event == "pull_request" else ".."
        revision = args.base + separator + args.head
        diff = subprocess.run(
            ["git", "diff", "--no-renames", "--name-only", "-z", revision, "--"],
            cwd=ROOT,
            capture_output=True,
        )
        if diff.returncode == 0 and reports_only(diff.stdout.split(b"\0")[:-1]):
            subprocess.run(
                ["git", "diff", "--check", revision, "--"], cwd=ROOT, check=True
            )
            full = False
    errors = check_reports(ROOT)
    if errors:
        raise SystemExit("\n".join(errors))
    print("Full checks" if full else "Acceptance report checks passed")
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
            print(f"full={str(full).lower()}", file=stream)


if __name__ == "__main__":
    main()
