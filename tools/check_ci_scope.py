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


def report_link_targets(content):
    """Read local destinations from the Markdown forms used by acceptance reports."""
    visible = []
    fence_character = None
    fence_size = 0
    for line in content.splitlines():
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
        if fence_character:
            if (
                marker
                and marker[1][0] == fence_character
                and len(marker[1]) >= fence_size
                and not line[marker.end() :].strip()
            ):
                fence_character = None
            continue
        if marker:
            fence_character = marker[1][0]
            fence_size = len(marker[1])
            continue
        line = re.sub(r"(?<!`)(`+)(?!`).*?(?<!`)\1(?!`)", "", line)
        visible.append(line)
    content = "\n".join(visible)

    inline = re.compile(
        r"""\]\(\s*(?:<(?P<angle>[^>\r\n]*)>|(?P<plain>[^\s()]+))
            (?:[ \t]+(?:"[^"\r\n]*"|'[^'\r\n]*'|\([^()\r\n]*\)))?\s*\)""",
        re.VERBOSE,
    )
    reference = re.compile(
        r"""^[ \t]{0,3}\[[^\]\r\n]+\]:[ \t]*
            (?:<(?P<angle>[^>\r\n]*)>|(?P<plain>\S+))
            (?:[ \t]+(?:"[^"\r\n]*"|'[^'\r\n]*'|\([^()\r\n]*\)))?[ \t]*$""",
        re.VERBOSE | re.MULTILINE,
    )
    for pattern in (inline, reference):
        for match in pattern.finditer(content):
            yield match.group("angle") or match.group("plain")


def check_reports(root):
    root = root.resolve()
    errors = []
    for path in sorted((root / "docs/acceptance").glob("*.md")):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            errors.append(f"{path.name}: expected repository file")
            continue
        content = path.read_text(encoding="utf-8")
        if not content.startswith("# "):
            errors.append(f"{path.name}: missing report title")
        for target in report_link_targets(content):
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
