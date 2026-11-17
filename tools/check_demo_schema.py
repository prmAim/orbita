"""Fail-closed, offline consistency check for explicitly qualified demo schemas."""

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JAVA = "backend/src/test/java/ru/orbita/broker/InterfaceDemoLedger.java"


def compact(text):
    return re.sub(r"\s+", "", text)


def require_once(text, expression, component):
    if compact(text).count(compact(expression)) != 1:
        raise ValueError(f"{component}: missing, duplicate or changed schema guard")


def check(root):
    policy = json.loads(
        (root / "deploy/demo/schema-compatibility.json").read_text(encoding="utf-8")
    )
    if set(policy) != {"current", "generate", "cleanup"}:
        raise ValueError("malformed schema qualification keys")
    current, generate, cleanup = (
        policy["current"],
        policy["generate"],
        policy["cleanup"],
    )
    # These are the supported manifest formats, not an invitation to accept new ones.
    if (
        not isinstance(current, str)
        or not re.fullmatch(r"[1-9][0-9]*", current)
        or current != generate
        or not isinstance(cleanup, dict)
        or set(cleanup) != {"1", "2"}
        or cleanup["1"] != ["15"]
        or not isinstance(cleanup["2"], list)
        or not cleanup["2"]
        or any(
            not isinstance(v, str) or not re.fullmatch(r"[1-9][0-9]*", v)
            for v in cleanup["2"]
        )
        or len(set(cleanup["2"])) != len(cleanup["2"])
        or not {"16", "17", current}.issubset(cleanup["2"])
    ):
        raise ValueError("malformed or unsupported schema/manifest qualification")
    migrations = []
    for path in (root / "backend/src/main/resources/db/migration").glob("V*__*.sql"):
        match = re.fullmatch(r"V([1-9][0-9]*)__.+\.sql", path.name)
        if not match:
            raise ValueError("unsupported migration version format")
        migrations.append(int(match[1]))
    qualified = {int(current), *(int(v) for values in cleanup.values() for v in values)}
    if not migrations or len(set(migrations)) != len(migrations):
        raise ValueError("missing or duplicate migrations")
    if max(migrations) != int(current) or not qualified.issubset(migrations):
        raise ValueError("unqualified latest schema or qualified migration missing")

    # Match executable guards, never version numbers in diagnostics or comments.
    java = (root / JAVA).read_text(encoding="utf-8")
    java = re.sub(r"/\*.*?\*/|//[^\n]*", "", java, flags=re.S)
    require_once(
        java,
        f'if (!row.next() || !"{generate}".equals(row.getString(1))) throw new IllegalStateException(',
        "Java harness",
    )
    powershell = (root / "deploy/demo-data.ps1").read_text(encoding="utf-8")
    powershell = re.sub(r"<#.*?#>|#[^\n]*", "", powershell, flags=re.S)
    require_once(
        powershell,
        f"if($baseline.schema -cne '{generate}'){{throw",
        "PowerShell Generate",
    )
    sql = (root / "deploy/demo/cleanup.sql").read_text(encoding="utf-8")
    sql = re.sub(r"/\*.*?\*/|--[^\n]*", "", sql, flags=re.S)
    require_once(
        sql, "where b.singleton and b.version in (1,2) for update;", "cleanup manifests"
    )
    require_once(
        sql,
        "if manifest_version=1 and schema_version is distinct from '15' then raise exception",
        "cleanup manifest 1",
    )
    checks = " and ".join(
        f"schema_version is distinct from '{v}'" for v in cleanup["2"]
    )
    require_once(
        sql,
        f"elsif manifest_version=2 and (({checks}) or to_regclass('public.account_read_document') is null or to_regclass('public.account_read_freshness') is null or to_regclass('public.account_read_revision') is null) then raise exception",
        "cleanup manifest 2",
    )
    return f"PASS demo schema {current}; Generate {generate}; cleanup 1:15; 2:{','.join(cleanup['2'])}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="repository root (default: script location)",
    )
    args = parser.parse_args()
    try:
        print(check(args.root))
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(f"FAIL demo schema: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
