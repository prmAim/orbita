"""Read-only development preflight; never installs or starts build/application services."""

import argparse
import hashlib
import json
import math
import os
import re
import shlex
import shutil
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlsplit

from dev_probe_process import ProbeFailure, capture

ROOT = Path(__file__).resolve().parents[1]
DENIED = re.compile(
    r"permission denied|access.*denied|unauthorized|forbidden|доступ", re.I
)


class CheckFailure(Exception):
    pass


def executable(directory, name):
    names = [name + ".exe", name + ".cmd"] if os.name == "nt" else [name]
    for filename in names:
        path = directory / filename
        if path.is_file():
            return path
    raise CheckFailure("MISSING executable")


def probe(command, timeout, *, docker=False):
    try:
        code, stdout, stderr = capture(command, timeout)
    except FileNotFoundError as error:
        raise CheckFailure("MISSING executable") from error
    except PermissionError as error:
        raise CheckFailure("ACCESS_DENIED launching executable") from error
    except ProbeFailure as error:
        raise CheckFailure(str(error)) from error
    except OSError as error:
        raise CheckFailure("UNAVAILABLE executable") from error
    output = stdout + stderr
    if code:
        status = (
            "ACCESS_DENIED"
            if DENIED.search(output)
            else "ENGINE_UNAVAILABLE"
            if docker
            else "FAILED"
        )
        raise CheckFailure(
            status + "; check local permissions/service (raw output withheld)"
        )
    return stdout if docker else output


def wrapper_distribution(backend, user_home):
    properties = dict(
        line.split("=", 1)
        for line in (backend / ".mvn/wrapper/maven-wrapper.properties")
        .read_text(encoding="utf-8")
        .splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    )
    url = properties["distributionUrl"]
    if os.environ.get("MVNW_REPOURL"):
        url = (
            os.environ["MVNW_REPOURL"]
            + "/org/apache/maven/"
            + url.split("/org/apache/maven/", 1)[1]
        )
    filename = Path(urlsplit(url).path).name
    match = re.fullmatch(r"apache-maven-([0-9.]+)-bin.zip", filename)
    if not match or properties.get("distributionType") != "only-script":
        raise CheckFailure(
            "UNSUPPORTED wrapper distribution; update preflight with the wrapper"
        )
    if os.name == "nt":
        digest = hashlib.sha256(url.encode("ascii")).hexdigest()
    else:
        digest = 0
        for char in url:
            digest = (digest * 31 + ord(char)) & 0xFFFFFFFF
        digest = format(digest, "x")
    home = user_home / "wrapper/dists" / f"apache-maven-{match[1]}" / digest
    return properties["wrapperVersion"], match[1], home


def quote(value):
    value = str(value)
    return (
        "'" + value.replace("'", "''") + "'" if os.name == "nt" else shlex.quote(value)
    )


def check(args):
    failures = []

    def report(label, action):
        try:
            return action()
        except PermissionError:
            message = "ACCESS_DENIED reading selected path"
        except FileNotFoundError:
            message = "MISSING selected path"
        except CheckFailure as error:
            message = str(error)
        except (OSError, ValueError, KeyError, IndexError, TypeError, ET.ParseError):
            message = "MALFORMED or unreadable configuration"
        print(f"FAIL {label}: {message}")
        failures.append(label)
        return None

    backend = ROOT / "backend"
    repository = args.maven_repository.expanduser().resolve()
    user_home = args.maven_user_home.expanduser().resolve()
    build = args.build_directory.expanduser().resolve()
    print(
        f"Maven user home: {user_home}\nMaven repository: {repository}\nBuild directory: {build}"
    )

    def jdk():
        required = ET.parse(backend / "pom.xml").findtext(
            "{*}properties/{*}java.version"
        )
        if args.java_home:
            home = args.java_home.expanduser().resolve()
            java = executable(home / "bin", "java")
        else:
            selected = shutil.which("java")
            if not selected:
                raise CheckFailure(
                    "MISSING java; pass --java-home for an installed JDK"
                )
            java = Path(selected).resolve()
            home = java.parent.parent
        print(f"Selected Java: {java}")
        output = probe([str(java), "-version"], args.timeout)
        match = re.search(r'(?:openjdk|java) version "([0-9][0-9A-Za-z.+_-]*)"', output)
        if not match or match[1].split(".")[0] != required:
            raise CheckFailure(f"INCOMPATIBLE version; required {required}")
        executable(home / "bin", "javac")
        print(f"PASS JDK {match[1]} (required {required})")
        return home

    java_home = report("JDK", jdk)

    def maven():
        version, distribution_version, home = wrapper_distribution(backend, user_home)
        for name in ("mvnw", "mvnw.cmd"):
            if not (backend / name).is_file():
                raise CheckFailure("MISSING Maven Wrapper script")
        print(
            f"Maven Wrapper {version}; distribution {distribution_version}; cache: {home}"
        )
        binary = executable(home / "bin", "mvn")
        if not (home / "lib" / f"maven-core-{distribution_version}.jar").is_file():
            raise CheckFailure("MISSING cached Maven core; no download attempted")
        print("PASS Maven distribution cached (not executed)")
        return binary

    maven_binary = report("Maven distribution", maven)

    def docker():
        binary = shutil.which(args.docker or "docker")
        if not binary:
            raise CheckFailure("MISSING Docker CLI")
        output = probe(
            [str(binary), "version", "--format", "{{json .Server.Version}}"],
            args.timeout,
            docker=True,
        )
        try:
            version = json.loads(output.strip())
        except ValueError as error:
            raise CheckFailure(
                "ENGINE_UNAVAILABLE; invalid Docker server response"
            ) from error
        if not isinstance(version, str) or not re.fullmatch(
            r"[0-9][0-9A-Za-z.+_-]*", version
        ):
            raise CheckFailure("ENGINE_UNAVAILABLE; missing Docker server version")
        print(f"PASS Docker Engine {version}; CLI: {binary}")

    report("Docker", docker)
    if failures:
        print("FAIL preflight; no build command issued")
        return 1
    arguments = [
        str(maven_binary),
        "--offline",
        "--batch-mode",
        "--no-transfer-progress",
        "-f",
        str(backend / "pom.xml"),
        f"-Dmaven.repo.local={repository}",
        f"-Dorbita.build.directory={build}",
        "spotless:check",
        "verify",
    ]
    command = " ".join(quote(value) for value in arguments)
    prefix = (
        f"$env:JAVA_HOME={quote(java_home)}; & "
        if os.name == "nt"
        else f"JAVA_HOME={quote(java_home)} "
    )
    print(f"VERIFY (run separately; frontend must be built first):\n{prefix}{command}")
    print(
        "PASS preflight; dependency cache completeness and integration acceptance NOT_RUN"
    )
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java-home", type=Path, default=os.environ.get("JAVA_HOME"))
    parser.add_argument(
        "--maven-user-home",
        type=Path,
        default=Path(os.environ.get("MAVEN_USER_HOME", Path.home() / ".m2")),
    )
    parser.add_argument(
        "--maven-repository",
        type=Path,
        help="explicit local repository; default: Maven user home/repository",
    )
    parser.add_argument("--build-directory", type=Path, default=ROOT / "backend/target")
    parser.add_argument("--docker", help="Docker CLI executable (default: PATH)")
    parser.add_argument(
        "--timeout",
        type=float,
        default=5,
        help="per-tool timeout in seconds (default: 5)",
    )
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0 or args.timeout > 60:
        parser.error("--timeout must be greater than 0 and at most 60")
    if args.maven_repository is None:
        args.maven_repository = args.maven_user_home / "repository"
    return check(args)


if __name__ == "__main__":
    sys.exit(main())
