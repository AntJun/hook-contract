"""Check public artifacts and all reachable git blobs for private contact data.

Gitleaks supplies the separate credential scan. This complementary audit rejects
contact email addresses and requires no-reply identities in commit metadata.
"""

import re
import subprocess
import sys
from pathlib import Path

EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
ALLOWED_SUFFIX = "@users.noreply.github.com"
ALLOWED = {"noreply@github.com"}


def safe_email(value):
    return value.endswith(ALLOWED_SUFFIX) or value in ALLOWED


def check_text(text):
    return all(safe_email(match.group()) for match in EMAIL.finditer(text))


def git(*args):
    return subprocess.check_output(["git", *args])


failures = 0
if "--history" in sys.argv:
    objects = git("rev-list", "--objects", "--all").decode().splitlines()
    checked = 0
    for line in objects:
        sha = line.split(" ", 1)[0]
        if git("cat-file", "-t", sha).strip() != b"blob":
            continue
        data = git("cat-file", "blob", sha)
        if not check_text(data.decode("utf-8", errors="replace")):
            print("Contact-data audit failed for a historical blob:", sha)
            failures += 1
        checked += 1
    identities = git("log", "--all", "--format=%ae%n%ce").decode().splitlines()
    if any(not safe_email(value) for value in identities):
        print("Commit email audit failed; only GitHub no-reply identities are permitted.")
        failures += 1
    print(f"Audited {checked} reachable git blobs and {len(identities)} commit email fields.")
for argument in sys.argv[1:]:
    if argument.startswith("--"):
        continue
    root = Path(argument)
    if not root.is_dir():
        raise SystemExit("Public artifact audit requires an existing directory.")
    count = 0
    for path in root.rglob("*"):
        if path.is_file():
            count += 1
            if not check_text(path.read_text(encoding="utf-8", errors="replace")):
                print("Contact-data audit failed for a generated public artifact.")
                failures += 1
    print(f"Audited {count} generated public files.")
if failures:
    raise SystemExit(1)
print("Public contact-data audit passed.")
