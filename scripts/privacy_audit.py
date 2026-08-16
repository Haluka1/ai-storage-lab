#!/usr/bin/env python3
"""Fail on private-workspace markers, likely credentials, or binary artifacts."""

from __future__ import annotations

import re
import sys
from pathlib import Path


EXCLUDED_DIRS = {".git", ".venv", "build", "__pycache__", ".pytest_cache"}
EXCLUDED_FILES = {Path("scripts/privacy_audit.py")}
FORBIDDEN_EXTENSIONS = {".pdf", ".doc", ".docx", ".zip", ".7z", ".rar"}

TEXT_RULES: list[tuple[str, re.Pattern[str]]] = [
    (
        "private-material keyword",
        re.compile(
            r"(?i:\binterview\b|DeepSeek|Kimi|MiniMax)|\bSeed\b|待填写|招聘|简历|面试|\bOwnership\b"
        ),
    ),
    ("private AI execution record", re.compile(r"(?i)Codex\s+prompt|raw\s+prompt|prompt\s+pack|AI\s+execution\s+(?:log|transcript)|AI\s*执行过程")),
    ("private workstation path", re.compile(r"/(?:root|home)/|[A-Za-z]:[\\/]Users[\\/]|/mnt/[a-z]/Users/")),
    ("kubeconfig reference", re.compile(r"(?i)kubeconfig")),
    (
        "private identity assignment",
        re.compile(
            r"(?i)\b(?:author|maintainer|user(?:name)?)\s*[:=]\s*[\"']?"
            r"(?!example\b|placeholder\b|redacted\b)[A-Za-z0-9][A-Za-z0-9_.-]{2,}"
        ),
    ),
    ("private IPv4 address", re.compile(r"\b(?:10(?:\.\d{1,3}){3}|192\.168(?:\.\d{1,3}){2}|172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2})\b")),
    ("cloud resource identifier", re.compile(r"\b(?:i|vol|d|lb)-[0-9a-f]{8,}\b|[a-z0-9.-]+\.(?:aliyuncs|amazonaws)\.com")),
    ("private key material", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("signed URL", re.compile(r"(?i)https?://\S+[?&](?:X-Amz-Signature|Signature|sig|token)=")),
    (
        "inline credential value",
        re.compile(
            r"(?i)(?:access[_ -]?key|secret[_ -]?key|api[_ -]?token|password)\s*[:=]\s*[\"']"
            r"(?!example|placeholder|redacted|changeme|S3_ACCESS_KEY|S3_SECRET_KEY)[^\"']{8,}[\"']"
        ),
    ),
]
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)


def candidates(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and not any(part in EXCLUDED_DIRS for part in path.relative_to(root).parts)
        and path.relative_to(root) not in EXCLUDED_FILES
    )


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    findings: list[str] = []
    scanned = 0
    for path in candidates(root):
        relative = path.relative_to(root)
        if path.suffix.lower() in FORBIDDEN_EXTENSIONS:
            findings.append(f"{relative}: forbidden private/binary artifact extension")
            continue
        raw = path.read_bytes()
        if b"\x00" in raw:
            findings.append(f"{relative}: unknown binary content requires manual review")
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            findings.append(f"{relative}: non-UTF-8 content requires manual review")
            continue
        scanned += 1
        for line_number, line in enumerate(text.splitlines(), 1):
            for label, rule in TEXT_RULES:
                if rule.search(line):
                    findings.append(f"{relative}:{line_number}: {label}")
            for email in EMAIL_RE.findall(line):
                domain = email.rsplit("@", 1)[1].lower()
                if domain not in {"example.com", "example.org", "example.invalid"}:
                    findings.append(f"{relative}:{line_number}: non-example email address")
    if findings:
        print("Privacy/secret audit: FAIL", file=sys.stderr)
        for finding in findings:
            print(f"  {finding}", file=sys.stderr)
        return 1
    print(f"Privacy/secret audit: PASS ({scanned} UTF-8 files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
