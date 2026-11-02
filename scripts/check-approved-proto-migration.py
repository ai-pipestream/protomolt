#!/usr/bin/env python3
"""Recognize only the approved pre-release UploadDocument transition (#675).

Approval: https://git.rokkon.com/ai-pipestream/protomolt/issues/675#issuecomment-17538
Implemented by f83b48fe1aad1e5aaafe4fc5007325b67c97e1d2. GitHub's main
predates that Forgejo commit. No rule is disabled: source identities AND the
complete diagnostic list must match. Remove this migration check once all
supported compatibility baselines contain that commit.
"""

import hashlib
import json
import sys
from pathlib import Path


def main():
    if len(sys.argv) != 4:
        raise SystemExit("Usage: check-approved-proto-migration.py <before> <after> <diagnostics>")
    before, after, diagnostics = map(Path, sys.argv[1:])
    expected = (
        (before, "68ea697b39c4cb2bc1773883d55c5be42273a9850739702daa9b489a99a7284d"),
        (after, "8c5a1b5894d597c9ed13ba3cc923a9c2f0cb3e88419b4e1499b194d9e76e2ef2"),
    )
    for source, digest in expected:
        if hashlib.sha256(source.read_bytes()).hexdigest() != digest:
            raise SystemExit("Refused #675 migration: schema source identity does not match approval.")

    path = "ai/protomolt/proto/repo/v2/document_service.proto"
    allowed = [
        (path, "RPC_SAME_SERVER_STREAMING",
         'RPC "UploadDocument" on service "DocumentService" changed from server unary to server streaming.'),
        (path, "FIELD_SAME_ONEOF",
         'Field "1" with name "saved" on message "UploadDocumentResponse" moved from outside to inside a oneof.'),
    ]
    # Buf emits JSON lines. Invalid/missing keys and malformed diagnostics fail
    # visibly; duplicates or additional findings cannot fit this exact list.
    actual = []
    for line in diagnostics.read_text().splitlines():
        item = json.loads(line)
        actual.append((item["path"], item["type"], item["message"]))
    if sorted(actual) != sorted(allowed):
        raise SystemExit("Refused #675 migration: diagnostics include an unapproved change.")
    print("Accepted ONLY the approved #675 UploadDocument transition; exact before/after "
          "sources and both Buf findings verified. All FILE rules remain enabled.")


if __name__ == "__main__":
    main()
