"""Golden vectors for the archive's derived operation IDs.

Encodes ai.protomolt.proto.repo.archive.v1.ArchiveOperationKey by hand, with
no protobuf library, and derives each UUIDv5 per RFC 9562: SHA-1 over the
namespace's 16 bytes followed by the key's binary encoding, then the version
and variant bits. ArchiveOperationKeysTest pins the same three vectors, so the
Java derivation and this independent encoder must agree byte for byte.

Run: python3 -I archive_operation_key_vectors.py. The last line is the PutEntry content
digest ArchiveOperationKeysTest pins for its sample request.
"""
import hashlib
import uuid

NAMESPACE = uuid.UUID("6e947da6-c6b8-49a7-9c65-fdf26dbd03e2")


def varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def string_field(number, text):
    data = text.encode("utf-8")
    if not data:
        return b""
    return varint(number << 3 | 2) + varint(len(data)) + data


def message_field(number, data):
    return varint(number << 3 | 2) + varint(len(data)) + data


def int64_field(number, value):
    if value == 0:
        return b""
    return varint(number << 3) + varint(value & 0xFFFFFFFFFFFFFFFF)


def key(kind, account, archive, entry_id, expected_version, content_sha256):
    address = string_field(1, account) + string_field(2, archive) + string_field(3, entry_id)
    return (string_field(1, kind) + message_field(2, address)
            + int64_field(3, expected_version) + string_field(4, content_sha256))


def uuid5(encoded):
    digest = bytearray(hashlib.sha1(NAMESPACE.bytes + encoded).digest()[:16])
    digest[6] = (digest[6] & 0x0F) | 0x50
    digest[8] = (digest[8] & 0x3F) | 0x80
    return uuid.UUID(bytes=bytes(digest))


VECTORS = [
    ("archive.put-entry", "acct-1", "reports", "report-2026.pdf", 0,
     hashlib.sha256(b"vector one").hexdigest()),
    ("archive.upload-rendition", "acct-1", "photos", "café/ü 東京", 0,
     hashlib.sha256(b"vector two").hexdigest()),
    ("archive.put-entry", "acct-2", "contracts", "s3://bucket/key:with/slashes?and=query", 7,
     hashlib.sha256(b"vector three").hexdigest()),
]

for vector in VECTORS:
    encoded = key(*vector)
    print(vector[5], encoded.hex(), uuid5(encoded))


def rendition(name, sub_key, data):
    descriptor = string_field(1, name) + string_field(2, sub_key)
    return message_field(1, descriptor) + message_field(2, data) if data else message_field(1, descriptor)


def put_entry_content_sha256():
    """ArchiveOperationKeysTest.put(): renditions by (name, sub_key), metadata in code point order."""
    address = string_field(1, "acct") + string_field(2, "reports") + string_field(3, "e1")
    encoded = message_field(1, address) + string_field(2, "Title")
    for name, sub_key, data in sorted([("original", "", b"pdf bytes"), ("page", "0002", b"page two"),
                                       ("page", "0001", b"page one")]):
        encoded += message_field(6, rendition(name, sub_key, data))
    metadata = {"z": "last", "￿": "bmp high", "\U0001F600": "supplementary", "a": "first"}
    for key in sorted(metadata, key=lambda k: k.encode("utf-8")):
        entry = varint(1 << 3 | 2) + varint(len(key.encode("utf-8"))) + key.encode("utf-8")
        value = metadata[key].encode("utf-8")
        entry += varint(2 << 3 | 2) + varint(len(value)) + value
        encoded += message_field(99, entry)
    return hashlib.sha256(encoded).hexdigest()


print("put-entry content", put_entry_content_sha256())
