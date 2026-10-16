#!/usr/bin/env python3
"""Cross-language verification of the SaveIntent golden vectors (requirements section 7).

Independent of the Java MessageFingerprints: this script parses every .pb in
the goldens directory with a hand-rolled protobuf wire decoder, requires the
canonical emission rules (fields in field-number order, minimal varints and
length prefixes, valid UTF-8 strings, no trailing bytes), re-encodes each
message canonically from its parsed fields, and requires byte identity with
the .pb. At the SaveIntent level it additionally requires metadata keys in
strictly ascending unsigned UTF-8 byte order and opaque attestations in
strictly ascending order of their locator encoding. Each vector's .sha256
file must hold the SHA-256 of the .pb bytes.

Usage: verify_save_intent_goldens.py <goldens-directory>
"""

import hashlib
import pathlib
import sys

VARINT = "varint"
STRING = "string"

# Every field of every message the vectors can contain. A field missing from
# its message, or a message field that decodes badly, fails the vector.
SCHEMA = {
    "SaveIntent": {1: VARINT, 2: "NodeAddress", 3: "ExpectedRevision", 4: VARINT,
                   5: "IntentWrite", 6: "PartSource", 7: STRING, 8: "SaveOwner",
                   9: "IntentMetadata", 10: "OpaqueAttestation", 11: STRING, 12: "SourceRevision"},
    "NodeAddress": {1: STRING, 2: STRING, 3: STRING, 4: STRING},
    "ExpectedRevision": {1: VARINT, 2: VARINT},
    "IntentWrite": {1: "PartWrite", 2: STRING, 3: VARINT},
    "PartWrite": {1: "PartSelector", 2: VARINT, 3: STRING},
    "PartSelector": {1: VARINT, 2: STRING},
    "PartSource": {1: "PartSelector", 2: "NodeAddress", 3: VARINT},
    "SaveOwner": {1: "IntakeOrigin", 2: "WriteProvenance"},
    "IntakeOrigin": {1: STRING, 2: STRING},
    "WriteProvenance": {1: STRING, 2: STRING, 3: STRING, 4: VARINT},
    "IntentMetadata": {1: STRING, 2: STRING},
    "OpaqueAttestation": {1: "OccurrenceLocator", 2: STRING, 3: "WriteProvenance", 4: "OpaqueSource"},
    "OccurrenceLocator": {1: "PartSelector", 2: "RepositorySchemaOccurrenceStep"},
    "RepositorySchemaOccurrenceStep": {1: VARINT, 2: VARINT},
    "SourceRevision": {1: "NodeAddress", 2: VARINT},
    "OpaqueSource": {1: "SourceRevision", 2: "OccurrenceLocator"},
}


def fail(message):
    raise ValueError(message)


def read_varint(data, pos):
    result = 0
    shift = 0
    start = pos
    while True:
        if pos >= len(data):
            fail("truncated varint")
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
        if shift > 70:
            fail("varint too long")
    if pos - start > 1 and byte == 0:
        fail("non-minimal varint")
    if pos - start != len(encode_varint(result)):
        fail("non-minimal varint encoding")
    return result, pos


def encode_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def parse_message(name, data):
    """Parses one message into [(field, wire_type, value)]; canonical order is required."""
    fields = []
    pos = 0
    previous = 0
    while pos < len(data):
        tag, pos = read_varint(data, pos)
        field, wire_type = tag >> 3, tag & 7
        if field == 0:
            fail("%s: field number 0" % name)
        if field < previous:
            fail("%s: field %d follows %d; fields must be in field-number order" % (name, field, previous))
        previous = field
        if field not in SCHEMA[name]:
            fail("%s: unknown field %d" % (name, field))
        kind = SCHEMA[name][field]
        if wire_type == 0:
            if kind != VARINT:
                fail("%s: field %d is not a varint" % (name, field))
            value, pos = read_varint(data, pos)
        elif wire_type == 2:
            length, pos = read_varint(data, pos)
            if pos + length > len(data):
                fail("%s: field %d overruns the message" % (name, field))
            value = data[pos:pos + length]
            pos += length
            if kind == VARINT:
                fail("%s: field %d must be a varint" % (name, field))
            if kind == STRING:
                value.decode("utf-8")
            if kind not in (VARINT, STRING):
                parse_message(kind, value)  # recurse: nested canonical order
        else:
            fail("%s: field %d uses wire type %d, which this encoding never emits" % (name, field, wire_type))
        fields.append((field, wire_type, value))
    return fields


def encode_message(fields):
    out = bytearray()
    for field, wire_type, value in fields:
        out += encode_varint((field << 3) | wire_type)
        if wire_type == 0:
            out += encode_varint(value)
        else:
            out += encode_varint(len(value))
            out += value
    return bytes(out)


def field_values(fields, number):
    return [value for field, _, value in fields if field == number]


def verify(name, data):
    fields = parse_message(name, data)
    if encode_message(fields) != data:
        fail("%s: canonical re-encode differs from the golden bytes" % name)
    if name != "SaveIntent":
        fail("goldens hold SaveIntent messages, not %s" % name)
    metadata_keys = [parse_message("IntentMetadata", entry)[0][2]
                     for entry in field_values(fields, 9)]
    if any(metadata_keys[i] >= metadata_keys[i + 1] for i in range(len(metadata_keys) - 1)):
        fail("metadata keys are not in strictly ascending unsigned UTF-8 order: %r" % metadata_keys)
    locators = [field_values(parse_message("OpaqueAttestation", attestation), 1)[0]
                for attestation in field_values(fields, 10)]
    if any(locators[i] >= locators[i + 1] for i in range(len(locators) - 1)):
        fail("opaque attestations are not in strictly ascending locator-encoding order")
    return field_values(fields, 11)[0].decode("utf-8")


def main():
    goldens = pathlib.Path(sys.argv[1])
    vectors = sorted(goldens.glob("*.pb"))
    if not vectors:
        fail("no .pb vectors in " + str(goldens))
    kinds = set()
    for vector in vectors:
        data = vector.read_bytes()
        kind = verify("SaveIntent", data)
        kinds.add(kind)
        digest_file = vector.with_suffix(".sha256")
        expected = digest_file.read_text(encoding="ascii").strip()
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected:
            fail("%s: digest %s differs from %s" % (vector.name, actual, expected))
        print("%s ok (%d bytes, kind %s, sha256 %s)" % (vector.name, len(data), kind, actual[:16]))
    if kinds != {"save", "delete"}:
        fail("the vectors must pin both operation kinds; found %r" % kinds)
    print("%d vectors verified" % len(vectors))


if __name__ == "__main__":
    main()
