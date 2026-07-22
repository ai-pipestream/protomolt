/**
 * Reflection-based protobuf → JSON Schema conversion: walk a DescMessage
 * (from generated code or a runtime FileRegistry built out of a
 * FileDescriptorSet) and emit the JSON-Schema subset the platform's
 * SchemaForm renderer understands. This replaces per-type hardcoding — any
 * message type a descriptor set describes gets a live form.
 *
 * Mapping rules (proto3-JSON-leaning, tuned for form UX):
 *  - scalars → string / integer / number / boolean; bytes → base64 string
 *  - enums → string enum of value names
 *  - singular messages → nested object schemas (cycle-guarded)
 *  - repeated → array of the element schema
 *  - map<string-ish, scalar> → schema-less object (key/value editor)
 *  - oneof members are annotated in their description
 *  - well-known types get form-friendly shapes (Timestamp/Duration as
 *    strings, Struct/Value/Any as raw JSON)
 *  - anything unexpressible drops `type` entirely → the renderer's honest
 *    JSON fallback
 */
import { ScalarType, type DescEnum, type DescField, type DescMessage } from '@bufbuild/protobuf'
import type { JsonSchema } from './converter'

export interface DescriptorSchemaOptions {
  /** Recursion depth for nested messages before falling back to JSON. */
  maxDepth?: number
}

/** JSON Schema for a message descriptor — feed it straight to SchemaForm. */
export function descriptorToJsonSchema(
  message: DescMessage,
  options: DescriptorSchemaOptions = {},
): JsonSchema {
  const maxDepth = options.maxDepth ?? 6
  return messageSchema(message, 0, maxDepth, new Set([message.typeName]))
}

// ---------------------------------------------------------------- messages

function messageSchema(
  message: DescMessage,
  depth: number,
  maxDepth: number,
  seen: ReadonlySet<string>,
): JsonSchema {
  const wkt = wellKnownSchema(message)
  if (wkt) return { ...wkt }

  const properties: Record<string, JsonSchema> = {}
  for (const field of message.fields) {
    properties[field.localName] = fieldSchema(field, depth, maxDepth, seen)
  }
  return {
    type: 'object',
    title: message.name,
    'x-proto-type': message.typeName,
    properties,
  }
}

function nestedMessageSchema(
  message: DescMessage,
  depth: number,
  maxDepth: number,
  seen: ReadonlySet<string>,
): JsonSchema {
  const wkt = wellKnownSchema(message)
  if (wkt) return { ...wkt }
  if (seen.has(message.typeName) || depth >= maxDepth) {
    // No `type` → the renderer's JSON fallback (honest about recursion).
    return {
      title: message.name,
      description: `${message.typeName} (recursive — edit as JSON)`,
      'x-proto-type': message.typeName,
    }
  }
  return messageSchema(message, depth, maxDepth, new Set(seen).add(message.typeName))
}

// ---------------------------------------------------------------- fields

function fieldSchema(
  field: DescField,
  depth: number,
  maxDepth: number,
  seen: ReadonlySet<string>,
): JsonSchema {
  const schema = fieldSchemaByKind(field, depth, maxDepth, seen)
  const notes: string[] = []
  if (schema.description) notes.push(String(schema.description))
  notes.push(`field ${field.number}`)
  if (field.oneof) notes.push(`oneof ${field.oneof.name}`)
  schema.description = notes.join(' · ')
  return schema
}

function fieldSchemaByKind(
  field: DescField,
  depth: number,
  maxDepth: number,
  seen: ReadonlySet<string>,
): JsonSchema {
  switch (field.fieldKind) {
    case 'scalar':
      return scalarSchema(field.scalar)
    case 'enum':
      return enumSchema(field.enum)
    case 'message':
      return nestedMessageSchema(field.message, depth + 1, maxDepth, seen)
    case 'list': {
      const items: JsonSchema =
        field.listKind === 'scalar'
          ? scalarSchema(field.scalar)
          : field.listKind === 'enum'
            ? enumSchema(field.enum)
            : nestedMessageSchema(field.message, depth + 1, maxDepth, seen)
      return { type: 'array', items }
    }
    case 'map': {
      if (field.mapKind === 'scalar' || field.mapKind === 'enum') {
        // Schema-less object → SchemaForm's key/value map editor.
        return { type: 'object', description: mapDescription(field) }
      }
      return { description: `${mapDescription(field)} (edit as JSON)` }
    }
  }
}

function mapDescription(field: DescField & { fieldKind: 'map' }): string {
  const value =
    field.mapKind === 'scalar'
      ? scalarTypeName(field.scalar)
      : field.mapKind === 'enum'
        ? shortName(field.enum.typeName)
        : shortName(field.message.typeName)
  return `map<${scalarTypeName(field.mapKey)}, ${value}>`
}

// ---------------------------------------------------------------- scalars / enums

const INT64_TYPES = new Set<ScalarType>([
  ScalarType.INT64,
  ScalarType.UINT64,
  ScalarType.FIXED64,
  ScalarType.SFIXED64,
  ScalarType.SINT64,
])

const FLOAT_TYPES = new Set<ScalarType>([ScalarType.DOUBLE, ScalarType.FLOAT])

function scalarSchema(scalar: ScalarType): JsonSchema {
  if (scalar === ScalarType.BOOL) return { type: 'boolean' }
  if (scalar === ScalarType.STRING) return { type: 'string' }
  if (scalar === ScalarType.BYTES) return { type: 'string', description: 'bytes (base64)' }
  if (FLOAT_TYPES.has(scalar)) return { type: 'number', description: scalarTypeName(scalar) }
  if (INT64_TYPES.has(scalar)) return { type: 'integer', description: scalarTypeName(scalar) }
  return { type: 'integer', description: scalarTypeName(scalar) }
}

function enumSchema(desc: DescEnum): JsonSchema {
  return {
    type: 'string',
    enum: desc.values.map((v) => v.name),
    description: shortName(desc.typeName),
  }
}

export function scalarTypeName(scalar: ScalarType): string {
  return ScalarType[scalar]?.toLowerCase() ?? 'scalar'
}

function shortName(typeName: string): string {
  return typeName.slice(typeName.lastIndexOf('.') + 1)
}

// ---------------------------------------------------------------- well-known types

function wellKnownSchema(message: DescMessage): JsonSchema | undefined {
  switch (message.typeName) {
    case 'google.protobuf.Timestamp':
      return { type: 'string', description: 'RFC 3339 timestamp, e.g. 2026-01-01T12:00:00Z' }
    case 'google.protobuf.Duration':
      return { type: 'string', description: "duration, e.g. '3.5s'" }
    case 'google.protobuf.FieldMask':
      return { type: 'string', description: 'field mask (comma-separated paths)' }
    case 'google.protobuf.Struct':
      return { description: 'arbitrary JSON object' }
    case 'google.protobuf.Value':
      return { description: 'arbitrary JSON value' }
    case 'google.protobuf.ListValue':
      return { type: 'array', items: { description: 'arbitrary JSON value' } }
    case 'google.protobuf.Any':
      return { description: 'google.protobuf.Any — {"@type": "...", ...} (edit as JSON)' }
    case 'google.protobuf.Empty':
      return { description: 'google.protobuf.Empty (no fields)' }
    case 'google.protobuf.DoubleValue':
    case 'google.protobuf.FloatValue':
      return { type: 'number', description: message.name }
    case 'google.protobuf.Int64Value':
    case 'google.protobuf.UInt64Value':
    case 'google.protobuf.Int32Value':
    case 'google.protobuf.UInt32Value':
      return { type: 'integer', description: message.name }
    case 'google.protobuf.BoolValue':
      return { type: 'boolean', description: message.name }
    case 'google.protobuf.StringValue':
      return { type: 'string', description: message.name }
    case 'google.protobuf.BytesValue':
      return { type: 'string', description: 'bytes (base64)' }
    default:
      return undefined
  }
}
