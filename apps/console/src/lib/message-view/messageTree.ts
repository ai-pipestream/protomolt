/**
 * The display contract for protobuf values: one decision per proto type, made once,
 * inherited by every message view in the console. Input is a message's canonical proto3
 * JSON plus its @bufbuild/protobuf descriptor; output is a render-ready tree.
 *
 * Per-type treatment:
 * - string        → plain text
 * - bool          → true/false badge
 * - int32/float/… → mono number (floats keep full precision in `detail`)
 * - int64 family  → mono number with thousands grouping in `detail` (JSON carries a string)
 * - enum          → the enum VALUE NAME as a pill, `= n` in `detail`
 * - bytes         → size badge + hex preview of the first 32 bytes; full base64 stays in `detail`
 * - Timestamp     → the ISO instant, tagged as Timestamp
 * - Duration      → as canonical text (`3.5s`), tagged as Duration
 * - wrappers      → unwrapped inner value, tagged with the wrapper type
 * - FieldMask     → the paths as a comma list
 * - Struct/Value/ListValue/Any → free-form JSON subtree (Any leads with its @type)
 * - message       → collapsible node of child fields; `(empty)` when no fields are set
 * - repeated      → group labeled `repeated T (n)`, children indexed `[i]`
 * - map           → group labeled `map<K, V> (n)`, children keyed by map key
 * - oneof member  → carries its oneof group name
 * - unset fields  → omitted (proto3 absence is meaning, not noise)
 */
import { ScalarType } from '@bufbuild/protobuf'
import type { DescField, DescMessage } from '@bufbuild/protobuf'
import type { JsonValue } from '@bufbuild/protobuf'

export type DisplayKind =
  | 'text'
  | 'number'
  | 'int64'
  | 'bool'
  | 'enum'
  | 'bytes'
  | 'wkt'
  | 'message'
  | 'group'
  | 'json'

export interface ViewNode {
  /** Field name, list index, or map key. */
  label: string
  /** Muted type annotation, e.g. `string`, `demo.shop.v1.Order.Status`, `repeated LineItem (3)`. */
  typeLabel: string
  kind: DisplayKind
  /** Primary rendered text for leaves. */
  display?: string
  /** Secondary text: enum number, full precision, byte count, full base64. */
  detail?: string
  /** Oneof group name when the field is a oneof member. */
  oneof?: string
  children?: ViewNode[]
}

const LONG_SCALARS = new Set<ScalarType>([
  ScalarType.INT64,
  ScalarType.UINT64,
  ScalarType.SINT64,
  ScalarType.FIXED64,
  ScalarType.SFIXED64,
])

const FLOAT_SCALARS = new Set<ScalarType>([ScalarType.FLOAT, ScalarType.DOUBLE])

const WRAPPERS: Record<string, string> = {
  'google.protobuf.StringValue': 'string',
  'google.protobuf.BytesValue': 'bytes',
  'google.protobuf.BoolValue': 'bool',
  'google.protobuf.Int32Value': 'int32',
  'google.protobuf.UInt32Value': 'uint32',
  'google.protobuf.Int64Value': 'int64',
  'google.protobuf.UInt64Value': 'uint64',
  'google.protobuf.FloatValue': 'float',
  'google.protobuf.DoubleValue': 'double',
}

const JSON_SHAPED = new Set([
  'google.protobuf.Struct',
  'google.protobuf.Value',
  'google.protobuf.ListValue',
  'google.protobuf.Any',
])

/** The render tree for one message instance (canonical proto3 JSON + its descriptor). */
export function buildMessageTree(desc: DescMessage, json: JsonValue): ViewNode[] {
  if (json === null || typeof json !== 'object' || Array.isArray(json)) {
    return [jsonNode('$', json)]
  }
  const nodes: ViewNode[] = []
  const record = json as Record<string, JsonValue>
  const claimed = new Set<string>()
  for (const field of desc.fields) {
    const key = field.jsonName in record ? field.jsonName : field.name
    if (!(key in record)) continue
    claimed.add(key)
    nodes.push(fieldNode(field, record[key]))
  }
  for (const [key, value] of Object.entries(record)) {
    if (!claimed.has(key)) nodes.push(jsonNode(key, value))
  }
  return nodes
}

function fieldNode(field: DescField, value: JsonValue): ViewNode {
  const oneof = field.oneof?.name
  switch (field.fieldKind) {
    case 'scalar':
      return { ...scalarNode(field.scalar, value), label: field.name, oneof }
    case 'enum': {
      const name = String(value)
      const known = field.enum.values.find(
        (v) => v.name === name || v.number === Number(value),
      )
      return {
        label: field.name,
        typeLabel: field.enum.typeName,
        kind: 'enum',
        display: known?.name ?? name,
        detail: known ? `= ${known.number}` : undefined,
        oneof,
      }
    }
    case 'message':
      return { ...messageNode(field.message, value), label: field.name, oneof }
    case 'list': {
      const items = Array.isArray(value) ? value : []
      const elementType =
        field.listKind === 'message'
          ? shortName(field.message.typeName)
          : field.listKind === 'enum'
            ? shortName(field.enum.typeName)
            : ScalarType[field.scalar]?.toLowerCase() ?? 'value'
      return {
        label: field.name,
        typeLabel: `repeated ${elementType} (${items.length})`,
        kind: 'group',
        oneof,
        children: items.map((item, i) => {
          if (field.listKind === 'message') {
            return { ...messageNode(field.message, item), label: `[${i}]` }
          }
          if (field.listKind === 'enum') {
            const known = field.enum.values.find(
              (v) => v.name === String(item) || v.number === Number(item),
            )
            return {
              label: `[${i}]`,
              typeLabel: '',
              kind: 'enum' as const,
              display: known?.name ?? String(item),
              detail: known ? `= ${known.number}` : undefined,
            }
          }
          return { ...scalarNode(field.scalar, item), label: `[${i}]`, typeLabel: '' }
        }),
      }
    }
    case 'map': {
      const entries =
        value !== null && typeof value === 'object' && !Array.isArray(value)
          ? Object.entries(value as Record<string, JsonValue>)
          : []
      const valueType =
        field.mapKind === 'message'
          ? shortName(field.message.typeName)
          : field.mapKind === 'enum'
            ? shortName(field.enum.typeName)
            : ScalarType[field.scalar]?.toLowerCase() ?? 'value'
      const keyType = ScalarType[field.mapKey]?.toLowerCase() ?? 'string'
      return {
        label: field.name,
        typeLabel: `map<${keyType}, ${valueType}> (${entries.length})`,
        kind: 'group',
        oneof,
        children: entries.map(([key, entry]) => {
          if (field.mapKind === 'message') {
            return { ...messageNode(field.message, entry), label: key }
          }
          if (field.mapKind === 'enum') {
            const known = field.enum.values.find(
              (v) => v.name === String(entry) || v.number === Number(entry),
            )
            return {
              label: key,
              typeLabel: '',
              kind: 'enum' as const,
              display: known?.name ?? String(entry),
              detail: known ? `= ${known.number}` : undefined,
            }
          }
          return { ...scalarNode(field.scalar, entry), label: key, typeLabel: '' }
        }),
      }
    }
  }
}

function scalarNode(scalar: ScalarType, value: JsonValue): ViewNode {
  const typeLabel = ScalarType[scalar]?.toLowerCase() ?? 'scalar'
  if (scalar === ScalarType.BOOL) {
    return { label: '', typeLabel, kind: 'bool', display: String(value) }
  }
  if (scalar === ScalarType.BYTES) {
    return { label: '', ...bytesView(String(value ?? '')), typeLabel }
  }
  if (LONG_SCALARS.has(scalar)) {
    const raw = String(value)
    return { label: '', typeLabel, kind: 'int64', display: raw, detail: grouped(raw) }
  }
  if (FLOAT_SCALARS.has(scalar)) {
    const num = typeof value === 'number' ? value : Number(value)
    const compact = Number.isFinite(num) ? compactFloat(num) : String(value)
    return {
      label: '',
      typeLabel,
      kind: 'number',
      display: compact,
      detail: compact === String(num) ? undefined : String(num),
    }
  }
  if (scalar === ScalarType.STRING) {
    return { label: '', typeLabel, kind: 'text', display: String(value ?? '') }
  }
  return { label: '', typeLabel, kind: 'number', display: String(value) }
}

function messageNode(desc: DescMessage, value: JsonValue): ViewNode {
  const typeName = desc.typeName
  if (typeName === 'google.protobuf.Timestamp' || typeName === 'google.protobuf.Duration') {
    return {
      label: '',
      typeLabel: shortName(typeName),
      kind: 'wkt',
      display: String(value),
    }
  }
  if (typeName === 'google.protobuf.FieldMask') {
    return { label: '', typeLabel: 'FieldMask', kind: 'wkt', display: String(value) }
  }
  if (typeName in WRAPPERS) {
    const inner = scalarNode(scalarTypeOf(WRAPPERS[typeName]), value)
    return { ...inner, label: '', typeLabel: shortName(typeName) }
  }
  if (JSON_SHAPED.has(typeName)) {
    const node = jsonNode('', value)
    return { ...node, typeLabel: shortName(typeName) }
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return { label: '', typeLabel: desc.typeName, kind: 'message', display: String(value) }
  }
  const children = buildMessageTree(desc, value)
  return {
    label: '',
    typeLabel: desc.typeName,
    kind: 'message',
    display: children.length === 0 ? '(empty)' : undefined,
    children,
  }
}

/**
 * Values the descriptor does not type (Struct, Value, unknown keys) still get the full
 * treatment: leaf kinds are inferred from the JSON value, so a Struct renders like any
 * other message rather than as raw JSON. Only containers carry a muted count tag.
 */
function jsonNode(label: string, value: JsonValue): ViewNode {
  if (Array.isArray(value)) {
    return {
      label,
      typeLabel: `list (${value.length})`,
      kind: 'json',
      children: value.map((item, i) => jsonNode(`[${i}]`, item)),
    }
  }
  if (value !== null && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, JsonValue>)
    return {
      label,
      typeLabel: `(${entries.length})`,
      kind: 'json',
      children: entries.map(([key, entry]) => jsonNode(key, entry)),
    }
  }
  if (typeof value === 'string') {
    return { label, typeLabel: '', kind: 'text', display: value }
  }
  if (typeof value === 'number') {
    return { label, typeLabel: '', kind: 'number', display: String(value) }
  }
  if (typeof value === 'boolean') {
    return { label, typeLabel: '', kind: 'bool', display: String(value) }
  }
  return { label, typeLabel: '', kind: 'json', display: 'null' }
}

function bytesView(base64: string): Omit<ViewNode, 'label'> {
  let bytes: Uint8Array
  try {
    bytes = Uint8Array.from(atob(base64), (c) => c.charCodeAt(0))
  } catch {
    return { typeLabel: 'bytes', kind: 'bytes', display: base64 }
  }
  const preview = Array.from(bytes.slice(0, 32))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join(' ')
  return {
    typeLabel: 'bytes',
    kind: 'bytes',
    display: `${bytes.length} B  ${preview}${bytes.length > 32 ? ' …' : ''}`,
    detail: base64,
  }
}

function scalarTypeOf(name: string): ScalarType {
  const upper = name.toUpperCase() as keyof typeof ScalarType
  return (ScalarType[upper] as ScalarType | undefined) ?? ScalarType.STRING
}

function shortName(typeName: string): string {
  return typeName.slice(typeName.lastIndexOf('.') + 1)
}

function grouped(raw: string): string | undefined {
  const negative = raw.startsWith('-')
  const digits = negative ? raw.slice(1) : raw
  if (!/^\d{5,}$/.test(digits)) return undefined
  const out = digits.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
  return (negative ? '-' : '') + out
}

function compactFloat(num: number): string {
  const text = String(num)
  return text.length > 10 ? num.toPrecision(6).replace(/\.?0+$/, '') : text
}
