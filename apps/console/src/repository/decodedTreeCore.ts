/**
 * The render tree for a decoded message, built by protobuf reflection over the
 * message itself. It produces the same ViewNode shape the console's message
 * views draw, without going through JSON: int64 values stay exact, bytes stay
 * bytes, unset fields are omitted and unknown fields are counted, not dropped.
 */
import { ScalarType, isMessage, type DescEnum, type DescField, type DescMessage, type Message } from '@bufbuild/protobuf'
import { reflect, type ReflectList, type ReflectMap, type ReflectMessage } from '@bufbuild/protobuf/reflect'
import { timestampDate, type Timestamp } from '@bufbuild/protobuf/wkt'
import type { ViewNode } from '@/lib/message-view/messageTree'

const LONG = new Set([ScalarType.INT64, ScalarType.UINT64, ScalarType.SINT64, ScalarType.FIXED64, ScalarType.SFIXED64])

function short(typeName: string): string {
  return typeName.slice(typeName.lastIndexOf('.') + 1)
}

function scalarLeaf(label: string, scalar: ScalarType, value: unknown): ViewNode {
  const typeLabel = ScalarType[scalar].toLowerCase()
  if (scalar === ScalarType.BOOL) return { label, typeLabel, kind: 'bool', display: String(value) }
  if (scalar === ScalarType.STRING) return { label, typeLabel, kind: 'text', display: String(value) }
  if (scalar === ScalarType.BYTES) {
    const bytes = value as Uint8Array
    const preview = Array.from(bytes.subarray(0, 32), (b) => b.toString(16).padStart(2, '0')).join(' ')
    return { label, typeLabel, kind: 'bytes', display: `${bytes.length} B  ${preview}${bytes.length > 32 ? ' ...' : ''}` }
  }
  if (LONG.has(scalar)) return { label, typeLabel, kind: 'int64', display: String(value) }
  return { label, typeLabel, kind: 'number', display: String(value) }
}

function enumLeaf(label: string, enumDesc: DescEnum, value: number): ViewNode {
  const named = enumDesc.value[value]
  return {
    label,
    typeLabel: enumDesc.typeName,
    kind: 'enum',
    display: named?.name ?? `UNKNOWN(${value})`,
    detail: `= ${value}`,
  }
}

function messageLeaf(label: string, r: ReflectMessage): ViewNode {
  if (r.desc.typeName === 'google.protobuf.Timestamp') {
    return { label, typeLabel: 'Timestamp', kind: 'wkt', display: timestampDate(r.message as Timestamp).toISOString() }
  }
  const children = messageNodes(r)
  return {
    label,
    typeLabel: r.desc.typeName,
    kind: 'message',
    display: children.length === 0 ? '(empty)' : undefined,
    children,
  }
}

function fieldNode(r: ReflectMessage, field: DescField): ViewNode {
  const oneof = field.oneof?.name
  const label = field.name
  switch (field.fieldKind) {
    case 'scalar':
      return { ...scalarLeaf(label, field.scalar, r.get(field)), oneof }
    case 'enum':
      return { ...enumLeaf(label, field.enum, r.get(field) as number), oneof }
    case 'message':
      return { ...messageLeaf(label, r.get(field) as ReflectMessage), oneof }
    case 'list': {
      const list = r.get(field) as ReflectList
      const element =
        field.listKind === 'message' ? short(field.message.typeName)
        : field.listKind === 'enum' ? short(field.enum.typeName)
        : ScalarType[field.scalar].toLowerCase()
      const children: ViewNode[] = []
      for (const [i, item] of list.entries()) {
        const at = `[${i}]`
        if (field.listKind === 'message') children.push(messageLeaf(at, item as ReflectMessage))
        else if (field.listKind === 'enum') children.push(enumLeaf(at, field.enum, item as number))
        else children.push({ ...scalarLeaf(at, field.scalar, item), typeLabel: '' })
      }
      return { label, typeLabel: `repeated ${element} (${list.size})`, kind: 'group', oneof, children }
    }
    case 'map': {
      const map = r.get(field) as ReflectMap
      const valueType =
        field.mapKind === 'message' ? short(field.message.typeName)
        : field.mapKind === 'enum' ? short(field.enum.typeName)
        : ScalarType[field.scalar].toLowerCase()
      const children: ViewNode[] = []
      const keys = [...map.keys()].sort((a, b) => (String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0))
      for (const key of keys) {
        const value = map.get(key)
        const at = String(key)
        if (field.mapKind === 'message') children.push(messageLeaf(at, value as ReflectMessage))
        else if (field.mapKind === 'enum') children.push(enumLeaf(at, field.enum, value as number))
        else children.push({ ...scalarLeaf(at, field.scalar, value), typeLabel: '' })
      }
      return {
        label,
        typeLabel: `map<${ScalarType[field.mapKey].toLowerCase()}, ${valueType}> (${map.size})`,
        kind: 'group',
        oneof,
        children,
      }
    }
  }
}

function messageNodes(r: ReflectMessage): ViewNode[] {
  const nodes = r.sortedFields.filter((f) => r.isSet(f)).map((f) => fieldNode(r, f))
  const unknown = r.getUnknown()
  if (unknown !== undefined && unknown.length > 0) {
    nodes.push({
      label: '(unknown fields)',
      typeLabel: `${unknown.length} not in the retained schema`,
      kind: 'group',
      children: unknown.map((u) => ({
        label: `#${u.no}`,
        typeLabel: `wire type ${u.wireType}`,
        kind: 'bytes',
        display: `${u.data.length} B`,
      })),
    })
  }
  return nodes
}

/** The tree of a decoded message's set fields. */
export function decodedTree(desc: DescMessage, message: Message): ViewNode[] {
  if (!isMessage(message, desc)) throw new Error(`The decoded value is not a ${desc.typeName}.`)
  return messageNodes(reflect(desc, message))
}
