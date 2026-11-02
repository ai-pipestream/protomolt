/**
 * The render tree for a decoded message, built by protobuf reflection over the
 * message itself. It produces the same ViewNode shape the console's message
 * views draw, without going through JSON: int64 values stay exact, bytes stay
 * bytes, unset fields are omitted and unknown fields are counted, not dropped.
 */
import { ScalarType, fromBinary, isMessage, type DescEnum, type DescField, type DescMessage, type Message } from '@bufbuild/protobuf'
import { reflect, type ReflectList, type ReflectMap, type ReflectMessage } from '@bufbuild/protobuf/reflect'
import { timestampDate, type Any, type Timestamp } from '@bufbuild/protobuf/wkt'
import type { ViewNode } from '@/lib/message-view/messageTree'

import { AnySchemas, mapStepKey, type AnyLocation } from './anySchemas'

export interface InspectionContext { schemas: AnySchemas; location: AnyLocation }
const PAGE_SIZE = 200
const MAX_DEPTH = 64

function childContext(context: InspectionContext | undefined, step: string): InspectionContext | undefined {
  return context && { schemas: context.schemas, location: { part: context.location.part, steps: [...context.location.steps, step] } }
}

/** A display budget, never a repository content limit. */
function page<T>(items: Iterable<T>, render: (item: T) => ViewNode): ViewNode[] {
  const iterator = items[Symbol.iterator]()
  const batch = (first: IteratorResult<T>): ViewNode[] => {
    const nodes: ViewNode[] = []
    let next = first
    while (!next.done && nodes.length < PAGE_SIZE) {
      try { nodes.push(render(next.value)) } catch (error) {
        nodes.push({ label: '(inspection failed)', kind: 'text', typeLabel: 'invalid entry', display: error instanceof Error ? error.message : String(error) })
      }
      next = iterator.next()
    }
    if (!next.done) {
      const rest = next
      let cached: ViewNode[] | undefined
      nodes.push({ label: '(more entries)', kind: 'group', typeLabel: `next ${PAGE_SIZE}`, loadChildren: () => cached ??= batch(rest) })
    }
    return nodes
  }
  return batch(iterator.next())
}

function branch(node: ViewNode, children: () => ViewNode[], context: InspectionContext | undefined): ViewNode {
  if (!context) return { ...node, children: children() }
  let cached: ViewNode[] | undefined
  return { ...node, loadChildren: () => {
    if (cached !== undefined) return cached
    try { cached = children() } catch (error) {
      cached = [{ label: '(inspection failed)', kind: 'text', typeLabel: node.typeLabel, display: error instanceof Error ? error.message : String(error) }]
    }
    return cached
  } }
}

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

function messageLeaf(label: string, r: ReflectMessage, context?: InspectionContext, depth = 0): ViewNode {
  if (depth >= MAX_DEPTH) return { label, typeLabel: r.desc.typeName, kind: 'text', display: 'Inspection depth limit reached; content is unchanged.' }
  if (r.desc.typeName === 'google.protobuf.Timestamp') {
    return { label, typeLabel: 'Timestamp', kind: 'wkt', display: timestampDate(r.message as Timestamp).toISOString() }
  }
  if (r.desc.typeName === 'google.protobuf.Any' && context) {
    const any = r.message as Any
    const status = context.schemas.statusOf(any, context.location)
    const reason = { opaque: 'Opaque: payload not decoded.', missing: 'No schema retained for this occurrence.', mismatch: 'Payload differs from its retained binding.', typed: 'Retained schema' }[status]
    return branch({ label, typeLabel: any.typeUrl, kind: 'message', detail: `${any.value.length} B · ${reason}` }, () => {
      const envelope = messageNodes(r, undefined, depth + 1)
      if (status !== 'typed') return envelope
      const binding = context.schemas.schemaOf(any, context.location)!
      try {
        const decoded = reflect(binding.desc, fromBinary(binding.desc, any.value))
        return [...envelope, messageLeaf('(decoded payload)', decoded, childContext(context, binding.boundary), depth + 1)]
      } catch (error) {
        return [...envelope, { label: '(invalid payload)', typeLabel: binding.desc.typeName, kind: 'text', display: error instanceof Error ? error.message : String(error) }]
      }
    }, context)
  }
  return branch({ label, typeLabel: r.desc.typeName, kind: 'message' }, () => messageNodes(r, context, depth + 1), context)
}

function fieldNode(r: ReflectMessage, field: DescField, context?: InspectionContext, depth = 0): ViewNode {
  const atField = childContext(context, `fieldNumber:${field.number}`)
  const oneof = field.oneof?.name
  const label = field.name
  switch (field.fieldKind) {
    case 'scalar':
      return { ...scalarLeaf(label, field.scalar, r.get(field)), oneof }
    case 'enum':
      return { ...enumLeaf(label, field.enum, r.get(field) as number), oneof }
    case 'message':
      return { ...messageLeaf(label, r.get(field) as ReflectMessage, atField, depth), oneof }
    case 'list': {
      const list = r.get(field) as ReflectList
      const element =
        field.listKind === 'message' ? short(field.message.typeName)
        : field.listKind === 'enum' ? short(field.enum.typeName)
        : ScalarType[field.scalar].toLowerCase()
      return branch({ label, typeLabel: `repeated ${element} (${list.size})`, kind: 'group', oneof },
        () => page(list.entries(), ([i, item]) => {
          const at = `[${i}]`
          if (field.listKind === 'message') return messageLeaf(at, item as ReflectMessage, childContext(atField, `repeatedIndex:${i}`), depth)
          if (field.listKind === 'enum') return enumLeaf(at, field.enum, item as number)
          return { ...scalarLeaf(at, field.scalar, item), typeLabel: '' }
        }), context)
    }
    case 'map': {
      const map = r.get(field) as ReflectMap
      const valueType =
        field.mapKind === 'message' ? short(field.message.typeName)
        : field.mapKind === 'enum' ? short(field.enum.typeName)
        : ScalarType[field.scalar].toLowerCase()
      return branch({ label, typeLabel: `map<${ScalarType[field.mapKey].toLowerCase()}, ${valueType}> (${map.size})`, kind: 'group', oneof },
        () => page([...map.entries()].sort(([a], [b]) => String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0), ([key, value]) => {
          const at = String(key)
          if (field.mapKind === 'message') return messageLeaf(at, value as ReflectMessage, childContext(atField, mapStepKey(field.mapKey, key)), depth)
          if (field.mapKind === 'enum') return enumLeaf(at, field.enum, value as number)
          return { ...scalarLeaf(at, field.scalar, value), typeLabel: '' }
        }), context)
    }
  }
}

function messageNodes(r: ReflectMessage, context?: InspectionContext, depth = 0): ViewNode[] {
  const nodes = page(r.sortedFields.filter((f) => r.isSet(f)), (f) => fieldNode(r, f, context, depth))
  const unknown = r.getUnknown()
  if (unknown !== undefined && unknown.length > 0) {
    nodes.push({
      label: '(unknown fields)',
      typeLabel: `${unknown.length} not in the retained schema`,
      kind: 'group',
      children: page(unknown, (u) => ({
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
export function decodedTree(desc: DescMessage, message: Message, context?: InspectionContext): ViewNode[] {
  if (!isMessage(message, desc)) throw new Error(`The decoded value is not a ${desc.typeName}.`)
  return desc.typeName === 'google.protobuf.Any'
    ? [messageLeaf('(root)', reflect(desc, message), context)]
    : messageNodes(reflect(desc, message), context)
}
