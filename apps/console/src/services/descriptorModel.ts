/**
 * Descriptor-set → display model for the type explorer. Parses the binary
 * FileDescriptorSet the registry serves (subject's latest schema plus
 * transitive references) into a file/message/enum/service tree with field
 * types, numbers and labels — the same @bufbuild/protobuf reflection API the
 * platform's field trees ride on.
 */
import {
  createFileRegistry,
  fromBinary,
  ScalarType,
  type DescEnum,
  type DescField,
  type DescFile,
  type DescMessage,
  type DescService,
  type FileRegistry,
} from '@bufbuild/protobuf'
import {
  FileDescriptorSetSchema,
  file_google_protobuf_any,
  file_google_protobuf_api,
  file_google_protobuf_descriptor,
  file_google_protobuf_duration,
  file_google_protobuf_empty,
  file_google_protobuf_field_mask,
  file_google_protobuf_source_context,
  file_google_protobuf_struct,
  file_google_protobuf_timestamp,
  file_google_protobuf_type,
  file_google_protobuf_wrappers,
} from '@bufbuild/protobuf/wkt'

export interface FieldRow {
  name: string
  number: number
  /** 'repeated' | 'optional' | 'required' | '' */
  label: string
  /** Rendered type ("string", "map<string, Person>", "Address"). */
  type: string
  /** Fully-qualified message/enum type this field points at (for linking). */
  refTypeName?: string
  /** Name of the containing oneof, when the field is a oneof member. */
  oneof?: string
}

export interface EnumValueRow {
  name: string
  number: number
}

export interface MethodRow {
  name: string
  /** 'unary' | 'server streaming' | 'client streaming' | 'bidi streaming' */
  kind: string
  inputType: string
  outputType: string
}

export interface TypeNode {
  kind: 'message' | 'enum' | 'service'
  /** Short name for display. */
  name: string
  /** Fully-qualified name (unique across the set — used as tree key). */
  typeName: string
  fields?: FieldRow[]
  values?: EnumValueRow[]
  methods?: MethodRow[]
  /** Nested messages/enums. */
  children?: TypeNode[]
}

export interface FileNode {
  /** Import path, e.g. "example/person.proto". */
  name: string
  package: string
  types: TypeNode[]
}

export interface DescriptorModel {
  files: FileNode[]
  /** All top-level and nested message type names (Try-it composer choices). */
  messageTypeNames: string[]
  registry: FileRegistry
}

// The registry serves the subject's own files; well-known imports
// (descriptor.proto for option schemas, timestamp, ...) are implicit in the
// protocol, so any the set omits are filled in from the runtime's bundled
// copies before linking.
// Dependency order matters: files link in sequence, so api.proto's imports
// (source_context, type) come before it.
const wellKnownProtos = [
  file_google_protobuf_descriptor,
  file_google_protobuf_any,
  file_google_protobuf_source_context,
  file_google_protobuf_type,
  file_google_protobuf_api,
  file_google_protobuf_duration,
  file_google_protobuf_empty,
  file_google_protobuf_field_mask,
  file_google_protobuf_struct,
  file_google_protobuf_timestamp,
  file_google_protobuf_wrappers,
].map((file) => file.proto)

/**
 * Parse the registry's binary descriptor-set response. Files link strictly
 * dependencies-first, so the set is topologically ordered here (registries
 * are not required to emit it that way), with any missing well-known
 * imports filled in from the runtime's bundled copies.
 */
export function registryFromDescriptorSet(bytes: Uint8Array): FileRegistry {
  const set = fromBinary(FileDescriptorSetSchema, bytes)
  const byName = new Map(wellKnownProtos.map((p) => [p.name ?? '', p]))
  for (const file of set.file) {
    byName.set(file.name ?? '', file)
  }
  const ordered: (typeof set.file)[number][] = []
  const visited = new Set<string>()
  const visit = (name: string) => {
    if (visited.has(name)) {
      return
    }
    visited.add(name)
    const proto = byName.get(name)
    if (!proto) {
      return // createFileRegistry reports the missing import precisely
    }
    for (const dep of proto.dependency) {
      visit(dep)
    }
    ordered.push(proto)
  }
  for (const file of set.file) {
    visit(file.name ?? '')
  }
  set.file = ordered
  return createFileRegistry(set)
}

export function buildDescriptorModel(bytes: Uint8Array): DescriptorModel {
  // The set's own files drive the tree; well-known fill-ins only back the
  // linker and stay out of the display model.
  const ownFiles = new Set(fromBinary(FileDescriptorSetSchema, bytes).file.map((f) => f.name))
  const registry = registryFromDescriptorSet(bytes)
  const files: FileNode[] = []
  const messageTypeNames: string[] = []

  for (const file of registry.files) {
    if (!ownFiles.has(file.proto.name ?? file.name)) {
      continue
    }
    files.push(fileNode(file, messageTypeNames))
  }
  // Subject's own file(s) last in the set? Keep deterministic: alphabetical.
  files.sort((a, b) => a.name.localeCompare(b.name))
  messageTypeNames.sort()
  return { files, messageTypeNames, registry }
}

function fileNode(file: DescFile, messageTypeNames: string[]): FileNode {
  const types: TypeNode[] = [
    ...file.messages.map((m) => messageNode(m, messageTypeNames)),
    ...file.enums.map(enumNode),
    ...file.services.map(serviceNode),
  ]
  return { name: file.name.endsWith('.proto') ? file.name : `${file.name}.proto`, package: file.proto.package ?? '', types }
}

function messageNode(message: DescMessage, messageTypeNames: string[]): TypeNode {
  messageTypeNames.push(message.typeName)
  const children: TypeNode[] = [
    ...message.nestedMessages
      // Skip synthetic map-entry messages — they render as map<k,v> fields.
      .filter((m) => !m.proto.options?.mapEntry)
      .map((m) => messageNode(m, messageTypeNames)),
    ...message.nestedEnums.map(enumNode),
  ]
  return {
    kind: 'message',
    name: message.name,
    typeName: message.typeName,
    fields: message.fields.map(fieldRow),
    ...(children.length ? { children } : {}),
  }
}

function enumNode(desc: DescEnum): TypeNode {
  return {
    kind: 'enum',
    name: desc.name,
    typeName: desc.typeName,
    values: desc.values.map((v) => ({ name: v.name, number: v.number })),
  }
}

function serviceNode(desc: DescService): TypeNode {
  return {
    kind: 'service',
    name: desc.name,
    typeName: desc.typeName,
    methods: desc.methods.map((m) => ({
      name: m.name,
      kind: METHOD_KINDS[m.methodKind] ?? m.methodKind,
      inputType: m.input.typeName,
      outputType: m.output.typeName,
    })),
  }
}

const METHOD_KINDS: Record<string, string> = {
  unary: 'unary',
  server_streaming: 'server streaming',
  client_streaming: 'client streaming',
  bidi_streaming: 'bidi streaming',
}

const SCALAR_NAMES: Record<number, string> = {
  [ScalarType.DOUBLE]: 'double',
  [ScalarType.FLOAT]: 'float',
  [ScalarType.INT64]: 'int64',
  [ScalarType.UINT64]: 'uint64',
  [ScalarType.INT32]: 'int32',
  [ScalarType.FIXED64]: 'fixed64',
  [ScalarType.FIXED32]: 'fixed32',
  [ScalarType.BOOL]: 'bool',
  [ScalarType.STRING]: 'string',
  [ScalarType.BYTES]: 'bytes',
  [ScalarType.UINT32]: 'uint32',
  [ScalarType.SFIXED32]: 'sfixed32',
  [ScalarType.SFIXED64]: 'sfixed64',
  [ScalarType.SINT32]: 'sint32',
  [ScalarType.SINT64]: 'sint64',
}

function scalarName(t: ScalarType): string {
  return SCALAR_NAMES[t] ?? 'scalar'
}

function shortName(typeName: string): string {
  return typeName.slice(typeName.lastIndexOf('.') + 1)
}

function fieldRow(f: DescField): FieldRow {
  const row: FieldRow = {
    name: f.name,
    number: f.number,
    label: fieldLabel(f),
    type: fieldType(f),
  }
  const ref = refType(f)
  if (ref) row.refTypeName = ref
  if (f.oneof) row.oneof = f.oneof.name
  return row
}

function fieldLabel(f: DescField): string {
  if (f.fieldKind === 'list') return 'repeated'
  if (f.fieldKind === 'map') return ''
  const proto = f.proto
  if (proto.proto3Optional) return 'optional'
  // proto2 labels: 1 optional, 2 required (label 3 repeated is fieldKind list).
  if (proto.label === 2) return 'required'
  return ''
}

function fieldType(f: DescField): string {
  switch (f.fieldKind) {
    case 'scalar':
      return scalarName(f.scalar)
    case 'enum':
      return shortName(f.enum.typeName)
    case 'message':
      return shortName(f.message.typeName)
    case 'list':
      return f.listKind === 'scalar'
        ? scalarName(f.scalar)
        : f.listKind === 'enum'
          ? shortName(f.enum.typeName)
          : shortName(f.message.typeName)
    case 'map': {
      const value =
        f.mapKind === 'scalar'
          ? scalarName(f.scalar)
          : f.mapKind === 'enum'
            ? shortName(f.enum.typeName)
            : shortName(f.message.typeName)
      return `map<${scalarName(f.mapKey)}, ${value}>`
    }
  }
}

function refType(f: DescField): string | undefined {
  switch (f.fieldKind) {
    case 'enum':
      return f.enum.typeName
    case 'message':
      return f.message.typeName
    case 'list':
      return f.listKind === 'enum'
        ? f.enum.typeName
        : f.listKind === 'message'
          ? f.message.typeName
          : undefined
    case 'map':
      return f.mapKind === 'enum'
        ? f.enum.typeName
        : f.mapKind === 'message'
          ? f.message.typeName
          : undefined
    default:
      return undefined
  }
}
