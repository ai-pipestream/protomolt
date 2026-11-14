/**
 * Trusted format-family icon and label classification for repository browsing.
 *
 * Primary classification uses declared IANA media types and authoritative
 * protobuf metadata. Filename extensions are consulted ONLY as a secondary hint
 * when `media_type` is absent or `application/octet-stream`, and are explicitly
 * marked as `extension-hint` rather than verified content.
 *
 * Security boundary:
 * - Icons are strictly static Vuetify MDI (`mdi-...`) identifiers from a closed
 *   allowlist — never provider-supplied SVG, data URIs, or HTML.
 * - Display strings are plain text stripped of control characters and bounded
 *   in length so untrusted `media_type` or `filename` metadata is safe for text
 *   rendering.
 * - Format icons describe stored or target formats only and never imply
 *   conversion capability.
 */

export type FormatFamily =
  | 'protobuf'
  | 'parquet'
  | 'avro'
  | 'json'
  | 'ndjson'
  | 'yaml'
  | 'xml'
  | 'delimited'
  | 'markdown'
  | 'html'
  | 'text'
  | 'pdf'
  | 'word'
  | 'spreadsheet'
  | 'presentation'
  | 'archive'
  | 'image'
  | 'audio'
  | 'video'
  | 'wasm'
  | 'binary'
  | 'unknown'

export type FormatHintSource =
  | 'authoritative-protobuf'
  | 'media-type'
  | 'extension-hint'
  | 'fallback'

export interface FormatClassifyOptions {
  /** Optional filename used ONLY when mediaType is empty or application/octet-stream. */
  filename?: string | null
  /** Whether the stored rendition or part is an authoritative protobuf message. */
  authoritativeProtobuf?: boolean
  /** Whether the stored rendition is derived from a protobuf source. */
  derived?: boolean
  /** Whether the derived rendition's source revision has since advanced. */
  stale?: boolean
  /** Whether the item references an external URI rather than inline bytes. */
  external?: boolean
}

export interface FormatDescriptor {
  family: FormatFamily
  /** Trusted static Vuetify MDI icon identifier (`mdi-...`). */
  icon: string
  /** Concise human-readable format family name (e.g. `Protobuf`, `Parquet`, `PDF`). */
  label: string
  /** Normalized media type without parameters, or empty string when absent. */
  normalizedMediaType: string
  /** Sanitized raw media type display string, bounded in length. */
  sanitizedMediaType: string
  /** How the format family was determined. */
  hintSource: FormatHintSource
  /** Accessible summary suitable for standalone `aria-label` usage. */
  accessibleLabel: string
}

const MAX_DISPLAY_MEDIA_TYPE_LENGTH = 96
const GENERIC_BINARY_MEDIA_TYPE = 'application/octet-stream'

interface FamilySpec {
  family: FormatFamily
  icon: string
  label: string
}

const FAMILY_SPECS: Readonly<Record<FormatFamily, FamilySpec>> = {
  protobuf: { family: 'protobuf', icon: 'mdi-cube-outline', label: 'Protobuf' },
  parquet: { family: 'parquet', icon: 'mdi-table-column', label: 'Parquet' },
  avro: { family: 'avro', icon: 'mdi-database-outline', label: 'Avro' },
  json: { family: 'json', icon: 'mdi-code-json', label: 'JSON' },
  ndjson: { family: 'ndjson', icon: 'mdi-format-list-bulleted-type', label: 'NDJSON' },
  yaml: { family: 'yaml', icon: 'mdi-file-code-outline', label: 'YAML' },
  xml: { family: 'xml', icon: 'mdi-xml', label: 'XML' },
  delimited: { family: 'delimited', icon: 'mdi-table-delimited', label: 'Delimited table' },
  markdown: { family: 'markdown', icon: 'mdi-language-markdown-outline', label: 'Markdown' },
  html: { family: 'html', icon: 'mdi-language-html5', label: 'HTML' },
  text: { family: 'text', icon: 'mdi-file-document-outline', label: 'Plain text' },
  pdf: { family: 'pdf', icon: 'mdi-file-pdf-box', label: 'PDF' },
  word: { family: 'word', icon: 'mdi-file-word-box-outline', label: 'Document' },
  spreadsheet: { family: 'spreadsheet', icon: 'mdi-file-excel-box-outline', label: 'Spreadsheet' },
  presentation: { family: 'presentation', icon: 'mdi-file-powerpoint-box-outline', label: 'Presentation' },
  archive: { family: 'archive', icon: 'mdi-folder-zip-outline', label: 'Archive' },
  image: { family: 'image', icon: 'mdi-image-outline', label: 'Image' },
  audio: { family: 'audio', icon: 'mdi-volume-high', label: 'Audio' },
  video: { family: 'video', icon: 'mdi-video-outline', label: 'Video' },
  wasm: { family: 'wasm', icon: 'mdi-memory', label: 'WebAssembly' },
  binary: { family: 'binary', icon: 'mdi-file-binary-outline', label: 'Binary' },
  unknown: { family: 'unknown', icon: 'mdi-file-outline', label: 'Unknown format' },
}

const PROTOBUF_MEDIA_TYPES = new Set<string>([
  'application/x-protobuf',
  'application/protobuf',
  'application/vnd.google.protobuf',
])

const EXACT_MEDIA_TYPE_FAMILIES: Readonly<Record<string, FormatFamily>> = {
  'application/x-protobuf': 'protobuf',
  'application/protobuf': 'protobuf',
  'application/vnd.google.protobuf': 'protobuf',
  'application/vnd.apache.parquet': 'parquet',
  'application/parquet': 'parquet',
  'application/x-parquet': 'parquet',
  'application/avro': 'avro',
  'application/vnd.apache.avro': 'avro',
  'application/json': 'json',
  'text/json': 'json',
  'application/x-ndjson': 'ndjson',
  'application/jsonl': 'ndjson',
  'application/json-seq': 'ndjson',
  'application/yaml': 'yaml',
  'application/x-yaml': 'yaml',
  'text/yaml': 'yaml',
  'text/x-yaml': 'yaml',
  'application/xml': 'xml',
  'text/xml': 'xml',
  'text/csv': 'delimited',
  'application/csv': 'delimited',
  'text/tab-separated-values': 'delimited',
  'text/markdown': 'markdown',
  'text/x-markdown': 'markdown',
  'text/html': 'html',
  'application/xhtml+xml': 'html',
  'text/plain': 'text',
  'application/pdf': 'pdf',
  'application/msword': 'word',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'word',
  'application/vnd.oasis.opendocument.text': 'word',
  'application/rtf': 'word',
  'application/vnd.ms-excel': 'spreadsheet',
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'spreadsheet',
  'application/vnd.oasis.opendocument.spreadsheet': 'spreadsheet',
  'application/vnd.ms-powerpoint': 'presentation',
  'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'presentation',
  'application/vnd.oasis.opendocument.presentation': 'presentation',
  'application/zip': 'archive',
  'application/x-zip-compressed': 'archive',
  'application/gzip': 'archive',
  'application/x-gzip': 'archive',
  'application/x-tar': 'archive',
  'application/x-bzip2': 'archive',
  'application/x-7z-compressed': 'archive',
  'application/zstd': 'archive',
  'application/wasm': 'wasm',
}

const EXTENSION_HINT_FAMILIES: Readonly<Record<string, FormatFamily>> = {
  pb: 'protobuf',
  proto: 'protobuf',
  binpb: 'protobuf',
  parquet: 'parquet',
  pq: 'parquet',
  avro: 'avro',
  json: 'json',
  ndjson: 'ndjson',
  jsonl: 'ndjson',
  yaml: 'yaml',
  yml: 'yaml',
  xml: 'xml',
  csv: 'delimited',
  tsv: 'delimited',
  md: 'markdown',
  markdown: 'markdown',
  html: 'html',
  htm: 'html',
  txt: 'text',
  log: 'text',
  pdf: 'pdf',
  doc: 'word',
  docx: 'word',
  odt: 'word',
  rtf: 'word',
  xls: 'spreadsheet',
  xlsx: 'spreadsheet',
  ods: 'spreadsheet',
  ppt: 'presentation',
  pptx: 'presentation',
  odp: 'presentation',
  zip: 'archive',
  tar: 'archive',
  gz: 'archive',
  tgz: 'archive',
  bz2: 'archive',
  '7z': 'archive',
  zst: 'archive',
  png: 'image',
  jpg: 'image',
  jpeg: 'image',
  gif: 'image',
  webp: 'image',
  svg: 'image',
  avif: 'image',
  mp3: 'audio',
  wav: 'audio',
  ogg: 'audio',
  flac: 'audio',
  m4a: 'audio',
  mp4: 'video',
  webm: 'video',
  mov: 'video',
  mkv: 'video',
  wasm: 'wasm',
}

/**
 * Strip ASCII/Unicode control characters and bound display length so untrusted
 * metadata values can never inject formatting tricks or unbounded strings.
 */
export function sanitizeDisplayMetadata(raw: string | null | undefined, maxLength = MAX_DISPLAY_MEDIA_TYPE_LENGTH): string {
  if (raw === null || raw === undefined) return ''
  let out = ''
  for (let i = 0; i < raw.length; i++) {
    const code = raw.charCodeAt(i)
    if (code < 0x20 || (code >= 0x7f && code <= 0x9f)) {
      out += ' '
    } else {
      out += raw[i]
    }
  }
  const collapsed = out.trim()
  if (collapsed.length <= maxLength) return collapsed
  return `${collapsed.slice(0, maxLength)}...`
}

/**
 * Normalize an IANA media type string by stripping parameters (`; charset=...`)
 * and lowercasing, returning `""` when blank or structurally invalid.
 */
export function normalizeMediaType(mediaType: string | null | undefined): string {
  if (mediaType === null || mediaType === undefined) return ''
  const semicolon = mediaType.indexOf(';')
  const essence = (semicolon >= 0 ? mediaType.slice(0, semicolon) : mediaType).trim().toLowerCase()
  if (essence === '') return ''
  const slash = essence.indexOf('/')
  if (slash <= 0 || slash === essence.length - 1 || essence.indexOf('/', slash + 1) !== -1) {
    return ''
  }
  for (let i = 0; i < essence.length; i++) {
    const c = essence.charCodeAt(i)
    const ok =
      (c >= 0x61 && c <= 0x7a)
      || (c >= 0x30 && c <= 0x39)
      || c === 0x2f // /
      || c === 0x2d // -
      || c === 0x2e // .
      || c === 0x2b // +
      || c === 0x5f // _
    if (!ok) return ''
  }
  return essence
}

/** Whether a declared media type identifies binary protobuf wire bytes. */
export function isProtobufMediaType(mediaType: string | null | undefined): boolean {
  const normalized = normalizeMediaType(mediaType)
  return PROTOBUF_MEDIA_TYPES.has(normalized) || normalized.endsWith('+proto')
}

function classifyByMediaType(normalized: string): FormatFamily | null {
  if (normalized === '') return null
  const exact = EXACT_MEDIA_TYPE_FAMILIES[normalized]
  if (exact !== undefined) return exact
  if (normalized.endsWith('+proto')) return 'protobuf'
  if (normalized.endsWith('+json')) return 'json'
  if (normalized.endsWith('+xml')) return 'xml'
  if (normalized.endsWith('+yaml')) return 'yaml'
  if (normalized.endsWith('+zip')) return 'archive'
  if (normalized.startsWith('image/')) return 'image'
  if (normalized.startsWith('audio/')) return 'audio'
  if (normalized.startsWith('video/')) return 'video'
  if (normalized.startsWith('text/')) return 'text'
  return null
}

function extractFilenameExtension(filename: string | null | undefined): string {
  if (filename === null || filename === undefined) return ''
  const trimmed = filename.trim()
  if (trimmed === '') return ''
  const lastSlash = Math.max(trimmed.lastIndexOf('/'), trimmed.lastIndexOf('\\'))
  const base = lastSlash >= 0 ? trimmed.slice(lastSlash + 1) : trimmed
  const dot = base.lastIndexOf('.')
  if (dot <= 0 || dot === base.length - 1) return ''
  return base.slice(dot + 1).toLowerCase()
}

function buildAccessibleLabel(
  spec: FamilySpec,
  hintSource: FormatHintSource,
  sanitizedMediaType: string,
  options?: FormatClassifyOptions,
): string {
  const parts: string[] = [spec.label]
  if (hintSource === 'extension-hint') {
    parts.push('inferred from filename extension')
  } else if (sanitizedMediaType !== '' && spec.family === 'unknown') {
    parts.push(`(${sanitizedMediaType})`)
  }
  if (options?.authoritativeProtobuf) {
    parts.push('authoritative protobuf')
  } else if (options?.derived) {
    parts.push('derived rendition')
  }
  if (options?.stale) {
    parts.push('stale source revision')
  }
  if (options?.external) {
    parts.push('external reference')
  }
  return parts.join(', ')
}

/**
 * Classify a stored or target format into a trusted icon and readable label.
 */
export function classifyFormat(
  mediaType?: string | null,
  options?: FormatClassifyOptions,
): FormatDescriptor {
  const sanitizedMediaType = sanitizeDisplayMetadata(mediaType)
  const normalizedMediaType = normalizeMediaType(mediaType)

  if (options?.authoritativeProtobuf) {
    const spec = FAMILY_SPECS.protobuf
    return {
      ...spec,
      normalizedMediaType,
      sanitizedMediaType,
      hintSource: 'authoritative-protobuf',
      accessibleLabel: buildAccessibleLabel(spec, 'authoritative-protobuf', sanitizedMediaType, options),
    }
  }

  const byType = classifyByMediaType(normalizedMediaType)
  if (byType !== null) {
    const spec = FAMILY_SPECS[byType]
    return {
      ...spec,
      normalizedMediaType,
      sanitizedMediaType,
      hintSource: 'media-type',
      accessibleLabel: buildAccessibleLabel(spec, 'media-type', sanitizedMediaType, options),
    }
  }

  // Filename extension is consulted ONLY when media_type is absent or generic octet-stream.
  if (normalizedMediaType === '' || normalizedMediaType === GENERIC_BINARY_MEDIA_TYPE) {
    const ext = extractFilenameExtension(options?.filename)
    const hinted = ext !== '' ? EXTENSION_HINT_FAMILIES[ext] : undefined
    if (hinted !== undefined) {
      const spec = FAMILY_SPECS[hinted]
      return {
        ...spec,
        normalizedMediaType,
        sanitizedMediaType,
        hintSource: 'extension-hint',
        accessibleLabel: buildAccessibleLabel(spec, 'extension-hint', sanitizedMediaType, options),
      }
    }
    if (normalizedMediaType === GENERIC_BINARY_MEDIA_TYPE) {
      const spec = FAMILY_SPECS.binary
      return {
        ...spec,
        normalizedMediaType,
        sanitizedMediaType,
        hintSource: 'media-type',
        accessibleLabel: buildAccessibleLabel(spec, 'media-type', sanitizedMediaType, options),
      }
    }
  }

  const spec = FAMILY_SPECS.unknown
  return {
    ...spec,
    normalizedMediaType,
    sanitizedMediaType,
    hintSource: 'fallback',
    accessibleLabel: buildAccessibleLabel(spec, 'fallback', sanitizedMediaType, options),
  }
}
