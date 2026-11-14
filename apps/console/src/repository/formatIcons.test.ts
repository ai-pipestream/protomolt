import { describe, expect, it } from 'vitest'
import {
  classifyFormat,
  isProtobufMediaType,
  normalizeMediaType,
  sanitizeDisplayMetadata,
} from './formatIcons'

describe('formatIcons', () => {
  it('classifies authoritative protobuf and protobuf media types with trusted icon and label', () => {
    const authoritative = classifyFormat('', { authoritativeProtobuf: true })
    expect(authoritative.family).toBe('protobuf')
    expect(authoritative.icon).toBe('mdi-cube-outline')
    expect(authoritative.label).toBe('Protobuf')
    expect(authoritative.hintSource).toBe('authoritative-protobuf')
    expect(authoritative.accessibleLabel).toContain('authoritative protobuf')

    const wire = classifyFormat('application/x-protobuf; proto=ai.protomolt.Test')
    expect(wire.family).toBe('protobuf')
    expect(wire.normalizedMediaType).toBe('application/x-protobuf')
    expect(wire.hintSource).toBe('media-type')
    expect(isProtobufMediaType('application/vnd.google.protobuf')).toBe(true)
    expect(isProtobufMediaType('application/vnd.custom+proto')).toBe(true)
    expect(isProtobufMediaType('application/pdf')).toBe(false)
  })

  it('classifies Parquet, Avro, JSON, NDJSON, YAML, XML, CSV, Markdown, HTML, PDF, Office, archives, and media families', () => {
    expect(classifyFormat('application/vnd.apache.parquet')).toMatchObject({
      family: 'parquet',
      icon: 'mdi-table-column',
      label: 'Parquet',
      hintSource: 'media-type',
    })
    expect(classifyFormat('application/parquet')).toMatchObject({ family: 'parquet' })
    expect(classifyFormat('application/vnd.apache.avro')).toMatchObject({ family: 'avro', label: 'Avro' })
    expect(classifyFormat('application/json; charset=utf-8')).toMatchObject({ family: 'json', label: 'JSON' })
    expect(classifyFormat('application/problem+json')).toMatchObject({ family: 'json' })
    expect(classifyFormat('application/x-ndjson')).toMatchObject({ family: 'ndjson', label: 'NDJSON' })
    expect(classifyFormat('application/yaml')).toMatchObject({ family: 'yaml', label: 'YAML' })
    expect(classifyFormat('application/atom+xml')).toMatchObject({ family: 'xml', label: 'XML' })
    expect(classifyFormat('text/csv')).toMatchObject({ family: 'delimited', label: 'Delimited table' })
    expect(classifyFormat('text/markdown')).toMatchObject({ family: 'markdown', label: 'Markdown' })
    expect(classifyFormat('text/html')).toMatchObject({ family: 'html', label: 'HTML' })
    expect(classifyFormat('text/plain')).toMatchObject({ family: 'text', label: 'Plain text' })
    expect(classifyFormat('application/pdf')).toMatchObject({ family: 'pdf', icon: 'mdi-file-pdf-box', label: 'PDF' })
    expect(classifyFormat('application/vnd.openxmlformats-officedocument.wordprocessingml.document')).toMatchObject({
      family: 'word',
      label: 'Document',
    })
    expect(classifyFormat('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')).toMatchObject({
      family: 'spreadsheet',
      label: 'Spreadsheet',
    })
    expect(classifyFormat('application/vnd.openxmlformats-officedocument.presentationml.presentation')).toMatchObject({
      family: 'presentation',
      label: 'Presentation',
    })
    expect(classifyFormat('application/zip')).toMatchObject({ family: 'archive', label: 'Archive' })
    expect(classifyFormat('image/png')).toMatchObject({ family: 'image', label: 'Image' })
    expect(classifyFormat('audio/flac')).toMatchObject({ family: 'audio', label: 'Audio' })
    expect(classifyFormat('video/mp4')).toMatchObject({ family: 'video', label: 'Video' })
    expect(classifyFormat('application/wasm')).toMatchObject({ family: 'wasm', label: 'WebAssembly' })
  })

  it('uses filename extension ONLY as a secondary hint when mediaType is empty or application/octet-stream', () => {
    const fromEmpty = classifyFormat('', { filename: 'dataset/slice-01.parquet' })
    expect(fromEmpty.family).toBe('parquet')
    expect(fromEmpty.hintSource).toBe('extension-hint')
    expect(fromEmpty.accessibleLabel).toContain('inferred from filename extension')

    const fromOctet = classifyFormat('application/octet-stream', { filename: 'report.pdf' })
    expect(fromOctet.family).toBe('pdf')
    expect(fromOctet.hintSource).toBe('extension-hint')

    // Declared media_type always wins over misleading filename extensions.
    const declaredTextWithExeSuffix = classifyFormat('text/plain', { filename: 'notes.parquet' })
    expect(declaredTextWithExeSuffix.family).toBe('text')
    expect(declaredTextWithExeSuffix.hintSource).toBe('media-type')

    // Unknown declared media_type does NOT trust filename extension; falls back deterministically.
    const unknownWithExtension = classifyFormat('application/x-proprietary-blob', { filename: 'sneaky.pdf' })
    expect(unknownWithExtension.family).toBe('unknown')
    expect(unknownWithExtension.icon).toBe('mdi-file-outline')
    expect(unknownWithExtension.hintSource).toBe('fallback')
  })

  it('falls back to generic Binary for unhinted octet-stream and Unknown format for arbitrary or hostile strings', () => {
    const pureOctet = classifyFormat('application/octet-stream')
    expect(pureOctet.family).toBe('binary')
    expect(pureOctet.label).toBe('Binary')

    const hostile = classifyFormat('<svg onload=alert(1)>/javascript:evil()\u0000\u001b[31m', {
      derived: true,
      stale: true,
    })
    expect(hostile.family).toBe('unknown')
    expect(hostile.icon).toBe('mdi-file-outline')
    expect(hostile.normalizedMediaType).toBe('')
    expect(hostile.sanitizedMediaType).not.toContain('\u0000')
    expect(hostile.sanitizedMediaType).not.toContain('\u001b')
    expect(hostile.accessibleLabel).toContain('derived rendition')
    expect(hostile.accessibleLabel).toContain('stale source revision')
  })

  it('normalizes valid media types and bounds display sanitization length', () => {
    expect(normalizeMediaType('  Application/Vnd.Apache.Parquet ; version=2 ')).toBe('application/vnd.apache.parquet')
    expect(normalizeMediaType('not-a-mime')).toBe('')
    expect(normalizeMediaType('text/plain/extra')).toBe('')
    const longInput = 'x'.repeat(200)
    const sanitized = sanitizeDisplayMetadata(longInput, 40)
    expect(sanitized.length).toBe(43)
    expect(sanitized.endsWith('...')).toBe(true)
  })
})
