import { describe, expect, it } from 'vitest'
import { escapeHtml, highlightProto, highlightProtoLines } from './protoHighlight'

describe('highlightProto', () => {
  it('escapes HTML in source text (no raw markup ever reaches v-html)', () => {
    const html = highlightProto('// <script>alert(1)</script>')
    expect(html).not.toContain('<script>')
    expect(html).toContain('&lt;script&gt;')
  })

  it('classes keywords, types, strings and numbers', () => {
    const html = highlightProto('message Person { string name = 1; }')
    expect(html).toContain('<span class="pshl-keyword">message</span>')
    expect(html).toContain('<span class="pshl-type">string</span>')
    expect(html).toContain('<span class="pshl-number">1</span>')
    const syntax = highlightProto('syntax = "proto3";')
    expect(syntax).toContain('<span class="pshl-keyword">syntax</span>')
    expect(syntax).toContain('<span class="pshl-string">&quot;proto3&quot;</span>')
  })

  it('does not class plain identifiers', () => {
    expect(highlightProto('Person')).toBe('Person')
  })

  it('colors line comments as a unit', () => {
    expect(highlightProto('int32 a = 1; // the message count')).toContain(
      '<span class="pshl-comment">// the message count</span>',
    )
  })
})

describe('highlightProtoLines', () => {
  it('keeps multi-line block comments colored across lines', () => {
    const lines = highlightProtoLines('/* first\nsecond\nthird */ message A {}')
    expect(lines[0]).toContain('pshl-comment')
    expect(lines[1]).toContain('pshl-comment')
    expect(lines[2]).toContain('pshl-comment')
    expect(lines[2]).toContain('<span class="pshl-keyword">message</span>')
  })

  it('returns one entry per line', () => {
    expect(highlightProtoLines('a\nb\nc')).toHaveLength(3)
  })
})

describe('escapeHtml', () => {
  it('escapes the four dangerous characters', () => {
    expect(escapeHtml('<a href="x">&</a>')).toBe('&lt;a href=&quot;x&quot;&gt;&amp;&lt;/a&gt;')
  })
})
