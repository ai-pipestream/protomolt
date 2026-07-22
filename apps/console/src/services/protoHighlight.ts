/**
 * Minimal, self-contained .proto syntax highlighter. The repo carries no
 * highlighting library and a schema console doesn't justify one — protobuf's
 * lexical grammar is tiny. Produces HTML with `pshl-*` classed spans; ALL
 * source text passes through escapeHtml, so output is safe to v-html.
 */

const KEYWORDS = new Set([
  'syntax', 'edition', 'package', 'import', 'option', 'message', 'enum',
  'service', 'rpc', 'returns', 'stream', 'repeated', 'optional', 'required',
  'oneof', 'map', 'reserved', 'extensions', 'extend', 'to', 'max', 'weak',
  'public', 'group',
])

const TYPES = new Set([
  'double', 'float', 'int32', 'int64', 'uint32', 'uint64', 'sint32', 'sint64',
  'fixed32', 'fixed64', 'sfixed32', 'sfixed64', 'bool', 'string', 'bytes',
])

// Longest-match-first token alternation. Comments and strings must win over
// everything; numbers before identifiers.
const TOKEN = new RegExp(
  [
    String.raw`\/\/[^\n]*`, // line comment
    String.raw`\/\*[\s\S]*?\*\/`, // block comment
    String.raw`"(?:[^"\\\n]|\\.)*"`, // double-quoted string
    String.raw`'(?:[^'\\\n]|\\.)*'`, // single-quoted string
    String.raw`\b\d+(?:\.\d+)?\b`, // number
    String.raw`[A-Za-z_][A-Za-z0-9_.]*`, // identifier / dotted name
  ].join('|'),
  'g',
)

export function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

function classify(token: string): string | null {
  if (token.startsWith('//') || token.startsWith('/*')) return 'pshl-comment'
  if (token.startsWith('"') || token.startsWith("'")) return 'pshl-string'
  if (/^\d/.test(token)) return 'pshl-number'
  if (KEYWORDS.has(token)) return 'pshl-keyword'
  if (TYPES.has(token)) return 'pshl-type'
  return null
}

/** Highlight one line (or any fragment) of proto source into HTML. */
export function highlightProto(source: string): string {
  let html = ''
  let last = 0
  TOKEN.lastIndex = 0
  for (let m = TOKEN.exec(source); m !== null; m = TOKEN.exec(source)) {
    html += escapeHtml(source.slice(last, m.index))
    const cls = classify(m[0])
    html += cls ? `<span class="${cls}">${escapeHtml(m[0])}</span>` : escapeHtml(m[0])
    last = m.index + m[0].length
  }
  html += escapeHtml(source.slice(last))
  return html
}

/** Highlight a whole schema into per-line HTML (for numbered gutters/diffs). */
export function highlightProtoLines(source: string): string[] {
  // Block comments spanning lines: highlight per-line is fine for protobuf
  // schemas in practice, but keep multi-line /* */ colored by tracking state.
  const lines = source.replace(/\r\n/g, '\n').split('\n')
  const out: string[] = []
  let inBlockComment = false
  for (const line of lines) {
    if (inBlockComment) {
      const end = line.indexOf('*/')
      if (end === -1) {
        out.push(`<span class="pshl-comment">${escapeHtml(line)}</span>`)
        continue
      }
      const head = line.slice(0, end + 2)
      const rest = line.slice(end + 2)
      out.push(`<span class="pshl-comment">${escapeHtml(head)}</span>` + highlightProto(rest))
      inBlockComment = false
      continue
    }
    // Does an unterminated /* start on this line (outside strings/line comments)?
    const opened = /\/\*(?![\s\S]*?\*\/)/.test(stripStringsAndLineComments(line))
    if (opened) {
      const start = line.indexOf('/*')
      out.push(
        highlightProto(line.slice(0, start)) +
          `<span class="pshl-comment">${escapeHtml(line.slice(start))}</span>`,
      )
      inBlockComment = true
    } else {
      out.push(highlightProto(line))
    }
  }
  return out
}

function stripStringsAndLineComments(line: string): string {
  return line
    .replace(/"(?:[^"\\]|\\.)*"/g, '""')
    .replace(/'(?:[^'\\]|\\.)*'/g, "''")
    .replace(/\/\/.*$/, '')
}
