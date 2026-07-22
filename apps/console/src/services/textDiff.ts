/**
 * Client-side line diff for schema text (the version diff view). Classic
 * LCS over lines — schema files are small (the registry caps request bodies
 * well under a megabyte), so the O(n·m) table is fine and dependency-free.
 */

export type DiffOp = 'same' | 'add' | 'del'

export interface DiffLine {
  op: DiffOp
  /** 1-based line number in the left (old) text; undefined for additions. */
  left?: number
  /** 1-based line number in the right (new) text; undefined for deletions. */
  right?: number
  text: string
}

export function diffLines(oldText: string, newText: string): DiffLine[] {
  const a = splitLines(oldText)
  const b = splitLines(newText)

  // LCS length table (a.length+1 × b.length+1).
  const n = a.length
  const m = b.length
  const table: Uint32Array[] = []
  for (let i = 0; i <= n; i++) table.push(new Uint32Array(m + 1))
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      table[i][j] =
        a[i] === b[j] ? table[i + 1][j + 1] + 1 : Math.max(table[i + 1][j], table[i][j + 1])
    }
  }

  const out: DiffLine[] = []
  let i = 0
  let j = 0
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      out.push({ op: 'same', left: i + 1, right: j + 1, text: a[i] })
      i++
      j++
    } else if (table[i + 1][j] >= table[i][j + 1]) {
      out.push({ op: 'del', left: i + 1, text: a[i] })
      i++
    } else {
      out.push({ op: 'add', right: j + 1, text: b[j] })
      j++
    }
  }
  while (i < n) {
    out.push({ op: 'del', left: i + 1, text: a[i] })
    i++
  }
  while (j < m) {
    out.push({ op: 'add', right: j + 1, text: b[j] })
    j++
  }
  return out
}

export interface DiffStats {
  added: number
  removed: number
  unchanged: number
}

export function diffStats(lines: DiffLine[]): DiffStats {
  const stats: DiffStats = { added: 0, removed: 0, unchanged: 0 }
  for (const line of lines) {
    if (line.op === 'add') stats.added++
    else if (line.op === 'del') stats.removed++
    else stats.unchanged++
  }
  return stats
}

function splitLines(text: string): string[] {
  if (text === '') return []
  return text.replace(/\r\n/g, '\n').split('\n')
}
