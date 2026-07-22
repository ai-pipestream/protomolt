import { describe, expect, it } from 'vitest'
import { diffLines, diffStats } from './textDiff'

describe('diffLines', () => {
  it('marks identical texts as all-same', () => {
    const diff = diffLines('a\nb', 'a\nb')
    expect(diff).toEqual([
      { op: 'same', left: 1, right: 1, text: 'a' },
      { op: 'same', left: 2, right: 2, text: 'b' },
    ])
    expect(diffStats(diff)).toEqual({ added: 0, removed: 0, unchanged: 2 })
  })

  it('detects an insertion with correct line numbers', () => {
    const diff = diffLines('a\nc', 'a\nb\nc')
    expect(diff).toEqual([
      { op: 'same', left: 1, right: 1, text: 'a' },
      { op: 'add', right: 2, text: 'b' },
      { op: 'same', left: 2, right: 3, text: 'c' },
    ])
  })

  it('detects a deletion', () => {
    const diff = diffLines('a\nb\nc', 'a\nc')
    expect(diff.filter((l) => l.op === 'del')).toEqual([{ op: 'del', left: 2, text: 'b' }])
  })

  it('renders a changed line as del+add', () => {
    const diff = diffLines('int32 age = 3;', 'string age = 3;')
    expect(diff.map((l) => l.op)).toEqual(['del', 'add'])
    expect(diffStats(diff)).toEqual({ added: 1, removed: 1, unchanged: 0 })
  })

  it('handles empty sides', () => {
    expect(diffLines('', 'a')).toEqual([{ op: 'add', right: 1, text: 'a' }])
    expect(diffLines('a', '')).toEqual([{ op: 'del', left: 1, text: 'a' }])
    expect(diffLines('', '')).toEqual([])
  })

  it('normalizes CRLF', () => {
    expect(diffLines('a\r\nb', 'a\nb').every((l) => l.op === 'same')).toBe(true)
  })
})
