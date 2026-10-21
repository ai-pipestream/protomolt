import { createHash, randomBytes } from 'node:crypto'
import { describe, expect, it } from 'vitest'
import { Sha256 } from './sha256'

function hashOf(chunks: Uint8Array[]): string {
  const h = new Sha256()
  for (const c of chunks) h.update(c)
  return h.hex()
}

describe('Sha256', () => {
  it('matches the FIPS 180-4 vectors', () => {
    const enc = new TextEncoder()
    expect(hashOf([])).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')
    expect(hashOf([enc.encode('abc')])).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')
    expect(hashOf([enc.encode('abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq')])).toBe(
      '248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1',
    )
  })

  it('agrees with node:crypto across lengths around the block and padding edges, however the input is split', () => {
    for (const length of [1, 55, 56, 57, 63, 64, 65, 119, 120, 128, 1000, 70_001]) {
      const data = new Uint8Array(randomBytes(length))
      const expected = createHash('sha256').update(data).digest('hex')
      expect(hashOf([data])).toBe(expected)
      // Uneven frames, including empty ones, as a stream delivers them.
      const chunks: Uint8Array[] = []
      for (let i = 0, step = 1; i < length; i += step, step = (step * 7) % 97 || 3) {
        chunks.push(data.subarray(i, Math.min(length, i + step)), new Uint8Array(0))
      }
      expect(hashOf(chunks)).toBe(expected)
    }
  })

  it('refuses use after the digest is taken', () => {
    const h = new Sha256()
    h.hex()
    expect(() => h.update(new Uint8Array(1))).toThrow('already finished')
    expect(() => h.hex()).toThrow('already finished')
  })
})
