/**
 * Incremental SHA-256 (FIPS 180-4). WebCrypto digests only a whole buffer, so
 * verifying a streamed download that way would hold the entire file in memory
 * at once; this hashes each frame as it arrives.
 */
const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
])

export class Sha256 {
  private readonly h = new Uint32Array([
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  ])
  private readonly block = new Uint8Array(64)
  private readonly w = new Uint32Array(64)
  private filled = 0
  private length = 0n
  private done = false

  update(data: Uint8Array): this {
    if (this.done) throw new Error('This SHA-256 was already finished.')
    this.length += BigInt(data.length)
    let i = 0
    if (this.filled > 0) {
      const take = Math.min(64 - this.filled, data.length)
      this.block.set(data.subarray(0, take), this.filled)
      this.filled += take
      i = take
      if (this.filled < 64) return this
      this.compress(this.block, 0)
      this.filled = 0
    }
    for (; i + 64 <= data.length; i += 64) this.compress(data, i)
    if (i < data.length) {
      this.block.set(data.subarray(i), 0)
      this.filled = data.length - i
    }
    return this
  }

  /** The digest as lowercase hex. */
  hex(): string {
    if (this.done) throw new Error('This SHA-256 was already finished.')
    this.done = true
    const bits = this.length * 8n
    this.block[this.filled++] = 0x80
    if (this.filled > 56) {
      this.block.fill(0, this.filled)
      this.compress(this.block, 0)
      this.filled = 0
    }
    this.block.fill(0, this.filled, 56)
    const view = new DataView(this.block.buffer)
    view.setBigUint64(56, bits, false)
    this.compress(this.block, 0)
    let out = ''
    for (const word of this.h) out += word.toString(16).padStart(8, '0')
    return out
  }

  private compress(data: Uint8Array, at: number): void {
    const w = this.w
    for (let t = 0; t < 16; t++) {
      const j = at + t * 4
      w[t] = (data[j] << 24) | (data[j + 1] << 16) | (data[j + 2] << 8) | data[j + 3]
    }
    for (let t = 16; t < 64; t++) {
      const x = w[t - 15]
      const y = w[t - 2]
      const s0 = ((x >>> 7) | (x << 25)) ^ ((x >>> 18) | (x << 14)) ^ (x >>> 3)
      const s1 = ((y >>> 17) | (y << 15)) ^ ((y >>> 19) | (y << 13)) ^ (y >>> 10)
      w[t] = (w[t - 16] + s0 + w[t - 7] + s1) | 0
    }
    let [a, b, c, d, e, f, g, h] = this.h
    for (let t = 0; t < 64; t++) {
      const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7))
      const ch = (e & f) ^ (~e & g)
      const t1 = (h + S1 + ch + K[t] + w[t]) | 0
      const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10))
      const maj = (a & b) ^ (a & c) ^ (b & c)
      const t2 = (S0 + maj) | 0
      h = g
      g = f
      f = e
      e = (d + t1) | 0
      d = c
      c = b
      b = a
      a = (t1 + t2) | 0
    }
    this.h[0] += a
    this.h[1] += b
    this.h[2] += c
    this.h[3] += d
    this.h[4] += e
    this.h[5] += f
    this.h[6] += g
    this.h[7] += h
  }
}
