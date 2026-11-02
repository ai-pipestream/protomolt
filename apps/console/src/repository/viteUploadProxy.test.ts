import { createServer as createHttpServer } from 'node:http'
import { fileURLToPath } from 'node:url'
import { createServer as createViteServer } from 'vite'
import { expect, it, vi } from 'vitest'

it.each(['/v1/documents:upload', '/v1/archive:upload'])(
  'forwards %s through the real development listener without changing the request or receipt',
  async (path) => {
    const requestBytes = Buffer.from([0, 255, 128, 13, 10, 42])
    const receipt = Buffer.from([10, 3, 111, 112, 49])
    const seen: { url?: string; cookie?: string; origin?: string; host?: string; bytes: Buffer }[] = []
    const upstream = createHttpServer(async (request, response) => {
      const chunks: Buffer[] = []
      for await (const chunk of request) chunks.push(Buffer.from(chunk))
      seen.push({
        url: request.url, cookie: request.headers.cookie, origin: request.headers.origin,
        host: request.headers.host, bytes: Buffer.concat(chunks),
      })
      response.writeHead(201, { 'Content-Type': 'application/protobuf', 'X-Operation-Id': 'op1' })
      response.end(receipt)
    })
    await new Promise<void>((resolve) => upstream.listen(0, '127.0.0.1', resolve))
    const address = upstream.address()
    if (!address || typeof address === 'string') throw new Error('No upstream TCP listener')
    vi.stubEnv('PROTOMOLT_REPOSITORY_URL', `http://127.0.0.1:${address.port}`)
    let vite: Awaited<ReturnType<typeof createViteServer>> | undefined
    try {
      vite = await createViteServer({
        configFile: fileURLToPath(new URL('../../vite.config.ts', import.meta.url)),
        root: fileURLToPath(new URL('../../', import.meta.url)),
        server: { host: '127.0.0.1', port: 0 },
        logLevel: 'silent',
      })
      await vite.listen()
      const listener = vite.httpServer?.address()
      if (!listener || typeof listener === 'string') throw new Error('No Vite TCP listener')
      const origin = `http://127.0.0.1:${listener.port}`
      const query = '?account_id=acct-a&archive=papers&entry_id=a%2Fb&rendition=original'
      const response = await fetch(origin + path + query, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/octet-stream',
          Cookie: 'protomolt_session=synthetic-session',
          Origin: origin,
        },
        body: requestBytes,
      })
      expect(response.status).toBe(201)
      expect(response.headers.get('content-type')).toBe('application/protobuf')
      expect(response.headers.get('x-operation-id')).toBe('op1')
      expect(Buffer.from(await response.arrayBuffer())).toEqual(receipt)
      expect(seen).toEqual([{
        url: path + query, cookie: 'protomolt_session=synthetic-session', origin,
        host: `127.0.0.1:${listener.port}`, bytes: requestBytes,
      }])
    } finally {
      await vite?.close()
      await new Promise<void>((resolve, reject) => upstream.close(error => error ? reject(error) : resolve()))
      vi.unstubAllEnvs()
    }
  },
)
