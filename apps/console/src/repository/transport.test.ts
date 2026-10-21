import { create } from '@bufbuild/protobuf'
import { Code, ConnectError, createClient, createRouterTransport } from '@connectrpc/connect'
import { describe, expect, it } from 'vitest'
import { DriveService, ListDrivesResponseSchema } from '@/gen/ai/protomolt/proto/repo/v1/drive_pb'
import { DocumentService, ReadBlobResponseSchema } from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { signInLocation, unauthenticatedInterceptor } from './transport'
import { rpcFailure, failureText, isOwnCancellation } from './rpcErrorCore'

function drives(fail: Code | null, onUnauthenticated: () => void) {
  const transport = createRouterTransport(
    (router) =>
      router.service(DriveService, {
        listDrives() {
          if (fail !== null) throw new ConnectError('refused here', fail)
          return create(ListDrivesResponseSchema)
        },
      }),
    { transport: { interceptors: [unauthenticatedInterceptor(onUnauthenticated)] } },
  )
  return createClient(DriveService, transport)
}

describe('unauthenticatedInterceptor', () => {
  it('starts sign-in on UNAUTHENTICATED and still fails the call', async () => {
    let redirects = 0
    const error = await drives(Code.Unauthenticated, () => redirects++).listDrives({ accountId: 'a' }).catch((e) => e)
    expect(redirects).toBe(1)
    expect(ConnectError.from(error).code).toBe(Code.Unauthenticated)
  })

  it.each([Code.Unavailable, Code.PermissionDenied])('does not start sign-in for status %s', async (code) => {
    let redirects = 0
    await drives(code, () => redirects++).listDrives({ accountId: 'a' }).catch(() => {})
    expect(redirects).toBe(0)
  })

  it('starts sign-in when a server stream ends with UNAUTHENTICATED after its first frames', async () => {
    let redirects = 0
    const transport = createRouterTransport(
      (router) =>
        router.service(DocumentService, {
          async *readBlob() {
            yield create(ReadBlobResponseSchema, { frame: { case: 'data', value: new Uint8Array([1]) } })
            throw new ConnectError('session expired', Code.Unauthenticated)
          },
        }),
      { transport: { interceptors: [unauthenticatedInterceptor(() => redirects++)] } },
    )
    const frames: unknown[] = []
    const error = await (async () => {
      for await (const frame of createClient(DocumentService, transport).readBlob({ blobId: 'b' })) frames.push(frame)
    })().catch((e) => e)
    expect(frames).toHaveLength(1)
    expect(ConnectError.from(error).code).toBe(Code.Unauthenticated)
    expect(redirects).toBe(1)
  })

  it('leaves successful calls alone', async () => {
    let redirects = 0
    await drives(null, () => redirects++).listDrives({ accountId: 'a' })
    expect(redirects).toBe(0)
  })
})

describe('signInLocation', () => {
  it('names the return path in the return parameter', () => {
    expect(signInLocation('/console/repository/accounts/a b?x=1')).toBe(
      '/console/sign-in?return=%2Fconsole%2Frepository%2Faccounts%2Fa%20b%3Fx%3D1',
    )
  })
})

describe('rpcFailure', () => {
  it('keeps the canonical code name and the raw server message', () => {
    const failure = rpcFailure(new ConnectError('account acct-b is not bound', Code.PermissionDenied))
    expect(failure).toEqual({ code: 'PERMISSION_DENIED', codeNumber: 7, message: 'account acct-b is not bound' })
    expect(failureText(failure)).toBe('PERMISSION_DENIED: account acct-b is not bound')
  })

  it('shows an error the console raised as CONSOLE with its own message, never as a server status', () => {
    expect(rpcFailure(new TypeError('x is undefined'))).toEqual({ code: 'CONSOLE', codeNumber: null, message: 'x is undefined' })
  })

  it('recognises only the cancellation this console caused', () => {
    const controller = new AbortController()
    const canceled = new ConnectError('canceled', Code.Canceled)
    expect(isOwnCancellation(canceled, controller.signal)).toBe(false)
    controller.abort()
    expect(isOwnCancellation(canceled, controller.signal)).toBe(true)
    expect(isOwnCancellation(new ConnectError('x', Code.Internal), controller.signal)).toBe(false)
  })
})
