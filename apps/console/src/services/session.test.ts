import { create, toBinary } from '@bufbuild/protobuf'
import { timestampFromDate } from '@bufbuild/protobuf/wkt'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ConsoleSessionStatusSchema } from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'
import {
  SESSION_PATH,
  SessionApi,
  SessionApiError,
  acceptableReturnPath,
  loginLocation,
  returnRoute,
  signInFailureMessage,
} from './session'

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

function proto(status: number, body: Uint8Array): Response {
  return new Response(new Blob([body as BlobPart]), { status, headers: { 'Content-Type': 'application/proto' } })
}

describe('SessionApi', () => {
  let fetchMock: ReturnType<typeof vi.fn<FetchLike>>
  let api: SessionApi

  beforeEach(() => {
    fetchMock = vi.fn<FetchLike>()
    api = new SessionApi(fetchMock)
  })

  it('decodes a live session with its principal, accounts and expiry', async () => {
    const expiresAt = timestampFromDate(new Date('2026-10-21T18:00:00Z'))
    const status = create(ConsoleSessionStatusSchema, {
      authenticated: true,
      principal: 'alice',
      accountIds: ['acct-a', 'acct-b'],
      expiresAt,
    })
    fetchMock.mockResolvedValue(proto(200, toBinary(ConsoleSessionStatusSchema, status)))
    const decoded = await api.status()
    expect(decoded.authenticated).toBe(true)
    expect(decoded.principal).toBe('alice')
    expect(decoded.accountIds).toEqual(['acct-a', 'acct-b'])
    expect(decoded.expiresAt?.seconds).toBe(expiresAt.seconds)
    expect(fetchMock).toHaveBeenCalledWith(SESSION_PATH, {
      method: 'GET',
      credentials: 'same-origin',
      headers: { Accept: 'application/proto' },
    })
  })

  it('treats the unauthenticated answer as data, not an exception', async () => {
    const status = create(ConsoleSessionStatusSchema, { authenticated: false })
    fetchMock.mockResolvedValue(proto(401, toBinary(ConsoleSessionStatusSchema, status)))
    const decoded = await api.status()
    expect(decoded.authenticated).toBe(false)
    expect(decoded.principal).toBe('')
    expect(decoded.accountIds).toEqual([])
  })

  it('names any other status answer as an error with the host text', async () => {
    fetchMock.mockResolvedValue(
      new Response('console sign-in is not configured: set PROTOMOLT_CONSOLE_OIDC_ISSUER', { status: 503 }),
    )
    await expect(api.status()).rejects.toMatchObject({
      name: 'SessionApiError',
      status: 503,
      message: `${SESSION_PATH} answered 503: console sign-in is not configured: set PROTOMOLT_CONSOLE_OIDC_ISSUER`,
    })
    fetchMock.mockResolvedValue(new Response(null, { status: 502 }))
    await expect(api.status()).rejects.toBeInstanceOf(SessionApiError)
  })

  it('logs out with a same-origin POST and nothing in the body', async () => {
    fetchMock.mockResolvedValue(new Response(null, { status: 204 }))
    await api.logout()
    expect(fetchMock).toHaveBeenCalledWith(`${SESSION_PATH}/logout`, {
      method: 'POST',
      credentials: 'same-origin',
    })
    fetchMock.mockResolvedValue(new Response('gone', { status: 503 }))
    await expect(api.logout()).rejects.toMatchObject({ status: 503 })
  })
})

describe('return paths', () => {
  it('accepts console paths and refuses anything that leaves the console', () => {
    expect(acceptableReturnPath('/console/repository')).toBe(true)
    expect(acceptableReturnPath('/console/repository/accounts/a?x=1')).toBe(true)
    for (const path of ['/', '/console', '/v1/documents', '//evil.test/console/', '/console/x#f', '/console/../v1', '/console/a b', 'https://evil.test/console/']) {
      expect(acceptableReturnPath(path), path).toBe(false)
    }
  })

  it('builds the login location with the return path encoded, falling back to the console root', () => {
    expect(loginLocation('/console/repository?account=a')).toBe(
      `${SESSION_PATH}/login?return=${encodeURIComponent('/console/repository?account=a')}`,
    )
    expect(loginLocation('/v1/documents')).toBe(`${SESSION_PATH}/login?return=${encodeURIComponent('/console/')}`)
  })

  it('maps the return query onto a router path under the console base', () => {
    expect(returnRoute('/console/repository/accounts/a')).toBe('/repository/accounts/a')
    expect(returnRoute('/console/')).toBe('/')
    expect(returnRoute('/elsewhere')).toBe('/')
    expect(returnRoute(null)).toBe('/')
  })
})

describe('failure reasons', () => {
  it('explains each reason the host names and says nothing for none', () => {
    expect(signInFailureMessage(null)).toBeNull()
    expect(signInFailureMessage('')).toBeNull()
    expect(signInFailureMessage('SIGN_IN_FAILURE_STATE_MISMATCH')).toContain('browser that started it')
    expect(signInFailureMessage('SIGN_IN_FAILURE_LOGIN_EXPIRED')).toContain('too long')
    expect(signInFailureMessage('SIGN_IN_FAILURE_PROVIDER_DENIED')).toContain('did not sign you in')
    expect(signInFailureMessage('SIGN_IN_FAILURE_TOKEN_EXCHANGE_FAILED')).toContain('refused the sign-in code')
    expect(signInFailureMessage('SIGN_IN_FAILURE_ID_TOKEN_INVALID')).toContain('another issuer')
    expect(signInFailureMessage('SIGN_IN_FAILURE_UNKNOWN_PRINCIPAL')).toContain('not set up')
    expect(signInFailureMessage('SOMETHING_NEW')).toBe('Sign-in failed (SOMETHING_NEW).')
  })
})
