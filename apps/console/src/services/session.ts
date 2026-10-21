/**
 * The console's sign-in session, as the repository host serves it under
 * /console/session. The session rides an HttpOnly cookie on same-origin
 * requests; page script never holds a credential. Login and the provider's
 * callback are full page navigations through the host, so the only calls made
 * from here are the status read and the logout, both binary protobuf.
 */
import { fromBinary } from '@bufbuild/protobuf'
import {
  ConsoleSessionStatusSchema,
  SignInFailure,
  SignInFailureSchema,
  type ConsoleSessionStatus,
} from '@/gen/ai/protomolt/proto/authz/v1/console_session_pb'

/** The host's session routes, relative to the page origin. */
export const SESSION_PATH = '/console/session'

/** The console path every sign-in return must stay under. */
export const RETURN_PREFIX = '/console/'

/** The console route a failed sign-in lands on; the host names the reason in `error`. */
export const SIGN_IN_ROUTE = '/sign-in'

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

/** A status or logout call the host did not answer as a session answer. */
export class SessionApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message)
    this.name = 'SessionApiError'
  }
}

/**
 * Whether the host will accept `path` as where a login returns to: the same
 * rule the host applies, so a refusal is shown before the navigation.
 */
export function acceptableReturnPath(path: string): boolean {
  if (!path.startsWith(RETURN_PREFIX) || path.includes('#') || path.includes('\\')) return false
  if (path.includes('/../') || path.endsWith('/..')) return false
  for (const char of path) {
    const code = char.charCodeAt(0)
    if (code <= 0x20 || code > 0x7e) return false
  }
  return true
}

/**
 * The host route that starts a login and comes back to `returnTo`, a full
 * console path such as `/console/repository`. A path the host would refuse
 * falls back to the console's root, so the sign-in still completes.
 */
export function loginLocation(returnTo: string): string {
  const target = acceptableReturnPath(returnTo) ? returnTo : RETURN_PREFIX
  return `${SESSION_PATH}/login?return=${encodeURIComponent(target)}`
}

/**
 * The console path the sign-in page returns to from its `return` query value:
 * an acceptable path as given, otherwise the console root. The router sees
 * paths without the `/console` base, so that prefix is stripped.
 */
export function returnRoute(returnTo: string | null | undefined): string {
  if (!returnTo || !acceptableReturnPath(returnTo)) return '/'
  return returnTo.slice(RETURN_PREFIX.length - 1)
}

/**
 * What a failed sign-in's reason means to a person. The host names the reason
 * with the protobuf enum value's name, such as SIGN_IN_FAILURE_LOGIN_EXPIRED.
 */
export function signInFailureMessage(reason: string | null | undefined): string | null {
  if (!reason) return null
  const value = SignInFailureSchema.values.find((candidate) => candidate.name === reason)
  switch (value?.number) {
    case SignInFailure.STATE_MISMATCH:
      return 'The sign-in did not come back to the browser that started it. Start it again.'
    case SignInFailure.LOGIN_EXPIRED:
      return 'The sign-in took too long. Start it again.'
    case SignInFailure.PROVIDER_DENIED:
      return 'The identity provider did not sign you in.'
    case SignInFailure.TOKEN_EXCHANGE_FAILED:
      return 'The identity provider refused the sign-in code. Start it again.'
    case SignInFailure.ID_TOKEN_INVALID:
      return 'The identity provider answered for another issuer, client or sign-in.'
    case SignInFailure.UNKNOWN_PRINCIPAL:
      return 'You are signed in at the identity provider, but your account is not set up for this console.'
    default:
      return `Sign-in failed (${reason}).`
  }
}

/** The status and logout calls. */
export class SessionApi {
  constructor(private readonly fetchImpl: FetchLike = (input, init) => globalThis.fetch(input, init)) {}

  /**
   * The session's status: authenticated with the principal, its bound accounts
   * and the expiry, or unauthenticated. Both are answers; anything else (the
   * host has no sign-in configured, or is down) is an error naming the status.
   */
  async status(): Promise<ConsoleSessionStatus> {
    const response = await this.fetchImpl(SESSION_PATH, {
      method: 'GET',
      credentials: 'same-origin',
      headers: { Accept: 'application/proto' },
    })
    if (response.status !== 200 && response.status !== 401) {
      throw new SessionApiError(response.status, await describe(response))
    }
    return fromBinary(ConsoleSessionStatusSchema, new Uint8Array(await response.arrayBuffer()))
  }

  /** Ends the session on the host and clears its cookie. */
  async logout(): Promise<void> {
    const response = await this.fetchImpl(`${SESSION_PATH}/logout`, {
      method: 'POST',
      credentials: 'same-origin',
    })
    if (response.status !== 204) {
      throw new SessionApiError(response.status, await describe(response))
    }
  }
}

async function describe(response: Response): Promise<string> {
  const text = (await response.text()).trim()
  return text ? `${SESSION_PATH} answered ${response.status}: ${text}` : `${SESSION_PATH} answered ${response.status}`
}
