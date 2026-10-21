/**
 * The repository screens' wire: Connect protocol with binary protobuf to the
 * Connect gateway on the repository host's HTTP port, same origin as the
 * console. There is no JSON format on this path.
 */
import { Code, ConnectError, type Interceptor, type Transport } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'

/**
 * Where the gateway serves Connect paths, relative to the page origin. The
 * browser calls `<base>/<package.Service>/<Method>`.
 */
export const REPOSITORY_CONNECT_BASE = '/connect'

/** The console route that starts a sign-in, given where to come back to. */
export function signInLocation(returnTo: string): string {
  return `/console/sign-in?return=${encodeURIComponent(returnTo)}`
}

/**
 * Calls `onUnauthenticated` when the gateway answers UNAUTHENTICATED (no
 * session, or an expired one), then rethrows, so the calling screen still
 * fails visibly instead of showing an empty result.
 */
export function unauthenticatedInterceptor(onUnauthenticated: () => void): Interceptor {
  return (next) => async (request) => {
    try {
      return await next(request)
    } catch (error) {
      if (ConnectError.from(error).code === Code.Unauthenticated) onUnauthenticated()
      throw error
    }
  }
}

/**
 * The browser transport. The session cookie rides on every same-origin call;
 * no credential is ever held by page script.
 */
export function repositoryTransport(onUnauthenticated: () => void): Transport {
  return createConnectTransport({
    baseUrl: REPOSITORY_CONNECT_BASE,
    useBinaryFormat: true,
    fetch: (input, init) => globalThis.fetch(input, { ...init, credentials: 'same-origin' }),
    interceptors: [unauthenticatedInterceptor(onUnauthenticated)],
  })
}
