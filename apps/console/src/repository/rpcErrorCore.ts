/**
 * How the repository screens report a failed call: the gRPC status code by its
 * canonical name and the server's own message, never a generic "something went
 * wrong" and never a guessed value in place of the missing answer.
 */
import { Code, ConnectError } from '@connectrpc/connect'

/** One failed call, as the screen shows it. */
export interface RpcFailure {
  /**
   * The canonical gRPC status name, for example `PERMISSION_DENIED`, or
   * `CONSOLE` when the console itself refused what it received (a stream that
   * broke its contract, a digest that does not match) or failed on its own.
   */
  code: string
  /** The status number, 1 to 16; null for a `CONSOLE` failure. */
  codeNumber: number | null
  /** The server's message (or the client error's message), without the code prefix. */
  message: string
}

const CODE_NAMES: Record<Code, string> = {
  [Code.Canceled]: 'CANCELLED',
  [Code.Unknown]: 'UNKNOWN',
  [Code.InvalidArgument]: 'INVALID_ARGUMENT',
  [Code.DeadlineExceeded]: 'DEADLINE_EXCEEDED',
  [Code.NotFound]: 'NOT_FOUND',
  [Code.AlreadyExists]: 'ALREADY_EXISTS',
  [Code.PermissionDenied]: 'PERMISSION_DENIED',
  [Code.ResourceExhausted]: 'RESOURCE_EXHAUSTED',
  [Code.FailedPrecondition]: 'FAILED_PRECONDITION',
  [Code.Aborted]: 'ABORTED',
  [Code.OutOfRange]: 'OUT_OF_RANGE',
  [Code.Unimplemented]: 'UNIMPLEMENTED',
  [Code.Internal]: 'INTERNAL',
  [Code.Unavailable]: 'UNAVAILABLE',
  [Code.DataLoss]: 'DATA_LOSS',
  [Code.Unauthenticated]: 'UNAUTHENTICATED',
}

/** The canonical gRPC name of a status code. */
export function codeName(code: Code): string {
  return CODE_NAMES[code]
}

/**
 * The failure a caught error describes. A ConnectError keeps its code and raw
 * message; any other thrown value is a `CONSOLE` failure with its own message,
 * so a refused answer or a client-side bug is shown rather than hidden, and is
 * never mistaken for a server status.
 */
export function rpcFailure(error: unknown): RpcFailure {
  if (!(error instanceof ConnectError)) {
    return { code: 'CONSOLE', codeNumber: null, message: error instanceof Error ? error.message : String(error) }
  }
  const connect = error
  return {
    code: codeName(connect.code),
    codeNumber: connect.code,
    message: connect.rawMessage,
  }
}

/** The one-line form of a failure: `NOT_FOUND: no document at ...`. */
export function failureText(failure: RpcFailure): string {
  return failure.message ? `${failure.code}: ${failure.message}` : failure.code
}

/**
 * True when the error is the cancellation this console caused itself, by
 * aborting a call's signal when its screen went away or a newer query replaced
 * it. Such a call has no result to show; every other failure is shown.
 */
export function isOwnCancellation(error: unknown, signal: AbortSignal): boolean {
  return signal.aborted && ConnectError.from(error).code === Code.Canceled
}
