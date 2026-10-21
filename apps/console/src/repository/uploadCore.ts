/**
 * Uploads through the repository host's streaming route, POST
 * /v1/documents:upload. The console generates the operation ID and shows it
 * before a byte is sent, so a save whose answer never arrives can still be
 * looked up. The browser sets Content-Length from the file (the route answers
 * 411 without one), progress counts bytes sent, and the route is asked for a
 * binary UploadDocumentResponse (Accept: application/proto) instead of its
 * JSON receipt; a refusal carries grpc-status and grpc-message headers. A
 * capacity refusal (HTTP 429, RESOURCE_EXHAUSTED) is not a failed save: it is
 * retried with the same operation ID.
 */
import { fromBinary } from '@bufbuild/protobuf'
import { Code, type Client } from '@connectrpc/connect'
import {
  UploadDocumentResponseSchema,
  type DocumentService,
  type UploadDocumentResponse,
} from '@/gen/ai/protomolt/proto/repo/v2/document_service_pb'
import { lookupOutcome, type OutcomeAnswer } from './lookupCore'
import { codeName, type RpcFailure } from './rpcErrorCore'
import { Sha256 } from './sha256'

/** The upload route's path, same origin as the console. */
export const UPLOAD_PATH = '/v1/documents:upload'

const PROTO = 'application/proto'

/** The upload form as the screen edits it. */
export interface UploadForm {
  accountId: string
  datasourceId: string
  drive: string
  /** Blank lets the route derive the document ID from the file's SHA-256. */
  docId: string
  /** Blank sends none. */
  connectorId: string
  /** Blank creates a new document; a revision number updates exactly that revision. */
  expectedRevision: string
  /** Blank sends the file's own type, and the route records application/octet-stream when that is empty too. */
  contentType: string
}

/** A form value that cannot be sent; `field` is the label the form shows. */
export class UploadInputError extends Error {
  constructor(
    readonly field: string,
    message: string,
  ) {
    super(`${field}: ${message}`)
    this.name = 'UploadInputError'
  }
}

/** What one send of the file carries; a retry sends the same plan again. */
export interface UploadPlan {
  operationId: string
  accountId: string
  url: string
  file: Blob
  sizeBytes: number
  sha256: string
}

export function emptyUploadForm(accountId = ''): UploadForm {
  return { accountId, datasourceId: '', drive: '', docId: '', connectorId: '', expectedRevision: '', contentType: '' }
}

function required(label: string, raw: string): string {
  if (raw.trim() === '') throw new UploadInputError(label, 'is blank')
  return raw
}

function optional(label: string, raw: string): string | null {
  if (raw === '') return null
  if (raw.trim() === '') throw new UploadInputError(label, 'is only whitespace')
  return raw
}

/** A fresh operation ID: unique per account, chosen before the first byte is sent. */
export function newOperationId(): string {
  return crypto.randomUUID()
}

/** How much of the file is read into memory at a time while hashing. */
export const HASH_SLICE_BYTES = 4 * 1024 * 1024

/** The SHA-256 of a file, read one slice at a time so the file is never held whole in memory. */
export async function fileSha256(file: Blob, signal: AbortSignal, onProgress?: (hashed: number) => void): Promise<string> {
  const hash = new Sha256()
  for (let at = 0; at < file.size; at += HASH_SLICE_BYTES) {
    if (signal.aborted) throw signal.reason
    const end = Math.min(file.size, at + HASH_SLICE_BYTES)
    hash.update(new Uint8Array(await file.slice(at, end).arrayBuffer()))
    onProgress?.(end)
  }
  return hash.hex()
}

/**
 * The request for one file. The declared SHA-256 lets the route verify the
 * bytes it streamed and, with no document ID, derive the ID from the content.
 */
export function planUpload(form: UploadForm, file: File, operationId: string, sha256: string): UploadPlan {
  const accountId = required('Account ID', form.accountId)
  const datasourceId = required('Datasource ID', form.datasourceId)
  const drive = required('Drive', form.drive)
  if (file.name === '') throw new UploadInputError('File', 'has no name')
  const params = new URLSearchParams({
    account_id: accountId,
    datasource_id: datasourceId,
    drive,
    filename: file.name,
    operation_id: operationId,
    sha256,
  })
  const docId = optional('Document ID', form.docId)
  if (docId !== null) params.set('doc_id', docId)
  const connectorId = optional('Connector ID', form.connectorId)
  if (connectorId !== null) params.set('connector_id', connectorId)
  const contentType = optional('Content type', form.contentType) ?? file.type
  if (contentType !== '') params.set('content_type', contentType)
  const expected = form.expectedRevision.trim()
  if (expected !== '') {
    let revision: bigint
    try {
      revision = BigInt(expected)
    } catch {
      throw new UploadInputError('Expected revision', `"${expected}" is not a revision number`)
    }
    if (revision <= 0n) throw new UploadInputError('Expected revision', 'must be greater than zero, or blank to create')
    params.set('expected_revision', revision.toString())
  }
  return { operationId, accountId, url: `${UPLOAD_PATH}?${params.toString()}`, file, sizeBytes: file.size, sha256 }
}

/** How one send ended. */
export type UploadSendResult =
  | { kind: 'saved'; response: UploadDocumentResponse }
  /** HTTP 429 or RESOURCE_EXHAUSTED: nothing was saved; send again with the same operation ID. */
  | { kind: 'capacity'; httpStatus: number; failure: RpcFailure }
  /** HTTP 401: no session, or an expired one; the screen starts sign-in. */
  | { kind: 'unauthenticated'; failure: RpcFailure }
  /** The route answered with a refusal. */
  | { kind: 'refused'; httpStatus: number; failure: RpcFailure }
  /** No answer arrived (network loss or cancel): the save may or may not have committed. */
  | { kind: 'no-answer'; reason: string }

/** The part of XMLHttpRequest a send uses; tests supply a fake. */
export type UploadRequest = Pick<
  XMLHttpRequest,
  | 'open'
  | 'setRequestHeader'
  | 'getResponseHeader'
  | 'send'
  | 'abort'
  | 'responseType'
  | 'withCredentials'
  | 'status'
  | 'response'
  | 'onload'
  | 'onerror'
  | 'onabort'
> & { readonly upload: Pick<XMLHttpRequestUpload, 'onprogress'> }

// The route's refusal in protobuf mode: the HTTP status plus grpc-status and
// grpc-message headers, with no body to parse. A route that answers in any
// other content type is reported as such, never parsed.
function refusal(request: UploadRequest): RpcFailure {
  const header = request.getResponseHeader('grpc-status')
  const message = request.getResponseHeader('grpc-message')
  if (header !== null) {
    const number = Number(header)
    if (Number.isInteger(number) && number >= 1 && number <= 16) {
      return { code: codeName(number as Code), codeNumber: number, message: message === null ? '' : decodeURIComponent(message) }
    }
  }
  const type = request.getResponseHeader('content-type') ?? '(none)'
  return {
    code: `HTTP ${request.status}`,
    codeNumber: null,
    message: `The route answered without a grpc-status header (content type ${type}); the console reads only protobuf answers.`,
  }
}

/** Sends the plan once. The signal aborts the send; what was committed is then unknown. */
export function sendUpload(
  plan: UploadPlan,
  newRequest: () => UploadRequest,
  onProgress: (sent: number, total: number) => void,
  signal: AbortSignal,
): Promise<UploadSendResult> {
  return new Promise((resolve, reject) => {
    const request = newRequest()
    request.open('POST', plan.url)
    request.withCredentials = true
    request.responseType = 'arraybuffer'
    request.setRequestHeader('Accept', PROTO)
    request.setRequestHeader('Content-Type', 'application/octet-stream')
    request.upload.onprogress = (event) => onProgress(event.loaded, plan.sizeBytes)
    const onAbort = () => request.abort()
    signal.addEventListener('abort', onAbort, { once: true })
    const done = (result: UploadSendResult) => {
      signal.removeEventListener('abort', onAbort)
      resolve(result)
    }
    request.onload = () => {
      if (request.status === 200) {
        const type = request.getResponseHeader('content-type') ?? ''
        if (!type.startsWith(PROTO)) {
          signal.removeEventListener('abort', onAbort)
          reject(new Error(`The upload route answered 200 as ${type || '(no content type)'}; the console reads only ${PROTO}.`))
          return
        }
        try {
          done({ kind: 'saved', response: fromBinary(UploadDocumentResponseSchema, new Uint8Array(request.response as ArrayBuffer)) })
        } catch (error) {
          signal.removeEventListener('abort', onAbort)
          reject(new Error(`The upload route's answer does not decode as UploadDocumentResponse: ${(error as Error).message}`))
        }
        return
      }
      const failure = refusal(request)
      if (request.status === 401 || failure.codeNumber === Code.Unauthenticated) done({ kind: 'unauthenticated', failure })
      else if (request.status === 429 || failure.codeNumber === Code.ResourceExhausted)
        done({ kind: 'capacity', httpStatus: request.status, failure })
      else done({ kind: 'refused', httpStatus: request.status, failure })
    }
    request.onerror = () => done({ kind: 'no-answer', reason: 'The connection failed before the route answered.' })
    request.onabort = () => done({ kind: 'no-answer', reason: 'The upload was cancelled before the route answered.' })
    request.send(plan.file)
  })
}

/** How often and how long the screen asks for the outcome after a send. */
export interface OutcomePolling {
  attempts: number
  intervalMs: number
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>
}

export const DEFAULT_POLLING: OutcomePolling = { attempts: 10, intervalMs: 1000 }

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason)
      return
    }
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    const onAbort = () => {
      clearTimeout(timer)
      reject(signal.reason)
    }
    signal.addEventListener('abort', onAbort, { once: true })
  })
}

/**
 * Asks GetOperationOutcome until it answers committed or rejected, or the
 * attempts run out. Each answer is reported as it arrives; the last one is
 * returned, so "no outcome yet" and "lookup unavailable" stay what they are.
 */
export async function followOutcome(
  client: Client<typeof DocumentService>,
  accountId: string,
  operationId: string,
  signal: AbortSignal,
  onAnswer: (answer: OutcomeAnswer, attempt: number) => void,
  polling: OutcomePolling = DEFAULT_POLLING,
): Promise<OutcomeAnswer> {
  const sleep = polling.sleep ?? abortableSleep
  let answer: OutcomeAnswer | null = null
  for (let attempt = 1; attempt <= polling.attempts; attempt++) {
    answer = await lookupOutcome(client, accountId, operationId, signal)
    onAnswer(answer, attempt)
    if (answer.state === 'committed' || answer.state === 'rejected') return answer
    if (attempt < polling.attempts) await sleep(polling.intervalMs, signal)
  }
  if (answer === null) throw new Error('Outcome polling needs at least one attempt.')
  return answer
}
