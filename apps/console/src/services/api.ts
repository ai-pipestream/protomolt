/**
 * Typed client for the ProtoMolt schema registry (Confluent subjects
 * protocol), reached same-origin through the BFF proxy at /api/protomolt.
 *
 * Every protocol quirk lives HERE, nowhere else:
 *  - subjects may contain slashes → every path segment is encodeURIComponent'd
 *  - config key asymmetry: PUT bodies/echoes use `compatibility`,
 *    GET responses use `compatibilityLevel`
 *  - errors are `{error_code, message}` envelopes → thrown as RegistryError
 *  - 409 registration failures carry violation text parsed by parseViolations()
 *  - `references` is OMITTED from envelopes when empty (normalized to [])
 *  - the descriptor-set extra returns binary `application/x-protobuf`
 */

export interface SchemaReference {
  name: string
  subject: string
  version: number
}

export interface SchemaVersion {
  subject: string
  id: number
  version: number
  schemaType: string
  schema: string
  references: SchemaReference[]
}

export interface SchemaById {
  schema: string
  schemaType: string
  references: SchemaReference[]
}

export const COMPATIBILITY_MODES = [
  'BACKWARD',
  'BACKWARD_TRANSITIVE',
  'FORWARD',
  'FORWARD_TRANSITIVE',
  'FULL',
  'FULL_TRANSITIVE',
  'NONE',
] as const

export type CompatibilityMode = (typeof COMPATIBILITY_MODES)[number]

/** A registry {error_code, message} envelope, thrown. */
export class RegistryError extends Error {
  constructor(
    public readonly status: number,
    public readonly errorCode: number,
    message: string,
  ) {
    super(message)
    this.name = 'RegistryError'
  }

  /** 40401 — the subject has no versions at all. */
  get isUnknownSubject(): boolean {
    return this.errorCode === 40401
  }

  /** 40403 — content lookup found no identical schema (or unknown global id). */
  get isSchemaNotFound(): boolean {
    return this.errorCode === 40403
  }

  /** 40408 — subject has no subject-level compatibility override. */
  get isConfigNotSet(): boolean {
    return this.errorCode === 40408
  }

  /** 409 — registration rejected by the compatibility write gate. */
  get isIncompatible(): boolean {
    return this.status === 409
  }
}

/** Human-readable message for any error the console surfaces. */
export function errorMessage(e: unknown): string {
  if (e instanceof RegistryError) return e.message
  return (e as { message?: string })?.message ?? String(e)
}

// ------------------------------------------------------------------ violations

export interface CompatViolation {
  /** Prior version the candidate clashed with ("v1"), when reported. */
  version?: string
  /** Stable SCREAMING_SNAKE rule id (e.g. FIELD_TYPE_CHANGED), '' if unparsed. */
  rule: string
  /** Fully-qualified path the rule fired at (e.g. example.Person.age). */
  path: string
  /** Human explanation of the change. */
  detail: string
  /** The raw violation line, always present. */
  raw: string
}

const INCOMPATIBLE_PREFIX = 'Schema being registered is incompatible with an earlier schema:'

// "v1: FIELD_TYPE_CHANGED at example.Person.age: field 1 changed type …"
const VIOLATION_LINE = /^(?:(v\d+):\s*)?([A-Z][A-Z0-9_]*) at (\S+): (.*)$/

/**
 * Parse the 409 message the server builds from the write gate's violation
 * lines (joined with "; " after the standard prefix). Lines that don't match
 * the "vN: RULE at path: detail" shape (e.g. "v2: comparison failed: …",
 * "candidate does not resolve: …") come back with the text in `detail`.
 */
export function parseViolations(message: string): CompatViolation[] {
  const body = message.startsWith(INCOMPATIBLE_PREFIX)
    ? message.slice(INCOMPATIBLE_PREFIX.length)
    : message
  return body
    .split('; ')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((raw) => {
      const m = VIOLATION_LINE.exec(raw)
      if (m) return { version: m[1], rule: m[2], path: m[3], detail: m[4], raw }
      const fallback = /^(v\d+):\s*(.*)$/.exec(raw)
      if (fallback) return { version: fallback[1], rule: '', path: '', detail: fallback[2], raw }
      return { rule: '', path: '', detail: raw, raw }
    })
}

// ------------------------------------------------------------------ client

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

const JSON_HEADERS = { 'Content-Type': 'application/vnd.schemaregistry.v1+json' }

export class RegistryApi {
  constructor(
    private readonly base: string = '/api/protomolt',
    private readonly fetchFn: FetchLike = (input, init) => fetch(input, init),
  ) {}

  // -------- health

  /** GET /health → true when the registry answers {"status":"UP"}. */
  async health(): Promise<boolean> {
    try {
      const res = await this.fetchFn(`${this.base}/health`)
      if (!res.ok) return false
      const body = (await res.json()) as { status?: string }
      return body?.status === 'UP'
    } catch {
      return false
    }
  }

  // -------- subjects

  /** GET /subjects */
  listSubjects(): Promise<string[]> {
    return this.json('GET', '/subjects')
  }

  /** GET /subjects/{s}/versions — ascending version numbers. */
  listVersions(subject: string): Promise<number[]> {
    return this.json('GET', `/subjects/${enc(subject)}/versions`)
  }

  /** GET /subjects/{s}/versions/{v|latest} */
  async getVersion(subject: string, version: number | 'latest'): Promise<SchemaVersion> {
    const envelope = await this.json<SchemaVersion>(
      'GET',
      `/subjects/${enc(subject)}/versions/${version}`,
    )
    return normalizeEnvelope(envelope)
  }

  /**
   * POST /subjects/{s}/versions — register. 409 (RegistryError.isIncompatible,
   * parse with parseViolations), 422 on invalid schema/references.
   */
  register(
    subject: string,
    schema: string,
    references: SchemaReference[] = [],
  ): Promise<{ id: number }> {
    return this.json('POST', `/subjects/${enc(subject)}/versions`, {
      schema,
      schemaType: 'PROTOBUF',
      references,
    })
  }

  /**
   * POST /subjects/{s} — content-identity lookup. Resolves with the matching
   * version envelope; a 40403 RegistryError means "no identical schema
   * registered" (i.e. registering WOULD create a new version).
   */
  async lookup(
    subject: string,
    schema: string,
    references: SchemaReference[] = [],
  ): Promise<SchemaVersion> {
    const envelope = await this.json<SchemaVersion>('POST', `/subjects/${enc(subject)}`, {
      schema,
      schemaType: 'PROTOBUF',
      references,
    })
    return normalizeEnvelope(envelope)
  }

  /** GET /schemas/ids/{id} */
  async getById(id: number): Promise<SchemaById> {
    const body = await this.json<SchemaById>('GET', `/schemas/ids/${id}`)
    return { ...body, references: body.references ?? [] }
  }

  // -------- config (the compatibility/compatibilityLevel key quirk)

  /** GET /config — response key is `compatibilityLevel`. */
  async globalConfig(): Promise<CompatibilityMode> {
    const body = await this.json<{ compatibilityLevel: CompatibilityMode }>('GET', '/config')
    return body.compatibilityLevel
  }

  /** PUT /config — body and echo use `compatibility`. */
  async setGlobalConfig(mode: CompatibilityMode): Promise<CompatibilityMode> {
    const body = await this.json<{ compatibility: CompatibilityMode }>('PUT', '/config', {
      compatibility: mode,
    })
    return body.compatibility
  }

  /**
   * GET /config/{s} — subject-level override, or null when the subject
   * inherits the global mode (the server 404s with 40408 in that case).
   */
  async subjectConfig(subject: string): Promise<CompatibilityMode | null> {
    try {
      const body = await this.json<{ compatibilityLevel: CompatibilityMode }>(
        'GET',
        `/config/${enc(subject)}`,
      )
      return body.compatibilityLevel
    } catch (e) {
      if (e instanceof RegistryError && e.isConfigNotSet) return null
      throw e
    }
  }

  /** PUT /config/{s} — body and echo use `compatibility`. */
  async setSubjectConfig(subject: string, mode: CompatibilityMode): Promise<CompatibilityMode> {
    const body = await this.json<{ compatibility: CompatibilityMode }>(
      'PUT',
      `/config/${enc(subject)}`,
      { compatibility: mode },
    )
    return body.compatibility
  }

  // -------- native extras

  /**
   * GET /protomolt/subjects/{s}/descriptor-set — the subject's latest schema
   * plus transitive references compiled to a binary FileDescriptorSet.
   */
  async descriptorSet(subject: string): Promise<Uint8Array> {
    const res = await this.fetchFn(`${this.base}/protomolt/subjects/${enc(subject)}/descriptor-set`)
    if (!res.ok) throw await toRegistryError(res)
    return new Uint8Array(await res.arrayBuffer())
  }

  // -------- plumbing

  private async json<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await this.fetchFn(`${this.base}${path}`, {
      method,
      ...(body === undefined ? {} : { headers: JSON_HEADERS, body: JSON.stringify(body) }),
    })
    if (!res.ok) throw await toRegistryError(res)
    return (await res.json()) as T
  }
}

function enc(segment: string): string {
  return encodeURIComponent(segment)
}

function normalizeEnvelope(envelope: SchemaVersion): SchemaVersion {
  // Confluent omits `references` entirely when empty.
  return { ...envelope, references: envelope.references ?? [] }
}

async function toRegistryError(res: Response): Promise<RegistryError> {
  let errorCode = res.status
  let message = `HTTP ${res.status}`
  try {
    const body = (await res.json()) as { error_code?: number; message?: string }
    if (typeof body?.error_code === 'number') errorCode = body.error_code
    if (typeof body?.message === 'string' && body.message) message = body.message
  } catch {
    // Non-JSON error body (proxy hiccup, HTML gateway page) — keep the HTTP status text.
  }
  return new RegistryError(res.status, errorCode, message)
}

/** Shared singleton for the console's views. */
export const registryApi = new RegistryApi()
