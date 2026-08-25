/**
 * The services workbench's client: the workspace verbs over the serve bridge
 * (ServiceRegister / ServiceList / ServiceInspect / ServiceInvoke / ServiceRefresh).
 * A registered profile's unary methods are also live catalog verbs, so this module
 * derives the verb name a method answers to — the same derivation the server uses.
 */

export interface ReflectedField {
  name: string
  /** Canonical proto3 JSON spelling; differs from name around underscores. */
  jsonName?: string
  number?: number
  /** Lower-case protobuf type: 'string', 'int64', 'message', 'enum', ... */
  type: string
  /** 'singular', 'repeated', or 'map'. */
  cardinality?: string
  /** Fully qualified message/enum type, '' for scalars. */
  typeName?: string
}

export interface ReflectedMethod {
  name: string
  fullName: string
  inputType: string
  outputType: string
  clientStreaming?: boolean
  serverStreaming?: boolean
  inputFields?: ReflectedField[]
  outputFields?: ReflectedField[]
}

export interface ReflectedService {
  name: string
  methods?: ReflectedMethod[]
}

export interface ServiceSummary {
  name: string
  description?: string
  endpoints?: string[]
  descriptorFingerprint?: string
}

export interface ServiceInspection {
  profile?: Record<string, unknown>
  services?: ReflectedService[]
}

export interface ServiceInvocation {
  serviceProfile?: string
  endpoint?: string
  method?: string
  methodType?: string
  ok: boolean
  status?: string
  responses?: Array<Record<string, unknown>>
  description?: string
}

const SERVE = '/api/serve/grpc-json/ProtoMoltService'

/** An error thrown by the serve bridge; gate findings ride along when present. */
export interface BridgeError extends Error {
  findings?: unknown[]
}

/**
 * Reads a bridge response: JSON body on success, the body's message as a thrown
 * error otherwise. The status check comes before parsing, so a proxy answering
 * an outage with HTML surfaces as its HTTP status rather than as a parse error.
 */
export async function unwrap<T>(response: Response): Promise<T> {
  const text = await response.text()
  let body: Record<string, unknown> | null = null
  try {
    body = JSON.parse(text) as Record<string, unknown>
  } catch {
    body = null
  }
  if (!response.ok) {
    const message = body?.message ?? body?.error ?? `HTTP ${response.status}`
    const error = new Error(String(message)) as BridgeError
    if (Array.isArray(body?.findings)) error.findings = body.findings
    throw error
  }
  if (body === null) {
    throw new Error(`the server answered ${response.status} with a non-JSON body`)
  }
  return body as T
}

/** Calls one ProtoMoltService verb over the serve bridge. */
export async function verb<T>(
  name: string,
  body: unknown,
  fetchFn: typeof fetch = fetch,
): Promise<T> {
  return unwrap(await fetchFn(`${SERVE}/${name}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  }))
}

/**
 * The registered profile exposing a contract, or null when none does. Profiles
 * are inspected concurrently (one slow endpoint must not stall the page), and
 * the first match in registration order wins; a profile whose endpoint refuses
 * inspection is not the profile today.
 */
export async function findProfileByContract(
  contract: string,
  fetchFn: typeof fetch = fetch,
): Promise<string | null> {
  const summaries = await listServices(fetchFn)
  const inspections = await Promise.all(summaries.map(async (summary) => {
    try {
      return await inspectService(summary.name, fetchFn)
    } catch {
      return null
    }
  }))
  for (let i = 0; i < summaries.length; i++) {
    if ((inspections[i]?.services ?? []).some((s) => s.name === contract)) {
      return summaries[i].name
    }
  }
  return null
}

/** Invokes a unary method through ServiceInvoke and answers its first reply. */
export async function invokeUnary<T>(
  profile: string,
  method: string,
  request: Record<string, unknown>,
  fetchFn: typeof fetch = fetch,
): Promise<T> {
  const result = await invokeMethod(profile, method, request, fetchFn)
  if (!result.ok) {
    throw new Error(result.description || result.status
        || `${method.slice(method.indexOf('/') + 1)} failed`)
  }
  return (result.responses?.[0] ?? {}) as T
}

export async function listServices(fetchFn: typeof fetch = fetch): Promise<ServiceSummary[]> {
  const result = await verb<{ services?: ServiceSummary[] }>('ServiceList', {}, fetchFn)
  return result.services ?? []
}

export async function inspectService(
  name: string,
  fetchFn: typeof fetch = fetch,
): Promise<ServiceInspection> {
  return verb('ServiceInspect', { name }, fetchFn)
}

/** Registers a profile with one plain or TLS endpoint; reflection fills the schema. */
export async function registerService(
  name: string,
  target: string,
  tls: boolean,
  description: string,
  fetchFn: typeof fetch = fetch,
): Promise<{ ok: boolean; error?: string; services?: ReflectedService[] }> {
  const [host, port] = splitTarget(target)
  return verb('ServiceRegister', {
    profile: {
      name,
      description,
      endpoints: [{
        name: 'default',
        host,
        port,
        // The endpoint declares its transport explicitly; unspecified is refused.
        transport: tls ? 'TRANSPORT_TLS' : 'TRANSPORT_PLAINTEXT',
      }],
    },
  }, fetchFn)
}

export async function refreshService(
  name: string,
  fetchFn: typeof fetch = fetch,
): Promise<Record<string, unknown>> {
  return verb('ServiceRefresh', { name }, fetchFn)
}

export async function invokeMethod(
  profile: string,
  method: string,
  request: Record<string, unknown>,
  fetchFn: typeof fetch = fetch,
): Promise<ServiceInvocation> {
  return verb('ServiceInvoke', { name: profile, method, request }, fetchFn)
}

/** "host:port" with a required port, refused early so the server never sees half a target. */
export function splitTarget(target: string): [string, number] {
  const at = target.lastIndexOf(':')
  const port = at > 0 ? Number(target.slice(at + 1)) : NaN
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`'${target}' is not host:port — the port picks out the listener to reflect`)
  }
  return [target.slice(0, at), port]
}

/** {@code ListOrders} becomes {@code list-orders} — the server's derivation, mirrored. */
export function kebab(name: string): string {
  let out = ''
  for (const character of name) {
    if (character === '_' || character === '.') {
      out += '-'
    } else if (character !== character.toLowerCase()) {
      if (out && !out.endsWith('-')) out += '-'
      out += character.toLowerCase()
    } else {
      out += character
    }
  }
  return out.toLowerCase()
}

/**
 * The catalog verb a registered method answers to: profile-qualified method name,
 * additionally service-qualified when two of the profile's services declare the
 * same method name. Client-streaming methods never register, so they neither get
 * a verb nor count toward a collision — the same rule the server applies.
 */
export function verbName(
  profile: string,
  method: ReflectedMethod,
  all: ReflectedService[],
): string {
  const simple = kebab(method.name)
  const collisions = all.filter((s) => !isReflectionService(s.name))
    .flatMap((s) => s.methods ?? [])
    .filter((m) => !m.clientStreaming && kebab(m.name) === simple).length
  if (collisions <= 1) return `${kebab(profile)}-${simple}`
  // fullName is "package.Service/Method"; the qualifier is the service's simple name.
  const service = method.fullName.slice(0, method.fullName.indexOf('/'))
  const serviceSimple = service.slice(service.lastIndexOf('.') + 1)
  return `${kebab(profile)}-${kebab(serviceSimple)}-${simple}`
}

/** The reflection protocol's own service, which never becomes verbs. */
export function isReflectionService(name: string): boolean {
  return name.startsWith('grpc.reflection.')
}

/** A starter request body for a method: one key per input field, zero-valued. */
export function requestSkeleton(method: ReflectedMethod): Record<string, unknown> {
  const skeleton: Record<string, unknown> = {}
  for (const field of method.inputFields ?? []) {
    skeleton[field.jsonName || field.name] = zeroValue(field)
  }
  return skeleton
}

function zeroValue(field: ReflectedField): unknown {
  // A map is repeated on the wire but an object in proto3 JSON.
  if (field.cardinality === 'map') return {}
  if (field.cardinality === 'repeated') return []
  switch (field.type) {
    case 'string':
    case 'bytes':
      return ''
    case 'bool':
      return false
    case 'message':
    case 'group':
      return {}
    default:
      // Every numeric protobuf type — and enums, whose number 0 proto3 JSON
      // accepts where an empty name would be refused.
      return 0
  }
}
