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
  /** 'singular' or 'repeated' (maps report as repeated). */
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

async function verb<T>(name: string, body: unknown, fetchFn: typeof fetch): Promise<T> {
  const response = await fetchFn(`${SERVE}/${name}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  const json = (await response.json()) as Record<string, unknown>
  if (!response.ok) {
    throw new Error(String(json.message ?? json.error ?? `HTTP ${response.status}`))
  }
  return json as T
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
      endpoints: [{ name: 'default', host, port, tls }],
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
