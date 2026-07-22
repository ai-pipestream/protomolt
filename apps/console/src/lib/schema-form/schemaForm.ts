/**
 * Helpers for the homegrown JSON-Schema form renderer (SchemaForm) — the
 * replacement for JSONForms. Supports the subset our registry schemas use;
 * see SchemaField.vue for the dispatch rules.
 */

/** "chunkOverlap" / "max_batch_size" → "Chunk Overlap" / "Max Batch Size". */
export function humanize(key: string): string {
  return key
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .trim()
}

/** Defaults from the schema, recursing into nested objects. */
export function defaultsFor(schema: any): any {
  if (!schema || !schema.properties) return {}
  const out: any = {}
  for (const [key, prop] of Object.entries<any>(schema.properties)) {
    if (prop.default !== undefined) {
      out[key] = prop.default
    } else if (prop.type === 'object' && prop.properties) {
      const nested = defaultsFor(prop)
      if (Object.keys(nested).length > 0) out[key] = nested
    }
  }
  return out
}

export type FieldKind =
  | 'lookup'
  | 'enum'
  | 'boolean'
  | 'number'
  | 'string'
  | 'primitive-array'
  | 'object-array'
  | 'object'
  | 'free-map'
  | 'json'

/** Classify a property schema into the renderer it gets. */
export function fieldKind(prop: any): FieldKind {
  if (!prop || typeof prop !== 'object') return 'json'
  if (prop['x-pipestream-lookup']) return 'lookup'
  if (Array.isArray(prop.enum)) return 'enum'
  switch (prop.type) {
    case 'boolean':
      return 'boolean'
    case 'integer':
    case 'number':
      return 'number'
    case 'string':
      return 'string'
    case 'array': {
      const items = prop.items ?? {}
      if (items.type === 'object') return 'object-array'
      if (items.type && items.type !== 'array') return 'primitive-array'
      return 'json'
    }
    case 'object':
      if (prop.properties && Object.keys(prop.properties).length > 0) return 'object'
      // Schema-less object: a free string map (params, headers, ...).
      return 'free-map'
    default:
      return 'json'
  }
}

/** Order properties by an optional uischema's Control scopes, rest appended. */
export function propertyOrder(schema: any, uischema: any): string[] {
  const keys = Object.keys(schema?.properties ?? {})
  const scoped: string[] = []
  const walk = (el: any) => {
    if (!el || typeof el !== 'object') return
    if (typeof el.scope === 'string') {
      const m = el.scope.match(/#\/properties\/([^/]+)$/)
      if (m && keys.includes(m[1]!)) scoped.push(m[1]!)
    }
    for (const child of el.elements ?? []) walk(child)
  }
  walk(uischema)
  return [...scoped, ...keys.filter((k) => !scoped.includes(k))]
}
