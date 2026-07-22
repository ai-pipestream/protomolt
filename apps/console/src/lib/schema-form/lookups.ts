/**
 * Lookup resolver registry for the x-pipestream-lookup schema convention.
 *
 * A module config schema property annotated with
 * `"x-pipestream-lookup": "<kind>"` declares that the field is a REFERENCE
 * (index plans, vector sets, embedding models, ...). The form renderer asks
 * this registry to resolve the kind to selectable options; the hosting app
 * registers one resolver per kind at startup (using whatever clients it has).
 * Schemas declare WHAT kind of reference a field is; the app owns HOW it is
 * resolved.
 */

export interface LookupOption {
  title: string
  value: string
}

export type LookupResolver = () => Promise<LookupOption[]>

const resolvers = new Map<string, LookupResolver>()

export function registerLookupResolver(kind: string, resolver: LookupResolver): void {
  resolvers.set(kind, resolver)
}

export function hasLookupResolver(kind: string): boolean {
  return resolvers.has(kind)
}

export async function resolveLookup(kind: string): Promise<LookupOption[]> {
  const resolver = resolvers.get(kind)
  if (!resolver) {
    return []
  }
  return resolver()
}
