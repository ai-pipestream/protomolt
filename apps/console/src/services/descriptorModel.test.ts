import { describe, expect, it } from 'vitest'
import { descriptorToJsonSchema } from '@/lib/forms/descriptor'
import { buildDescriptorModel } from './descriptorModel'
import { personDescriptorSetBytes } from './descriptorFixture'

describe('buildDescriptorModel', () => {
  const model = buildDescriptorModel(personDescriptorSetBytes())
  const file = model.files[0]
  const person = file.types.find((t) => t.name === 'Person')!

  it('parses the file with its package', () => {
    expect(model.files).toHaveLength(1)
    expect(file.name).toContain('example/person')
    expect(file.package).toBe('example')
  })

  it('collects message type names for the composer', () => {
    expect(model.messageTypeNames).toContain('example.Person')
    expect(model.messageTypeNames).toContain('example.Address')
    // Synthetic map entries are not composable types.
    expect(model.messageTypeNames).not.toContain('example.Person.AttrsEntry')
  })

  it('renders field rows with numbers, labels and types', () => {
    const byName = Object.fromEntries(person.fields!.map((f) => [f.name, f]))
    expect(byName.name).toMatchObject({ number: 1, type: 'string', label: '' })
    expect(byName.age).toMatchObject({ number: 2, type: 'int32' })
    expect(byName.tags).toMatchObject({ number: 3, type: 'string', label: 'repeated' })
    expect(byName.home).toMatchObject({ type: 'Address', refTypeName: 'example.Address' })
    expect(byName.attrs).toMatchObject({ type: 'map<string, string>' })
    expect(byName.kind).toMatchObject({ type: 'Kind', refTypeName: 'example.Kind' })
    expect(byName.email).toMatchObject({ oneof: 'contact' })
    expect(byName.phone).toMatchObject({ oneof: 'contact' })
  })

  it('hides synthetic map-entry messages from the tree', () => {
    expect(person.children ?? []).toHaveLength(0)
  })

  it('renders enums with values', () => {
    const kind = file.types.find((t) => t.name === 'Kind')!
    expect(kind.kind).toBe('enum')
    expect(kind.values).toEqual([
      { name: 'KIND_UNSPECIFIED', number: 0 },
      { name: 'KIND_ADMIN', number: 1 },
    ])
  })

  it('renders services with method kinds and types', () => {
    const service = file.types.find((t) => t.name === 'PersonService')!
    expect(service.kind).toBe('service')
    expect(service.methods).toEqual([
      { name: 'GetPerson', kind: 'unary', inputType: 'example.Person', outputType: 'example.Person' },
      { name: 'Watch', kind: 'server streaming', inputType: 'example.Person', outputType: 'example.Person' },
    ])
  })
})

describe('descriptorToJsonSchema (protobuf-forms)', () => {
  const model = buildDescriptorModel(personDescriptorSetBytes())
  const person = model.registry.getMessage('example.Person')!
  const schema = descriptorToJsonSchema(person)

  it('produces an object schema keyed by localName', () => {
    expect(schema.type).toBe('object')
    expect(Object.keys(schema.properties!)).toEqual(
      expect.arrayContaining(['name', 'age', 'tags', 'home', 'attrs', 'kind', 'email']),
    )
  })

  it('maps scalars, lists, maps, enums and nested messages', () => {
    const p = schema.properties!
    expect(p.name.type).toBe('string')
    expect(p.age.type).toBe('integer')
    expect(p.tags).toMatchObject({ type: 'array', items: { type: 'string' } })
    expect(p.attrs.type).toBe('object')
    expect(p.attrs.properties).toBeUndefined() // free key/value map editor
    expect(p.kind).toMatchObject({ type: 'string', enum: ['KIND_UNSPECIFIED', 'KIND_ADMIN'] })
    expect(p.home).toMatchObject({ type: 'object' })
    expect(p.home.properties!.city.type).toBe('string')
  })

  it('annotates field numbers and oneof membership in descriptions', () => {
    const p = schema.properties!
    expect(p.age.description).toContain('field 2')
    expect(p.email.description).toContain('oneof contact')
  })
})
