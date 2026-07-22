import { describe, expect, it } from 'vitest'
import { fromJson, toBinary } from '@bufbuild/protobuf'
import { FileDescriptorSetSchema } from '@bufbuild/protobuf/wkt'
import { registryFromDescriptorSet } from '@/services/descriptorModel'
import { personDescriptorSetBytes } from '@/services/descriptorFixture'
import { buildMessageTree, type ViewNode } from './messageTree'

function node(nodes: ViewNode[], label: string): ViewNode {
  const found = nodes.find((n) => n.label === label)
  if (!found) throw new Error(`no node labeled ${label} in ${nodes.map((n) => n.label)}`)
  return found
}

describe('buildMessageTree', () => {
  const registry = registryFromDescriptorSet(personDescriptorSetBytes())
  const person = registry.getMessage('example.Person')!

  it('renders scalars, enums and oneofs with their declared treatments', () => {
    const tree = buildMessageTree(person, {
      name: 'Ada',
      age: 36,
      kind: 'KIND_ADMIN',
      email: 'ada@example.com',
    })
    expect(node(tree, 'name')).toMatchObject({ kind: 'text', display: 'Ada', typeLabel: 'string' })
    expect(node(tree, 'age')).toMatchObject({ kind: 'number', display: '36' })
    // Enum values render by NAME with the number as detail.
    expect(node(tree, 'kind')).toMatchObject({
      kind: 'enum',
      display: 'KIND_ADMIN',
      detail: '= 1',
      typeLabel: 'example.Kind',
    })
    // Oneof members carry their group.
    expect(node(tree, 'email').oneof).toBe('contact')
  })

  it('groups repeated and map fields with counts and typed children', () => {
    const tree = buildMessageTree(person, {
      tags: ['a', 'b'],
      attrs: { color: 'red' },
      home: { city: 'Oslo' },
    })
    const tags = node(tree, 'tags')
    expect(tags.typeLabel).toBe('repeated string (2)')
    expect(tags.children![0]).toMatchObject({ label: '[0]', display: 'a' })

    const attrs = node(tree, 'attrs')
    expect(attrs.typeLabel).toBe('map<string, string> (1)')
    expect(attrs.children![0]).toMatchObject({ label: 'color', display: 'red' })

    const home = node(tree, 'home')
    expect(home.kind).toBe('message')
    expect(node(home.children!, 'city').display).toBe('Oslo')
  })

  it('omits unset fields and falls back to json for unknown keys', () => {
    const tree = buildMessageTree(person, { name: 'x', mystery: { a: 1 } })
    expect(tree.map((n) => n.label)).toEqual(['name', 'mystery'])
    expect(node(tree, 'mystery').kind).toBe('json')
  })

  it('treats int64 and bytes per the display contract', () => {
    const set = fromJson(FileDescriptorSetSchema, {
      file: [
        {
          name: 'wide/wide.proto',
          package: 'wide',
          syntax: 'proto3',
          messageType: [
            {
              name: 'Wide',
              field: [
                { name: 'big', number: 1, type: 'TYPE_INT64', label: 'LABEL_OPTIONAL', jsonName: 'big' },
                { name: 'blob', number: 2, type: 'TYPE_BYTES', label: 'LABEL_OPTIONAL', jsonName: 'blob' },
                { name: 'ratio', number: 3, type: 'TYPE_DOUBLE', label: 'LABEL_OPTIONAL', jsonName: 'ratio' },
              ],
            },
          ],
        },
      ],
    })
    const wide = registryFromDescriptorSet(toBinary(FileDescriptorSetSchema, set))
      .getMessage('wide.Wide')!
    const tree = buildMessageTree(wide, {
      big: '1700000000123',
      blob: btoa('hi'),
      ratio: 0.30000000000000004,
    })
    // int64 arrives as a JSON string; shown as the number with grouping as detail.
    expect(node(tree, 'big')).toMatchObject({
      kind: 'int64',
      display: '1700000000123',
      detail: '1,700,000,000,123',
    })
    // bytes show size + hex preview, never a base64 wall; base64 stays as detail.
    const blob = node(tree, 'blob')
    expect(blob.kind).toBe('bytes')
    expect(blob.display).toBe('2 B  68 69')
    expect(blob.detail).toBe(btoa('hi'))
    // doubles compact for display, full precision in detail.
    const ratio = node(tree, 'ratio')
    expect(ratio.display).toBe('0.3')
    expect(ratio.detail).toBe('0.30000000000000004')
  })
})
