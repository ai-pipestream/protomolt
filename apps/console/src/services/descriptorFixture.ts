/**
 * Test fixture: a small but representative FileDescriptorSet (message with
 * scalars, repeated, map, enum, oneof, nested message; a service with a
 * streaming method) built from descriptor JSON — no compiler needed.
 */
import { fromJson, toBinary } from '@bufbuild/protobuf'
import { FileDescriptorSetSchema } from '@bufbuild/protobuf/wkt'

export function personDescriptorSetBytes(): Uint8Array {
  const set = fromJson(FileDescriptorSetSchema, {
    file: [
      {
        name: 'example/person.proto',
        package: 'example',
        syntax: 'proto3',
        messageType: [
          {
            name: 'Person',
            field: [
              { name: 'name', number: 1, type: 'TYPE_STRING', label: 'LABEL_OPTIONAL', jsonName: 'name' },
              { name: 'age', number: 2, type: 'TYPE_INT32', label: 'LABEL_OPTIONAL', jsonName: 'age' },
              { name: 'tags', number: 3, type: 'TYPE_STRING', label: 'LABEL_REPEATED', jsonName: 'tags' },
              {
                name: 'home',
                number: 4,
                type: 'TYPE_MESSAGE',
                typeName: '.example.Address',
                label: 'LABEL_OPTIONAL',
                jsonName: 'home',
              },
              {
                name: 'attrs',
                number: 5,
                type: 'TYPE_MESSAGE',
                typeName: '.example.Person.AttrsEntry',
                label: 'LABEL_REPEATED',
                jsonName: 'attrs',
              },
              {
                name: 'kind',
                number: 6,
                type: 'TYPE_ENUM',
                typeName: '.example.Kind',
                label: 'LABEL_OPTIONAL',
                jsonName: 'kind',
              },
              {
                name: 'email',
                number: 7,
                type: 'TYPE_STRING',
                label: 'LABEL_OPTIONAL',
                oneofIndex: 0,
                jsonName: 'email',
              },
              {
                name: 'phone',
                number: 8,
                type: 'TYPE_STRING',
                label: 'LABEL_OPTIONAL',
                oneofIndex: 0,
                jsonName: 'phone',
              },
            ],
            oneofDecl: [{ name: 'contact' }],
            nestedType: [
              {
                name: 'AttrsEntry',
                field: [
                  { name: 'key', number: 1, type: 'TYPE_STRING', label: 'LABEL_OPTIONAL', jsonName: 'key' },
                  { name: 'value', number: 2, type: 'TYPE_STRING', label: 'LABEL_OPTIONAL', jsonName: 'value' },
                ],
                options: { mapEntry: true },
              },
            ],
          },
          {
            name: 'Address',
            field: [
              { name: 'city', number: 1, type: 'TYPE_STRING', label: 'LABEL_OPTIONAL', jsonName: 'city' },
            ],
          },
        ],
        enumType: [
          {
            name: 'Kind',
            value: [
              { name: 'KIND_UNSPECIFIED', number: 0 },
              { name: 'KIND_ADMIN', number: 1 },
            ],
          },
        ],
        service: [
          {
            name: 'PersonService',
            method: [
              { name: 'GetPerson', inputType: '.example.Person', outputType: '.example.Person' },
              {
                name: 'Watch',
                inputType: '.example.Person',
                outputType: '.example.Person',
                serverStreaming: true,
              },
            ],
          },
        ],
      },
    ],
  })
  return toBinary(FileDescriptorSetSchema, set)
}
