import { create, type Message } from '@bufbuild/protobuf'
import type { GenMessage } from '@bufbuild/protobuf/codegenv2'

export interface JsonSchema {
  type?: string
  properties?: Record<string, JsonSchema>
  items?: JsonSchema
  enum?: any[]
  required?: string[]
  title?: string
  description?: string
  default?: any
  minimum?: number
  maximum?: number
  [key: string]: any
}

export interface ConversionOptions {
  // Add UI hints for better form rendering
  addUiHints?: boolean
  // Include field descriptions from proto comments
  includeComments?: boolean
  // Custom field transformers for specific field types
  fieldTransformers?: Record<string, (fieldName: string) => Partial<JsonSchema>>
}

export class ProtobufToJsonSchemaConverter {
  private options: ConversionOptions

  constructor(options: ConversionOptions = {}) {
    this.options = {
      addUiHints: true,
      includeComments: true,
      ...options
    }
  }

  /**
   * Convert a protobuf message schema to JSON Schema
   * This is a simplified approach that works with @bufbuild/protobuf generated schemas
   */
  convertMessageSchema<T extends Message>(messageSchema: GenMessage<T>): JsonSchema {
    // Create a default instance to analyze the structure
    const defaultInstance = create(messageSchema)
    
    const schema: JsonSchema = {
      type: 'object',
      properties: {},
      required: [],
      title: this.extractMessageName(messageSchema.typeName)
    }

    // For now, we'll create basic schemas for known message types
    // This is a simplified approach - a full implementation would need
    // to introspect the message descriptor
    
    if (messageSchema.typeName.includes('ApplyMappingRequest')) {
      schema.properties = {
        document: {
          type: 'object',
          title: 'Document',
          description: 'The PipeDoc to apply mappings to',
          properties: {
            docId: { type: 'string', title: 'Document ID' },
            title: { type: 'string', title: 'Title' },
            body: { type: 'string', title: 'Body', 'ui:widget': 'textarea' }
          }
        },
        rules: {
          type: 'array',
          title: 'Mapping Rules',
          items: {
            type: 'object',
            properties: {
              candidateMappings: {
                type: 'array',
                title: 'Candidate Mappings',
                items: {
                  type: 'object',
                  properties: {
                    sourceField: { type: 'string', title: 'Source Field' },
                    targetField: { type: 'string', title: 'Target Field' },
                    transformationType: { type: 'string', title: 'Transformation Type' }
                  }
                }
              }
            }
          }
        }
      }
    } else if (messageSchema.typeName.includes('PipeDoc')) {
      schema.properties = {
        docId: { type: 'string', title: 'Document ID' },
        title: { type: 'string', title: 'Title' },
        body: { type: 'string', title: 'Body', 'ui:widget': 'textarea' },
        originalMimeType: { type: 'string', title: 'Original MIME Type' },
        lastProcessed: { type: 'string', title: 'Last Processed', format: 'date-time' },
        metadata: {
          type: 'object',
          title: 'Metadata',
          additionalProperties: { type: 'string' }
        }
      }
    } else {
      // Generic fallback
      schema.properties = {
        data: {
          type: 'object',
          title: 'Data',
          description: 'Message data'
        }
      }
    }

    // Add UI hints if enabled
    if (this.options.addUiHints) {
      this.addUiHints(schema)
    }

    return schema
  }

  /**
   * Extract a readable message name from the full type name
   */
  private extractMessageName(typeName: string): string {
    const parts = typeName.split('.')
    return parts[parts.length - 1] || 'Message'
  }

  /**
   * Add UI hints for better form rendering
   */
  private addUiHints(schema: JsonSchema): void {
    // Add general UI hints
    if (!schema['ui:options']) {
      schema['ui:options'] = {}
    }

    // Add hints for specific field types
    if (schema.properties) {
      Object.entries(schema.properties).forEach(([fieldName, fieldSchema]) => {
        if (fieldSchema.type === 'string' && fieldName.toLowerCase().includes('body')) {
          fieldSchema['ui:widget'] = 'textarea'
          fieldSchema['ui:options'] = { rows: 5 }
        }
        
        if (fieldSchema.type === 'array') {
          fieldSchema['ui:options'] = { addable: true, removable: true }
        }
      })
    }
  }

  /**
   * Format field name for display
   */
  private formatFieldName(name: string): string {
    return name
      .replace(/([A-Z])/g, ' $1')
      .replace(/^./, str => str.toUpperCase())
      .trim()
  }
}
