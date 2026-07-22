import { describe, expect, it } from 'vitest'
import {
  buildAgentPrompt,
  buildMcpCommand,
  buildSinkConnectorConfig,
  buildSinkCurl,
} from './directions'

describe('connect directions', () => {
  const mcpInput = {
    serveUrl: 'http://molt-host:8080/',
    target: 'inference-host:9000',
    services: ['inference.GRPCInferenceService'],
  }

  it('builds the mcp add command, with the token only when configured', () => {
    expect(buildMcpCommand(mcpInput)).toBe(
      'claude mcp add --transport http protomolt \\\n  http://molt-host:8080/mcp',
    )
    expect(buildMcpCommand({ ...mcpInput, apiToken: 's3cret' })).toContain(
      '--header "api_token: s3cret"',
    )
  })

  it('tells the agent to reflect when possible and to read the subject otherwise', () => {
    expect(buildAgentPrompt(mcpInput)).toContain('reflect tool')
    const registered = buildAgentPrompt({ ...mcpInput, subject: 'kserve/v2/inference.proto' })
    expect(registered).toContain('subject "kserve/v2/inference.proto"')
    expect(registered).not.toContain('reflect tool')
  })

  it('generates a ready-to-post sink connector document', () => {
    const doc = buildSinkConnectorConfig({
      name: 'orders-to-grpc',
      topics: 'orders',
      target: 'orders-svc:9090',
      method: 'shop.v1.OrderService/RecordBatch',
      descriptorSetBase64: 'AAAA',
      valueFormat: 'confluent',
    }) as { name: string; config: Record<string, string> }
    expect(doc.name).toBe('orders-to-grpc')
    expect(doc.config['connector.class'])
      .toBe('ai.pipestream.proto.kafka.connect.GrpcSinkConnector')
    expect(doc.config['grpc.method']).toBe('shop.v1.OrderService/RecordBatch')
    expect(doc.config['value.format']).toBe('confluent')
    expect(doc.config['value.converter'])
      .toBe('org.apache.kafka.connect.converters.ByteArrayConverter')
  })

  it('switches to the string converter for json values and embeds the curl', () => {
    const input = {
      name: 'n',
      topics: 't',
      target: 'x:1',
      method: 'a.B/C',
      descriptorSetBase64: 'AAAA',
      valueFormat: 'json' as const,
      apiToken: 'k',
    }
    const doc = buildSinkConnectorConfig(input) as { config: Record<string, string> }
    expect(doc.config['value.converter'])
      .toBe('org.apache.kafka.connect.storage.StringConverter')
    expect(doc.config['grpc.api.token']).toBe('k')
    expect(buildSinkCurl(input)).toContain('POST http://localhost:8083/connectors')
  })
})
