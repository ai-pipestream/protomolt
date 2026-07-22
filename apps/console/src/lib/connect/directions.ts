/**
 * The "Connect a gRPC service" generators: given what we learn about a service
 * (its target, its descriptor set, a chosen method), produce the exact copy-paste
 * artifacts that make it agent-operable (MCP) and pipeline-operable (Kafka Connect).
 */

export interface McpDirectionsInput {
  /** The protomolt-serve HTTP base an agent will connect to, e.g. http://host:8080. */
  serveUrl: string
  /** The gRPC target of the user's service, e.g. inference-host:9000. */
  target: string
  /** Service full names discovered on the target (or from the schema). */
  services: string[]
  /** Registry subject holding the schema, when reflection was unavailable. */
  subject?: string
  apiToken?: string
}

export function buildMcpCommand(input: McpDirectionsInput): string {
  const parts = [
    'claude mcp add --transport http protomolt',
    `${trimSlash(input.serveUrl)}/mcp`,
  ]
  if (input.apiToken) {
    parts.push(`--header "api_token: ${input.apiToken}"`)
  }
  return parts.join(' \\\n  ')
}

/** A prompt the user hands their agent so it knows the service exists. */
export function buildAgentPrompt(input: McpDirectionsInput): string {
  const service = input.services[0] ?? 'the service'
  const schemaLine = input.subject
    ? `Its schema is registered as subject "${input.subject}" - read it from the ` +
      `registry resources, or pass {"type": "..."} for its message types.`
    : `It supports gRPC reflection - call the reflect tool on the target first ` +
      `and feed the returned descriptorSetBase64 to the other tools.`
  return [
    `There is a gRPC service at ${input.target} (${input.services.join(', ') || service}).`,
    schemaLine,
    `Use grpc-invoke to call its methods, list-types to see its messages, and`,
    `generate-stubs if you want a native client.`,
  ].join('\n')
}

export interface SinkConfigInput {
  /** Connector name, e.g. orders-to-grpc. */
  name: string
  /** Kafka topics to consume. */
  topics: string
  /** The gRPC target the sink calls. */
  target: string
  /** Fully qualified method, package.Service/Method. */
  method: string
  /** Base64 FileDescriptorSet declaring the service. */
  descriptorSetBase64: string
  /** How record values decode: protobuf | confluent | json. */
  valueFormat: 'protobuf' | 'confluent' | 'json'
  apiToken?: string
}

/** The ready-to-POST Kafka Connect connector document. */
export function buildSinkConnectorConfig(input: SinkConfigInput): Record<string, unknown> {
  const config: Record<string, string> = {
    'connector.class': 'ai.pipestream.proto.kafka.connect.GrpcSinkConnector',
    'tasks.max': '1',
    topics: input.topics,
    'grpc.target': input.target,
    'grpc.method': input.method,
    'schema.descriptor.set.base64': input.descriptorSetBase64,
    'value.format': input.valueFormat,
    'value.converter': 'org.apache.kafka.connect.converters.ByteArrayConverter',
    'key.converter': 'org.apache.kafka.connect.converters.ByteArrayConverter',
  }
  if (input.valueFormat === 'json') {
    config['value.converter'] = 'org.apache.kafka.connect.storage.StringConverter'
  }
  if (input.apiToken) {
    config['grpc.api.token'] = input.apiToken
  }
  return { name: input.name, config }
}

/** The curl that installs the connector on a Connect worker. */
export function buildSinkCurl(input: SinkConfigInput, workerUrl = 'http://localhost:8083'): string {
  return [
    `curl -s -X POST ${trimSlash(workerUrl)}/connectors \\`,
    `  -H 'Content-Type: application/json' \\`,
    `  -d '${JSON.stringify(buildSinkConnectorConfig(input), null, 2).replace(/'/g, "'\\''")}'`,
  ].join('\n')
}

function trimSlash(url: string): string {
  return url.endsWith('/') ? url.slice(0, -1) : url
}
