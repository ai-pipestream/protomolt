// A deliberately broken stdio MCP server. The lane's self-test points the
// inspector at it to show that each protocol break fails the run.
//
// MCP_BREAK selects the defect ("none" serves a correct baseline):
//   protocol-version  initialize answers with a version no client supports
//   jsonrpc-version   every response claims "jsonrpc": "1.0"
//   tool-schema       tools/list returns an inputSchema that is not an object schema
//   response-id       responses carry an id the client never sent
import { createInterface } from "node:readline";

const BREAK = process.env.MCP_BREAK ?? "";
const KNOWN = ["none", "protocol-version", "jsonrpc-version", "tool-schema", "response-id"];
if (!KNOWN.includes(BREAK)) {
  process.stderr.write(`MCP_BREAK must be one of ${KNOWN.join(", ")}\n`);
  process.exit(2);
}

function reply(id, result) {
  const message = {
    jsonrpc: BREAK === "jsonrpc-version" ? "1.0" : "2.0",
    id: BREAK === "response-id" ? `not-${id}` : id,
    result,
  };
  process.stdout.write(JSON.stringify(message) + "\n");
}

function fail(id, code, message) {
  process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id, error: { code, message } }) + "\n");
}

const lines = createInterface({ input: process.stdin });
lines.on("line", (line) => {
  let message;
  try {
    message = JSON.parse(line);
  } catch {
    fail(null, -32700, "Parse error");
    return;
  }
  if (message.id === undefined) {
    return;
  }
  switch (message.method) {
    case "initialize":
      reply(message.id, {
        protocolVersion: BREAK === "protocol-version" ? "1999-01-01" : "2025-06-18",
        capabilities: { tools: {}, resources: {} },
        serverInfo: { name: "broken", version: "0" },
      });
      break;
    case "tools/list":
      reply(message.id, {
        tools: [{
          name: "compile",
          description: "Broken fixture tool.",
          inputSchema: BREAK === "tool-schema"
            ? { type: "string" }
            : { type: "object", properties: {}, additionalProperties: false },
        }],
      });
      break;
    case "tools/call":
      reply(message.id, { content: [{ type: "text", text: "{}" }], structuredContent: {} });
      break;
    case "resources/list":
      reply(message.id, { resources: [] });
      break;
    default:
      fail(message.id, -32601, `Method not found: ${message.method}`);
  }
});
