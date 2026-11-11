// Compatibility lane: drives ProtoMolt's MCP servers with the official MCP
// Inspector CLI, pinned in package-lock.json, and checks what it reports.
//
//   node run.mjs --stdio <protomolt-mcp> --serve <protomolt-serve>
//   node run.mjs --self-test
//
// The first form qualifies the stdio server and the streamable-HTTP server on
// protomolt-serve. --self-test points the same base checks at a deliberately
// broken fixture server: the correct baseline must pass and every break must
// fail, which shows the lane catches protocol defects.
//
// Every check runs; the process exits 1 when any fails. Nothing is skipped.
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const INSPECTOR_PACKAGE = "@modelcontextprotocol/inspector";
const INSPECTOR = join(HERE, "node_modules", INSPECTOR_PACKAGE,
  "clients", "launcher", "build", "index.js");
const FIXTURE = join(HERE, "fixtures", "broken-server.mjs");
const PROTOCOL_VERSION = "2025-06-18";
// A JVM server can be slow to start on a shared runner; the fixture is not.
const CONNECT_TIMEOUT_MS = "60000";
const FIXTURE_CONNECT_TIMEOUT_MS = "5000";
const OPERATOR_TOKEN = "inspector-operator-token";
const READER_TOKEN = "inspector-reader-token";
const FIXTURE_BREAKS = ["protocol-version", "jsonrpc-version", "response-id", "tool-schema"];

// Exit codes the pinned inspector documents for --cli runs.
const EXIT_OK = 0;
const EXIT_AUTH = 3;
const EXIT_TOOL = 5;

const scratch = mkdtempSync(join(tmpdir(), "protomolt-mcp-inspector-"));
// The inspector keeps a catalog and client config under HOME; isolate both so a
// developer's own catalog or stored OAuth state cannot change a result.
const inspectorEnv = {
  ...process.env,
  HOME: scratch,
  MCP_CATALOG_PATH: join(scratch, "catalog.json"),
  MCP_CLIENT_CONFIG_PATH: join(scratch, "client.json"),
};

function inspect(target, args) {
  const timeout = target.includes(FIXTURE) ? FIXTURE_CONNECT_TIMEOUT_MS : CONNECT_TIMEOUT_MS;
  const run = spawnSync(process.execPath, [INSPECTOR, "--cli", ...target,
    "--connect-timeout", timeout, "--format", "json", "-q", ...args], {
    env: inspectorEnv,
    encoding: "utf8",
    timeout: 180_000,
    maxBuffer: 64 * 1024 * 1024,
  });
  return {
    exit: run.status,
    output: parse(run.stdout),
    error: parse(run.stderr)?.error ?? null,
    stderr: run.stderr ?? "",
  };
}

function parse(text) {
  if (!text || !text.trim()) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

class Report {
  constructor(name) {
    this.name = name;
    this.passed = [];
    this.failed = [];
  }

  check(label, body) {
    try {
      body();
      this.passed.push(label);
      console.log(`  ok    ${label}`);
    } catch (e) {
      this.failed.push(label);
      console.log(`  FAIL  ${label}: ${e.message}`);
    }
  }
}

function expect(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function expectExit(result, exit) {
  expect(result.exit === exit,
    `inspector exit ${result.exit}, expected ${exit}; ${result.stderr.slice(0, 400).trim()}`);
}

/** Checks any conforming server must pass; the self-test reuses them on the fixture. */
function baseChecks(report, target, headers = []) {
  let tools = [];
  report.check("initialize negotiates the protocol", () => {
    const init = inspect(target, ["--method", "initialize", ...headers]);
    expectExit(init, EXIT_OK);
    const result = init.output?.result;
    expect(result?.protocolVersion === PROTOCOL_VERSION,
      `negotiated ${result?.protocolVersion}, expected ${PROTOCOL_VERSION}`);
    expect(typeof result?.serverInfo?.name === "string", "serverInfo.name is missing");
    expect(result?.capabilities?.tools !== undefined, "tools capability is not advertised");
  });
  report.check("tools/list passes the inspector's strict schema portability check", () => {
    const listed = inspect(target, ["--method", "tools/list", "--strict", ...headers]);
    expectExit(listed, EXIT_OK);
    tools = listed.output?.result?.tools;
    expect(Array.isArray(tools) && tools.length > 0, "tools/list returned no tools");
    const names = new Set();
    for (const tool of tools) {
      expect(typeof tool.name === "string" && tool.name.length > 0, "a tool has no name");
      expect(!names.has(tool.name), `tool ${tool.name} is listed twice`);
      names.add(tool.name);
      expect(tool.inputSchema?.type === "object",
        `tool ${tool.name} inputSchema type is ${JSON.stringify(tool.inputSchema?.type)}, `
          + "the MCP schema requires an object");
    }
  });
  return () => tools;
}

function stdioChecks(report, binary) {
  const target = [binary];
  const tools = baseChecks(report, target);
  report.check("tools/call runs compile and returns structured content", () => {
    const call = inspect(target, ["--method", "tools/call", "--tool-name", "compile",
      "--tool-args-json", JSON.stringify({
        sources: { "t.proto": "syntax = \"proto3\"; message T { int32 n = 1; }" },
      })]);
    expectExit(call, EXIT_OK);
    expect(call.output?.result?.structuredContent?.ok === true,
      "compile did not report ok in structuredContent");
  });
  report.check("tools/call with malformed arguments is an isError result", () => {
    const call = inspect(target, ["--method", "tools/call", "--tool-name", "compile",
      "--tool-args-json", JSON.stringify({ "no-such-field": 1 })]);
    expectExit(call, EXIT_TOOL);
    expect(call.output?.result?.isError === true, "result.isError is not true");
    expect(call.output?.result?.structuredContent?.error === "invalid-input",
      "the error does not name invalid-input");
  });
  report.check("tools/call of an unknown tool is refused", () => {
    const call = inspect(target, ["--method", "tools/call", "--tool-name", "no-such-tool"]);
    expectExit(call, EXIT_TOOL);
    expect(call.error?.code === "tool_not_found", `error code ${call.error?.code}`);
  });
  report.check("resources/read serves the workspace bootstrap matching tools/list", () => {
    const listed = inspect(target, ["--method", "resources/list"]);
    expectExit(listed, EXIT_OK);
    const uris = (listed.output?.result?.resources ?? []).map((r) => r.uri);
    expect(uris.includes("protomolt://workspace"), `resources/list returned ${uris}`);
    const read = inspect(target, ["--method", "resources/read", "--uri", "protomolt://workspace"]);
    expectExit(read, EXIT_OK);
    const content = read.output?.result?.contents?.[0];
    expect(content?.mimeType === "application/json", `mimeType ${content?.mimeType}`);
    const workspace = JSON.parse(content.text);
    expect(workspace.toolCatalog?.count === tools().length,
      `workspace counts ${workspace.toolCatalog?.count} tools, tools/list served ${tools().length}`);
  });
  report.check("resources/read of an unknown URI is an error", () => {
    const read = inspect(target, ["--method", "resources/read", "--uri", "protomolt://no-such"]);
    expect(read.exit !== EXIT_OK, "an unknown resource was read successfully");
    expect((read.error?.message ?? "").includes("Unknown resource"), `error ${read.error?.message}`);
  });
  report.check("resources/templates/list answers with a list", () => {
    const templates = inspect(target, ["--method", "resources/templates/list"]);
    expectExit(templates, EXIT_OK);
    expect(Array.isArray(templates.output?.result?.resourceTemplates),
      "resourceTemplates is not a list");
  });
  report.check("prompts are not advertised, and prompts/list is empty", () => {
    const init = inspect(target, ["--method", "initialize"]);
    expect(init.output?.result?.capabilities?.prompts === undefined,
      "the server now advertises prompts; add prompt checks to this lane");
    const prompts = inspect(target, ["--method", "prompts/list"]);
    expectExit(prompts, EXIT_OK);
    expect(prompts.output?.result?.prompts?.length === 0, "prompts/list is not empty");
  });
}

async function httpChecks(report, binary) {
  const policy = join(scratch, "access-policy.json");
  writeFileSync(policy, JSON.stringify({
    principals: [{
      name: "inspector-reader",
      credentialSha256: [createHash("sha256").update(READER_TOKEN).digest("hex")],
      scopes: ["schema-read"],
    }],
  }));
  const port = await freePort();
  const url = `http://127.0.0.1:${port}/mcp`;
  const serve = spawn(binary, ["--host", "127.0.0.1", "--http-port", String(port),
    "--grpc-port", "0", "--api-token", OPERATOR_TOKEN, "--access-policy", policy], {
    stdio: ["ignore", "ignore", "pipe"],
  });
  let log = "";
  serve.stderr.on("data", (chunk) => {
    log += chunk;
  });
  try {
    await awaitHttp(url, serve, () => log);
    const target = [url];
    const operator = ["--header", `api_token: ${OPERATOR_TOKEN}`];
    const reader = ["--header", `api_token: ${READER_TOKEN}`];
    const operatorTools = baseChecks(report, target, operator);
    report.check("a request without a credential is refused", () => {
      const refused = inspect(target, ["--method", "tools/list", "--stored-auth-only"]);
      expectExit(refused, EXIT_AUTH);
      expect(refused.error?.code === "auth_required", `error code ${refused.error?.code}`);
    });
    report.check("an unknown credential is refused", () => {
      const refused = inspect(target, ["--method", "tools/list", "--stored-auth-only",
        "--header", "api_token: not-a-credential"]);
      expectExit(refused, EXIT_AUTH);
      expect(refused.error?.code === "auth_required", `error code ${refused.error?.code}`);
    });
    let readerTools = [];
    report.check("a scoped principal lists a strict subset of the operator's tools", () => {
      const listed = inspect(target, ["--method", "tools/list", "--strict", ...reader]);
      expectExit(listed, EXIT_OK);
      readerTools = listed.output?.result?.tools ?? [];
      const all = new Set(operatorTools().map((t) => t.name));
      expect(readerTools.length > 0, "the schema-read principal sees no tools");
      expect(readerTools.length < all.size, "the scoped principal sees every tool");
      for (const tool of readerTools) {
        expect(all.has(tool.name), `${tool.name} is not an operator tool`);
      }
    });
    report.check("a scoped principal cannot call a tool outside its scope", () => {
      const visible = new Set(readerTools.map((t) => t.name));
      const hidden = operatorTools().find((t) => !visible.has(t.name));
      expect(hidden !== undefined, "no tool is hidden from the scoped principal");
      const call = inspect(target, ["--method", "tools/call", "--tool-name", hidden.name,
        "--tool-args-json", "{}", ...reader]);
      expectExit(call, EXIT_TOOL);
      expect(call.error?.code === "tool_not_found", `error code ${call.error?.code}`);
    });
    report.check("a scoped principal is denied a resource outside its scope", () => {
      const read = inspect(target, ["--method", "resources/read",
        "--uri", "protomolt://delegation/tasks", ...reader]);
      expect(read.exit !== EXIT_OK, "the delegation collection was readable");
      expect((read.error?.message ?? "").startsWith("permission-denied:"),
        `error ${read.error?.message}`);
    });
    report.check("the workspace bootstrap counts the scoped principal's own tools", () => {
      const read = inspect(target, ["--method", "resources/read",
        "--uri", "protomolt://workspace", ...reader]);
      expectExit(read, EXIT_OK);
      const workspace = JSON.parse(read.output.result.contents[0].text);
      expect(workspace.toolCatalog?.count === readerTools.length,
        `workspace counts ${workspace.toolCatalog?.count}, tools/list served ${readerTools.length}`);
    });
    report.check("the operator runs compile over HTTP", () => {
      const call = inspect(target, ["--method", "tools/call", "--tool-name", "compile",
        "--tool-args-json", JSON.stringify({
          sources: { "t.proto": "syntax = \"proto3\"; message T { int32 n = 1; }" },
        }), ...operator]);
      expectExit(call, EXIT_OK);
      expect(call.output?.result?.structuredContent?.ok === true, "compile did not report ok");
    });
  } finally {
    serve.kill("SIGTERM");
  }
}

function freePort() {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

/** Waits until the MCP endpoint answers; an unauthenticated POST gets 401 once serving. */
async function awaitHttp(url, child, log) {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`protomolt-serve exited ${child.exitCode}:\n${log()}`);
    }
    try {
      const response = await fetch(url, { method: "POST" });
      if (response.status === 401) {
        return;
      }
    } catch {
      // Not listening yet.
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`protomolt-serve did not answer on ${url} within 120s:\n${log()}`);
}

function selfTest() {
  const baseline = new Report("fixture baseline");
  console.log("fixture baseline (must pass)");
  baseChecks(baseline, ["-e", "MCP_BREAK=none", process.execPath, FIXTURE]);
  let ok = baseline.failed.length === 0;
  for (const name of FIXTURE_BREAKS) {
    const broken = new Report(`break ${name}`);
    console.log(`break ${name} (must fail)`);
    baseChecks(broken, ["-e", `MCP_BREAK=${name}`, process.execPath, FIXTURE]);
    if (broken.failed.length === 0) {
      console.log(`  FAIL  the lane did not detect break ${name}`);
      ok = false;
    } else {
      console.log(`  ok    break ${name} detected`);
    }
  }
  return ok;
}

function versions() {
  const lock = JSON.parse(readFileSync(join(HERE, "package-lock.json"), "utf8"));
  const pinned = lock.packages?.[`node_modules/${INSPECTOR_PACKAGE}`]?.version;
  const reported = spawnSync(process.execPath, [INSPECTOR, "--version"],
    { env: inspectorEnv, encoding: "utf8" }).stdout.trim();
  if (pinned !== reported) {
    throw new Error(`inspector reports ${reported}, package-lock.json pins ${pinned}`);
  }
  console.log(`node ${process.version}; ${INSPECTOR_PACKAGE} ${reported}`);
}

async function main() {
  const args = process.argv.slice(2);
  try {
    versions();
    if (args.length === 1 && args[0] === "--self-test") {
      const ok = selfTest();
      console.log(ok ? "SELF-TEST PASS" : "SELF-TEST FAIL");
      return ok ? 0 : 1;
    }
    const stdio = valueOf(args, "--stdio");
    const serve = valueOf(args, "--serve");
    const stdioReport = new Report("stdio");
    console.log(`stdio: ${stdio}`);
    stdioChecks(stdioReport, stdio);
    const httpReport = new Report("streamable HTTP");
    console.log(`streamable HTTP: ${serve}`);
    await httpChecks(httpReport, serve);
    const passed = stdioReport.passed.length + httpReport.passed.length;
    const failed = stdioReport.failed.length + httpReport.failed.length;
    console.log(`${failed === 0 ? "PASS" : "FAIL"} (failed: ${failed}, passed: ${passed})`);
    return failed === 0 ? 0 : 1;
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
}

function valueOf(args, flag) {
  const at = args.indexOf(flag);
  if (at < 0 || at + 1 >= args.length) {
    throw new Error(
      "usage: node run.mjs --stdio <protomolt-mcp> --serve <protomolt-serve> | --self-test");
  }
  return args[at + 1];
}

process.exitCode = await main();
